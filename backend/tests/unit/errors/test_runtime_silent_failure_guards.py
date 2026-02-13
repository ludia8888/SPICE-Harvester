from __future__ import annotations

import ast
from pathlib import Path

import pytest

_RUNTIME_EXCLUDE_TOP_LEVEL = {"tests", "scripts"}
_CORE_PATHS = (
    Path("shared/services/events/outbox_runtime.py"),
    Path("shared/utils/worker_runner.py"),
    Path("shared/services/kafka/processed_event_worker.py"),
)
# Best-effort metrics hook: allowed to avoid impacting message handling path.
_CORE_EXCEPT_PASS_ALLOWLIST = {
    (Path("shared/services/kafka/processed_event_worker.py"), "_on_success"),
}


def _iter_runtime_python_files(*, backend_dir: Path):
    for path in backend_dir.rglob("*.py"):
        rel = path.relative_to(backend_dir)
        if rel.parts and rel.parts[0] in _RUNTIME_EXCLUDE_TOP_LEVEL:
            continue
        yield path, rel


def _read_ast(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"))


def _is_exception_type(node: ast.expr | None) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "Exception"
    if isinstance(node, ast.Tuple):
        return any(_is_exception_type(elt) for elt in node.elts)
    return False


class _ReturnInFinallyVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.violations: list[int] = []

    def visit_Try(self, node: ast.Try) -> None:
        for stmt in node.finalbody:
            for sub in ast.walk(stmt):
                if isinstance(sub, ast.Return):
                    self.violations.append(sub.lineno)
        self.generic_visit(node)


class _ExceptPassVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.function_stack: list[str] = []
        self.violations: list[tuple[int, str]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Try(self, node: ast.Try) -> None:
        current_fn = self.function_stack[-1] if self.function_stack else "<module>"
        for handler in node.handlers:
            if not _is_exception_type(handler.type):
                continue
            if len(handler.body) == 1 and isinstance(handler.body[0], ast.Pass):
                self.violations.append((handler.lineno, current_fn))
        self.generic_visit(node)


@pytest.mark.unit
def test_runtime_has_no_return_in_finally() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    failures: list[str] = []

    for path, rel in _iter_runtime_python_files(backend_dir=backend_dir):
        try:
            tree = _read_ast(path)
        except Exception:
            continue
        visitor = _ReturnInFinallyVisitor()
        visitor.visit(tree)
        for lineno in visitor.violations:
            failures.append(f"{rel}:{lineno}")

    if not failures:
        return

    lines = ["`return` inside `finally` is forbidden in runtime code:"]
    lines.extend(f"- {ref}" for ref in sorted(failures))
    raise AssertionError("\n".join(lines))


@pytest.mark.unit
def test_core_paths_disallow_except_exception_pass() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    failures: list[str] = []

    for rel in _CORE_PATHS:
        path = backend_dir / rel
        tree = _read_ast(path)
        visitor = _ExceptPassVisitor()
        visitor.visit(tree)

        for lineno, function_name in visitor.violations:
            key = (rel, function_name)
            if key in _CORE_EXCEPT_PASS_ALLOWLIST:
                continue
            failures.append(f"{rel}:{lineno} ({function_name})")

    if not failures:
        return

    lines = ["`except Exception: pass` is forbidden on core fail-fast paths:"]
    lines.extend(f"- {ref}" for ref in sorted(failures))
    raise AssertionError("\n".join(lines))
