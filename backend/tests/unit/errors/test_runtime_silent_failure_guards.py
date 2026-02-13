from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

_RUNTIME_EXCLUDE_TOP_LEVEL = {"tests", "scripts"}
_STRICT_RUNTIME_GLOBS = (
    "action_worker/main.py",
    "pipeline_worker/main.py",
    "instance_worker/main.py",
    "objectify_worker/main.py",
    "ontology_worker/main.py",
    "projection_worker/main.py",
    "message_relay/main.py",
    "connector_trigger_service/main.py",
    "action_outbox_worker/main.py",
    "writeback_materializer_worker/main.py",
    "shared/services/kafka/processed_event_worker.py",
    "shared/services/kafka/dlq_publisher.py",
    "shared/services/pipeline/pipeline_executor.py",
    "shared/services/pipeline/pipeline_preflight_utils.py",
    "shared/services/pipeline/pipeline_definition_validator.py",
    "shared/services/pipeline/output_plugins.py",
    "shared/config/settings.py",
    "shared/errors/runtime_exception_policy.py",
    "shared/utils/json_utils.py",
    "shared/utils/worker_runner.py",
    "shared/observability/context_propagation.py",
    "shared/observability/logging.py",
    "bff/middleware/auth.py",
    "oms/services/async_terminus.py",
    "oms/services/terminus/base.py",
    "oms/services/terminus/document.py",
    "oms/services/terminus/ontology.py",
    "oms/services/terminus/query.py",
    "mcp_servers/pipeline_mcp_errors.py",
    "mcp_servers/ontology_mcp_server.py",
    "mcp_servers/pipeline_tools/dataset_tools.py",
    "mcp_servers/pipeline_tools/pipeline_tools.py",
    "mcp_servers/pipeline_tools/plan_tools.py",
    "pipeline_worker/spark_transform_engine.py",
    "bff/routers/pipeline_ops_preflight.py",
)


def _iter_runtime_python_files(*, backend_dir: Path):
    for path in backend_dir.rglob("*.py"):
        rel = path.relative_to(backend_dir)
        if rel.parts and rel.parts[0] in _RUNTIME_EXCLUDE_TOP_LEVEL:
            continue
        yield path, rel


class _ReturnInFinallyVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.violations: list[int] = []

    def visit_Try(self, node: ast.Try) -> None:
        for stmt in node.finalbody:
            for sub in ast.walk(stmt):
                if isinstance(sub, ast.Return):
                    self.violations.append(sub.lineno)
        self.generic_visit(node)


@pytest.mark.unit
def test_runtime_has_no_return_in_finally() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    failures: list[str] = []

    for path, rel in _iter_runtime_python_files(backend_dir=backend_dir):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
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
def test_runtime_scope_disallows_silent_failures() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    script_path = backend_dir / "shared" / "tools" / "error_taxonomy_audit.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--backend-root",
        str(backend_dir),
        "--fail-on-bare-except",
        "--fail-on-suppress-exception",
        "--fail-on-silent-broad-except",
        "--fail-on-lineage-fail-open",
        "--fail-on-streamjoin-strategy-ignored",
        "--fail-on-output-kind-metadata-gap",
        "--fail-on-preflight-swallowed",
        "--fail-on-commented-export",
        "--fail-on-doc-only-module",
    ]
    for pattern in _STRICT_RUNTIME_GLOBS:
        cmd.extend(["--runtime-scope-glob", pattern])
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    output = (result.stdout or "") + ("\n" if result.stdout and result.stderr else "") + (result.stderr or "")
    raise AssertionError(f"Runtime silent-failure guard failed:\n{output}")
