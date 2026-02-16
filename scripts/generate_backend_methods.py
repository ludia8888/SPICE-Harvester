#!/usr/bin/env python3
"""Generate backend method/design references from backend/**/*.py using the AST."""

from __future__ import annotations

import argparse
import ast
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set

SCOPE_LINE = "backend/**/*.py (including scripts and tests, excluding __pycache__)"
REPO_ROOT = Path(__file__).resolve().parents[1]
DESIGN_SECTION_KEYS = {
    "responsibilities": "responsibilities",
    "invariants": "invariants",
    "failure modes": "failure_modes",
    "extension points": "extension_points",
    "dependencies": "dependencies_doc",
}


@dataclass
class FunctionInfo:
    signature: str
    line: int
    doc: str
    has_doc: bool


@dataclass
class ClassInfo:
    name: str
    line: int
    doc: str
    has_doc: bool
    methods: List[FunctionInfo]


@dataclass
class ModuleDesignInfo:
    module_summary: str
    has_module_doc: bool
    responsibilities: List[str]
    invariants: List[str]
    failure_modes: List[str]
    extension_points: List[str]
    dependencies_doc: List[str]
    internal_imports: List[str]
    external_imports: List[str]
    public_api: List[str]
    top_level_function_count: int
    class_count: int
    method_count: int
    async_function_count: int
    try_count: int
    raise_count: int
    broad_except_count: int
    bare_except_count: int
    finally_return_count: int
    source_line_count: int
    code_line_count: int
    top_level_function_doc_coverage: str
    class_doc_coverage: str
    method_doc_coverage: str


@dataclass
class FileInfo:
    group: str
    path: str
    functions: List[FunctionInfo]
    classes: List[ClassInfo]
    design: ModuleDesignInfo


@dataclass
class DesignTotals:
    module_count: int
    module_doc_count: int
    modules_with_broad_except: int
    modules_with_bare_except: int
    modules_with_finally_return: int


@dataclass
class GroupDesignSummary:
    group: str
    module_count: int
    module_doc_count: int
    broad_except_modules: int
    broad_except_total: int
    bare_except_total: int
    total_public_api: int
    total_async_functions: int
    total_code_lines: int


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def summarize_docstring(node: ast.AST) -> tuple[str, bool]:
    doc = ast.get_docstring(node)
    if not doc:
        return "no docstring", False
    for line in doc.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped, True
    return "no docstring", False


def format_signature(node: ast.AST) -> str:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise TypeError("node must be FunctionDef or AsyncFunctionDef")
    prefix = "async " if isinstance(node, ast.AsyncFunctionDef) else ""
    parts: List[str] = []
    for arg in node.args.posonlyargs:
        parts.append(arg.arg)
    for arg in node.args.args:
        parts.append(arg.arg)
    if node.args.vararg:
        parts.append(f"*{node.args.vararg.arg}")
    for arg in node.args.kwonlyargs:
        parts.append(arg.arg)
    if node.args.kwarg:
        parts.append(f"**{node.args.kwarg.arg}")
    return f"{prefix}{node.name}({', '.join(parts)})"


def _coverage_label(documented: int, total: int) -> str:
    if total <= 0:
        return "0/0 (n/a)"
    pct = int((documented / total) * 100)
    return f"{documented}/{total} ({pct}%)"


def _normalize_section_line(line: str) -> str:
    normalized = line.strip().lower()
    while normalized.startswith(("#", "-", "*", " ")):
        normalized = normalized[1:].lstrip()
    if normalized.endswith(":"):
        normalized = normalized[:-1].strip()
    return normalized


def _parse_design_sections(module_doc: Optional[str]) -> Dict[str, List[str]]:
    parsed: Dict[str, List[str]] = {
        "responsibilities": [],
        "invariants": [],
        "failure_modes": [],
        "extension_points": [],
        "dependencies_doc": [],
    }
    if not module_doc:
        return parsed

    current_key: Optional[str] = None
    for raw_line in module_doc.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        normalized = _normalize_section_line(line)
        section = DESIGN_SECTION_KEYS.get(normalized)
        if section:
            current_key = section
            continue

        if current_key is not None:
            item = line.lstrip("-* ").strip()
            if item:
                parsed[current_key].append(item)

    return parsed


def _is_exception_name(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == "Exception"


def _is_broad_exception_handler(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    if _is_exception_name(handler.type):
        return True
    if isinstance(handler.type, ast.Tuple):
        return any(_is_exception_name(item) for item in handler.type.elts)
    return False


def _detect_internal_roots(backend_root: Path) -> Set[str]:
    roots: Set[str] = {"backend"}
    for child in backend_root.iterdir():
        if child.is_dir() and (child / "__init__.py").exists():
            roots.add(child.name)
    for file_path in backend_root.glob("*.py"):
        roots.add(file_path.stem)
    return roots


def _classify_import(module_name: str, internal_roots: Set[str]) -> tuple[str, str]:
    root = module_name.split(".", 1)[0] if module_name else ""
    if root in internal_roots:
        return "internal", module_name
    return "external", root or module_name


def _collect_imports(tree: ast.Module, internal_roots: Set[str]) -> tuple[List[str], List[str]]:
    internal: Set[str] = set()
    external: Set[str] = set()

    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                kind, value = _classify_import(alias.name, internal_roots)
                if kind == "internal":
                    internal.add(value)
                else:
                    external.add(value)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                mod = node.module or ""
                marker = "." * node.level + mod
                internal.add(marker)
                continue
            module_name = node.module or ""
            if not module_name:
                continue
            kind, value = _classify_import(module_name, internal_roots)
            if kind == "internal":
                internal.add(value)
            else:
                external.add(value)

    return sorted(internal), sorted(external)


def _collect_finally_return_count(tree: ast.Module) -> int:
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            for final_node in node.finalbody:
                count += sum(1 for child in ast.walk(final_node) if isinstance(child, ast.Return))
    return count


def _count_source_lines(source: str) -> tuple[int, int]:
    lines = source.splitlines()
    total_lines = len(lines)
    code_lines = 0
    for raw in lines:
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        code_lines += 1
    return total_lines, code_lines


def _module_role_hint(path: str) -> str:
    lowered = path.lower()
    if "/routers/" in lowered:
        return "HTTP contract/endpoint routing"
    if "/services/" in lowered:
        return "service/domain orchestration"
    if "/registries/" in lowered:
        return "persistence/data-access adapter"
    if lowered.endswith("/main.py"):
        return "service entrypoint and lifecycle wiring"
    if "worker" in lowered:
        return "asynchronous background processing"
    if "/models" in lowered:
        return "domain/request-response schema definitions"
    if "/scripts/" in lowered:
        return "operational or migration automation"
    return "general backend module"


def _module_risk_score(design: ModuleDesignInfo) -> int:
    return (
        design.broad_except_count * 5
        + design.bare_except_count * 12
        + design.finally_return_count * 10
        + max(design.try_count - design.raise_count, 0)
    )


def collect_file_info(root: Path, path: Path, internal_roots: Set[str]) -> FileInfo:
    rel_path = path.relative_to(root)
    group = rel_path.parts[0] if len(rel_path.parts) > 1 else rel_path.name
    display_path = f"backend/{rel_path.as_posix()}"

    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(rel_path))
    source_line_count, code_line_count = _count_source_lines(source)

    module_summary, has_module_doc = summarize_docstring(tree)
    section_data = _parse_design_sections(ast.get_docstring(tree))
    internal_imports, external_imports = _collect_imports(tree, internal_roots)

    functions: List[FunctionInfo] = []
    classes: List[ClassInfo] = []
    public_api: List[str] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fn_doc, fn_has_doc = summarize_docstring(node)
            functions.append(
                FunctionInfo(
                    signature=format_signature(node),
                    line=node.lineno,
                    doc=fn_doc,
                    has_doc=fn_has_doc,
                )
            )
            if not node.name.startswith("_"):
                public_api.append(node.name)
        elif isinstance(node, ast.ClassDef):
            cls_doc, cls_has_doc = summarize_docstring(node)
            methods: List[FunctionInfo] = []
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    method_doc, method_has_doc = summarize_docstring(item)
                    methods.append(
                        FunctionInfo(
                            signature=format_signature(item),
                            line=item.lineno,
                            doc=method_doc,
                            has_doc=method_has_doc,
                        )
                    )
            classes.append(
                ClassInfo(
                    name=node.name,
                    line=node.lineno,
                    doc=cls_doc,
                    has_doc=cls_has_doc,
                    methods=methods,
                )
            )
            if not node.name.startswith("_"):
                public_api.append(node.name)

    try_count = sum(1 for node in ast.walk(tree) if isinstance(node, ast.Try))
    raise_count = sum(1 for node in ast.walk(tree) if isinstance(node, ast.Raise))
    bare_except_count = sum(
        1 for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler) and node.type is None
    )
    broad_except_count = sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler) and _is_broad_exception_handler(node)
    )
    finally_return_count = _collect_finally_return_count(tree)
    async_function_count = sum(1 for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef))

    method_count = sum(len(cls.methods) for cls in classes)
    function_doc_count = sum(1 for fn in functions if fn.has_doc)
    class_doc_count = sum(1 for cls in classes if cls.has_doc)
    method_doc_count = sum(1 for cls in classes for method in cls.methods if method.has_doc)

    design = ModuleDesignInfo(
        module_summary=module_summary,
        has_module_doc=has_module_doc,
        responsibilities=section_data["responsibilities"],
        invariants=section_data["invariants"],
        failure_modes=section_data["failure_modes"],
        extension_points=section_data["extension_points"],
        dependencies_doc=section_data["dependencies_doc"],
        internal_imports=internal_imports,
        external_imports=external_imports,
        public_api=sorted(set(public_api)),
        top_level_function_count=len(functions),
        class_count=len(classes),
        method_count=method_count,
        async_function_count=async_function_count,
        try_count=try_count,
        raise_count=raise_count,
        broad_except_count=broad_except_count,
        bare_except_count=bare_except_count,
        finally_return_count=finally_return_count,
        source_line_count=source_line_count,
        code_line_count=code_line_count,
        top_level_function_doc_coverage=_coverage_label(function_doc_count, len(functions)),
        class_doc_coverage=_coverage_label(class_doc_count, len(classes)),
        method_doc_coverage=_coverage_label(method_doc_count, method_count),
    )

    return FileInfo(
        group=group,
        path=display_path,
        functions=functions,
        classes=classes,
        design=design,
    )


def current_timestamp(scope_path: Path) -> str:
    """
    Prefer a deterministic timestamp derived from git history so generated docs
    remain stable across docs-only commits and repeated `sphinx-build` runs.
    """
    rev = (os.environ.get("SPICE_DOCS_GIT_REF") or "HEAD").strip() or "HEAD"

    try:
        rel_scope = str(scope_path.resolve().relative_to(REPO_ROOT))
    except Exception:
        rel_scope = str(scope_path)

    try:
        ts = subprocess.check_output(
            ["git", "log", "-1", "--format=%cI", rev, "--", rel_scope],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if ts:
            return ts
    except Exception:
        pass

    try:
        ts = subprocess.check_output(
            ["git", "show", "-s", "--format=%cI", rev],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if ts:
            return ts
    except Exception:
        pass

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_existing_timestamp(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("> Generated:"):
            return line.split(":", 1)[1].strip()
    return None


def render_method_index(files: Sequence[FileInfo], timestamp: str) -> str:
    lines: List[str] = []
    lines.append("# Backend Method Index")
    lines.append("")
    lines.append(f"> Generated: {timestamp}")
    lines.append(f"> Scope: {SCOPE_LINE}")
    lines.append("")

    files_sorted = sorted(files, key=lambda info: (info.group, info.path))
    group = None
    for info in files_sorted:
        if info.group != group:
            group = info.group
            lines.append(f"## {group}")
            lines.append("")

        lines.append(f"### `{info.path}`")
        if info.functions:
            lines.append("- **Functions**")
            for fn in info.functions:
                lines.append(f"  - `{fn.signature}` (line {fn.line}): {fn.doc}")
        if info.classes:
            lines.append("- **Classes**")
            for cls in info.classes:
                lines.append(f"  - `{cls.name}` (line {cls.line}): {cls.doc}")
                for method in cls.methods:
                    lines.append(
                        f"    - `{method.signature}` (line {method.line}): {method.doc}"
                    )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _joined_or_placeholder(items: Sequence[str], *, limit: int = 8) -> str:
    if not items:
        return "not documented"
    shown = list(items[:limit])
    suffix = "" if len(items) <= limit else f" (+{len(items) - limit} more)"
    return "; ".join(shown) + suffix


def _design_totals(files: Sequence[FileInfo]) -> DesignTotals:
    return DesignTotals(
        module_count=len(files),
        module_doc_count=sum(1 for info in files if info.design.has_module_doc),
        modules_with_broad_except=sum(1 for info in files if info.design.broad_except_count > 0),
        modules_with_bare_except=sum(1 for info in files if info.design.bare_except_count > 0),
        modules_with_finally_return=sum(1 for info in files if info.design.finally_return_count > 0),
    )


def _group_design_summaries(files: Sequence[FileInfo]) -> List[GroupDesignSummary]:
    groups: Dict[str, GroupDesignSummary] = {}
    for info in files:
        summary = groups.get(info.group)
        if summary is None:
            summary = GroupDesignSummary(
                group=info.group,
                module_count=0,
                module_doc_count=0,
                broad_except_modules=0,
                broad_except_total=0,
                bare_except_total=0,
                total_public_api=0,
                total_async_functions=0,
                total_code_lines=0,
            )
            groups[info.group] = summary
        summary.module_count += 1
        summary.module_doc_count += 1 if info.design.has_module_doc else 0
        summary.broad_except_modules += 1 if info.design.broad_except_count > 0 else 0
        summary.broad_except_total += info.design.broad_except_count
        summary.bare_except_total += info.design.bare_except_count
        summary.total_public_api += len(info.design.public_api)
        summary.total_async_functions += info.design.async_function_count
        summary.total_code_lines += info.design.code_line_count

    return sorted(groups.values(), key=lambda row: row.group)


def _top_hotspots(files: Sequence[FileInfo], *, limit: int = 15) -> List[FileInfo]:
    ranked = sorted(
        files,
        key=lambda info: (
            _module_risk_score(info.design),
            info.design.broad_except_count,
            info.design.try_count,
            info.design.code_line_count,
        ),
        reverse=True,
    )
    return [info for info in ranked if _module_risk_score(info.design) > 0][:limit]


def _entrypoint_modules(files: Sequence[FileInfo]) -> List[FileInfo]:
    entries = [info for info in files if info.path.endswith("/main.py")]
    return sorted(entries, key=lambda info: info.path)


def _render_package_scoreboard(files: Sequence[FileInfo]) -> str:
    summaries = _group_design_summaries(files)
    lines = [
        "| Package | Modules | Module Doc Coverage | Broad-Except Modules | Broad Except Count | Async Functions | Public API | Code Lines |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in summaries:
        coverage = _coverage_label(row.module_doc_count, row.module_count)
        lines.append(
            f"| `{row.group}` | {row.module_count} | {coverage} | {row.broad_except_modules} | "
            f"{row.broad_except_total} | {row.total_async_functions} | {row.total_public_api} | {row.total_code_lines} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def _render_hotspots(files: Sequence[FileInfo]) -> str:
    hotspots = _top_hotspots(files)
    lines = [
        "| Module | Risk Score | Broad Except | Bare Except | Finally Return | Try | Raise | Code Lines |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    if not hotspots:
        lines.append("| - | 0 | 0 | 0 | 0 | 0 | 0 | 0 |")
        return "\n".join(lines).rstrip() + "\n"

    for info in hotspots:
        d = info.design
        lines.append(
            f"| `{info.path}` | {_module_risk_score(d)} | {d.broad_except_count} | {d.bare_except_count} | "
            f"{d.finally_return_count} | {d.try_count} | {d.raise_count} | {d.code_line_count} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def _render_entrypoint_risk_map(files: Sequence[FileInfo]) -> str:
    entries = _entrypoint_modules(files)
    lines = [
        "| Entrypoint | Async Functions | Broad Except | Try | Raise | Code Lines |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    if not entries:
        lines.append("| - | 0 | 0 | 0 | 0 | 0 |")
        return "\n".join(lines).rstrip() + "\n"
    for info in entries:
        d = info.design
        lines.append(
            f"| `{info.path}` | {d.async_function_count} | {d.broad_except_count} | "
            f"{d.try_count} | {d.raise_count} | {d.code_line_count} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def _onboarding_score(info: FileInfo) -> tuple[int, str]:
    path = info.path.lower()
    score = 0
    reasons: List[str] = []

    if path.endswith("/main.py"):
        score += 90
        reasons.append("entrypoint lifecycle")
    if "/routers/" in path:
        score += 70
        reasons.append("API contract surface")
    if "/services/" in path:
        score += 45
        reasons.append("domain/service orchestration")
    if "/registries/" in path:
        score += 30
        reasons.append("storage adapter")
    if "foundry" in path:
        score += 40
        reasons.append("Foundry v2 compatibility")
    if "ontology" in path:
        score += 25
        reasons.append("ontology model contract")
    if "query" in path:
        score += 20
        reasons.append("query/search behavior")
    if info.design.code_line_count >= 1200:
        score += 10
        reasons.append("high-impact module size")
    if len(info.design.public_api) >= 25:
        score += 8
        reasons.append("broad callable surface")

    reason = ", ".join(reasons[:3]) if reasons else "general backend context"
    return score, reason


def _render_onboarding_read_order(files: Sequence[FileInfo], *, limit: int = 20) -> str:
    ranked: List[tuple[int, str, FileInfo]] = []
    for info in files:
        score, reason = _onboarding_score(info)
        if score <= 0:
            continue
        ranked.append((score, reason, info))

    ranked.sort(key=lambda row: (row[0], _module_risk_score(row[2].design), row[2].design.code_line_count), reverse=True)
    lines = ["| Priority | Module | Why First |", "| --- | --- | --- |"]
    for index, (_, reason, info) in enumerate(ranked[:limit], start=1):
        lines.append(f"| {index} | `{info.path}` | {reason} |")
    if len(lines) == 2:
        lines.append("| 1 | - | onboarding candidates unavailable |")
    return "\n".join(lines).rstrip() + "\n"


def render_design_reference(files: Sequence[FileInfo], timestamp: str) -> str:
    totals = _design_totals(files)
    total_code_lines = sum(info.design.code_line_count for info in files)
    lines: List[str] = []
    lines.append("# Backend Design Reference")
    lines.append("")
    lines.append(f"> Generated: {timestamp}")
    lines.append(f"> Scope: {SCOPE_LINE}")
    lines.append("> Source: AST + docstring extraction (module/class/function) via `scripts/generate_backend_methods.py`.")
    lines.append("")

    lines.append("## Coverage Summary")
    lines.append("")
    lines.append(f"- Modules scanned: **{totals.module_count}**")
    lines.append(f"- Modules with module docstring: **{totals.module_doc_count}/{totals.module_count}**")
    lines.append(f"- Modules with broad `except Exception`: **{totals.modules_with_broad_except}**")
    lines.append(f"- Modules with bare `except:`: **{totals.modules_with_bare_except}**")
    lines.append(f"- Modules with `return` inside `finally`: **{totals.modules_with_finally_return}**")
    lines.append(f"- Total code lines (non-empty, non-comment): **{total_code_lines}**")
    lines.append("")

    lines.append("## Package Scoreboard")
    lines.append("")
    lines.append(_render_package_scoreboard(files).rstrip())
    lines.append("")

    lines.append("## Engineering Hotspots")
    lines.append("")
    lines.append(_render_hotspots(files).rstrip())
    lines.append("")

    lines.append("## Entrypoint Risk Map")
    lines.append("")
    lines.append(_render_entrypoint_risk_map(files).rstrip())
    lines.append("")
    lines.append("## New Developer Read Order (First 60-90 Minutes)")
    lines.append("")
    lines.append("> [!TIP]")
    lines.append("> Start with lifecycle entrypoints, then API routers, then domain services and storage adapters.")
    lines.append("")
    lines.append(_render_onboarding_read_order(files).rstrip())
    lines.append("")

    files_sorted = sorted(files, key=lambda info: (info.group, info.path))
    group = None
    for info in files_sorted:
        if info.group != group:
            group = info.group
            lines.append(f"## {group}")
            lines.append("")

        d = info.design
        lines.append(f"### `{info.path}`")
        lines.append(f"- Module summary: {d.module_summary}")
        lines.append(f"- Responsibilities: {_joined_or_placeholder(d.responsibilities)}")
        lines.append(f"- Invariants: {_joined_or_placeholder(d.invariants)}")
        lines.append(f"- Failure modes: {_joined_or_placeholder(d.failure_modes)}")
        lines.append(f"- Extension points: {_joined_or_placeholder(d.extension_points)}")
        lines.append(f"- Dependencies (doc): {_joined_or_placeholder(d.dependencies_doc)}")
        lines.append(f"- Inferred role: {_module_role_hint(info.path)}")
        lines.append(
            f"- Source footprint: total_lines={d.source_line_count} | code_lines={d.code_line_count} | risk_score={_module_risk_score(d)}"
        )
        lines.append(
            f"- API surface: public={len(d.public_api)} | top-level functions={d.top_level_function_count} | "
            f"classes={d.class_count} | methods={d.method_count}"
        )
        lines.append(
            f"- Runtime signals: async_functions={d.async_function_count} | try={d.try_count} | raise={d.raise_count} | "
            f"broad_except={d.broad_except_count} | bare_except={d.bare_except_count} | finally_return={d.finally_return_count}"
        )
        lines.append(
            f"- Doc coverage: module={'yes' if d.has_module_doc else 'no'} | "
            f"top-level functions={d.top_level_function_doc_coverage} | classes={d.class_doc_coverage} | methods={d.method_doc_coverage}"
        )
        lines.append(f"- Internal imports ({len(d.internal_imports)}): {_joined_or_placeholder(d.internal_imports)}")
        lines.append(f"- External imports ({len(d.external_imports)}): {_joined_or_placeholder(d.external_imports)}")
        lines.append(f"- Public API names: {_joined_or_placeholder(d.public_api, limit=12)}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "backend",
        help="Backend root to scan (default: repo_root/backend)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docs" / "BACKEND_METHODS.md",
        help="Output markdown file path for method index",
    )
    parser.add_argument(
        "--design-output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docs" / "BACKEND_DESIGN.md",
        help="Output markdown file path for design reference",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if generated outputs would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    backend_root: Path = args.root
    method_output: Path = args.output
    design_output: Path = args.design_output

    if not backend_root.exists():
        raise SystemExit(f"Backend root not found: {backend_root}")

    internal_roots = _detect_internal_roots(backend_root)
    files: List[FileInfo] = [
        collect_file_info(backend_root, path, internal_roots) for path in iter_python_files(backend_root)
    ]

    timestamp = current_timestamp(backend_root)
    if args.check:
        existing_timestamp = read_existing_timestamp(method_output)
        if existing_timestamp:
            timestamp = existing_timestamp

    method_content = render_method_index(files, timestamp)
    design_content = render_design_reference(files, timestamp)

    if args.check:
        method_existing = method_output.read_text(encoding="utf-8") if method_output.exists() else ""
        design_existing = design_output.read_text(encoding="utf-8") if design_output.exists() else ""
        dirty = False
        if method_existing != method_content:
            print(f"{method_output} is out of date. Run: python scripts/generate_backend_methods.py", flush=True)
            dirty = True
        if design_existing != design_content:
            print(f"{design_output} is out of date. Run: python scripts/generate_backend_methods.py", flush=True)
            dirty = True
        return 1 if dirty else 0

    method_output.parent.mkdir(parents=True, exist_ok=True)
    design_output.parent.mkdir(parents=True, exist_ok=True)
    method_output.write_text(method_content, encoding="utf-8")
    design_output.write_text(design_content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
