#!/usr/bin/env python3
"""Audit enterprise error taxonomy consistency across the codebase.

Checks:
1. `classified_http_exception(status, ..., code=ErrorCode.X)` status/code consistency
   against `_ERROR_CODE_SPECS` in `enterprise_catalog.py`.
2. Optional reporting/failing for raw `HTTPException(...)` usage.
3. Optional reporting/failing for raw string payload codes
   (`"code": "..."` / `"error_code": "..."` / `"api_code": "..."` / `"legacy_code": "..."` / `"external_code": "..."`).
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_HTTP_BY_CLASS: Dict[str, int] = {
    "VALIDATION": 400,
    "SECURITY": 400,
    "AUTH": 401,
    "PERMISSION": 403,
    "NOT_FOUND": 404,
    "CONFLICT": 409,
    "LIMIT": 429,
    "TIMEOUT": 504,
    "UNAVAILABLE": 503,
    "INTERNAL": 500,
    "STATE": 409,
    "INTEGRATION": 502,
}


@dataclass(frozen=True)
class CodeSpec:
    code: str
    expected_status: Optional[int]
    error_class: Optional[str]


@dataclass(frozen=True)
class Mismatch:
    path: str
    line: int
    code: str
    status: int
    expected: int
    error_class: Optional[str]


@dataclass(frozen=True)
class RawHttpCall:
    path: str
    line: int
    has_code: bool
    status: Optional[int]
    code: Optional[str]
    code_is_enum: bool


def _attr_chain(node: ast.AST) -> List[str]:
    parts: List[str] = []
    cur: ast.AST = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    return list(reversed(parts))


def _extract_error_code(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Attribute):
        chain = _attr_chain(node)
        if len(chain) >= 2 and chain[0] == "ErrorCode":
            return chain[1]
    return None


def _extract_status(node: ast.AST) -> Optional[int]:
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    if isinstance(node, ast.Attribute):
        chain = _attr_chain(node)
        if len(chain) == 2 and chain[0] == "status" and chain[1].startswith("HTTP_"):
            parts = chain[1].split("_")
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1])
    return None


def _iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "tests" in path.parts:
            continue
        yield path


def _iter_runtime_files(root: Path, *, runtime_scope_glob: Optional[List[str]] = None) -> Iterable[Path]:
    scope_patterns = list(runtime_scope_glob or [])
    for path in root.rglob("*.py"):
        rel = path.relative_to(root)
        if rel.parts and rel.parts[0] in {"tests", "scripts"}:
            continue
        rel_text = str(rel)
        if scope_patterns:
            if not any(fnmatch.fnmatch(rel_text, pattern) for pattern in scope_patterns):
                continue
        yield path


def _is_exception_type(node: Optional[ast.AST]) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Name):
        return node.id == "Exception"
    if isinstance(node, ast.Tuple):
        return any(_is_exception_type(item) for item in node.elts)
    return False


def _is_suppress_call(func: ast.AST) -> bool:
    if isinstance(func, ast.Name):
        return func.id == "suppress"
    if isinstance(func, ast.Attribute):
        return func.attr == "suppress"
    return False


def _handler_contains_log_call(handler: ast.ExceptHandler) -> bool:
    for node in ast.walk(handler):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in {
            "debug",
            "info",
            "warning",
            "error",
            "exception",
            "critical",
        }:
            return True
    return False


def _handler_contains_raise(handler: ast.ExceptHandler) -> bool:
    for node in ast.walk(handler):
        if isinstance(node, ast.Raise):
            return True
    return False


def _handler_contains_runtime_policy_call(handler: ast.ExceptHandler) -> bool:
    allowed_calls = {
        "log_exception_rate_limited",
        "preserve_primary_exception",
        "fallback_value",
        "record_lineage_or_raise",
    }
    for node in ast.walk(handler):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in allowed_calls:
            return True
        if isinstance(func, ast.Attribute) and func.attr in allowed_calls:
            return True
    return False


def _try_contains_lineage_calls(try_node: ast.Try) -> bool:
    lineage_call_attrs = {"record_link", "record_event_envelope", "enqueue_backfill"}
    for stmt in try_node.body:
        for node in ast.walk(stmt):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in lineage_call_attrs:
                return True
    return False


def _count_runtime_guard_patterns(
    root: Path,
    *,
    runtime_scope_glob: Optional[List[str]] = None,
) -> Dict[str, List[Tuple[str, int]]]:
    issues: Dict[str, List[Tuple[str, int]]] = {
        "return_in_finally": [],
        "bare_except": [],
        "suppress_exception": [],
        "silent_broad_except": [],
        "broad_except_no_log": [],
        "lineage_fail_open": [],
        "action_permission_profile_gap": [],
        "streamjoin_strategy_ignored": [],
        "output_kind_metadata_gap": [],
        "preflight_swallowed_error": [],
        "dataset_write_mode_gap": [],
        "dataset_required_columns_gap": [],
        "dataset_write_format_gap": [],
        "error_monitoring_gap": [],
        "observability_status_gap": [],
    }
    for path in _iter_runtime_files(root, runtime_scope_glob=runtime_scope_glob):
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except SyntaxError:
            continue
        rel = str(path.relative_to(root))

        if rel == "shared/services/pipeline/output_plugins.py":
            gap_patterns = (
                r"OUTPUT_KIND_GEOTEMPORAL\s*:\s*_RequiredMetadataPlugin\([^)]*required_fields=\(\)",
                r"OUTPUT_KIND_MEDIA\s*:\s*_RequiredMetadataPlugin\([^)]*required_fields=\(\)",
                r"OUTPUT_KIND_VIRTUAL\s*:\s*_RequiredMetadataPlugin\([^)]*required_fields=\(\)",
            )
            if any(re.search(pattern, source, flags=re.DOTALL) for pattern in gap_patterns):
                issues["output_kind_metadata_gap"].append((str(path), 1))
            if "validate_dataset_output_metadata(" not in source:
                issues["dataset_required_columns_gap"].append((str(path), 1))

        if rel == "shared/observability/metrics.py":
            required_snippets = (
                "def record_error_envelope(",
                "def record_runtime_fallback(",
                "spice_errors_total",
                "spice_runtime_fallback_total",
            )
            if any(snippet not in source for snippet in required_snippets):
                issues["error_monitoring_gap"].append((str(path), 1))

        if rel == "shared/errors/error_response.py":
            required_snippets = (
                "def _record_error_observability(",
                "def _emit_error_log(",
                "_record_error_observability(",
            )
            if any(snippet not in source for snippet in required_snippets):
                issues["error_monitoring_gap"].append((str(path), 1))

        if rel == "shared/errors/runtime_exception_policy.py":
            if source.count("_record_runtime_fallback_metric(") < 2:
                issues["error_monitoring_gap"].append((str(path), 1))

        if rel == "shared/services/core/service_factory.py":
            required_snippets = (
                '"/observability/status"',
                "record_observability_bootstrap",
                "get_metrics_runtime_status(",
            )
            if any(snippet not in source for snippet in required_snippets):
                issues["observability_status_gap"].append((str(path), 1))

        if rel == "shared/services/pipeline/pipeline_preflight_utils.py":
            if "validate_dataset_output_metadata(" not in source:
                issues["dataset_required_columns_gap"].append((str(path), 1))

        if rel == "pipeline_worker/main.py":
            if "resolve_dataset_write_policy(" not in source:
                issues["dataset_write_mode_gap"].append((str(path), 1))
            if "{\"parquet\", \"json\", \"csv\", \"avro\", \"orc\"}" not in source:
                issues["dataset_write_format_gap"].append((str(path), 1))

        action_permission_required_snippets = {
            "action_worker/main.py": (
                "resolve_action_permission_profile(",
                "action_permission_profile_invalid",
                "requires_action_data_access_enforcement(",
            ),
            "oms/services/action_simulation_service.py": (
                "resolve_action_permission_profile(",
                "action_permission_profile_invalid",
                "requires_action_data_access_enforcement(",
            ),
            "oms/routers/action_async.py": (
                "resolve_action_permission_profile(",
                "permission_profile_invalid",
                "requires_action_data_access_enforcement(",
            ),
            "oms/services/ontology_resource_validator.py": (
                "resolve_action_permission_profile(",
                "ActionPermissionProfileError",
            ),
            "shared/utils/action_permission_profile.py": (
                "PERMISSION_MODEL_ONTOLOGY_ROLES",
                "PERMISSION_MODEL_DATASOURCE_DERIVED",
                "resolve_action_permission_profile(",
            ),
        }
        required_snippets = action_permission_required_snippets.get(rel)
        if required_snippets:
            if any(snippet not in source for snippet in required_snippets):
                issues["action_permission_profile_gap"].append((str(path), 1))

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_apply_stream_join":
                has_apply_join_return = False
                has_stream_spec_call = False
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Return) and isinstance(sub.value, ast.Call):
                        func = sub.value.func
                        if isinstance(func, ast.Name) and func.id == "_apply_join":
                            has_apply_join_return = True
                        if isinstance(func, ast.Attribute) and func.attr == "_apply_join":
                            has_apply_join_return = True
                    if isinstance(sub, ast.Call):
                        func = sub.func
                        if isinstance(func, ast.Name) and func.id == "resolve_stream_join_spec":
                            has_stream_spec_call = True
                        if isinstance(func, ast.Attribute) and func.attr == "resolve_stream_join_spec":
                            has_stream_spec_call = True
                if has_apply_join_return and not has_stream_spec_call:
                    issues["streamjoin_strategy_ignored"].append((str(path), node.lineno))
            if isinstance(node, ast.Try):
                contains_lineage_calls = _try_contains_lineage_calls(node)
                for stmt in node.finalbody:
                    for sub in ast.walk(stmt):
                        if isinstance(sub, ast.Return):
                            issues["return_in_finally"].append((str(path), sub.lineno))
                for handler in node.handlers:
                    if handler.type is None:
                        issues["bare_except"].append((str(path), handler.lineno))
                        continue
                    if not _is_exception_type(handler.type):
                        continue
                    body = handler.body
                    if len(body) == 1 and isinstance(body[0], (ast.Pass, ast.Return, ast.Continue, ast.Break)):
                        issues["silent_broad_except"].append((str(path), handler.lineno))
                    if (
                        (not _handler_contains_log_call(handler))
                        and (not _handler_contains_raise(handler))
                        and (not _handler_contains_runtime_policy_call(handler))
                    ):
                        issues["broad_except_no_log"].append((str(path), handler.lineno))
                    if (
                        contains_lineage_calls
                        and (not _handler_contains_raise(handler))
                        and (not _handler_contains_runtime_policy_call(handler))
                    ):
                        issues["lineage_fail_open"].append((str(path), handler.lineno))
                    if rel == "bff/routers/pipeline_ops_preflight.py":
                        has_return = any(isinstance(sub, ast.Return) for sub in ast.walk(handler))
                        has_raise = any(isinstance(sub, ast.Raise) for sub in ast.walk(handler))
                        if has_return and not has_raise:
                            issues["preflight_swallowed_error"].append((str(path), handler.lineno))
            elif isinstance(node, ast.With):
                for item in node.items:
                    expr = item.context_expr
                    if not isinstance(expr, ast.Call):
                        continue
                    if not _is_suppress_call(expr.func):
                        continue
                    if not expr.args:
                        continue
                    if _is_exception_type(expr.args[0]):
                        issues["suppress_exception"].append((str(path), node.lineno))
    return issues


def _find_commented_exports(
    root: Path,
    *,
    runtime_scope_glob: Optional[List[str]] = None,
) -> List[Tuple[str, int]]:
    hits: List[Tuple[str, int]] = []
    for path in _iter_runtime_files(root, runtime_scope_glob=runtime_scope_glob):
        if path.name != "__init__.py":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for idx, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped.startswith("#"):
                continue
            payload = stripped[1:].strip()
            if payload.startswith("from ") or payload.startswith("import ") or payload.startswith("__all__"):
                hits.append((str(path), idx))
    return hits


def _find_doc_only_modules(
    root: Path,
    *,
    runtime_scope_glob: Optional[List[str]] = None,
) -> List[Tuple[str, int]]:
    hits: List[Tuple[str, int]] = []
    for path in _iter_runtime_files(root, runtime_scope_glob=runtime_scope_glob):
        if path.name == "__init__.py":
            continue
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (OSError, SyntaxError):
            continue
        if not tree.body:
            continue

        non_doc_nodes: List[ast.stmt] = []
        for node in tree.body:
            if (
                isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                continue
            non_doc_nodes.append(node)

        if not non_doc_nodes:
            hits.append((str(path), 1))
            continue

        # Import-only modules are legitimate facades/re-export modules and should not be
        # treated as garbage legacy code. This guard targets modules that are effectively
        # empty (docstring/comments only).
        if all(isinstance(node, (ast.Import, ast.ImportFrom)) for node in non_doc_nodes):
            continue

    return hits


def _normalize_route_path(*parts: str) -> str:
    tokens: List[str] = []
    for part in parts:
        for token in str(part or "").split("/"):
            token = token.strip()
            if token:
                tokens.append(token)
    return "/" + "/".join(tokens) if tokens else "/"


def _extract_apirouter_prefix(tree: ast.AST) -> str:
    for node in getattr(tree, "body", []):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "router" for target in node.targets):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        if not isinstance(node.value.func, ast.Name) or node.value.func.id != "APIRouter":
            continue
        for kw in node.value.keywords:
            if kw.arg == "prefix" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                return kw.value.value
    return ""


def _iter_router_routes(tree: ast.AST) -> Iterable[Tuple[str, str, int, str]]:
    methods = {"get", "post", "put", "patch", "delete", "options", "head"}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            if not isinstance(dec.func, ast.Attribute):
                continue
            if dec.func.attr not in methods:
                continue
            if not isinstance(dec.func.value, ast.Name) or dec.func.value.id != "router":
                continue
            route_path = ""
            if dec.args and isinstance(dec.args[0], ast.Constant) and isinstance(dec.args[0].value, str):
                route_path = dec.args[0].value
            yield dec.func.attr.upper(), route_path, node.lineno, node.name


def _collect_app_route_collisions(root: Path) -> List[Tuple[str, int, str]]:
    app_specs: List[Tuple[str, Dict[str, str]]] = [
        ("bff/main.py", {"bff.routers": "bff/routers", "shared.routers": "shared/routers"}),
        ("oms/main.py", {"oms.routers": "oms/routers", "shared.routers": "shared/routers"}),
    ]
    collisions: List[Tuple[str, int, str]] = []

    for main_rel, module_roots in app_specs:
        main_path = root / main_rel
        if not main_path.exists():
            continue
        try:
            main_tree = ast.parse(main_path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError):
            continue

        import_map: Dict[str, Path] = {}
        for node in getattr(main_tree, "body", []):
            if not isinstance(node, ast.ImportFrom):
                continue
            if not node.module or node.module not in module_roots:
                continue
            base = root / module_roots[node.module]
            for alias in node.names:
                alias_name = alias.asname or alias.name
                import_map[alias_name] = base / f"{alias.name}.py"

        include_entries: List[Tuple[str, str, int]] = []
        for node in ast.walk(main_tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute) or node.func.attr != "include_router":
                continue
            if not node.args:
                continue
            arg = node.args[0]
            module_name: Optional[str] = None
            if (
                isinstance(arg, ast.Attribute)
                and arg.attr == "router"
                and isinstance(arg.value, ast.Name)
            ):
                module_name = arg.value.id
            if not module_name:
                continue
            include_prefix = ""
            for kw in node.keywords:
                if kw.arg == "prefix" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    include_prefix = kw.value.value
            include_entries.append((module_name, include_prefix, node.lineno))

        seen: Dict[Tuple[str, str], Tuple[str, int, str, int]] = {}
        for module_name, include_prefix, include_line in include_entries:
            router_path = import_map.get(module_name)
            if not router_path or not router_path.exists():
                continue
            try:
                router_tree = ast.parse(router_path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError):
                continue
            router_prefix = _extract_apirouter_prefix(router_tree)
            for method, route_path, route_line, func_name in _iter_router_routes(router_tree):
                full_path = _normalize_route_path(include_prefix, router_prefix, route_path)
                key = (method, full_path)
                origin = (str(router_path), route_line, func_name, include_line)
                if key in seen:
                    previous = seen[key]
                    detail = (
                        f"route collision in {main_rel}: {method} {full_path} "
                        f"({previous[0]}:{previous[1]} via include@{previous[3]}) vs "
                        f"({origin[0]}:{origin[1]} via include@{origin[3]})"
                    )
                    collisions.append((str(main_path), include_line, detail))
                else:
                    seen[key] = origin

    return collisions


def _find_duplicate_symbols(
    root: Path,
    *,
    runtime_scope_glob: Optional[List[str]] = None,
) -> List[Tuple[str, int, str]]:
    hits: List[Tuple[str, int, str]] = []
    for path in _iter_runtime_files(root, runtime_scope_glob=runtime_scope_glob):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError):
            continue

        module_seen: Dict[str, int] = {}
        for node in getattr(tree, "body", []):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name in module_seen:
                    detail = (
                        f"duplicate module function `{node.name}` "
                        f"(first:{module_seen[node.name]}, again:{node.lineno})"
                    )
                    hits.append((str(path), node.lineno, detail))
                else:
                    module_seen[node.name] = node.lineno
            elif isinstance(node, ast.ClassDef):
                class_seen: Dict[str, int] = {}
                for item in node.body:
                    if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    if item.name in class_seen:
                        detail = (
                            f"duplicate class method `{node.name}.{item.name}` "
                            f"(first:{class_seen[item.name]}, again:{item.lineno})"
                        )
                        hits.append((str(path), item.lineno, detail))
                    else:
                        class_seen[item.name] = item.lineno
    return hits


def _run_vulture_high_confidence(root: Path) -> Dict[str, List[str]]:
    cmd = [
        sys.executable,
        "-m",
        "vulture",
        ".",
        "--exclude",
        "tests/*,scripts/*,.venv/*,venv/*",
        "--min-confidence",
        "100",
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(root),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return {
            "runtime": [],
            "tests": [],
            "scripts": [],
            "errors": [f"vulture invocation failed: {exc}"],
        }

    runtime: List[str] = []
    tests: List[str] = []
    scripts: List[str] = []
    errors: List[str] = []

    for line in (result.stdout or "").splitlines():
        row = line.strip()
        if not row:
            continue
        path = row.split(":", 1)[0]
        if path.startswith("tests/") or "/tests/" in path:
            tests.append(row)
        elif path.startswith("scripts/") or "/scripts/" in path:
            scripts.append(row)
        else:
            runtime.append(row)

    stderr = (result.stderr or "").strip()
    if stderr:
        errors.append(stderr)

    return {
        "runtime": runtime,
        "tests": tests,
        "scripts": scripts,
        "errors": errors,
    }


def _parse_catalog_specs(catalog_path: Path) -> Dict[str, CodeSpec]:
    tree = ast.parse(catalog_path.read_text(encoding="utf-8"))
    specs: Dict[str, CodeSpec] = {}

    def process_dict(node: ast.Dict) -> None:
        for key_node, value_node in zip(node.keys, node.values):
            if not isinstance(value_node, ast.Call):
                continue
            if not (isinstance(value_node.func, ast.Name) and value_node.func.id == "EnterpriseErrorSpec"):
                continue
            code = _extract_error_code(key_node)
            if not code:
                continue
            error_class: Optional[str] = None
            default_http: Optional[int] = None
            for kw in value_node.keywords:
                if kw.arg == "error_class" and isinstance(kw.value, ast.Attribute):
                    chain = _attr_chain(kw.value)
                    if len(chain) == 2 and chain[0] == "EnterpriseClass":
                        error_class = chain[1]
                elif (
                    kw.arg == "default_http_status"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, int)
                ):
                    default_http = kw.value.value
            expected = default_http if default_http is not None else DEFAULT_HTTP_BY_CLASS.get(error_class or "")
            specs[code] = CodeSpec(code=code, expected_status=expected, error_class=error_class)

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "_ERROR_CODE_SPECS"
                    and isinstance(node.value, ast.Dict)
                ):
                    process_dict(node.value)
        elif isinstance(node, ast.AnnAssign):
            if (
                isinstance(node.target, ast.Name)
                and node.target.id == "_ERROR_CODE_SPECS"
                and isinstance(node.value, ast.Dict)
            ):
                process_dict(node.value)

    return specs


def _audit_classified(
    root: Path,
    specs: Dict[str, CodeSpec],
) -> Tuple[int, int, int, List[Mismatch], List[Tuple[str, int, str, int]]]:
    total = 0
    with_code = 0
    with_literal_status = 0
    mismatches: List[Mismatch] = []
    unknown: List[Tuple[str, int, str, int]] = []

    for path in _iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func_name: Optional[str] = None
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr
            if func_name != "classified_http_exception":
                continue
            total += 1

            status_value: Optional[int] = None
            code_value: Optional[str] = None

            if node.args:
                status_value = _extract_status(node.args[0])
            for kw in node.keywords:
                if kw.arg == "code":
                    code_value = _extract_error_code(kw.value)
                    if code_value:
                        with_code += 1
                    break

            if status_value is None or code_value is None:
                continue
            with_literal_status += 1

            spec = specs.get(code_value)
            if spec is None:
                unknown.append((str(path), node.lineno, code_value, status_value))
                continue
            if spec.expected_status is None:
                continue
            if status_value != spec.expected_status:
                mismatches.append(
                    Mismatch(
                        path=str(path),
                        line=node.lineno,
                        code=code_value,
                        status=status_value,
                        expected=spec.expected_status,
                        error_class=spec.error_class,
                    )
                )

    return total, with_code, with_literal_status, mismatches, unknown


def _dict_has_code_key(node: ast.AST) -> bool:
    if not isinstance(node, ast.Dict):
        return False
    for key in node.keys:
        if isinstance(key, ast.Constant) and key.value == "code":
            return True
    return False


def _extract_code_literal(node: ast.AST) -> Tuple[Optional[str], bool]:
    enum_code = _extract_error_code(node)
    if enum_code:
        return enum_code, True
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        value = node.value.strip()
        if value:
            return value, False
    return None, False


def _extract_dict_code(node: ast.AST) -> Tuple[Optional[str], bool, bool]:
    if not isinstance(node, ast.Dict):
        return None, False, False
    found_code_key = False
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and key.value == "code":
            found_code_key = True
            code, is_enum = _extract_code_literal(value)
            if code is not None:
                return code, is_enum, True
    return None, False, found_code_key


def _count_raw_http_exception(root: Path) -> List[RawHttpCall]:
    hits: List[RawHttpCall] = []
    for path in _iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            is_http_exception = False
            if isinstance(node.func, ast.Name) and node.func.id == "HTTPException":
                is_http_exception = True
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "HTTPException":
                is_http_exception = True
            if not is_http_exception:
                continue

            status_node: Optional[ast.AST] = None
            for kw in node.keywords:
                if kw.arg == "status_code":
                    status_node = kw.value
                    break
            if status_node is None and node.args:
                status_node = node.args[0]
            status_value = _extract_status(status_node) if status_node is not None else None

            detail_node: Optional[ast.AST] = None
            for kw in node.keywords:
                if kw.arg == "detail":
                    detail_node = kw.value
                    break
            if detail_node is None and len(node.args) >= 2:
                detail_node = node.args[1]
            code_value: Optional[str] = None
            code_is_enum = False
            has_code = False
            if detail_node is not None:
                code_value, code_is_enum, has_code = _extract_dict_code(detail_node)
                if not has_code:
                    has_code = _dict_has_code_key(detail_node)
            hits.append(
                RawHttpCall(
                    path=str(path),
                    line=node.lineno,
                    has_code=has_code,
                    status=status_value,
                    code=code_value,
                    code_is_enum=code_is_enum,
                )
            )
    return hits


def _audit_raw_http_status_codes(
    calls: List[RawHttpCall],
    specs: Dict[str, CodeSpec],
) -> Tuple[int, List[Mismatch], List[Tuple[str, int, str, int]]]:
    audited = 0
    mismatches: List[Mismatch] = []
    unknown: List[Tuple[str, int, str, int]] = []

    for call in calls:
        if call.status is None or call.code is None:
            continue
        if not call.code_is_enum and call.code not in specs:
            continue
        audited += 1
        spec = specs.get(call.code)
        if spec is None:
            unknown.append((call.path, call.line, call.code, call.status))
            continue
        if spec.expected_status is None:
            continue
        if call.status != spec.expected_status:
            mismatches.append(
                Mismatch(
                    path=call.path,
                    line=call.line,
                    code=call.code,
                    status=call.status,
                    expected=spec.expected_status,
                    error_class=spec.error_class,
                )
            )
    return audited, mismatches, unknown


def _count_raw_string_codes(root: Path) -> List[Tuple[str, int, str]]:
    pattern = re.compile(r'"(?:code|error_code|api_code|legacy_code|external_code)"\s*:\s*"([A-Z][A-Z0-9_]+)"')
    hits: List[Tuple[str, int, str]] = []
    for path in _iter_python_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for idx, line in enumerate(text.splitlines(), start=1):
            match = pattern.search(line)
            if match:
                hits.append((str(path), idx, match.group(1)))
    return hits


def _print_counter(label: str, rows: Iterable[Tuple[str, int]], top_n: int = 20) -> None:
    counter = Counter(path for path, _ in rows)
    if not counter:
        print(f"{label}: 0")
        return
    print(f"{label}: {sum(counter.values())}")
    for path, count in counter.most_common(top_n):
        print(f"  {count:4d}  {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit enterprise error taxonomy consistency")
    parser.add_argument("--backend-root", default="backend", help="Path to backend root")
    parser.add_argument("--fail-on-raw-http", action="store_true", help="Fail when raw HTTPException is found")
    parser.add_argument(
        "--fail-on-raw-http-without-code",
        action="store_true",
        help="Fail when raw HTTPException(detail=...) is missing detail.code",
    )
    parser.add_argument(
        "--fail-on-raw-code",
        action="store_true",
        help='Fail when raw code-like payload strings are found (`code/error_code/api_code/legacy_code/external_code`)',
    )
    parser.add_argument(
        "--fail-on-bare-except",
        action="store_true",
        help="Fail when `except:` (bare except) exists in runtime files",
    )
    parser.add_argument(
        "--fail-on-suppress-exception",
        action="store_true",
        help="Fail when `suppress(Exception)` exists in runtime files",
    )
    parser.add_argument(
        "--fail-on-silent-broad-except",
        action="store_true",
        help="Fail on runtime `except Exception` handlers that are pass/return/continue/break or no-log/no-raise",
    )
    parser.add_argument(
        "--fail-on-lineage-fail-open",
        action="store_true",
        help="Fail when lineage write calls are wrapped by broad exception handlers that do not re-raise",
    )
    parser.add_argument(
        "--fail-on-action-permission-profile-gap",
        action="store_true",
        help="Fail when action permission model/profile enforcement contract is missing in validator/runtime paths",
    )
    parser.add_argument(
        "--fail-on-streamjoin-strategy-ignored",
        action="store_true",
        help="Fail when streamJoin execution path delegates to generic join without strategy-aware spec handling",
    )
    parser.add_argument(
        "--fail-on-output-kind-metadata-gap",
        action="store_true",
        help="Fail when output kind plugins leave geotemporal/media/virtual metadata requirements empty",
    )
    parser.add_argument(
        "--fail-on-preflight-swallowed",
        action="store_true",
        help="Fail when pipeline preflight catches broad exceptions and returns fallback without re-raise",
    )
    parser.add_argument(
        "--fail-on-dataset-write-mode-gap",
        action="store_true",
        help="Fail when runtime dataset write-mode semantics are not resolved through shared policy",
    )
    parser.add_argument(
        "--fail-on-dataset-required-columns-gap",
        action="store_true",
        help="Fail when dataset required-column validation is missing in output plugin/preflight paths",
    )
    parser.add_argument(
        "--fail-on-dataset-write-format-gap",
        action="store_true",
        help="Fail when dataset output format support does not cover parquet|json|csv|avro|orc",
    )
    parser.add_argument(
        "--fail-on-error-monitoring-gap",
        action="store_true",
        help="Fail when error taxonomy observability hooks are missing from metrics/error runtime paths",
    )
    parser.add_argument(
        "--fail-on-observability-status-gap",
        action="store_true",
        help="Fail when service-level observability status endpoint/bootstrap hooks are missing",
    )
    parser.add_argument(
        "--runtime-scope-glob",
        action="append",
        default=[],
        help="Optional glob(s) for runtime audit scope under backend root (repeatable)",
    )
    parser.add_argument(
        "--fail-on-commented-export",
        action="store_true",
        help="Fail when commented import/export lines exist in runtime __init__.py files",
    )
    parser.add_argument(
        "--fail-on-doc-only-module",
        action="store_true",
        help="Fail when runtime modules contain only docstrings/imports (potential legacy garbage modules)",
    )
    parser.add_argument(
        "--fail-on-route-collision",
        action="store_true",
        help="Fail when FastAPI routes collide within the same app include graph",
    )
    parser.add_argument(
        "--fail-on-duplicate-symbol",
        action="store_true",
        help="Fail when duplicate module/class function symbols are found in runtime files",
    )
    parser.add_argument(
        "--report-vulture-high-confidence",
        action="store_true",
        help="Report vulture --min-confidence 100 findings (runtime/tests/scripts split)",
    )
    parser.add_argument(
        "--fail-on-vulture-runtime-high-confidence",
        action="store_true",
        help="Fail when vulture --min-confidence 100 reports runtime candidates",
    )
    args = parser.parse_args()

    root = Path(args.backend_root).resolve()
    catalog = root / "shared/errors/enterprise_catalog.py"
    if not catalog.exists():
        print(f"Catalog not found: {catalog}", file=sys.stderr)
        return 2

    specs = _parse_catalog_specs(catalog)
    total, with_code, with_literal_status, mismatches, unknown = _audit_classified(root, specs)
    raw_http = _count_raw_http_exception(root)
    raw_http_with_code = [(row.path, row.line) for row in raw_http if row.has_code]
    raw_http_without_code = [(row.path, row.line) for row in raw_http if not row.has_code]
    raw_audited, raw_mismatches, raw_unknown = _audit_raw_http_status_codes(raw_http, specs)
    raw_codes = _count_raw_string_codes(root)
    runtime_issues = _count_runtime_guard_patterns(
        root,
        runtime_scope_glob=list(args.runtime_scope_glob or []),
    )
    commented_exports = _find_commented_exports(
        root,
        runtime_scope_glob=list(args.runtime_scope_glob or []),
    )
    doc_only_modules = _find_doc_only_modules(
        root,
        runtime_scope_glob=list(args.runtime_scope_glob or []),
    )
    route_collisions = _collect_app_route_collisions(root)
    duplicate_symbols = _find_duplicate_symbols(
        root,
        runtime_scope_glob=list(args.runtime_scope_glob or []),
    )
    vulture_report: Optional[Dict[str, List[str]]] = None
    if args.report_vulture_high_confidence or args.fail_on_vulture_runtime_high_confidence:
        vulture_report = _run_vulture_high_confidence(root)

    print("=== Error Taxonomy Audit ===")
    print(f"Catalog specs: {len(specs)}")
    print(f"classified_http_exception calls: {total}")
    print(f"  with code: {with_code}")
    print(f"  with literal status+code: {with_literal_status}")
    print(f"Status/code mismatches: {len(mismatches)}")
    print(f"Unknown ErrorCode in classified calls: {len(unknown)}")
    print(f"Raw HTTPException literal status+code audited: {raw_audited}")
    print(f"Raw HTTPException status/code mismatches: {len(raw_mismatches)}")
    print(f"Unknown ErrorCode in raw HTTPException: {len(raw_unknown)}")
    print(f"Runtime return-in-finally: {len(runtime_issues['return_in_finally'])}")
    print(f"Runtime bare except: {len(runtime_issues['bare_except'])}")
    print(f"Runtime suppress(Exception): {len(runtime_issues['suppress_exception'])}")
    print(f"Runtime silent broad except: {len(runtime_issues['silent_broad_except'])}")
    print(f"Runtime broad except without log/raise: {len(runtime_issues['broad_except_no_log'])}")
    print(f"Runtime lineage fail-open handlers: {len(runtime_issues['lineage_fail_open'])}")
    print(f"Runtime action permission profile gaps: {len(runtime_issues['action_permission_profile_gap'])}")
    print(f"Runtime streamJoin strategy ignored handlers: {len(runtime_issues['streamjoin_strategy_ignored'])}")
    print(f"Runtime output kind metadata gap handlers: {len(runtime_issues['output_kind_metadata_gap'])}")
    print(f"Runtime preflight swallowed handlers: {len(runtime_issues['preflight_swallowed_error'])}")
    print(f"Runtime dataset write-mode gaps: {len(runtime_issues['dataset_write_mode_gap'])}")
    print(f"Runtime dataset required-columns gaps: {len(runtime_issues['dataset_required_columns_gap'])}")
    print(f"Runtime dataset write-format gaps: {len(runtime_issues['dataset_write_format_gap'])}")
    print(f"Runtime error monitoring gaps: {len(runtime_issues['error_monitoring_gap'])}")
    print(f"Runtime observability status gaps: {len(runtime_issues['observability_status_gap'])}")
    print(f"Runtime commented exports in __init__.py: {len(commented_exports)}")
    print(f"Runtime doc-only modules: {len(doc_only_modules)}")
    print(f"App route collisions: {len(route_collisions)}")
    print(f"Runtime duplicate symbols: {len(duplicate_symbols)}")
    if vulture_report is not None:
        print(f"Vulture high-confidence runtime candidates: {len(vulture_report.get('runtime', []))}")
        print(f"Vulture high-confidence test candidates: {len(vulture_report.get('tests', []))}")
        print(f"Vulture high-confidence script candidates: {len(vulture_report.get('scripts', []))}")
        print(f"Vulture invocation errors: {len(vulture_report.get('errors', []))}")

    if mismatches:
        print("\\nTop mismatches:")
        for row in mismatches[:120]:
            print(
                f"  {row.path}:{row.line} code={row.code} status={row.status} "
                f"expected={row.expected} class={row.error_class}"
            )
    if unknown:
        print("\\nUnknown ErrorCode usages:")
        for path, line, code, status in unknown[:80]:
            print(f"  {path}:{line} code={code} status={status}")
    if raw_mismatches:
        print("\\nTop raw HTTPException mismatches:")
        for row in raw_mismatches[:120]:
            print(
                f"  {row.path}:{row.line} code={row.code} status={row.status} "
                f"expected={row.expected} class={row.error_class}"
            )
    if raw_unknown:
        print("\\nUnknown ErrorCode usages in raw HTTPException:")
        for path, line, code, status in raw_unknown[:80]:
            print(f"  {path}:{line} code={code} status={status}")

    print()
    _print_counter("Raw HTTPException usages (all)", [(row.path, row.line) for row in raw_http])
    _print_counter("Raw HTTPException usages (with embedded detail.code)", raw_http_with_code)
    _print_counter("Raw HTTPException usages (without detail.code)", raw_http_without_code)
    print(f"Raw string payload codes (code/error_code/api_code/legacy_code/external_code): {len(raw_codes)}")
    for path, line, code in raw_codes[:120]:
        print(f"  {path}:{line} code={code}")
    if runtime_issues["return_in_finally"]:
        print("\\nRuntime return-in-finally hits:")
        for path, line in runtime_issues["return_in_finally"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["bare_except"]:
        print("\\nRuntime bare except hits:")
        for path, line in runtime_issues["bare_except"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["suppress_exception"]:
        print("\\nRuntime suppress(Exception) hits:")
        for path, line in runtime_issues["suppress_exception"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["silent_broad_except"]:
        print("\\nRuntime silent broad except hits:")
        for path, line in runtime_issues["silent_broad_except"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["broad_except_no_log"]:
        print("\\nRuntime broad except no-log/no-raise hits:")
        for path, line in runtime_issues["broad_except_no_log"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["lineage_fail_open"]:
        print("\\nRuntime lineage fail-open hits:")
        for path, line in runtime_issues["lineage_fail_open"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["action_permission_profile_gap"]:
        print("\\nRuntime action permission profile gap hits:")
        for path, line in runtime_issues["action_permission_profile_gap"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["streamjoin_strategy_ignored"]:
        print("\\nRuntime streamJoin strategy ignored hits:")
        for path, line in runtime_issues["streamjoin_strategy_ignored"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["output_kind_metadata_gap"]:
        print("\\nRuntime output kind metadata gap hits:")
        for path, line in runtime_issues["output_kind_metadata_gap"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["preflight_swallowed_error"]:
        print("\\nRuntime preflight swallowed hits:")
        for path, line in runtime_issues["preflight_swallowed_error"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["dataset_write_mode_gap"]:
        print("\\nRuntime dataset write-mode gap hits:")
        for path, line in runtime_issues["dataset_write_mode_gap"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["dataset_required_columns_gap"]:
        print("\\nRuntime dataset required-columns gap hits:")
        for path, line in runtime_issues["dataset_required_columns_gap"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["dataset_write_format_gap"]:
        print("\\nRuntime dataset write-format gap hits:")
        for path, line in runtime_issues["dataset_write_format_gap"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["error_monitoring_gap"]:
        print("\\nRuntime error monitoring gap hits:")
        for path, line in runtime_issues["error_monitoring_gap"][:120]:
            print(f"  {path}:{line}")
    if runtime_issues["observability_status_gap"]:
        print("\\nRuntime observability status gap hits:")
        for path, line in runtime_issues["observability_status_gap"][:120]:
            print(f"  {path}:{line}")
    if commented_exports:
        print("\\nRuntime commented export hits (__init__.py):")
        for path, line in commented_exports[:120]:
            print(f"  {path}:{line}")
    if doc_only_modules:
        print("\\nRuntime doc-only module hits:")
        for path, line in doc_only_modules[:120]:
            print(f"  {path}:{line}")
    if route_collisions:
        print("\\nApp route collision hits:")
        for path, line, detail in route_collisions[:120]:
            print(f"  {path}:{line} {detail}")
    if duplicate_symbols:
        print("\\nRuntime duplicate symbol hits:")
        for path, line, detail in duplicate_symbols[:120]:
            print(f"  {path}:{line} {detail}")
    if vulture_report is not None:
        if vulture_report.get("errors"):
            print("\\nVulture invocation errors:")
            for row in vulture_report["errors"][:20]:
                print(f"  {row}")
        if vulture_report.get("runtime"):
            print("\\nVulture high-confidence runtime candidates:")
            for row in vulture_report["runtime"][:120]:
                print(f"  {row}")
        if vulture_report.get("tests"):
            print("\\nVulture high-confidence test candidates:")
            for row in vulture_report["tests"][:120]:
                print(f"  {row}")
        if vulture_report.get("scripts"):
            print("\\nVulture high-confidence script candidates:")
            for row in vulture_report["scripts"][:120]:
                print(f"  {row}")

    exit_code = 0
    if mismatches or unknown or raw_mismatches or raw_unknown:
        exit_code = 1
    if args.fail_on_raw_http and raw_http:
        exit_code = 1
    if args.fail_on_raw_http_without_code and raw_http_without_code:
        exit_code = 1
    if args.fail_on_raw_code and raw_codes:
        exit_code = 1
    if args.fail_on_bare_except and runtime_issues["bare_except"]:
        exit_code = 1
    if args.fail_on_suppress_exception and runtime_issues["suppress_exception"]:
        exit_code = 1
    if args.fail_on_silent_broad_except and (
        runtime_issues["silent_broad_except"] or runtime_issues["broad_except_no_log"]
    ):
        exit_code = 1
    if args.fail_on_lineage_fail_open and runtime_issues["lineage_fail_open"]:
        exit_code = 1
    if args.fail_on_action_permission_profile_gap and runtime_issues["action_permission_profile_gap"]:
        exit_code = 1
    if args.fail_on_streamjoin_strategy_ignored and runtime_issues["streamjoin_strategy_ignored"]:
        exit_code = 1
    if args.fail_on_output_kind_metadata_gap and runtime_issues["output_kind_metadata_gap"]:
        exit_code = 1
    if args.fail_on_preflight_swallowed and runtime_issues["preflight_swallowed_error"]:
        exit_code = 1
    if args.fail_on_dataset_write_mode_gap and runtime_issues["dataset_write_mode_gap"]:
        exit_code = 1
    if args.fail_on_dataset_required_columns_gap and runtime_issues["dataset_required_columns_gap"]:
        exit_code = 1
    if args.fail_on_dataset_write_format_gap and runtime_issues["dataset_write_format_gap"]:
        exit_code = 1
    if args.fail_on_error_monitoring_gap and runtime_issues["error_monitoring_gap"]:
        exit_code = 1
    if args.fail_on_observability_status_gap and runtime_issues["observability_status_gap"]:
        exit_code = 1
    if args.fail_on_commented_export and commented_exports:
        exit_code = 1
    if args.fail_on_doc_only_module and doc_only_modules:
        exit_code = 1
    if args.fail_on_route_collision and route_collisions:
        exit_code = 1
    if args.fail_on_duplicate_symbol and duplicate_symbols:
        exit_code = 1
    if args.fail_on_vulture_runtime_high_confidence and vulture_report and vulture_report.get("runtime"):
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
