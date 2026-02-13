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
import re
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

    exit_code = 0
    if mismatches or unknown or raw_mismatches or raw_unknown:
        exit_code = 1
    if args.fail_on_raw_http and raw_http:
        exit_code = 1
    if args.fail_on_raw_http_without_code and raw_http_without_code:
        exit_code = 1
    if args.fail_on_raw_code and raw_codes:
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
