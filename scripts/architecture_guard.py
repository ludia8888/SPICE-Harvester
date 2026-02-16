#!/usr/bin/env python3
"""
Architecture guardrails for CI.

Checks:
1) Forbidden package dependency edges are not reintroduced.
2) Auto-computed architecture checklist has no FAIL rows.
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = REPO_ROOT / "backend"
EXCLUDED_DIR_SEGMENTS = {"tests", "scripts", "examples", "perf", "__pycache__"}
CHECKLIST_ROW_PATTERN = re.compile(
    r"^\|\s*(\d+)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*\*\*(PASS|FAIL)\*\*\s*\|"
)


@dataclass(frozen=True)
class ForbiddenEdge:
    src_pkg: str
    dst_pkg: str
    reason: str


FORBIDDEN_EDGES: tuple[ForbiddenEdge, ...] = (
    ForbiddenEdge(
        src_pkg="shared",
        dst_pkg="objectify_worker",
        reason="Shared layer must not depend on worker implementation packages",
    ),
    ForbiddenEdge(
        src_pkg="shared",
        dst_pkg="funnel",
        reason="Shared layer must remain funnel-agnostic",
    ),
    ForbiddenEdge(
        src_pkg="bff",
        dst_pkg="mcp_servers",
        reason="BFF must consume MCP client abstractions via shared/services, not mcp_servers runtime package",
    ),
)


def _iter_production_backend_python_files() -> list[Path]:
    files: list[Path] = []
    for path in BACKEND_ROOT.rglob("*.py"):
        try:
            rel = path.relative_to(BACKEND_ROOT)
        except ValueError:
            continue
        if len(rel.parts) < 2:
            continue
        if any(segment in EXCLUDED_DIR_SEGMENTS for segment in rel.parts):
            continue
        files.append(path)
    return sorted(files, key=lambda item: str(item.relative_to(REPO_ROOT)))


def _top_module(import_name: str | None) -> str:
    if not import_name:
        return ""
    return import_name.split(".", 1)[0].strip()


def _find_forbidden_imports(files: Iterable[Path]) -> list[str]:
    rules = {(rule.src_pkg, rule.dst_pkg): rule for rule in FORBIDDEN_EDGES}
    violations: list[str] = []

    for path in files:
        rel = path.relative_to(BACKEND_ROOT)
        src_pkg = rel.parts[0]
        text = path.read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dst_pkg = _top_module(alias.name)
                    rule = rules.get((src_pkg, dst_pkg))
                    if rule:
                        violations.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                            f"`{src_pkg} -> {dst_pkg}` forbidden ({rule.reason})"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.level > 0:
                    continue
                dst_pkg = _top_module(node.module)
                rule = rules.get((src_pkg, dst_pkg))
                if rule:
                    violations.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                        f"`{src_pkg} -> {dst_pkg}` forbidden ({rule.reason})"
                    )
    return violations


def _compute_checklist_failures() -> list[str]:
    with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        subprocess.run(
            [
                sys.executable,
                "scripts/generate_architecture_reference.py",
                "--output",
                str(tmp_path),
            ],
            cwd=str(REPO_ROOT),
            check=True,
            capture_output=True,
            text=True,
        )
        content = tmp_path.read_text(encoding="utf-8")
    finally:
        tmp_path.unlink(missing_ok=True)

    failures: list[str] = []
    for line in content.splitlines():
        m = CHECKLIST_ROW_PATTERN.match(line.strip())
        if not m:
            continue
        idx, label, ratio, target, status = m.groups()
        if status == "FAIL":
            failures.append(f"check #{idx} `{label}` failed: ratio={ratio}, target={target}")
    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict-checklist",
        action="store_true",
        default=True,
        help="Fail when any checklist row is FAIL (default: true).",
    )
    parser.add_argument(
        "--no-strict-checklist",
        action="store_false",
        dest="strict_checklist",
        help="Do not fail on checklist FAIL rows (for temporary local analysis).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    files = _iter_production_backend_python_files()
    violations = _find_forbidden_imports(files)
    checklist_failures = _compute_checklist_failures() if args.strict_checklist else []

    if not violations and not checklist_failures:
        print("Architecture guard passed.")
        return 0

    print("Architecture guard failed.")
    if violations:
        print("\n[forbidden-imports]")
        for item in violations:
            print(f"- {item}")
    if checklist_failures:
        print("\n[quality-checklist]")
        for item in checklist_failures:
            print(f"- {item}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
