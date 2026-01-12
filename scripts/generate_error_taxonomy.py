#!/usr/bin/env python3
"""Generate docs/ERROR_TAXONOMY.md from backend/shared/errors/enterprise_catalog.py."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_catalog():
    repo_root = _repo_root()
    sys.path.insert(0, str(repo_root / "backend"))

    logging.disable(logging.CRITICAL)
    from shared.errors import enterprise_catalog  # noqa: WPS433

    return enterprise_catalog


def _md_table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> str:
    lines: List[str] = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _render() -> str:
    catalog = _load_catalog()
    status_hint_fallback = 500

    now = datetime.now(timezone.utc).date().isoformat()
    lines: List[str] = []
    lines.append("# Enterprise Error Taxonomy")
    lines.append("")
    lines.append(f"> Updated: {now}")
    lines.append("")
    lines.append(
        "This file is auto-generated from `backend/shared/errors/enterprise_catalog.py`.\n"
        "Run: `python scripts/generate_error_taxonomy.py`."
    )
    lines.append("")

    lines.append("`SUBSYS` values: `BFF`, `OMS`, `OBJ`, `PIP`, `PRJ`, `CON`, `SHR`, `GEN`.")
    lines.append("")

    lines.append("## Core API Errors (ErrorCode)")
    lines.append("")
    core_rows: List[List[str]] = []
    for legacy, spec in sorted(catalog._ERROR_CODE_SPECS.items(), key=lambda item: item[0].value):  # type: ignore[attr-defined]
        legacy_code = legacy.value
        retry_policy = catalog._resolve_default_retry_policy(spec)  # type: ignore[attr-defined]
        human_required = catalog._resolve_human_required(spec)  # type: ignore[attr-defined]
        safe_next_actions = catalog._resolve_safe_next_actions(  # type: ignore[attr-defined]
            spec,
            legacy_code=legacy_code,
            retry_policy=retry_policy,
            human_required=human_required,
        )
        core_rows.append(
            [
                legacy_code,
                spec.code_template.replace("{subsystem}", "{SUBSYS}"),
                spec.domain.value,
                spec.error_class.value,
                spec.severity.value,
                spec.title,
                str(catalog._resolve_retryable(spec)).lower(),  # type: ignore[attr-defined]
                retry_policy.value,
                str(human_required).lower(),
                catalog._resolve_runbook_ref(spec, legacy_code=legacy_code),  # type: ignore[attr-defined]
                ",".join(action.value for action in safe_next_actions),
                str(catalog._resolve_http_status_hint(spec, status_hint_fallback)),  # type: ignore[attr-defined]
            ]
        )
    lines.append(
        _md_table(
            [
                "Legacy code",
                "Enterprise code",
                "Domain",
                "Class",
                "Severity",
                "Title",
                "Retryable",
                "Default retry policy",
                "Human required",
                "Runbook ref",
                "Safe next actions",
                "HTTP status hint",
            ],
            core_rows,
        )
    )
    lines.append("")

    lines.append("## Objectify Job Errors")
    lines.append("")
    obj_rows: List[List[str]] = []
    for legacy, spec in sorted(catalog._OBJECTIFY_ERROR_SPECS.items(), key=lambda item: item[0]):  # type: ignore[attr-defined]
        legacy_code = legacy
        retry_policy = catalog._resolve_default_retry_policy(spec)  # type: ignore[attr-defined]
        human_required = catalog._resolve_human_required(spec)  # type: ignore[attr-defined]
        safe_next_actions = catalog._resolve_safe_next_actions(  # type: ignore[attr-defined]
            spec,
            legacy_code=legacy_code,
            retry_policy=retry_policy,
            human_required=human_required,
        )
        obj_rows.append(
            [
                legacy_code,
                spec.code_template.format(subsystem="OBJ"),
                spec.domain.value,
                spec.error_class.value,
                spec.severity.value,
                spec.title,
                str(catalog._resolve_retryable(spec)).lower(),  # type: ignore[attr-defined]
                retry_policy.value,
                str(human_required).lower(),
                catalog._resolve_runbook_ref(spec, legacy_code=legacy_code),  # type: ignore[attr-defined]
                ",".join(action.value for action in safe_next_actions),
                str(catalog._resolve_http_status_hint(spec, status_hint_fallback)),  # type: ignore[attr-defined]
            ]
        )
    lines.append(
        _md_table(
            [
                "Legacy code",
                "Enterprise code",
                "Domain",
                "Class",
                "Severity",
                "Title",
                "Retryable",
                "Default retry policy",
                "Human required",
                "Runbook ref",
                "Safe next actions",
                "HTTP status hint",
            ],
            obj_rows,
        )
    )
    lines.append("")

    lines.append("## External Codes")
    lines.append("")
    ext_rows: List[List[str]] = []
    for legacy, spec in sorted(catalog._EXTERNAL_CODE_SPECS.items(), key=lambda item: item[0]):  # type: ignore[attr-defined]
        legacy_code = legacy
        retry_policy = catalog._resolve_default_retry_policy(spec)  # type: ignore[attr-defined]
        human_required = catalog._resolve_human_required(spec)  # type: ignore[attr-defined]
        safe_next_actions = catalog._resolve_safe_next_actions(  # type: ignore[attr-defined]
            spec,
            legacy_code=legacy_code,
            retry_policy=retry_policy,
            human_required=human_required,
        )
        ext_rows.append(
            [
                legacy_code,
                spec.code_template.replace("{subsystem}", "{SUBSYS}"),
                spec.domain.value,
                spec.error_class.value,
                spec.severity.value,
                spec.title,
                str(catalog._resolve_retryable(spec)).lower(),  # type: ignore[attr-defined]
                retry_policy.value,
                str(human_required).lower(),
                catalog._resolve_runbook_ref(spec, legacy_code=legacy_code),  # type: ignore[attr-defined]
                ",".join(action.value for action in safe_next_actions),
                str(catalog._resolve_http_status_hint(spec, status_hint_fallback)),  # type: ignore[attr-defined]
            ]
        )
    lines.append(
        _md_table(
            [
                "Legacy code",
                "Enterprise code",
                "Domain",
                "Class",
                "Severity",
                "Title",
                "Retryable",
                "Default retry policy",
                "Human required",
                "Runbook ref",
                "Safe next actions",
                "HTTP status hint",
            ],
            ext_rows,
        )
    )
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=_repo_root() / "docs" / "ERROR_TAXONOMY.md",
        help="Path to ERROR_TAXONOMY.md (default: docs/ERROR_TAXONOMY.md)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path: Path = args.output

    if not output_path.exists():
        raise SystemExit(f"Missing output file: {output_path}")

    updated = _render()
    current = output_path.read_text(encoding="utf-8")

    if args.check:
        if current != updated:
            print(f"{output_path} is out of date. Run: python scripts/generate_error_taxonomy.py")
            return 1
        return 0

    output_path.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
