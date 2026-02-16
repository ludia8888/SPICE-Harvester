#!/usr/bin/env python3
"""Generate fully auto-managed API reference markdown from BFF OpenAPI."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

METHOD_ORDER = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
SUPPORTED_PATH_PREFIXES = ("/api/v1", "/api/v2")


def _method_sort_key(method: str) -> Tuple[int, str]:
    try:
        return (METHOD_ORDER.index(method), method)
    except ValueError:
        return (len(METHOD_ORDER), method)


def _load_openapi() -> Dict:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "backend"))

    # Suppress noisy startup logs from app import.
    logging.disable(logging.CRITICAL)
    from bff import main as bff_main  # noqa: WPS433

    return bff_main.app.openapi()


@dataclass(frozen=True)
class OperationRow:
    method: str
    path: str
    tag: str
    version: str
    domain: str
    summary: str
    operation_id: str
    deprecated: bool
    has_security: bool


def _extract_path_parts(path: str) -> tuple[str, str]:
    normalized = str(path or "").strip()
    if normalized.startswith("/api/v1/"):
        version = "v1"
        tail = normalized[len("/api/v1/") :]
    elif normalized.startswith("/api/v2/"):
        version = "v2"
        tail = normalized[len("/api/v2/") :]
    elif normalized == "/api/v1":
        version = "v1"
        tail = ""
    elif normalized == "/api/v2":
        version = "v2"
        tail = ""
    else:
        version = "other"
        tail = normalized.lstrip("/")
    domain = tail.split("/", 1)[0] if tail else "root"
    domain = domain or "root"
    return version, domain


def _collect_operations(schema: Dict) -> List[OperationRow]:
    paths = schema.get("paths", {})
    operations: List[OperationRow] = []

    for path, ops in paths.items():
        if not any(path.startswith(prefix) for prefix in SUPPORTED_PATH_PREFIXES):
            continue
        for method, meta in (ops or {}).items():
            method_upper = method.upper()
            if method_upper not in METHOD_ORDER:
                continue
            if not isinstance(meta, dict):
                continue
            tags = meta.get("tags") or ["Untagged"]
            tag = str(tags[0] if tags else "Untagged")
            version, domain = _extract_path_parts(path)
            summary = str(meta.get("summary") or meta.get("description") or "").strip() or "-"
            operation_id = str(meta.get("operationId") or "").strip() or "-"
            deprecated = bool(meta.get("deprecated"))
            has_security = bool(meta.get("security") or schema.get("security"))
            operations.append(
                OperationRow(
                    method=method_upper,
                    path=path,
                    tag=tag,
                    version=version,
                    domain=domain,
                    summary=summary,
                    operation_id=operation_id,
                    deprecated=deprecated,
                    has_security=has_security,
                )
            )
    operations.sort(key=lambda row: (row.path, _method_sort_key(row.method)))
    return operations


def _group_by_tag(operations: Sequence[OperationRow]) -> Dict[str, List[OperationRow]]:
    grouped: Dict[str, List[OperationRow]] = {}
    for row in operations:
        grouped.setdefault(row.tag, []).append(row)
    for rows in grouped.values():
        rows.sort(key=lambda row: (row.path, _method_sort_key(row.method)))
    return grouped


def _format_method_badge(method: str) -> str:
    return f"`{method}`"


def _render_route_volume_summary(operations: Sequence[OperationRow]) -> str:
    version_counts: Dict[str, int] = {}
    domain_counts: Dict[str, int] = {}
    deprecated_count = 0
    secured_count = 0

    for row in operations:
        version_counts[row.version] = version_counts.get(row.version, 0) + 1
        domain_counts[row.domain] = domain_counts.get(row.domain, 0) + 1
        deprecated_count += 1 if row.deprecated else 0
        secured_count += 1 if row.has_security else 0

    lines: List[str] = []
    lines.append(f"- Total documented endpoints: **{len(operations)}**")
    lines.append(f"- Deprecated endpoints: **{deprecated_count}**")
    lines.append(f"- Security-enabled endpoints: **{secured_count}**")
    lines.append("")
    lines.append("| API Version | Endpoint Count |")
    lines.append("| --- | --- |")
    for version in sorted(version_counts):
        lines.append(f"| `{version}` | {version_counts[version]} |")
    lines.append("")
    lines.append("| Top Domains (first path segment) | Endpoint Count |")
    lines.append("| --- | --- |")
    for domain, count in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))[:15]:
        lines.append(f"| `{domain}` | {count} |")
    return "\n".join(lines).rstrip() + "\n"


def _render_onboarding_callout(operations: Sequence[OperationRow]) -> str:
    paths = {row.path for row in operations}

    def _first_match(candidates: Sequence[str]) -> str:
        for candidate in candidates:
            if candidate in paths:
                return candidate
        return "-"

    ontology_list = _first_match(("/api/v2/ontologies", "/api/v1/databases"))
    ontology_meta = _first_match(("/api/v2/ontologies/{ontology}/fullMetadata",))
    object_search = _first_match(("/api/v2/ontologies/{ontology}/objects/{objectType}/search",))
    lineage = _first_match(("/api/v1/lineage/{db_name}/dataset/{dataset_id}", "/api/v1/lineage"))

    lines: List[str] = []
    lines.append("> [!TIP]")
    lines.append("> New API consumer quick-start (recommended order)")
    lines.append("")
    lines.append("1. Confirm service liveness and auth behavior with a small read endpoint.")
    lines.append(f"2. Enumerate ontology scope: `{ontology_list}`")
    lines.append(f"3. Inspect full metadata contract: `{ontology_meta}`")
    lines.append(f"4. Execute object search queries: `{object_search}`")
    lines.append(f"5. Validate lineage visibility for impact analysis: `{lineage}`")
    return "\n".join(lines).rstrip() + "\n"


def _render_tag_catalog(operations: Sequence[OperationRow]) -> str:
    grouped = _group_by_tag(operations)
    lines: List[str] = []
    for tag in sorted(grouped, key=str.lower):
        rows = grouped[tag]
        lines.append(f"### {tag}")
        lines.append("")
        lines.append("| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for row in rows:
            lines.append(
                f"| {_format_method_badge(row.method)} | `{row.path}` | {row.summary} | `{row.version}` | "
                f"{'yes' if row.has_security else 'no'} | {'yes' if row.deprecated else 'no'} | `{row.operation_id}` |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_api_reference(schema: Dict) -> str:
    info = schema.get("info", {})
    title = str(info.get("title") or "API")
    version = str(info.get("version") or "unknown")
    openapi_version = str(schema.get("openapi") or "unknown")
    operations = _collect_operations(schema)

    lines: List[str] = []
    lines.append("# API Reference (Auto-Generated)")
    lines.append("")
    lines.append("> Generated from BFF OpenAPI by `scripts/generate_api_reference.py`.")
    lines.append("> Do not edit manually.")
    lines.append("")
    lines.append("## OpenAPI Metadata")
    lines.append("")
    lines.append(f"- Title: `{title}`")
    lines.append(f"- Version: `{version}`")
    lines.append(f"- OpenAPI: `{openapi_version}`")
    lines.append("")
    lines.append("## Developer Quick Start")
    lines.append("")
    lines.append(_render_onboarding_callout(operations).rstrip())
    lines.append("")
    lines.append("## Endpoint Coverage Summary")
    lines.append("")
    lines.append(_render_route_volume_summary(operations).rstrip())
    lines.append("")
    lines.append("## Endpoint Catalog (`/api/v1`, `/api/v2`)")
    lines.append("")
    lines.append(_render_tag_catalog(operations).rstrip())

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docs" / "API_REFERENCE.md",
        help="Path to API reference markdown (default: docs/API_REFERENCE.md)",
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

    schema = _load_openapi()
    updated = _render_api_reference(schema)
    current = output_path.read_text(encoding="utf-8") if output_path.exists() else ""

    if args.check:
        if current != updated:
            print(f"{output_path} is out of date. Run: python scripts/generate_api_reference.py")
            return 1
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
