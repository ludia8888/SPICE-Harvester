#!/usr/bin/env python3
"""Generate an API v1/v2 overlap report (BFF + OMS) from OpenAPI.

Goal
----
This repository is actively converging on Palantir Foundry's public API surface,
which is predominantly `/api/v2/*` (Foundry-style contracts). At the same time,
legacy/internal product surfaces still exist under `/api/v1/*`.

This script produces a focused report answering:
- How many v1 vs v2 endpoints exist per service?
- Which v1 endpoint families overlap Foundry-style v2 domains (datasets/ontologies/connectivity/orchestration)?
- Which v1 endpoint families are clearly internal-only (admin/monitoring/etc.)?

The report is *not* a full API reference catalog. For that, use:
`docs/API_REFERENCE.md` (BFF) and the OpenAPI schema for OMS.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD")


@dataclass(frozen=True)
class Operation:
    service: str
    method: str
    path: str
    tags: tuple[str, ...]
    operation_id: str

    @property
    def version(self) -> str:
        path = self.path
        if path.startswith("/api/v1/") or path == "/api/v1":
            return "v1"
        if path.startswith("/api/v2/") or path == "/api/v2":
            return "v2"
        if path.startswith("/api/"):
            return "api-other"
        return "other"

    @property
    def domain(self) -> str:
        path = self.path
        if path.startswith("/api/v1/"):
            tail = path[len("/api/v1/") :]
        elif path.startswith("/api/v2/"):
            tail = path[len("/api/v2/") :]
        elif path.startswith("/api/"):
            tail = path[len("/api/") :]
        else:
            tail = path.lstrip("/")
        seg = tail.split("/", 1)[0] if tail else "root"
        return seg or "root"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_openapi(service: str) -> Dict[str, Any]:
    repo_root = _repo_root()
    sys.path.insert(0, str(repo_root / "backend"))

    # Suppress noisy startup logs from app import.
    logging.disable(logging.CRITICAL)
    try:
        if service == "bff":
            from bff import main as mod  # noqa: WPS433
        elif service == "oms":
            from oms import main as mod  # noqa: WPS433
        else:
            raise ValueError(f"unknown service: {service}")
        schema = mod.app.openapi()
    finally:
        logging.disable(logging.NOTSET)
    return schema


def _collect_operations(service: str, schema: Mapping[str, Any]) -> list[Operation]:
    ops: list[Operation] = []
    paths = schema.get("paths") if isinstance(schema.get("paths"), dict) else {}
    for path, methods in (paths or {}).items():
        if not isinstance(methods, dict):
            continue
        for method, meta in methods.items():
            method_upper = str(method or "").upper()
            if method_upper not in METHODS:
                continue
            if not isinstance(meta, dict):
                continue
            tags_raw = meta.get("tags")
            tags: tuple[str, ...] = tuple(str(t) for t in (tags_raw or []) if str(t).strip()) if isinstance(tags_raw, list) else ()
            operation_id = str(meta.get("operationId") or "").strip()
            ops.append(
                Operation(
                    service=service,
                    method=method_upper,
                    path=str(path),
                    tags=tags,
                    operation_id=operation_id,
                )
            )
    ops.sort(key=lambda item: (item.path, item.method))
    return ops


def _count_by(items: Iterable[Operation], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = getattr(item, key)
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def _unique_paths(ops: Sequence[Operation]) -> list[str]:
    seen: set[str] = set()
    paths: list[str] = []
    for op in ops:
        if op.path in seen:
            continue
        seen.add(op.path)
        paths.append(op.path)
    return paths


def _paths_with_prefix(ops: Sequence[Operation], prefix: str) -> list[str]:
    return sorted({op.path for op in ops if op.path.startswith(prefix)})


def _paths_matching_any(ops: Sequence[Operation], needles: Sequence[str]) -> list[str]:
    lowered = [n.lower() for n in needles]
    matched: set[str] = set()
    for op in ops:
        path_lower = op.path.lower()
        if any(n in path_lower for n in lowered):
            matched.add(op.path)
    return sorted(matched)


def _render_table(rows: Sequence[Sequence[str]]) -> str:
    if not rows:
        return ""
    header = rows[0]
    body = rows[1:]
    lines: list[str] = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in header) + " |")
    for row in body:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n"


def _render_paths_block(paths: Sequence[str], *, limit: int = 40) -> str:
    if not paths:
        return "_(none)_\n"
    shown = list(paths)[: max(0, int(limit))]
    lines = [f"- `{path}`" for path in shown]
    if len(paths) > len(shown):
        lines.append(f"- … (+{len(paths) - len(shown)} more)")
    return "\n".join(lines).rstrip() + "\n"


def _render_report(*, bff_ops: Sequence[Operation], oms_ops: Sequence[Operation]) -> str:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    lines: list[str] = []
    lines.append("# API Version Overlap Report (Auto-Generated)")
    lines.append("")
    lines.append("> Generated by `scripts/generate_api_version_overlap_report.py`.")
    lines.append("> Do not edit manually.")
    lines.append("")
    lines.append(f"- Generated at (UTC): `{now}`")
    lines.append("")

    def _summary_section(service: str, ops: Sequence[Operation]) -> None:
        version_counts = _count_by(ops, "version")
        domain_counts = _count_by(ops, "domain")
        lines.append(f"## {service.upper()} Summary")
        lines.append("")
        lines.append(f"- Total operations: **{len(ops)}** (method+path)")
        lines.append("")
        lines.append(_render_table([("Version", "Count"), *[(f"`{k}`", str(v)) for k, v in version_counts.items()]]).rstrip())
        lines.append("")
        top_domains = list(domain_counts.items())[:15]
        lines.append(_render_table([("Top domain (first segment)", "Count"), *[(f"`{k}`", str(v)) for k, v in top_domains]]).rstrip())
        lines.append("")

    _summary_section("bff", bff_ops)
    _summary_section("oms", oms_ops)

    # --- Canonical Foundry-style surface (v2) ---
    lines.append("## Canonical Foundry-Style Surface (v2)")
    lines.append("")
    lines.append("This repository treats `/api/v2/*` as the Foundry-aligned public surface.")
    lines.append("`/api/v1/*` is considered internal/legacy unless explicitly required for product flows.")
    lines.append("")
    lines.append("### BFF v2 domains")
    lines.append("")
    bff_v2_domains = _count_by([op for op in bff_ops if op.version == "v2"], "domain")
    lines.append(_render_table([("Domain", "Ops"), *[(f"`{k}`", str(v)) for k, v in bff_v2_domains.items()]]).rstrip())
    lines.append("")
    lines.append("### OMS v2 domains")
    lines.append("")
    oms_v2_domains = _count_by([op for op in oms_ops if op.version == "v2"], "domain")
    lines.append(_render_table([("Domain", "Ops"), *[(f"`{k}`", str(v)) for k, v in oms_v2_domains.items()]]).rstrip())
    lines.append("")

    # --- Overlap analysis ---
    lines.append("## v1 Surfaces That Overlap Foundry Domains (Candidates for Deprecation)")
    lines.append("")
    lines.append("These are *not* guaranteed 1:1 contract duplicates, but they touch the same product domains.")
    lines.append("They are the first places to look when removing legacy / duplicate surfaces.")
    lines.append("")

    overlap_specs = [
        {
            "title": "Datasets",
            "v2_prefixes": ["/api/v2/datasets"],
            "v1_needles": ["/datasets", "dataset", "ingest-requests", "raw-file"],
        },
        {
            "title": "Ontologies / Ontology resources",
            "v2_prefixes": ["/api/v2/ontologies"],
            "v1_needles": ["/ontology", "ontology/records", "ontology/resources"],
        },
        {
            "title": "Objects (instances) / Writes",
            "v2_prefixes": [
                "/api/v2/ontologies/{ontologyRid}/objects",
                "/api/v2/ontologies/{ontology}/objects",
            ],
            "v1_needles": ["/instances", "/instance/", "/classes", "/class/"],
        },
        {
            "title": "Actions",
            "v2_prefixes": [
                "/api/v2/ontologies/{ontologyRid}/actions",
                "/api/v2/ontologies/{ontology}/actions",
            ],
            "v1_needles": ["/actions"],
        },
        {
            "title": "Connectivity",
            "v2_prefixes": ["/api/v2/connectivity"],
            "v1_needles": ["backing-datasources", "connect", "connector", "source"],
        },
        {
            "title": "Orchestration / Pipelines",
            "v2_prefixes": ["/api/v2/orchestration"],
            "v1_needles": ["/pipelines", "/build", "/deploy", "/schedule"],
        },
    ]

    for spec in overlap_specs:
        title = str(spec["title"])
        v2_prefixes = list(spec["v2_prefixes"])
        v1_needles = list(spec["v1_needles"])

        lines.append(f"### {title}")
        lines.append("")

        bff_v2 = sorted({p for prefix in v2_prefixes for p in _paths_with_prefix(bff_ops, prefix)})
        oms_v2 = sorted({p for prefix in v2_prefixes for p in _paths_with_prefix(oms_ops, prefix)})
        lines.append("- v2 (BFF)")
        lines.append(_render_paths_block(bff_v2, limit=25).rstrip())
        lines.append("")
        lines.append("- v2 (OMS)")
        lines.append(_render_paths_block(oms_v2, limit=25).rstrip())
        lines.append("")

        bff_v1_candidates = [
            p for p in _paths_matching_any([op for op in bff_ops if op.version == "v1"], v1_needles) if p.startswith("/api/v1/")
        ]
        oms_v1_candidates = [
            p for p in _paths_matching_any([op for op in oms_ops if op.version == "v1"], v1_needles) if p.startswith("/api/v1/")
        ]
        lines.append("- v1 candidates (BFF)")
        lines.append(_render_paths_block(bff_v1_candidates, limit=25).rstrip())
        lines.append("")
        lines.append("- v1 candidates (OMS)")
        lines.append(_render_paths_block(oms_v1_candidates, limit=25).rstrip())
        lines.append("")

    # --- Internal-only v1 domains ---
    lines.append("## v1 Domains That Look Internal-Only (Not Foundry API Reference)")
    lines.append("")
    internal_domains = {"admin", "monitoring", "config", "health", "tasks", "audit", "ci-webhooks", "context7", "context-tools", "schema-changes", "ops"}

    def _internal_rows(service: str, ops: Sequence[Operation]) -> list[tuple[str, int]]:
        v1_ops = [op for op in ops if op.version == "v1"]
        by_domain: dict[str, int] = {}
        for op in v1_ops:
            if op.domain in internal_domains:
                by_domain[op.domain] = by_domain.get(op.domain, 0) + 1
        return sorted(by_domain.items(), key=lambda kv: (-kv[1], kv[0]))

    bff_internal = _internal_rows("bff", bff_ops)
    oms_internal = _internal_rows("oms", oms_ops)

    lines.append("### BFF")
    lines.append("")
    lines.append(_render_table([("Domain", "Ops"), *[(f"`{k}`", str(v)) for k, v in bff_internal]]).rstrip())
    lines.append("")
    lines.append("### OMS")
    lines.append("")
    lines.append(_render_table([("Domain", "Ops"), *[(f"`{k}`", str(v)) for k, v in oms_internal]]).rstrip())
    lines.append("")

    lines.append("## Notes / Next Steps")
    lines.append("")
    lines.append("- For the full BFF endpoint catalog (v1+v2), see `docs/API_REFERENCE.md`.")
    lines.append("- `docs/FOUNDRY_V1_TO_V2_MIGRATION.md` documents removed v1 compat routes and the Foundry v2 doc baseline.")
    lines.append("- When removing v1 routes, migrate *call sites first* (frontend + internal clients), then delete handlers and add OpenAPI guards.")
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=_repo_root() / "docs" / "reference" / "_generated" / "API_VERSION_OVERLAP.md",
        help="Path to write the markdown report",
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

    bff_schema = _load_openapi("bff")
    oms_schema = _load_openapi("oms")
    bff_ops = _collect_operations("bff", bff_schema)
    oms_ops = _collect_operations("oms", oms_schema)

    rendered = _render_report(bff_ops=bff_ops, oms_ops=oms_ops)
    current = output_path.read_text(encoding="utf-8") if output_path.exists() else ""

    if args.check:
        if current != rendered:
            print(f"{output_path} is out of date. Run: python scripts/generate_api_version_overlap_report.py")
            return 1
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
