#!/usr/bin/env python3
"""Generate portal API reference MDX pages from BFF OpenAPI spec.

Produces per-domain MDX files under docs-portal/docs/api/ with endpoint tables,
request/response examples, and parameter details extracted directly from
the live FastAPI OpenAPI schema.

The overview page gets an auto-generated endpoint summary section injected
at the bottom, replacing the static counts with real data.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
PORTAL_DOCS = REPO_ROOT / "docs-portal" / "docs"
PORTAL_API = PORTAL_DOCS / "api"
PORTAL_GENERATED = REPO_ROOT / "docs-portal" / "static" / "generated"

METHOD_ORDER = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
METHOD_COLORS = {
    "GET": "info",
    "POST": "success",
    "PUT": "warning",
    "PATCH": "warning",
    "DELETE": "danger",
}


def _method_sort_key(method: str) -> Tuple[int, str]:
    try:
        return (METHOD_ORDER.index(method), method)
    except ValueError:
        return (len(METHOD_ORDER), method)


def _load_openapi() -> Dict:
    sys.path.insert(0, str(REPO_ROOT / "backend"))
    logging.disable(logging.CRITICAL)
    from bff import main as bff_main  # noqa: WPS433

    return bff_main.app.openapi()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Parameter:
    name: str
    location: str  # path, query, header, cookie
    required: bool
    param_type: str
    description: str


@dataclass
class Endpoint:
    method: str
    path: str
    summary: str
    description: str
    operation_id: str
    tags: List[str]
    deprecated: bool
    has_security: bool
    parameters: List[Parameter] = field(default_factory=list)
    request_body_schema: Optional[str] = None
    response_codes: Dict[str, str] = field(default_factory=dict)


def _extract_type_str(schema: Dict[str, Any]) -> str:
    if not schema:
        return "any"
    if "$ref" in schema:
        ref = schema["$ref"]
        return ref.split("/")[-1]
    t = schema.get("type", "")
    if t == "array":
        items = schema.get("items", {})
        return f"array<{_extract_type_str(items)}>"
    if t == "object":
        return "object"
    fmt = schema.get("format")
    if fmt:
        return f"{t} ({fmt})"
    return t or "any"


def _collect_endpoints(spec: Dict) -> List[Endpoint]:
    paths = spec.get("paths", {})
    global_security = bool(spec.get("security"))
    endpoints: List[Endpoint] = []

    for path, ops in paths.items():
        if not isinstance(ops, dict):
            continue
        for method, meta in ops.items():
            method_upper = method.upper()
            if method_upper not in METHOD_ORDER:
                continue
            if not isinstance(meta, dict):
                continue

            params: List[Parameter] = []
            for p in meta.get("parameters", []):
                schema = p.get("schema", {})
                params.append(Parameter(
                    name=p.get("name", ""),
                    location=p.get("in", "query"),
                    required=p.get("required", False),
                    param_type=_extract_type_str(schema),
                    description=(p.get("description") or "").strip(),
                ))

            req_body = meta.get("requestBody", {})
            req_schema = None
            if req_body:
                content = req_body.get("content", {})
                json_ct = content.get("application/json", {})
                if json_ct:
                    req_schema = _extract_type_str(json_ct.get("schema", {}))

            resp_codes: Dict[str, str] = {}
            for code, resp in (meta.get("responses") or {}).items():
                resp_codes[str(code)] = (resp.get("description") or "").strip()

            endpoints.append(Endpoint(
                method=method_upper,
                path=path,
                summary=(meta.get("summary") or "").strip(),
                description=(meta.get("description") or "").strip(),
                operation_id=(meta.get("operationId") or "").strip(),
                tags=meta.get("tags") or ["Untagged"],
                deprecated=bool(meta.get("deprecated")),
                has_security=bool(meta.get("security") or global_security),
                parameters=params,
                request_body_schema=req_schema,
                response_codes=resp_codes,
            ))

    endpoints.sort(key=lambda e: (e.path, _method_sort_key(e.method)))
    return endpoints


# ---------------------------------------------------------------------------
# v2 / v1 grouping
# ---------------------------------------------------------------------------

def _classify_version(path: str) -> str:
    if path.startswith("/api/v2/") or path == "/api/v2":
        return "v2"
    elif path.startswith("/api/v1/") or path == "/api/v1":
        return "v1"
    return "other"


def _classify_v2_domain(path: str) -> str:
    """Classify v2 endpoints into functional domains."""
    p = path.replace("/api/v2/", "").replace("/api/", "")
    if "objectTypes" in p or "objectType" in p:
        if "outgoingLinkTypes" in p or "linkType" in p:
            return "links"
        return "object-types"
    if "/objects/" in p:
        if "/search" in p:
            return "search"
        if "/links/" in p:
            return "links"
        return "objects"
    if "actionTypes" in p or "/actions" in p:
        return "actions"
    if "queryTypes" in p:
        return "queries"
    if "interfaceTypes" in p:
        return "interfaces"
    if "sharedPropertyTypes" in p:
        return "shared-properties"
    if "valueTypes" in p:
        return "value-types"
    if "ontologies" in p or "ontology" in p:
        return "ontologies"
    return "other"


def _classify_v1_domain(path: str) -> str:
    """Classify v1 endpoints into broad categories."""
    p = path.replace("/api/v1/", "")
    first_seg = p.split("/")[0] if p else "root"
    domain_map = {
        "databases": "object-types",
        "ontology": "object-types",
        "ontology-extensions": "object-types",
        "object-types": "object-types",
        "link-types": "object-types",
        "instances": "objects",
        "instances-async": "objects",
        "query": "search",
        "mapping": "search",
        "actions": "actions",
        "pipeline": "pipeline",
        "pipeline-plans": "pipeline",
        "datasets": "pipeline",
        "objectify": "objectify",
        "lineage": "lineage",
        "graph": "lineage",
        "admin": "admin",
        "health": "health",
        "monitoring": "health",
        "config": "health",
        "data-connector": "connectors",
        "ai": "ai",
        "agent": "ai",
        "agent-proxy": "ai",
        "ontology-agent": "ai",
        "context7": "ai",
        "context-tools": "ai",
        "summary": "ai",
        "tasks": "tasks",
        "websocket": "websocket",
        "audit": "governance",
        "governance": "governance",
        "schema-changes": "governance",
        "ops": "operations",
        "ci-webhooks": "operations",
        "document-bundles": "other",
        "command-status": "other",
    }
    return domain_map.get(first_seg, "other")


# ---------------------------------------------------------------------------
# MDX renderers
# ---------------------------------------------------------------------------

V2_DOMAIN_META = {
    "ontologies": {"title": "Ontologies", "order": 1},
    "object-types": {"title": "Object Types", "order": 2},
    "objects": {"title": "Objects", "order": 3},
    "search": {"title": "Search", "order": 4},
    "actions": {"title": "Actions", "order": 5},
    "links": {"title": "Links & Link Types", "order": 6},
    "queries": {"title": "Query Types", "order": 7},
    "interfaces": {"title": "Interface Types", "order": 8},
    "shared-properties": {"title": "Shared Property Types", "order": 9},
    "value-types": {"title": "Value Types", "order": 10},
    "other": {"title": "Other v2 Endpoints", "order": 99},
}

V1_DOMAIN_META = {
    "object-types": {"title": "Object Types & Schema", "order": 1},
    "objects": {"title": "Object Instances", "order": 2},
    "search": {"title": "Search & Query", "order": 3},
    "actions": {"title": "Actions", "order": 4},
    "pipeline": {"title": "Pipeline", "order": 5},
    "objectify": {"title": "Objectify", "order": 6},
    "lineage": {"title": "Lineage & Graph", "order": 7},
    "connectors": {"title": "Data Connectors", "order": 8},
    "ai": {"title": "AI & Agents", "order": 9},
    "governance": {"title": "Governance & Audit", "order": 10},
    "admin": {"title": "Admin", "order": 11},
    "health": {"title": "Health & Monitoring", "order": 12},
    "tasks": {"title": "Tasks", "order": 13},
    "websocket": {"title": "WebSocket", "order": 14},
    "operations": {"title": "Operations", "order": 15},
    "other": {"title": "Other", "order": 99},
}


def _render_endpoint_row(ep: Endpoint) -> str:
    dep_badge = " ~~deprecated~~" if ep.deprecated else ""
    auth = "Yes" if ep.has_security else "No"
    summary = ep.summary or ep.description[:80] or "-"
    summary = summary.replace("|", "\\|")
    return (
        f"| `{ep.method}` | `{ep.path}` | {summary}{dep_badge} | {auth} |"
    )


def _render_domain_section(
    domain_key: str,
    endpoints: List[Endpoint],
    meta_map: Dict[str, Dict],
) -> str:
    meta = meta_map.get(domain_key, {"title": domain_key.title(), "order": 50})
    lines: List[str] = []
    lines.append(f"### {meta['title']} ({len(endpoints)} endpoints)")
    lines.append("")
    lines.append("| Method | Path | Summary | Auth |")
    lines.append("|--------|------|---------|------|")
    for ep in endpoints:
        lines.append(_render_endpoint_row(ep))
    lines.append("")
    return "\n".join(lines)


def _render_v2_reference(v2_endpoints: List[Endpoint]) -> str:
    """Render the auto-generated v2 reference MDX."""
    grouped: Dict[str, List[Endpoint]] = defaultdict(list)
    for ep in v2_endpoints:
        domain = _classify_v2_domain(ep.path)
        grouped[domain].append(ep)

    lines: List[str] = []
    lines.append("---")
    lines.append("title: v2 API Reference (Auto-Generated)")
    lines.append("sidebar_label: v2 Complete Reference")
    lines.append("sidebar_position: 10")
    lines.append("---")
    lines.append("")
    lines.append("# Foundry v2 API Reference")
    lines.append("")
    lines.append(":::info Auto-Generated")
    lines.append("This page is auto-generated from the live BFF OpenAPI schema.")
    lines.append("Do not edit manually. Run `python scripts/generate_portal_api_reference.py`.")
    lines.append(":::")
    lines.append("")
    lines.append(f"**Total v2 endpoints: {len(v2_endpoints)}**")
    lines.append("")

    for domain_key in sorted(grouped, key=lambda k: V2_DOMAIN_META.get(k, {}).get("order", 50)):
        lines.append(_render_domain_section(domain_key, grouped[domain_key], V2_DOMAIN_META))

    return "\n".join(lines)


def _render_v1_reference(v1_endpoints: List[Endpoint]) -> str:
    """Render the auto-generated v1 reference MDX."""
    grouped: Dict[str, List[Endpoint]] = defaultdict(list)
    for ep in v1_endpoints:
        domain = _classify_v1_domain(ep.path)
        grouped[domain].append(ep)

    lines: List[str] = []
    lines.append("---")
    lines.append("title: v1 Internal API Reference (Auto-Generated)")
    lines.append("sidebar_label: v1 Internal Reference")
    lines.append("sidebar_position: 11")
    lines.append("---")
    lines.append("")
    lines.append("# Internal v1 API Reference")
    lines.append("")
    lines.append(":::warning Deprecated")
    lines.append(
        "The v1 API surface is internal/legacy compatibility scope and is not Foundry public ontology API."
    )
    lines.append(
        "Use the [Foundry-aligned v2 API](./auto-v2-reference) for new external integrations."
    )
    lines.append(":::")
    lines.append("")
    lines.append(":::info Auto-Generated")
    lines.append("This page is auto-generated from the live BFF OpenAPI schema.")
    lines.append("Do not edit manually. Run `python scripts/generate_portal_api_reference.py`.")
    lines.append(":::")
    lines.append("")
    lines.append(f"**Total v1 endpoints: {len(v1_endpoints)}**")
    lines.append("")

    for domain_key in sorted(grouped, key=lambda k: V1_DOMAIN_META.get(k, {}).get("order", 50)):
        lines.append(_render_domain_section(domain_key, grouped[domain_key], V1_DOMAIN_META))

    return "\n".join(lines)


def _render_api_overview_summary(endpoints: List[Endpoint]) -> str:
    """Render the auto-generated summary section for the overview page."""
    v2 = [e for e in endpoints if _classify_version(e.path) == "v2"]
    v1 = [e for e in endpoints if _classify_version(e.path) == "v1"]

    # v2 domain breakdown
    v2_grouped: Dict[str, int] = defaultdict(int)
    for ep in v2:
        domain = _classify_v2_domain(ep.path)
        meta = V2_DOMAIN_META.get(domain, {"title": domain.title()})
        v2_grouped[meta["title"]] += 1

    lines: List[str] = []
    lines.append(f"### v2 Endpoints ({len(v2)})")
    lines.append("")
    lines.append("| Category | Endpoints | Base Path |")
    lines.append("|----------|-----------|-----------|")

    v2_base_paths = {
        "Ontologies": "`/api/v2/ontologies`",
        "Object Types": "`/api/v2/ontologies/{ontology}/objectTypes`",
        "Objects": "`/api/v2/ontologies/{ontology}/objects/{objectType}`",
        "Search": "`/api/v2/ontologies/{ontology}/objects/{objectType}/search`",
        "Actions": "`/api/v2/ontologies/{ontology}/actionTypes`",
        "Links & Link Types": "`/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes`",
        "Query Types": "`/api/v2/ontologies/{ontology}/queryTypes`",
        "Interface Types": "`/api/v2/ontologies/{ontology}/interfaceTypes`",
        "Shared Property Types": "`/api/v2/ontologies/{ontology}/sharedPropertyTypes`",
        "Value Types": "`/api/v2/ontologies/{ontology}/valueTypes`",
    }

    for cat in sorted(v2_grouped, key=lambda k: V2_DOMAIN_META.get(
        next((dk for dk, dv in V2_DOMAIN_META.items() if dv["title"] == k), "other"), {}
    ).get("order", 50)):
        base = v2_base_paths.get(cat, "`/api/v2/...`")
        lines.append(f"| {cat} | {v2_grouped[cat]} | {base} |")

    lines.append("")
    lines.append(f"### v1 Endpoints ({len(v1)})")
    lines.append("")
    lines.append("v1 routes are internal SPICE-specific/compatibility surfaces and may not map to Foundry public APIs.")
    lines.append("For Foundry-style contracts, prefer [v2 reference pages](./auto-v2-reference).")
    lines.append("The [v1 reference pages](./auto-v1-reference) are retained only for migration and internal operations.")

    return "\n".join(lines)


def _render_openapi_json(spec: Dict) -> str:
    """Export the OpenAPI spec as JSON for static hosting."""
    # Keep output deterministic across runs to stabilize docs pre-commit checks.
    return json.dumps(spec, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate(check: bool = False) -> int:
    spec = _load_openapi()
    endpoints = _collect_endpoints(spec)

    v2 = [e for e in endpoints if _classify_version(e.path) == "v2"]
    v1 = [e for e in endpoints if _classify_version(e.path) == "v1"]

    outputs: Dict[Path, str] = {}

    # 1. v2 complete reference
    outputs[PORTAL_API / "auto-v2-reference.mdx"] = _render_v2_reference(v2)

    # 2. v1 complete reference
    outputs[PORTAL_API / "auto-v1-reference.mdx"] = _render_v1_reference(v1)

    # 3. Overview endpoint summary (injected into overview.mdx marker)
    outputs[PORTAL_API / "auto-endpoint-summary.md"] = _render_api_overview_summary(endpoints)

    # 4. OpenAPI JSON for static hosting
    PORTAL_GENERATED.mkdir(parents=True, exist_ok=True)
    outputs[PORTAL_GENERATED / "bff-openapi.json"] = _render_openapi_json(spec)

    # Write or check
    failures: List[str] = []
    for path, content in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if check:
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if existing != content:
                failures.append(str(path))
                print(f"  OUT OF DATE: {path}")
        else:
            path.write_text(content, encoding="utf-8")
            print(f"  WROTE: {path}")

    if check and failures:
        print(
            f"\n{len(failures)} API reference file(s) out of date. "
            f"Run: python scripts/generate_portal_api_reference.py"
        )
        return 1

    if not check:
        print(f"\nGenerated API reference: {len(v2)} v2 + {len(v1)} v1 = {len(endpoints)} total endpoints")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return generate(check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
