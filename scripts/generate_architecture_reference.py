#!/usr/bin/env python3
"""Generate fully auto-managed architecture reference markdown."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCH_PATH = REPO_ROOT / "docs" / "ARCHITECTURE.md"
COMPOSE_MAIN = REPO_ROOT / "docker-compose.full.yml"

@dataclass
class ComposeService:
    name: str
    ports: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    extends_file: Optional[str] = None
    extends_service: Optional[str] = None


def _strip_quotes(value: str) -> str:
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        return value[1:-1]
    return value


def _strip_inline_comment(value: str) -> str:
    if "#" in value:
        return value.split("#", 1)[0].rstrip()
    return value


def _parse_compose(path: Path) -> Dict[str, ComposeService]:
    services: Dict[str, ComposeService] = {}
    lines = path.read_text(encoding="utf-8").splitlines()

    in_services = False
    services_indent = 0
    current_service: Optional[ComposeService] = None
    current_section: Optional[str] = None
    section_indent = 0

    for raw in lines:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        stripped = raw.strip()

        if stripped == "services:":
            in_services = True
            services_indent = indent
            current_service = None
            current_section = None
            continue

        if in_services and indent <= services_indent and not stripped.startswith("-"):
            in_services = False
            current_service = None
            current_section = None

        if not in_services:
            continue

        if indent == services_indent + 2 and stripped.endswith(":"):
            name = stripped[:-1].strip()
            current_service = services.setdefault(name, ComposeService(name=name))
            current_section = None
            continue

        if current_service is None:
            continue

        if stripped.startswith("ports:"):
            current_section = "ports"
            section_indent = indent
            continue
        if stripped.startswith("depends_on:"):
            current_section = "depends_on"
            section_indent = indent
            continue
        if stripped.startswith("extends:"):
            current_section = "extends"
            section_indent = indent
            continue

        if current_section == "ports":
            if indent > section_indent and stripped.startswith("- "):
                value = _strip_quotes(_strip_inline_comment(stripped[2:].strip()))
                current_service.ports.append(value)
                continue
            if indent <= section_indent:
                current_section = None

        if current_section == "depends_on":
            if indent > section_indent:
                if stripped.startswith("- "):
                    value = _strip_quotes(_strip_inline_comment(stripped[2:].strip()))
                    current_service.depends_on.append(value)
                    continue
                if stripped.endswith(":"):
                    current_service.depends_on.append(stripped[:-1].strip())
                    continue
            if indent <= section_indent:
                current_section = None

        if current_section == "extends":
            if indent > section_indent and ":" in stripped:
                key, value = stripped.split(":", 1)
                value = _strip_quotes(value.strip())
                if key.strip() == "file":
                    current_service.extends_file = value
                elif key.strip() == "service":
                    current_service.extends_service = value
                continue
            if indent <= section_indent:
                current_section = None

    return services


def _resolve_service(
    service: ComposeService,
    compose_path: Path,
    compose_cache: Dict[Path, Dict[str, ComposeService]],
    seen: Optional[set[str]] = None,
) -> ComposeService:
    if seen is None:
        seen = set()
    key = f"{compose_path}:{service.name}"
    if key in seen:
        return ComposeService(name=service.name, ports=list(service.ports), depends_on=list(service.depends_on))
    seen.add(key)

    ports = list(service.ports)
    depends = list(service.depends_on)

    if service.extends_file and service.extends_service:
        base_path = (compose_path.parent / service.extends_file).resolve()
        base_services = compose_cache.setdefault(base_path, _parse_compose(base_path))
        base = base_services.get(service.extends_service)
        if base:
            resolved_base = _resolve_service(base, base_path, compose_cache, seen)
            ports = resolved_base.ports + ports
            depends = resolved_base.depends_on + depends

    return ComposeService(
        name=service.name,
        ports=_dedupe(ports),
        depends_on=_dedupe(depends),
    )


def _dedupe(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _load_resolved_services() -> List[ComposeService]:
    compose_cache: Dict[Path, Dict[str, ComposeService]] = {}
    services = _parse_compose(COMPOSE_MAIN)
    compose_cache[COMPOSE_MAIN] = services

    resolved: List[ComposeService] = []
    for svc in services.values():
        resolved.append(_resolve_service(svc, COMPOSE_MAIN, compose_cache))

    return sorted(resolved, key=lambda item: item.name)


def _render_mermaid_block(lines: List[str]) -> str:
    return "\n".join(["```mermaid", *lines, "```"]).rstrip() + "\n"


def _mermaid_label(text: str) -> str:
    escaped = str(text).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _compose_inventory(services: Sequence[ComposeService]) -> str:
    lines: List[str] = []
    lines.append(f"Source: `{COMPOSE_MAIN.name}` (with extends resolved).")
    lines.append("")
    lines.append("| Service | Ports | Depends On |")
    lines.append("| --- | --- | --- |")
    for svc in services:
        ports = "<br/>".join(svc.ports) if svc.ports else "-"
        deps = "<br/>".join(svc.depends_on) if svc.depends_on else "-"
        lines.append(f"| `{svc.name}` | {ports} | {deps} |")

    return "\n".join(lines).rstrip() + "\n"


def _service_dependency_graph(services: Sequence[ComposeService]) -> str:
    graph_lines: List[str] = []
    graph_lines.append("graph TD")
    node_ids = {svc.name: f"svc_{svc.name.replace('-', '_')}" for svc in services}
    for svc in services:
        graph_lines.append(f"  {node_ids[svc.name]}[{_mermaid_label(svc.name)}]")
    for svc in services:
        for dep in svc.depends_on:
            if dep not in node_ids:
                node_ids[dep] = f"svc_{dep.replace('-', '_')}"
                graph_lines.append(f"  {node_ids[dep]}[{_mermaid_label(dep)}]")
            graph_lines.append(f"  {node_ids[svc.name]} --> {node_ids[dep]}")
    return _render_mermaid_block(graph_lines)


def _classify_service_role(name: str) -> str:
    lowered = name.lower()
    if lowered in {"postgres", "redis", "elasticsearch", "kafka", "zookeeper", "minio", "lakefs"}:
        return "data"
    if lowered in {"prometheus", "grafana", "jaeger", "otel-collector", "alertmanager", "kafka-ui"}:
        return "observability"
    if lowered.endswith("-worker") or lowered.endswith("-scheduler") or lowered.endswith("-service"):
        return "workers"
    if lowered.endswith("-init") or lowered.endswith("-migrations") or "migrations" in lowered:
        return "bootstrap"
    if lowered in {"bff", "oms", "funnel", "agent"}:
        return "api"
    return "other"


def _runtime_topology_summary(services: Sequence[ComposeService]) -> str:
    role_to_services: Dict[str, List[str]] = {
        "api": [],
        "workers": [],
        "data": [],
        "observability": [],
        "bootstrap": [],
        "other": [],
    }
    for svc in services:
        role_to_services[_classify_service_role(svc.name)].append(svc.name)

    lines: List[str] = []
    lines.append(f"- Total runtime services: **{len(services)}**")
    lines.append(f"- API services: **{len(role_to_services['api'])}**")
    lines.append(f"- Background/worker services: **{len(role_to_services['workers'])}**")
    lines.append(f"- Data platform services: **{len(role_to_services['data'])}**")
    lines.append(f"- Observability services: **{len(role_to_services['observability'])}**")
    lines.append("")
    lines.append("| Role | Count | Services |")
    lines.append("| --- | --- | --- |")
    for role in ("api", "workers", "data", "observability", "bootstrap", "other"):
        names = sorted(role_to_services[role])
        joined = "<br/>".join(names) if names else "-"
        lines.append(f"| `{role}` | {len(names)} | {joined} |")
    return "\n".join(lines).rstrip() + "\n"


def _external_interfaces(services: Sequence[ComposeService]) -> str:
    lines: List[str] = []
    lines.append("| Service | Published Ports |")
    lines.append("| --- | --- |")
    for svc in services:
        if not svc.ports:
            continue
        lines.append(f"| `{svc.name}` | {'<br/>'.join(svc.ports)} |")
    if len(lines) == 2:
        lines.append("| - | - |")
    return "\n".join(lines).rstrip() + "\n"


def _critical_runtime_paths(services: Sequence[ComposeService]) -> str:
    names = {svc.name for svc in services}
    blocks: List[str] = []

    if {"bff", "oms", "elasticsearch", "postgres"}.issubset(names):
        blocks.append("### Query Path (API -> Object Search)")
        blocks.append("")
        blocks.append(
            _render_mermaid_block(
                [
                    "flowchart LR",
                    f"  bff[{_mermaid_label('BFF')}] --> oms[{_mermaid_label('OMS')}]",
                    f"  oms --> es[{_mermaid_label('Elasticsearch')}]",
                    f"  oms --> pg[{_mermaid_label('Postgres')}]",
                ]
            ).rstrip()
        )
        blocks.append("")

    if {"bff", "kafka", "action-worker", "lakefs", "postgres"}.issubset(names):
        blocks.append("### Action Path (Submit -> Async Apply)")
        blocks.append("")
        blocks.append(
            _render_mermaid_block(
                [
                    "flowchart LR",
                    f"  bff[{_mermaid_label('BFF')}] --> kafka[{_mermaid_label('Kafka')}]",
                    f"  kafka --> aw[{_mermaid_label('action-worker')}]",
                    f"  aw --> lakefs[{_mermaid_label('LakeFS')}]",
                    f"  aw --> pg[{_mermaid_label('Postgres')}]",
                    f"  aw --> relay[{_mermaid_label('message-relay')}]",
                ]
            ).rstrip()
        )
        blocks.append("")

    if {"oms", "kafka", "projection-worker", "elasticsearch", "redis"}.issubset(names):
        blocks.append("### Projection Path (Change -> Read Model)")
        blocks.append("")
        blocks.append(
            _render_mermaid_block(
                [
                    "flowchart LR",
                    f"  oms[{_mermaid_label('OMS')}] --> kafka[{_mermaid_label('Kafka')}]",
                    f"  kafka --> pw[{_mermaid_label('projection-worker')}]",
                    f"  pw --> es[{_mermaid_label('Elasticsearch')}]",
                    f"  pw --> redis[{_mermaid_label('Redis')}]",
                ]
            ).rstrip()
        )
        blocks.append("")

    if not blocks:
        return "- Critical path diagrams are unavailable because required services were not detected.\n"
    return "\n".join(blocks).rstrip() + "\n"


def _onboarding_quick_start() -> str:
    lines: List[str] = []
    lines.append("> [!TIP]")
    lines.append("> First-day onboarding path (recommended)")
    lines.append("")
    lines.append("1. Start core runtime services and verify API health.")
    lines.append("2. Read BFF/OMS entrypoints to understand dependency wiring.")
    lines.append("3. Follow the Foundry v2 ontology route and OMS object search route end-to-end.")
    lines.append("4. Validate docs generation to ensure documentation and code stay in lockstep.")
    lines.append("")
    lines.append("```bash")
    lines.append("# 1) Start stack")
    lines.append("docker compose -f docker-compose.full.yml up -d")
    lines.append("")
    lines.append("# 2) Verify docs are in sync")
    lines.append("python scripts/check_docs.py")
    lines.append("```")
    return "\n".join(lines).rstrip() + "\n"


def _onboarding_change_map() -> str:
    candidates = [
        ("Add/modify Foundry v2 ontology API", "backend/bff/routers/foundry_ontology_v2.py"),
        ("Add/modify OMS Object Search DSL", "backend/oms/routers/query.py"),
        ("BFF query-builder contract/examples", "backend/bff/routers/query.py"),
        ("Service startup wiring (BFF)", "backend/bff/main.py"),
        ("Service startup wiring (OMS)", "backend/oms/main.py"),
        ("Async action execution", "backend/action_worker/main.py"),
        ("Projection/read-model pipeline", "backend/projection_worker/main.py"),
        ("Event storage/persistence primitives", "backend/shared/services/storage/event_store.py"),
    ]
    lines = ["| Common Task | Primary File |", "| --- | --- |"]
    for task, rel_path in candidates:
        path = REPO_ROOT / rel_path
        if path.exists():
            lines.append(f"| {task} | `{rel_path}` |")
    return "\n".join(lines).rstrip() + "\n"


def _entrypoints() -> str:
    entries = []
    for path in (REPO_ROOT / "backend").rglob("main.py"):
        if "__pycache__" in path.parts:
            continue
        if "tests" in path.parts:
            continue
        rel = path.relative_to(REPO_ROOT)
        entries.append(str(rel))
    entries = sorted(entries)
    lines = ["- " + f"`{entry}`" for entry in entries]
    return "\n".join(lines).rstrip() + "\n"


def _infer_router_source(main_path: Path, router_expr: str) -> str:
    root_name = router_expr.split(".", 1)[0].strip()
    if not root_name:
        return "-"

    candidates = [
        main_path.parent / "routers" / f"{root_name}.py",
        REPO_ROOT / "backend" / "shared" / "routers" / f"{root_name}.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate.relative_to(REPO_ROOT))
    return "-"


def _parse_routers(path: Path) -> List[Tuple[str, str, str, str]]:
    content = path.read_text(encoding="utf-8")
    entries: List[Tuple[str, str, str, str]] = []
    pattern = re.compile(r"app\.include_router\((.*?)\)", re.DOTALL)

    for match in pattern.finditer(content):
        call_expr = match.group(1).strip()
        if not call_expr:
            continue
        router_expr = call_expr.split(",", 1)[0].strip()
        prefix_match = re.search(r'prefix\s*=\s*"([^"]+)"', call_expr)
        tags_match = re.search(r"tags\s*=\s*\[([^\]]+)\]", call_expr)
        prefix = prefix_match.group(1) if prefix_match else "router-defined"
        tags = tags_match.group(1).strip().replace('"', "") if tags_match else "-"
        source = _infer_router_source(path, router_expr)
        entries.append((router_expr, prefix, tags, source))

    return sorted(entries, key=lambda item: item[0])


def _render_router_inventory(entries: Sequence[Tuple[str, str, str, str]]) -> str:
    output = ["| Router | Prefix | Tags | Source File |", "| --- | --- | --- | --- |"]
    for router, prefix, tags, source in entries:
        source_cell = f"`{source}`" if source != "-" else "-"
        output.append(f"| `{router}` | `{prefix}` | {tags} | {source_cell} |")
    return "\n".join(output).rstrip() + "\n"


def _render_router_summary(entries: Sequence[Tuple[str, str, str, str]]) -> str:
    prefixes = {prefix for _, prefix, _, _ in entries}
    tagged = sum(1 for _, _, tags, _ in entries if tags != "-")
    with_source = sum(1 for _, _, _, source in entries if source != "-")
    return (
        f"- Routers detected: **{len(entries)}**\n"
        f"- Distinct prefixes: **{len(prefixes)}**\n"
        f"- Routers with explicit tags: **{tagged}**\n"
        f"- Routers with resolved source files: **{with_source}**\n"
    )


def _render_architecture_reference() -> str:
    services = _load_resolved_services()
    compose_inventory = _compose_inventory(services)
    compose_graph = _service_dependency_graph(services)
    topology_summary = _runtime_topology_summary(services)
    external_interfaces = _external_interfaces(services)
    critical_paths = _critical_runtime_paths(services)
    onboarding_quick_start = _onboarding_quick_start()
    onboarding_change_map = _onboarding_change_map()
    entrypoints = _entrypoints()
    bff_router_entries = _parse_routers(REPO_ROOT / "backend" / "bff" / "main.py")
    oms_router_entries = _parse_routers(REPO_ROOT / "backend" / "oms" / "main.py")
    funnel_router_entries = _parse_routers(REPO_ROOT / "backend" / "funnel" / "main.py")

    lines: List[str] = []
    lines.append("# Architecture Reference (Auto-Generated)")
    lines.append("")
    lines.append("> Generated by `scripts/generate_architecture_reference.py`.")
    lines.append("> Do not edit manually.")
    lines.append("")
    lines.append("## Developer Onboarding Quick Start")
    lines.append("")
    lines.append(onboarding_quick_start.rstrip())
    lines.append("")
    lines.append("## Runtime Topology Summary")
    lines.append("")
    lines.append(topology_summary.rstrip())
    lines.append("")
    lines.append("## External Interfaces (Published Ports)")
    lines.append("")
    lines.append(external_interfaces.rstrip())
    lines.append("")
    lines.append("## Service Inventory (`docker-compose.full.yml`)")
    lines.append("")
    lines.append(compose_inventory.rstrip())
    lines.append("")
    lines.append("## Service Dependency Graph")
    lines.append("")
    lines.append(compose_graph.rstrip())
    lines.append("")
    lines.append("## Critical Runtime Paths")
    lines.append("")
    lines.append(critical_paths.rstrip())
    lines.append("")
    lines.append("## Development Change Map")
    lines.append("")
    lines.append(onboarding_change_map.rstrip())
    lines.append("")
    lines.append("## Service Entry Points (`backend/*/main.py`)")
    lines.append("")
    lines.append(entrypoints.rstrip())
    lines.append("")
    lines.append("## Router Inventory (BFF)")
    lines.append("")
    lines.append(_render_router_summary(bff_router_entries).rstrip())
    lines.append("")
    lines.append(_render_router_inventory(bff_router_entries).rstrip())
    lines.append("")
    lines.append("## Router Inventory (OMS)")
    lines.append("")
    lines.append(_render_router_summary(oms_router_entries).rstrip())
    lines.append("")
    lines.append(_render_router_inventory(oms_router_entries).rstrip())
    lines.append("")
    lines.append("## Router Inventory (Funnel)")
    lines.append("")
    lines.append(_render_router_summary(funnel_router_entries).rstrip())
    lines.append("")
    lines.append(_render_router_inventory(funnel_router_entries).rstrip())
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=ARCH_PATH,
        help="Architecture markdown file to update.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    content = _render_architecture_reference()
    current = args.output.read_text(encoding="utf-8") if args.output.exists() else ""

    if args.check:
        if current != content:
            print(f"{args.output} is out of date. Run: python scripts/generate_architecture_reference.py")
            return 1
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
