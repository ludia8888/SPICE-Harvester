#!/usr/bin/env python3
"""Generate auto-managed architecture reference blocks in docs/ARCHITECTURE.md."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCH_PATH = REPO_ROOT / "docs" / "ARCHITECTURE.md"
COMPOSE_MAIN = REPO_ROOT / "docker-compose.full.yml"

MARKERS = {
    "COMPOSE_INVENTORY": "AUTO-GENERATED ARCH: COMPOSE_INVENTORY",
    "COMPOSE_GRAPH": "AUTO-GENERATED ARCH: COMPOSE_GRAPH",
    "ENTRYPOINTS": "AUTO-GENERATED ARCH: ENTRYPOINTS",
    "BFF_ROUTERS": "AUTO-GENERATED ARCH: BFF_ROUTERS",
    "OMS_ROUTERS": "AUTO-GENERATED ARCH: OMS_ROUTERS",
    "FUNNEL_ROUTERS": "AUTO-GENERATED ARCH: FUNNEL_ROUTERS",
}

METHOD_ORDER = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


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


def _compose_inventory() -> Tuple[str, str]:
    compose_cache: Dict[Path, Dict[str, ComposeService]] = {}
    services = _parse_compose(COMPOSE_MAIN)
    compose_cache[COMPOSE_MAIN] = services

    resolved: List[ComposeService] = []
    for svc in services.values():
        resolved.append(_resolve_service(svc, COMPOSE_MAIN, compose_cache))

    resolved_sorted = sorted(resolved, key=lambda item: item.name)

    lines: List[str] = []
    lines.append(f"Source: `{COMPOSE_MAIN.name}` (with extends resolved).")
    lines.append("")
    lines.append("| Service | Ports | Depends On |")
    lines.append("| --- | --- | --- |")
    for svc in resolved_sorted:
        ports = "<br/>".join(svc.ports) if svc.ports else "-"
        deps = "<br/>".join(svc.depends_on) if svc.depends_on else "-"
        lines.append(f"| `{svc.name}` | {ports} | {deps} |")

    graph_lines = []
    graph_lines.append("```mermaid")
    graph_lines.append("graph TD")
    node_ids = {svc.name: f"svc_{svc.name.replace('-', '_')}" for svc in resolved_sorted}
    for svc in resolved_sorted:
        graph_lines.append(f"  {node_ids[svc.name]}[{svc.name}]")
    for svc in resolved_sorted:
        for dep in svc.depends_on:
            if dep not in node_ids:
                node_ids[dep] = f"svc_{dep.replace('-', '_')}"
                graph_lines.append(f"  {node_ids[dep]}[{dep}]")
            graph_lines.append(f"  {node_ids[svc.name]} --> {node_ids[dep]}")
    graph_lines.append("```")

    return "\n".join(lines).rstrip() + "\n", "\n".join(graph_lines).rstrip() + "\n"


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


def _parse_routers(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    entries: List[Tuple[str, str, str]] = []
    pattern = re.compile(r"app\.include_router\(([^,]+)(.*)\)")

    for line in lines:
        match = pattern.search(line)
        if not match:
            continue
        router = match.group(1).strip()
        rest = match.group(2)
        prefix_match = re.search(r'prefix\s*=\s*"([^"]+)"', rest)
        tags_match = re.search(r"tags\s*=\s*\[([^\]]+)\]", rest)
        prefix = prefix_match.group(1) if prefix_match else "router-defined"
        tags = tags_match.group(1).strip().replace('"', "") if tags_match else "-"
        entries.append((router, prefix, tags))

    entries = sorted(entries, key=lambda item: item[0])
    output = ["| Router | Prefix | Tags |", "| --- | --- | --- |"]
    for router, prefix, tags in entries:
        output.append(f"| `{router}` | `{prefix}` | {tags} |")
    return "\n".join(output).rstrip() + "\n"


def _replace_block(content: str, marker: str, block: str) -> str:
    begin = f"<!-- BEGIN {marker} -->"
    end = f"<!-- END {marker} -->"
    if begin not in content or end not in content:
        raise ValueError(f"Missing markers for {marker}")
    before, rest = content.split(begin, 1)
    _, after = rest.split(end, 1)
    return f"{before}{begin}\n{block}{end}{after}"


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
    if not args.output.exists():
        raise SystemExit(f"Missing architecture doc: {args.output}")

    content = args.output.read_text(encoding="utf-8")

    compose_inventory, compose_graph = _compose_inventory()
    content = _replace_block(content, MARKERS["COMPOSE_INVENTORY"], compose_inventory)
    content = _replace_block(content, MARKERS["COMPOSE_GRAPH"], compose_graph)
    content = _replace_block(content, MARKERS["ENTRYPOINTS"], _entrypoints())
    content = _replace_block(
        content,
        MARKERS["BFF_ROUTERS"],
        _parse_routers(REPO_ROOT / "backend" / "bff" / "main.py"),
    )
    content = _replace_block(
        content,
        MARKERS["OMS_ROUTERS"],
        _parse_routers(REPO_ROOT / "backend" / "oms" / "main.py"),
    )
    content = _replace_block(
        content,
        MARKERS["FUNNEL_ROUTERS"],
        _parse_routers(REPO_ROOT / "backend" / "funnel" / "main.py"),
    )

    if args.check:
        if args.output.read_text(encoding="utf-8") != content:
            print(f"{args.output} is out of date. Run: python scripts/generate_architecture_reference.py")
            return 1
        return 0

    args.output.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
