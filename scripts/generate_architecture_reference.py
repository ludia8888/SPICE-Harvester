#!/usr/bin/env python3
"""Generate fully auto-managed architecture reference markdown."""

from __future__ import annotations

import argparse
import ast
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCH_PATH = REPO_ROOT / "docs" / "ARCHITECTURE.md"
COMPOSE_MAIN = REPO_ROOT / "docker-compose.full.yml"
BACKEND_ROOT = REPO_ROOT / "backend"

EXCLUDED_DIR_SEGMENTS = {"tests", "scripts", "examples", "perf", "__pycache__"}
IO_IMPORT_PREFIXES = (
    "httpx",
    "requests",
    "aiohttp",
    "boto3",
    "sqlalchemy",
    "psycopg",
    "psycopg2",
    "redis",
    "confluent_kafka",
    "kafka",
    "elasticsearch",
    "opensearch",
    "opensearchpy",
    "pymongo",
    "pyspark",
    "pandas",
)
CORE_PATH_PREFIXES = (
    "backend/shared/services/core/",
    "backend/shared/utils/",
    "backend/shared/interfaces/",
)

@dataclass
class ComposeService:
    name: str
    ports: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    extends_file: Optional[str] = None
    extends_service: Optional[str] = None


@dataclass
class _FunctionMetric:
    file_rel: str
    qualname: str
    lineno: int
    length: int
    complexity: int


@dataclass
class _FileMetric:
    file_rel: str
    lines: int = 0
    function_count: int = 0
    class_count: int = 0
    max_function_length: int = 0
    max_function_complexity: int = 0


@dataclass
class _ClassMetric:
    key: str
    file_rel: str
    simple_name: str
    lineno: int
    base_names: List[str]


@dataclass
class _ChecklistMetric:
    index: int
    label: str
    numerator: int
    denominator: int
    target_percent: float
    basis: str

    @property
    def ratio_percent(self) -> float:
        if self.denominator <= 0:
            return 0.0
        return (self.numerator / self.denominator) * 100.0

    @property
    def status(self) -> str:
        if self.ratio_percent <= self.target_percent:
            return "PASS"
        return "FAIL"

    @property
    def ratio_text(self) -> str:
        return f"{self.numerator}/{self.denominator} ({self.ratio_percent:.2f}%)"


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


def _is_production_backend_file(path: Path) -> bool:
    if path.suffix != ".py":
        return False
    try:
        rel = path.relative_to(BACKEND_ROOT)
    except ValueError:
        return False
    if len(rel.parts) < 2:
        # Skip backend root-level helpers like conftest.py from architecture metrics.
        return False
    return not any(segment in EXCLUDED_DIR_SEGMENTS for segment in rel.parts)


def _iter_production_backend_python_files() -> List[Path]:
    files = [p for p in BACKEND_ROOT.rglob("*.py") if _is_production_backend_file(p)]
    return sorted(files, key=lambda item: str(item.relative_to(REPO_ROOT)))


def _top_module(import_name: Optional[str]) -> str:
    if not import_name:
        return ""
    return import_name.split(".", 1)[0].strip()


def _extract_base_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id.strip()
    if isinstance(node, ast.Attribute):
        return str(node.attr).strip()
    if isinstance(node, ast.Subscript):
        return _extract_base_name(node.value)
    if isinstance(node, ast.Call):
        return _extract_base_name(node.func)
    return ""


def _estimate_function_complexity(node: ast.AST) -> int:
    complexity = 1
    branch_nodes = (
        ast.If,
        ast.For,
        ast.AsyncFor,
        ast.While,
        ast.Try,
        ast.With,
        ast.AsyncWith,
        ast.IfExp,
        ast.Match,
    )
    for child in ast.walk(node):
        if isinstance(child, branch_nodes):
            complexity += 1
        elif isinstance(child, ast.ExceptHandler):
            complexity += 1
        elif isinstance(child, ast.BoolOp):
            complexity += max(0, len(child.values) - 1)
        elif isinstance(child, ast.comprehension):
            complexity += 1
    return complexity


def _find_scc_components(graph: Dict[str, set[str]]) -> List[List[str]]:
    index = 0
    stack: List[str] = []
    on_stack: set[str] = set()
    indices: Dict[str, int] = {}
    lowlink: Dict[str, int] = {}
    components: List[List[str]] = []

    def strongconnect(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlink[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for neighbor in graph.get(node, set()):
            if neighbor not in indices:
                strongconnect(neighbor)
                lowlink[node] = min(lowlink[node], lowlink[neighbor])
            elif neighbor in on_stack:
                lowlink[node] = min(lowlink[node], indices[neighbor])

        if lowlink[node] != indices[node]:
            return

        component: List[str] = []
        while stack:
            w = stack.pop()
            on_stack.discard(w)
            component.append(w)
            if w == node:
                break
        components.append(sorted(component))

    for node in sorted(graph.keys()):
        if node not in indices:
            strongconnect(node)

    return components


def _render_architecture_quality_checklist() -> str:
    files = _iter_production_backend_python_files()
    if not files:
        return "- No backend Python files found for checklist analysis.\n"

    package_names = sorted(
        {
            str(path.relative_to(BACKEND_ROOT).parts[0])
            for path in files
            if len(path.relative_to(BACKEND_ROOT).parts) >= 2
        }
    )
    internal_packages = set(package_names)
    worker_packages = {
        pkg for pkg in package_names if pkg.endswith("_worker")
    } | {"pipeline_scheduler", "connector_trigger_service", "message_relay"}

    package_edges: Dict[str, set[str]] = {pkg: set() for pkg in package_names}
    reverse_edges: Dict[str, set[str]] = {pkg: set() for pkg in package_names}
    total_internal_cross_imports = 0
    layer_leak_imports: List[Tuple[str, int, str, str]] = []
    io_core_direct_files: set[str] = set()
    core_file_count = 0

    file_metrics: Dict[str, _FileMetric] = {}
    function_metrics: List[_FunctionMetric] = []
    class_metrics: Dict[str, _ClassMetric] = {}
    class_name_to_keys: Dict[str, List[str]] = defaultdict(list)

    for path in files:
        rel_repo = path.relative_to(REPO_ROOT).as_posix()
        rel_backend = path.relative_to(BACKEND_ROOT)
        file_rel = rel_backend.as_posix()
        src_pkg = str(rel_backend.parts[0])

        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = len(text.splitlines())
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue

        file_stat = _FileMetric(file_rel=file_rel, lines=lines)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dst_pkg = _top_module(alias.name)
                    if dst_pkg in internal_packages and dst_pkg != src_pkg:
                        total_internal_cross_imports += 1
                        package_edges[src_pkg].add(dst_pkg)
                        reverse_edges[dst_pkg].add(src_pkg)
                        if src_pkg == "shared" and dst_pkg != "shared":
                            layer_leak_imports.append((file_rel, node.lineno, src_pkg, dst_pkg))
                        elif src_pkg in worker_packages and dst_pkg == "bff":
                            layer_leak_imports.append((file_rel, node.lineno, src_pkg, dst_pkg))
                    if rel_repo.startswith(CORE_PATH_PREFIXES) and alias.name.startswith(IO_IMPORT_PREFIXES):
                        io_core_direct_files.add(file_rel)
            elif isinstance(node, ast.ImportFrom):
                if node.level > 0:
                    continue
                dst_pkg = _top_module(node.module)
                if dst_pkg in internal_packages and dst_pkg != src_pkg:
                    total_internal_cross_imports += 1
                    package_edges[src_pkg].add(dst_pkg)
                    reverse_edges[dst_pkg].add(src_pkg)
                    if src_pkg == "shared" and dst_pkg != "shared":
                        layer_leak_imports.append((file_rel, node.lineno, src_pkg, dst_pkg))
                    elif src_pkg in worker_packages and dst_pkg == "bff":
                        layer_leak_imports.append((file_rel, node.lineno, src_pkg, dst_pkg))
                if rel_repo.startswith(CORE_PATH_PREFIXES) and (node.module or "").startswith(IO_IMPORT_PREFIXES):
                    io_core_direct_files.add(file_rel)

        if rel_repo.startswith(CORE_PATH_PREFIXES):
            core_file_count += 1

        def visit(node: ast.AST, class_stack: List[str]) -> None:
            if isinstance(node, ast.ClassDef):
                file_stat.class_count += 1
                class_key = f"{file_rel}:{'.'.join(class_stack + [node.name])}:{node.lineno}"
                base_names = [
                    base_name
                    for base_name in (_extract_base_name(base) for base in node.bases)
                    if base_name
                ]
                class_metrics[class_key] = _ClassMetric(
                    key=class_key,
                    file_rel=file_rel,
                    simple_name=node.name,
                    lineno=node.lineno,
                    base_names=base_names,
                )
                class_name_to_keys[node.name].append(class_key)
                for child in node.body:
                    visit(child, class_stack + [node.name])
                return

            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                file_stat.function_count += 1
                start = node.lineno
                end = getattr(node, "end_lineno", node.lineno)
                length = max(1, end - start + 1)
                complexity = _estimate_function_complexity(node)
                file_stat.max_function_length = max(file_stat.max_function_length, length)
                file_stat.max_function_complexity = max(file_stat.max_function_complexity, complexity)
                function_metrics.append(
                    _FunctionMetric(
                        file_rel=file_rel,
                        qualname=".".join(class_stack + [node.name]) if class_stack else node.name,
                        lineno=start,
                        length=length,
                        complexity=complexity,
                    )
                )
                for child in node.body:
                    visit(child, class_stack)
                return

            for child in ast.iter_child_nodes(node):
                visit(child, class_stack)

        visit(tree, [])
        file_metrics[file_rel] = file_stat

    # Inheritance depth (approximate cross-module resolution by simple class names).
    parents_by_class: Dict[str, List[str]] = {}
    for class_key, info in class_metrics.items():
        resolved_parents: List[str] = []
        for base_name in info.base_names:
            matches = class_name_to_keys.get(base_name, [])
            if len(matches) == 1:
                resolved_parents.append(matches[0])
                continue
            same_file = [key for key in matches if key.startswith(f"{info.file_rel}:")]
            if len(same_file) == 1:
                resolved_parents.append(same_file[0])
        parents_by_class[class_key] = resolved_parents

    depth_cache: Dict[str, int] = {}
    visiting: set[str] = set()

    def class_depth(class_key: str) -> int:
        if class_key in depth_cache:
            return depth_cache[class_key]
        if class_key in visiting:
            return 1
        visiting.add(class_key)
        parents = parents_by_class.get(class_key, [])
        if not parents:
            depth = 1
        else:
            depth = 1 + max(class_depth(parent) for parent in parents)
        visiting.discard(class_key)
        depth_cache[class_key] = depth
        return depth

    deep_inheritance_count = sum(1 for class_key in class_metrics if class_depth(class_key) >= 3)

    scc_components = _find_scc_components(package_edges)
    cycle_components = [component for component in scc_components if len(component) > 1]
    cycle_packages = {pkg for component in cycle_components for pkg in component}

    high_coupling_modules = 0
    for pkg in package_names:
        fan_in = len(reverse_edges.get(pkg, set()))
        fan_out = len(package_edges.get(pkg, set()))
        if (fan_in + fan_out) >= 4 or fan_in >= 5 or fan_out >= 4:
            high_coupling_modules += 1

    total_files = len(file_metrics)
    total_functions = len(function_metrics)
    total_classes = len(class_metrics)
    safe_core_denominator = core_file_count if core_file_count > 0 else 1
    safe_files_denominator = total_files if total_files > 0 else 1
    safe_functions_denominator = total_functions if total_functions > 0 else 1
    safe_classes_denominator = total_classes if total_classes > 0 else 1
    safe_package_denominator = len(package_names) if package_names else 1
    safe_import_denominator = total_internal_cross_imports if total_internal_cross_imports > 0 else 1

    cohesion_risk_files = sum(
        1
        for metric in file_metrics.values()
        if (
            (metric.function_count >= 20 and metric.class_count == 0)
            or metric.max_function_complexity >= 60
            or (metric.lines >= 1500 and metric.function_count >= 20)
        )
    )
    single_responsibility_risk_files = sum(
        1
        for metric in file_metrics.values()
        if (
            metric.lines >= 1000
            or metric.function_count >= 30
            or (metric.class_count >= 15 and metric.function_count >= 20)
        )
    )
    heavy_responsibility_functions = sum(
        1 for metric in function_metrics if metric.complexity >= 25 or metric.length >= 120
    )
    complex_functions = sum(1 for metric in function_metrics if metric.complexity >= 15)
    long_methods = sum(1 for metric in function_metrics if metric.length >= 80)

    checklist = [
        _ChecklistMetric(
            index=1,
            label="계층 간 누수",
            numerator=len(layer_leak_imports),
            denominator=safe_import_denominator,
            target_percent=0.50,
            basis="layer_leak_imports / internal_cross_imports",
        ),
        _ChecklistMetric(
            index=2,
            label="의존성 튐(패키지 순환)",
            numerator=len(cycle_packages),
            denominator=safe_package_denominator,
            target_percent=0.00,
            basis="packages_in_scc(>1) / packages",
        ),
        _ChecklistMetric(
            index=3,
            label="I/O와 Core 직접 연결",
            numerator=len(io_core_direct_files),
            denominator=safe_core_denominator,
            target_percent=5.00,
            basis="io_importing_core_files / core_files",
        ),
        _ChecklistMetric(
            index=4,
            label="모듈 결합도 과다",
            numerator=high_coupling_modules,
            denominator=safe_package_denominator,
            target_percent=15.00,
            basis="high_coupling_modules / modules",
        ),
        _ChecklistMetric(
            index=5,
            label="파일 응집도 저하",
            numerator=cohesion_risk_files,
            denominator=safe_files_denominator,
            target_percent=20.00,
            basis="cohesion_risk_files / files",
        ),
        _ChecklistMetric(
            index=6,
            label="파일 단일 책임 위반",
            numerator=single_responsibility_risk_files,
            denominator=safe_files_denominator,
            target_percent=12.00,
            basis="single_responsibility_risk_files / files",
        ),
        _ChecklistMetric(
            index=7,
            label="함수 단일 책임 위반",
            numerator=heavy_responsibility_functions,
            denominator=safe_functions_denominator,
            target_percent=10.00,
            basis="(cc>=25 or len>=120) / functions",
        ),
        _ChecklistMetric(
            index=8,
            label="연속 상속 깊이(>=3)",
            numerator=deep_inheritance_count,
            denominator=safe_classes_denominator,
            target_percent=5.00,
            basis="classes_depth>=3 / classes",
        ),
        _ChecklistMetric(
            index=9,
            label="복잡도 과다(CC>=15)",
            numerator=complex_functions,
            denominator=safe_functions_denominator,
            target_percent=15.00,
            basis="cc>=15 / functions",
        ),
        _ChecklistMetric(
            index=10,
            label="롱메서드(len>=80)",
            numerator=long_methods,
            denominator=safe_functions_denominator,
            target_percent=8.00,
            basis="len>=80 / functions",
        ),
    ]

    longest_functions = sorted(
        function_metrics,
        key=lambda metric: (metric.length, metric.complexity),
        reverse=True,
    )[:3]
    most_complex_functions = sorted(
        function_metrics,
        key=lambda metric: (metric.complexity, metric.length),
        reverse=True,
    )[:3]

    lines: List[str] = []
    lines.append("- Scope: `backend/**/*.py` (excluding tests/scripts/examples/perf)")
    lines.append(
        "- Population: "
        f"files **{total_files}**, functions **{total_functions}**, classes **{total_classes}**, "
        f"internal cross-imports **{total_internal_cross_imports}**"
    )
    lines.append("")
    lines.append("| # | Check | Ratio | Target | Status | Metric Basis |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for metric in checklist:
        lines.append(
            f"| {metric.index} | {metric.label} | {metric.ratio_text} | <= {metric.target_percent:.2f}% | "
            f"**{metric.status}** | `{metric.basis}` |"
        )

    lines.append("")
    lines.append("### Top Risk Signals")
    lines.append("")

    if layer_leak_imports:
        lines.append("- Layer leaks (sample):")
        for file_rel, lineno, src_pkg, dst_pkg in layer_leak_imports[:5]:
            lines.append(f"  - `{file_rel}:{lineno}` (`{src_pkg}` -> `{dst_pkg}`)")
    else:
        lines.append("- Layer leaks: none detected")

    if cycle_components:
        cycle_labels = ["/".join(component) for component in cycle_components[:5]]
        lines.append(f"- Dependency cycles: {', '.join(f'`{label}`' for label in cycle_labels)}")
    else:
        lines.append("- Dependency cycles: none detected")

    if io_core_direct_files:
        sample = ", ".join(f"`{name}`" for name in sorted(io_core_direct_files)[:5])
        lines.append(f"- I/O-core direct links (sample): {sample}")
    else:
        lines.append("- I/O-core direct links: none detected")

    if longest_functions:
        longest_text = ", ".join(
            f"`{item.file_rel}:{item.lineno}` ({item.length} lines)"
            for item in longest_functions
        )
        lines.append(f"- Longest functions: {longest_text}")

    if most_complex_functions:
        complex_text = ", ".join(
            f"`{item.file_rel}:{item.lineno}` (CC={item.complexity})"
            for item in most_complex_functions
        )
        lines.append(f"- Most complex functions: {complex_text}")

    return "\n".join(lines).rstrip() + "\n"


def _render_architecture_reference() -> str:
    services = _load_resolved_services()
    compose_inventory = _compose_inventory(services)
    compose_graph = _service_dependency_graph(services)
    topology_summary = _runtime_topology_summary(services)
    architecture_quality = _render_architecture_quality_checklist()
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
    lines.append("## Architecture Quality Checklist (Auto-Computed)")
    lines.append("")
    lines.append(architecture_quality.rstrip())
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
