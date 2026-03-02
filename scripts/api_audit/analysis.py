#!/usr/bin/env python3
"""Analysis engine for OpenAPI duplicate/legacy audit."""

from __future__ import annotations

import ast
import inspect
import json
import os
import random
import re
import statistics
import textwrap
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

try:
    from .inventory import EndpointRecord, InventoryCollection, SERVICE_CONFIG, repo_root
except ImportError:  # pragma: no cover
    from inventory import EndpointRecord, InventoryCollection, SERVICE_CONFIG, repo_root  # type: ignore

WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
FOUNDRY_V2_DOMAINS = {
    "ontologies",
    "datasets",
    "orchestration",
    "connectivity",
}
INTERNAL_V1_DOMAINS = {
    "admin",
    "monitoring",
    "config",
    "health",
    "tasks",
    "audit",
    "ops",
    "ci-webhooks",
    "context7",
    "context-tools",
    "schema-changes",
}

SINK_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "HTTP_PROXY": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bhttpx\.(get|post|put|patch|delete|request)\b",
            r"\baiohttp\.(client|clientsession)\b",
            r"\brequests\.(get|post|put|patch|delete|request)\b",
            r"\bcall_.*api\b",
            r"\bclient\.(get|post|put|patch|delete|request)\b",
        )
    ],
    "REGISTRY_DB": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\basyncpg\b",
            r"\bsqlalchemy\b",
            r"\bsession\.execute\b",
            r"\bexecute\(",
            r"\bfetch(one|all|val)?\(",
            r"\bpostgres\b",
            r"\bregistry\b",
        )
    ],
    "QUEUE": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bkafka\b",
            r"\bproduce\(",
            r"\bpublish\(",
            r"\boutbox\b",
            r"\benqueue\(",
            r"\bmessage\b",
        )
    ],
    "SEARCH_ES": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\belasticsearch\b",
            r"\bes_client\b",
            r"\bindex\(",
            r"\bsearch\(",
            r"\bprojection\b",
        )
    ],
    "OBJECT_STORE": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bs3\b",
            r"\bminio\b",
            r"\blakefs\b",
            r"\bbucket\b",
            r"\bupload\b",
            r"\bobject[_-]?store\b",
        )
    ],
}


@dataclass
class EndpointAnalysis:
    endpoint_id: str
    sink_categories: list[str]
    sink_evidence: list[str]
    level1_calls: list[str]
    level2_calls: list[str]
    no_sink_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EndpointPair:
    left_endpoint_id: str
    right_endpoint_id: str
    score: float
    path_score: float
    op_score: float
    contract_score: float
    sink_score: float
    domain_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EndpointCluster:
    cluster_id: str
    cluster_type: str
    endpoint_ids: list[str]
    representative_method: str
    canonical_paths: list[str]
    sink_categories: list[str]
    risk_level: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ThresholdCalibration:
    duplicate_threshold: float
    similar_threshold: float
    positive_seed_count: int
    negative_seed_count: int
    positive_score_mean: float
    negative_score_mean: float
    histogram: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RuntimeCheckResult:
    endpoint_id: str
    service: str
    method: str
    path: str
    url: str
    status_code: int
    expected_statuses: list[str]
    contract_match: bool
    fallback_4xx_match: bool
    evaluation_mode: str
    passed: bool
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RuntimeValidationReport:
    generated_at: str
    mode: str
    risk_filter: str
    pending_reason: str | None
    service_health: dict[str, Any]
    selected_target_count: int
    checks: list[RuntimeCheckResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "mode": self.mode,
            "risk_filter": self.risk_filter,
            "pending_reason": self.pending_reason,
            "service_health": self.service_health,
            "checks": [check.to_dict() for check in self.checks],
            "summary": {
                "total": len(self.checks),
                "target_count": self.selected_target_count,
                "passed": sum(1 for check in self.checks if check.passed),
                "failed": sum(1 for check in self.checks if not check.passed),
                "server_errors": sum(1 for check in self.checks if check.status_code >= 500),
                "contract_mismatch_count": sum(
                    1
                    for check in self.checks
                    if check.status_code > 0 and check.status_code < 500 and not check.contract_match
                ),
                "fallback_4xx_count": sum(1 for check in self.checks if check.fallback_4xx_match),
            },
        }


@dataclass
class AnalysisBundle:
    endpoint_analysis: dict[str, EndpointAnalysis]
    endpoint_pairs: list[EndpointPair]
    clusters: list[EndpointCluster]
    endpoint_cluster_map: dict[str, str]
    endpoint_verdicts: dict[str, dict[str, Any]]
    thresholds: ThresholdCalibration
    coverage: dict[str, Any]


def _tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"[^a-zA-Z0-9]+", (value or "").lower()) if token}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / float(len(union))


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = max(0.0, min(100.0, percentile)) * (len(sorted_values) - 1) / 100.0
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction


def _clip(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _extract_call_symbol(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _extract_call_symbol(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    if isinstance(node, ast.Call):
        return _extract_call_symbol(node.func)
    if isinstance(node, ast.Subscript):
        return _extract_call_symbol(node.value)
    return ""


def _extract_calls_from_callable(target: Callable[..., Any]) -> tuple[list[str], str]:
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        return [], ""
    if not source.strip():
        return [], ""

    dedented = textwrap.dedent(source)
    try:
        parsed = ast.parse(dedented)
    except SyntaxError:
        return [], dedented

    calls: list[str] = []
    for node in ast.walk(parsed):
        if isinstance(node, ast.Call):
            symbol = _extract_call_symbol(node.func)
            if symbol:
                calls.append(symbol)
    return sorted(set(calls)), dedented


def _resolve_callee(target: Callable[..., Any], symbol: str) -> Callable[..., Any] | None:
    globals_map = getattr(target, "__globals__", {})
    if not isinstance(globals_map, dict):
        return None

    parts = [part for part in symbol.split(".") if part]
    if not parts:
        return None

    root = globals_map.get(parts[0])
    if root is None:
        return None
    current: Any = root
    for part in parts[1:]:
        if not hasattr(current, part):
            return None
        current = getattr(current, part)

    if inspect.ismethod(current):
        current = current.__func__
    if inspect.isfunction(current) or inspect.iscoroutinefunction(current):
        return current
    return None


def _in_repo_source(target: Callable[..., Any]) -> bool:
    source_file = inspect.getsourcefile(target)
    if not source_file:
        return False
    source_path = Path(source_file).resolve()
    root = repo_root().resolve()
    try:
        source_path.relative_to(root)
    except ValueError:
        return False
    return True


def _find_sink_hits(calls: list[str], source_text: str, *, level: str) -> tuple[set[str], list[str]]:
    sink_categories: set[str] = set()
    sink_evidence: list[str] = []
    joined_calls = "\n".join(calls)
    inspection_text = f"{joined_calls}\n{source_text}".lower()

    for sink_category, patterns in SINK_PATTERNS.items():
        for pattern in patterns:
            match = pattern.search(inspection_text)
            if not match:
                continue
            sink_categories.add(sink_category)
            evidence = match.group(0).strip()
            sink_evidence.append(f"{level}:{sink_category}:{evidence}")
            break
    return sink_categories, sorted(set(sink_evidence))


def _endpoint_flow_analysis(
    endpoint_id: str,
    endpoint_callable: Callable[..., Any] | None,
) -> EndpointAnalysis:
    if not callable(endpoint_callable):
        return EndpointAnalysis(
            endpoint_id=endpoint_id,
            sink_categories=[],
            sink_evidence=[],
            level1_calls=[],
            level2_calls=[],
            no_sink_reason="endpoint callable unavailable",
        )

    level1_calls, level1_source = _extract_calls_from_callable(endpoint_callable)
    sink_categories, sink_evidence = _find_sink_hits(level1_calls, level1_source, level="level1")
    level2_calls_collected: set[str] = set()

    for symbol in level1_calls[:40]:
        resolved = _resolve_callee(endpoint_callable, symbol)
        if resolved is None or not _in_repo_source(resolved):
            continue
        level2_calls, level2_source = _extract_calls_from_callable(resolved)
        for call in level2_calls:
            level2_calls_collected.add(call)
        nested_categories, nested_evidence = _find_sink_hits(level2_calls, level2_source, level=f"level2:{symbol}")
        sink_categories |= nested_categories
        sink_evidence.extend(nested_evidence)

    sink_evidence = sorted(set(sink_evidence))
    no_sink_reason = None if sink_categories else "no sink keyword matched in depth=2"
    return EndpointAnalysis(
        endpoint_id=endpoint_id,
        sink_categories=sorted(sink_categories),
        sink_evidence=sink_evidence,
        level1_calls=level1_calls,
        level2_calls=sorted(level2_calls_collected),
        no_sink_reason=no_sink_reason,
    )


def _pair_similarity(
    left: EndpointRecord,
    right: EndpointRecord,
    left_analysis: EndpointAnalysis,
    right_analysis: EndpointAnalysis,
) -> EndpointPair:
    left_path_tokens = _tokenize(left.canonical_path)
    right_path_tokens = _tokenize(right.canonical_path)
    left_operation_tokens = _tokenize(left.operation_id)
    right_operation_tokens = _tokenize(right.operation_id)
    left_contract_tokens = _tokenize(f"{left.request_fingerprint} {left.response_fingerprint}")
    right_contract_tokens = _tokenize(f"{right.request_fingerprint} {right.response_fingerprint}")
    left_sinks = set(left_analysis.sink_categories)
    right_sinks = set(right_analysis.sink_categories)

    method_match = 1.0 if left.method == right.method else 0.0
    path_score = _jaccard(left_path_tokens, right_path_tokens)
    op_score = _jaccard(left_operation_tokens, right_operation_tokens)
    contract_score = _jaccard(left_contract_tokens, right_contract_tokens)
    sink_score = _jaccard(left_sinks, right_sinks)
    domain_score = 1.0 if left.domain == right.domain else 0.0

    score = (
        (0.10 * method_match)
        + (0.35 * path_score)
        + (0.20 * op_score)
        + (0.20 * contract_score)
        + (0.10 * sink_score)
        + (0.05 * domain_score)
    )
    if left.method != right.method:
        score = min(score, 0.55)

    return EndpointPair(
        left_endpoint_id=left.endpoint_id,
        right_endpoint_id=right.endpoint_id,
        score=round(score, 4),
        path_score=round(path_score, 4),
        op_score=round(op_score, 4),
        contract_score=round(contract_score, 4),
        sink_score=round(sink_score, 4),
        domain_score=round(domain_score, 4),
    )


def _build_histogram(values: list[float], *, step: float = 0.05) -> list[dict[str, Any]]:
    if not values:
        return []
    buckets: dict[str, int] = defaultdict(int)
    for value in values:
        lower = int(value / step) * step
        upper = lower + step
        if upper > 1.0:
            upper = 1.0
        label = f"{lower:.2f}-{upper:.2f}"
        buckets[label] += 1
    return [{"bucket": bucket, "count": buckets[bucket]} for bucket in sorted(buckets.keys())]


def _calibrate_thresholds(pairs: list[EndpointPair], endpoints_by_id: dict[str, EndpointRecord]) -> ThresholdCalibration:
    pair_scores = [pair.score for pair in pairs]
    positive_seed_scores: list[float] = []
    negative_seed_scores: list[float] = []

    for pair in pairs:
        left = endpoints_by_id[pair.left_endpoint_id]
        right = endpoints_by_id[pair.right_endpoint_id]
        if left.service != right.service and left.method == right.method and left.canonical_path == right.canonical_path:
            positive_seed_scores.append(pair.score)
        left_tokens = _tokenize(left.canonical_path)
        right_tokens = _tokenize(right.canonical_path)
        token_overlap = len(left_tokens & right_tokens)
        if left.domain != right.domain and token_overlap == 0:
            negative_seed_scores.append(pair.score)

    if len(negative_seed_scores) > 400:
        random.seed(42)
        negative_seed_scores = random.sample(negative_seed_scores, 400)

    if positive_seed_scores and negative_seed_scores:
        pos_p25 = _percentile(positive_seed_scores, 25)
        neg_p95 = _percentile(negative_seed_scores, 95)
        duplicate_threshold = _clip(max(pos_p25, neg_p95 + 0.08), 0.70, 0.95)
        similar_threshold = _clip(max(neg_p95 + 0.02, duplicate_threshold - 0.18), 0.45, duplicate_threshold - 0.05)
    else:
        duplicate_threshold = 0.80
        similar_threshold = 0.60

    return ThresholdCalibration(
        duplicate_threshold=round(float(duplicate_threshold), 2),
        similar_threshold=round(float(similar_threshold), 2),
        positive_seed_count=len(positive_seed_scores),
        negative_seed_count=len(negative_seed_scores),
        positive_score_mean=round(statistics.mean(positive_seed_scores), 4) if positive_seed_scores else 0.0,
        negative_score_mean=round(statistics.mean(negative_seed_scores), 4) if negative_seed_scores else 0.0,
        histogram=_build_histogram(pair_scores),
    )


def _connected_components(nodes: list[str], edges: list[tuple[str, str]]) -> list[list[str]]:
    adjacency: dict[str, set[str]] = {node: set() for node in nodes}
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)

    visited: set[str] = set()
    components: list[list[str]] = []
    for node in nodes:
        if node in visited:
            continue
        stack = [node]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            stack.extend(adjacency[current] - visited)
        components.append(sorted(component))
    return components


def _classify_cluster(
    component: list[str],
    pair_lookup: dict[tuple[str, str], EndpointPair],
    endpoints_by_id: dict[str, EndpointRecord],
    endpoint_analysis: dict[str, EndpointAnalysis],
    thresholds: ThresholdCalibration,
) -> EndpointCluster:
    sorted_component = sorted(component)
    methods = {endpoints_by_id[endpoint_id].method for endpoint_id in sorted_component}
    canonical_paths = sorted({endpoints_by_id[endpoint_id].canonical_path for endpoint_id in sorted_component})
    sink_categories = sorted(
        {
            sink
            for endpoint_id in sorted_component
            for sink in endpoint_analysis[endpoint_id].sink_categories
        }
    )

    cluster_type = "unique"
    if len(sorted_component) > 1:
        pair_scores: list[float] = []
        for index, left_id in enumerate(sorted_component):
            for right_id in sorted_component[index + 1 :]:
                key = tuple(sorted((left_id, right_id)))
                pair = pair_lookup.get(key)
                if pair is not None:
                    pair_scores.append(pair.score)
        min_score = min(pair_scores) if pair_scores else 0.0
        all_same_method = len(methods) == 1
        all_same_canonical_path = len(canonical_paths) == 1
        if all_same_method and all_same_canonical_path and min_score >= thresholds.duplicate_threshold:
            cluster_type = "duplicate_equivalent"
        else:
            cluster_type = "similar_family"

    risk_level = "low"
    if cluster_type == "duplicate_equivalent":
        services = {endpoints_by_id[endpoint_id].service for endpoint_id in sorted_component}
        versions = {endpoints_by_id[endpoint_id].version for endpoint_id in sorted_component}
        method = endpoints_by_id[sorted_component[0]].method
        if ("v1" in versions and "v2" in versions and method in WRITE_METHODS) or len(services) > 1:
            risk_level = "high"
        elif "v1" in versions and "v2" in versions:
            risk_level = "medium"
        else:
            risk_level = "medium"
    elif cluster_type == "similar_family":
        versions = {endpoints_by_id[endpoint_id].version for endpoint_id in sorted_component}
        methods_set = {endpoints_by_id[endpoint_id].method for endpoint_id in sorted_component}
        if "v1" in versions and "v2" in versions and methods_set & WRITE_METHODS:
            risk_level = "high"
        elif "v1" in versions and "v2" in versions:
            risk_level = "medium"

    cluster_id = f"cluster-{abs(hash('|'.join(sorted_component))) % 10_000_000:07d}"
    return EndpointCluster(
        cluster_id=cluster_id,
        cluster_type=cluster_type,
        endpoint_ids=sorted_component,
        representative_method=sorted(methods)[0] if methods else "UNKNOWN",
        canonical_paths=canonical_paths,
        sink_categories=sink_categories,
        risk_level=risk_level,
    )


def _classify_endpoint_verdict(
    endpoint: EndpointRecord,
    component: list[str],
    endpoints_by_id: dict[str, EndpointRecord],
) -> dict[str, Any]:
    peers = [endpoints_by_id[peer_id] for peer_id in component if peer_id != endpoint.endpoint_id]
    has_v2_peer = any(peer.version == "v2" for peer in peers)
    has_cross_service_same_path = any(
        peer.service != endpoint.service and peer.method == endpoint.method and peer.canonical_path == endpoint.canonical_path
        for peer in peers
    )
    has_oms_peer_same_path = any(peer.service == "oms" and peer.canonical_path == endpoint.canonical_path for peer in peers)

    architecture_role = "primary"
    if endpoint.service == "bff" and has_oms_peer_same_path:
        architecture_role = "proxy_mirror"

    verdict = "other"
    if not endpoint.include_in_schema:
        verdict = "hidden_internal"
    elif endpoint.version == "v2":
        verdict = "v2_foundry" if endpoint.domain in FOUNDRY_V2_DOMAINS else "v2_non_foundry"
    elif endpoint.version == "v1":
        if has_v2_peer:
            verdict = "v1_compat_legacy_candidate"
        elif endpoint.domain in INTERNAL_V1_DOMAINS:
            verdict = "v1_internal"
        else:
            verdict = "v1_active_no_v2"
    elif endpoint.version == "api-other":
        verdict = "api_other"
    else:
        verdict = "non_api"

    risk = "low"
    if verdict == "v1_compat_legacy_candidate" and endpoint.method in WRITE_METHODS:
        risk = "high"
    elif verdict == "v1_compat_legacy_candidate":
        risk = "medium"
    elif has_cross_service_same_path:
        risk = "medium"
    if architecture_role == "proxy_mirror":
        risk = max(risk, "medium", key=lambda item: {"low": 0, "medium": 1, "high": 2}[item])

    evidence = {
        "has_v2_peer": has_v2_peer,
        "has_cross_service_same_path": has_cross_service_same_path,
        "architecture_role": architecture_role,
    }
    return {
        "legacy_verdict": verdict,
        "risk_level": risk,
        "architecture_role": architecture_role,
        "evidence": evidence,
    }


def analyze_inventory(collection: InventoryCollection) -> AnalysisBundle:
    endpoints = collection.endpoints
    endpoints_by_id = {endpoint.endpoint_id: endpoint for endpoint in endpoints}

    endpoint_analysis: dict[str, EndpointAnalysis] = {}
    for endpoint in endpoints:
        endpoint_analysis[endpoint.endpoint_id] = _endpoint_flow_analysis(
            endpoint.endpoint_id,
            collection.endpoint_callables.get(endpoint.endpoint_id),
        )

    endpoint_pairs: list[EndpointPair] = []
    pair_lookup: dict[tuple[str, str], EndpointPair] = {}
    for left_index, left in enumerate(endpoints):
        for right in endpoints[left_index + 1 :]:
            pair = _pair_similarity(
                left,
                right,
                endpoint_analysis[left.endpoint_id],
                endpoint_analysis[right.endpoint_id],
            )
            endpoint_pairs.append(pair)
            pair_lookup[tuple(sorted((pair.left_endpoint_id, pair.right_endpoint_id)))] = pair

    thresholds = _calibrate_thresholds(endpoint_pairs, endpoints_by_id)

    similar_edges: list[tuple[str, str]] = []
    for pair in endpoint_pairs:
        if pair.score >= thresholds.similar_threshold:
            similar_edges.append((pair.left_endpoint_id, pair.right_endpoint_id))

    components = _connected_components([endpoint.endpoint_id for endpoint in endpoints], similar_edges)
    clusters = [
        _classify_cluster(
            component,
            pair_lookup,
            endpoints_by_id,
            endpoint_analysis,
            thresholds,
        )
        for component in components
    ]
    clusters.sort(key=lambda cluster: (cluster.cluster_type, cluster.risk_level, cluster.cluster_id))
    endpoint_cluster_map = {
        endpoint_id: cluster.cluster_id
        for cluster in clusters
        for endpoint_id in cluster.endpoint_ids
    }

    cluster_members = {cluster.cluster_id: cluster.endpoint_ids for cluster in clusters}
    endpoint_verdicts: dict[str, dict[str, Any]] = {}
    for endpoint in endpoints:
        cluster_id = endpoint_cluster_map.get(endpoint.endpoint_id)
        component = cluster_members.get(cluster_id, [endpoint.endpoint_id]) if cluster_id else [endpoint.endpoint_id]
        endpoint_verdicts[endpoint.endpoint_id] = _classify_endpoint_verdict(endpoint, component, endpoints_by_id)

    no_sink_endpoints = [analysis.endpoint_id for analysis in endpoint_analysis.values() if not analysis.sink_categories]
    sink_missing_reason = [
        analysis.endpoint_id
        for analysis in endpoint_analysis.values()
        if not analysis.sink_categories and not analysis.no_sink_reason
    ]
    hidden_unclassified = [
        endpoint.endpoint_id
        for endpoint in endpoints
        if not endpoint.include_in_schema and endpoint_verdicts.get(endpoint.endpoint_id, {}).get("legacy_verdict") == "other"
    ]
    legacy_unclassified = [
        endpoint.endpoint_id
        for endpoint in endpoints
        if endpoint_verdicts.get(endpoint.endpoint_id, {}).get("legacy_verdict") == "other"
    ]
    coverage = {
        "endpoint_count": len(endpoints),
        "pair_count": len(endpoint_pairs),
        "cluster_count": len(clusters),
        "no_sink_count": len(no_sink_endpoints),
        "no_sink_endpoint_ids": no_sink_endpoints,
        "sink_missing_reason_count": len(sink_missing_reason),
        "sink_missing_reason_endpoint_ids": sink_missing_reason,
        "hidden_unclassified_count": len(hidden_unclassified),
        "hidden_unclassified_endpoint_ids": hidden_unclassified,
        "legacy_unclassified_count": len(legacy_unclassified),
        "legacy_unclassified_endpoint_ids": legacy_unclassified,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    return AnalysisBundle(
        endpoint_analysis=endpoint_analysis,
        endpoint_pairs=endpoint_pairs,
        clusters=clusters,
        endpoint_cluster_map=endpoint_cluster_map,
        endpoint_verdicts=endpoint_verdicts,
        thresholds=thresholds,
        coverage=coverage,
    )


def _service_base_url(service: str) -> str:
    cfg = SERVICE_CONFIG[service]
    return (os.getenv(cfg["base_url_env"]) or cfg["default_base_url"]).strip().rstrip("/")


def _replace_path_param(param_name: str) -> str:
    lowered = param_name.lower()
    if "db" in lowered or "database" in lowered or "ontology" in lowered:
        return "audit_db"
    if "branch" in lowered:
        return "main"
    if "objecttype" in lowered:
        return "Order"
    if "actiontype" in lowered:
        return "EscalateOrder"
    if lowered.endswith("id") or "rid" in lowered or "uuid" in lowered:
        return "00000000-0000-0000-0000-000000000000"
    return "sample"


def _materialize_path(path_template: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        return _replace_path_param(match.group(1))

    return re.sub(r"\{([^}]+)\}", _replace, path_template)


def _request_headers() -> dict[str, str]:
    admin_token = (os.getenv("ADMIN_TOKEN") or "change_me").strip() or "change_me"
    headers = {
        "X-Admin-Token": admin_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    bearer = (os.getenv("SMOKE_USER_BEARER") or os.getenv("USER_BEARER_TOKEN") or "").strip()
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    return headers


def _status_matches_contract(status: int, expected: list[str]) -> bool:
    if not expected:
        return status < 500
    status_str = str(status)
    if status_str in expected:
        return True
    if "default" in {value.lower() for value in expected}:
        return True
    classes = {value.upper() for value in expected if len(value) == 3 and value.endswith("XX")}
    return f"{status // 100}XX" in classes


_FALLBACK_4XX_ACCEPT = {400, 401, 403, 404, 405, 409, 410, 412, 415, 422, 429}


def _evaluate_status(status: int, expected: list[str]) -> tuple[bool, bool, str]:
    strict_match = _status_matches_contract(status, expected)
    fallback_match = (400 <= status < 500) and (status in _FALLBACK_4XX_ACCEPT)
    if strict_match:
        return True, False, "openapi_contract"
    if fallback_match:
        return False, True, "fallback_4xx"
    return False, False, "contract_mismatch"


def _check_service_health(services: list[str], timeout: float) -> dict[str, Any]:
    health: dict[str, Any] = {}
    headers = _request_headers()
    for service in services:
        url = f"{_service_base_url(service)}/openapi.json"
        request = urllib.request.Request(url=url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                status = int(response.status)
            health[service] = {"reachable": status == 200, "status_code": status}
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            health[service] = {"reachable": status == 200, "status_code": status, "error": f"HTTPError: {status}"}
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            health[service] = {"reachable": False, "error": f"{type(exc).__name__}: {exc}"}
    return health


def _risk_rank(value: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(value.lower(), 0)


def _pick_runtime_targets(
    inventory_payload: dict[str, Any],
    *,
    risk_filter: str,
) -> list[dict[str, Any]]:
    endpoint_rows = inventory_payload.get("endpoints", [])
    if not isinstance(endpoint_rows, list):
        return []
    risk_minimum = _risk_rank(risk_filter)

    selected: list[dict[str, Any]] = []
    for row in endpoint_rows:
        if not isinstance(row, dict):
            continue
        risk = str(row.get("risk_level") or "low").lower()
        if _risk_rank(risk) < risk_minimum:
            continue
        if not bool(row.get("include_in_schema", True)):
            continue
        selected.append(row)
    selected.sort(key=lambda row: (str(row.get("service")), str(row.get("path")), str(row.get("method"))))
    return selected


def run_runtime_validation(
    inventory_payload: dict[str, Any],
    *,
    risk_filter: str = "high",
    timeout_seconds: float = 5.0,
) -> RuntimeValidationReport:
    services = sorted({str(row.get("service")) for row in inventory_payload.get("endpoints", []) if isinstance(row, dict)})
    service_health = _check_service_health(services, timeout_seconds)
    unreachable = [service for service, status in service_health.items() if not bool(status.get("reachable"))]

    if unreachable:
        return RuntimeValidationReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            mode="phase2",
            risk_filter=risk_filter,
            pending_reason=f"runtime stack not fully reachable: {', '.join(unreachable)}",
            service_health=service_health,
            selected_target_count=0,
            checks=[],
        )

    headers = _request_headers()
    targets = _pick_runtime_targets(inventory_payload, risk_filter=risk_filter)
    checks: list[RuntimeCheckResult] = []

    for row in targets:
        service = str(row.get("service"))
        method = str(row.get("method") or "GET").upper()
        path = str(row.get("path") or "")
        expected_statuses = [str(code) for code in (row.get("documented_statuses") or [])]
        url = f"{_service_base_url(service)}{_materialize_path(path)}"

        payload_bytes = b"{}" if method in WRITE_METHODS else None
        request = urllib.request.Request(url=url, method=method, headers=headers, data=payload_bytes)

        status_code = 0
        body_error: str | None = None
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
                status_code = int(response.status)
        except urllib.error.HTTPError as http_error:
            status_code = int(http_error.code)
            try:
                body_error = http_error.read().decode("utf-8", errors="ignore")[:500]
            except Exception:
                body_error = None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            checks.append(
                RuntimeCheckResult(
                    endpoint_id=str(row.get("endpoint_id")),
                    service=service,
                    method=method,
                    path=path,
                    url=url,
                    status_code=0,
                    expected_statuses=expected_statuses,
                    contract_match=False,
                    fallback_4xx_match=False,
                    evaluation_mode="request_error",
                    passed=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
            continue

        contract_match, fallback_4xx_match, evaluation_mode = _evaluate_status(status_code, expected_statuses)
        passed = status_code < 500 and (contract_match or fallback_4xx_match)
        checks.append(
            RuntimeCheckResult(
                endpoint_id=str(row.get("endpoint_id")),
                service=service,
                method=method,
                path=path,
                url=url,
                status_code=status_code,
                expected_statuses=expected_statuses,
                contract_match=contract_match,
                fallback_4xx_match=fallback_4xx_match,
                evaluation_mode=evaluation_mode,
                passed=passed,
                error=body_error if (not passed and body_error) else None,
            )
        )

    return RuntimeValidationReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        mode="phase2",
        risk_filter=risk_filter,
        pending_reason=None,
        service_health=service_health,
        selected_target_count=len(targets),
        checks=checks,
    )


def merge_phase1_results(
    collection: InventoryCollection,
    analysis: AnalysisBundle,
) -> dict[str, Any]:
    risk_rank = {"low": 0, "medium": 1, "high": 2}
    clusters_by_id = {cluster.cluster_id: cluster for cluster in analysis.clusters}
    endpoint_rows: list[dict[str, Any]] = []
    for endpoint in collection.endpoints:
        endpoint_row = endpoint.to_dict()
        endpoint_row["sink_categories"] = analysis.endpoint_analysis[endpoint.endpoint_id].sink_categories
        endpoint_row["sink_evidence"] = analysis.endpoint_analysis[endpoint.endpoint_id].sink_evidence
        endpoint_row["level1_calls"] = analysis.endpoint_analysis[endpoint.endpoint_id].level1_calls
        endpoint_row["level2_calls"] = analysis.endpoint_analysis[endpoint.endpoint_id].level2_calls
        endpoint_row["no_sink_reason"] = analysis.endpoint_analysis[endpoint.endpoint_id].no_sink_reason
        cluster_id = analysis.endpoint_cluster_map.get(endpoint.endpoint_id)
        endpoint_row["cluster_id"] = cluster_id
        endpoint_row.update(analysis.endpoint_verdicts.get(endpoint.endpoint_id, {}))
        cluster = clusters_by_id.get(cluster_id or "")
        if cluster is not None:
            endpoint_row["cluster_type"] = cluster.cluster_type
            endpoint_row["cluster_risk_level"] = cluster.risk_level
            current_risk = str(endpoint_row.get("risk_level") or "low").lower()
            if risk_rank.get(cluster.risk_level, 0) > risk_rank.get(current_risk, 0):
                endpoint_row["risk_level"] = cluster.risk_level
        endpoint_rows.append(endpoint_row)

    cluster_rows = [cluster.to_dict() for cluster in analysis.clusters]
    pair_rows = [pair.to_dict() for pair in analysis.endpoint_pairs]
    pair_rows.sort(key=lambda row: row["score"], reverse=True)

    summary = {
        "endpoint_count": len(endpoint_rows),
        "cluster_count": len(cluster_rows),
        "duplicate_equivalent_clusters": sum(1 for row in cluster_rows if row["cluster_type"] == "duplicate_equivalent"),
        "similar_family_clusters": sum(1 for row in cluster_rows if row["cluster_type"] == "similar_family"),
        "legacy_candidate_count": sum(
            1 for row in endpoint_rows if row.get("legacy_verdict") == "v1_compat_legacy_candidate"
        ),
        "hidden_route_count": sum(1 for row in endpoint_rows if not bool(row.get("include_in_schema", True))),
        "high_risk_count": sum(1 for row in endpoint_rows if str(row.get("risk_level")) == "high"),
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "phase1",
        "summary": summary,
        "coverage": analysis.coverage,
        "thresholds": analysis.thresholds.to_dict(),
        "service_diffs": {name: diff.to_dict() for name, diff in collection.service_diffs.items()},
        "route_stats": {name: stats.to_dict() for name, stats in collection.route_stats.items()},
        "endpoints": endpoint_rows,
        "clusters": cluster_rows,
        "top_pairs": pair_rows[:400],
    }
    return payload


def attach_phase2_results(payload: dict[str, Any], runtime_report: RuntimeValidationReport) -> dict[str, Any]:
    updated = dict(payload)
    updated["phase"] = "phase2"
    updated["phase2"] = runtime_report.to_dict()
    return updated
