"""
Lineage (provenance) query router for BFF.

Exposes first-class, queryable lineage graphs to the frontend via BFF.
"""

from shared.observability.tracing import trace_endpoint

from collections import deque
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Depends, Query, status
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.registry_deps import get_objectify_registry
from bff.services.lineage_out_of_date_service import LineageOutOfDateService
from shared.dependencies.providers import LineageStoreDep
from shared.dependencies.providers import AuditLogStoreDep
from shared.models.lineage_edge_types import EDGE_AGGREGATE_EMITTED_EVENT
from shared.models.lineage import LineageDirection
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.services.registries.objectify_registry import ObjectifyRegistry

router = APIRouter(prefix="/lineage", tags=["Lineage"])

# TODO(향후 개발): 셀/필드 단위 provenance(end-to-end)
# 현재 Lineage API는 "event/aggregate/artifact" 수준의 그래프(포인터) 추적에 초점을 둡니다.
# 실무에서 "이 값이 엑셀의 몇 행/몇 열에서 왔는지"까지 증명하려면, 그래프를 셀 단위로 키우지 말고:
# - S3에 per-document provenance blob 저장 (예: provenance/<db>/<index>/<doc_id>/<event_id>.json.gz)
# - ES 문서에는 provenance_ref(S3 key/etag 등)만 저장
# - Lineage에는 포인터 엣지만 기록: event -> artifact:s3:provenance_blob -> artifact:es:<index>/<doc_id>
# - 구조 분석(crop/pivot/merged fill) 이후에도 cell 좌표가 유지되도록 import 파이프라인에서 좌표 매핑을 carry
# 이렇게 하면 그래프 폭발 없이 "원천까지" 추적 + 감사(Audit) 근거 제시가 가능합니다.


def _parse_artifact_node_id(node_id: str) -> Tuple[Optional[str], str]:
    """
    Parse artifact node id: artifact:<kind>:<...>

    Returns (kind, remainder). If not an artifact node, kind is None.
    """
    if not isinstance(node_id, str):
        return None, ""
    if not node_id.startswith("artifact:"):
        return None, node_id
    parts = node_id.split(":", 2)
    if len(parts) < 2:
        return None, node_id
    kind = parts[1] if len(parts) >= 2 else None
    remainder = parts[2] if len(parts) >= 3 else ""
    return kind, remainder


def _suggest_remediation_actions(*, artifacts: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """
    Recommend safe operational actions.

    Event-sourcing systems typically prefer "rebuild/replay" over "delete".
    This returns a plan-like set of suggestions without executing anything.
    """
    actions: list[Dict[str, Any]] = []
    for art in artifacts:
        kind = art.get("kind")
        node_id = art.get("node_id")
        label = art.get("label")

        if kind == "es":
            actions.append(
                {
                    "action": "REBUILD_PROJECTION",
                    "target_kind": "es",
                    "target": node_id,
                    "rationale": "Prefer projection rebuild over ad-hoc deletes to avoid consistency gaps",
                }
            )
        elif kind == "graph":
            actions.append(
                {
                    "action": "REPLAY_TO_TARGET",
                    "target_kind": "graph",
                    "target": node_id,
                    "rationale": "Prefer reset/re-materialize from event store over manual graph edits",
                }
            )
        elif kind == "s3":
            actions.append(
                {
                    "action": "REPLAY_OR_VERIFY",
                    "target_kind": "s3",
                    "target": label or node_id,
                    "rationale": "Rebuild or verify derived blobs via replay; treat S3 objects as reproducible artifacts",
                }
            )
        else:
            actions.append(
                {
                    "action": "INSPECT",
                    "target_kind": kind,
                    "target": label or node_id,
                    "rationale": "Unknown artifact kind; inspect before taking action",
                }
            )
    return actions


def _edge_projection_name(edge: Any) -> str:
    metadata = getattr(edge, "metadata", None)
    if isinstance(metadata, dict):
        projection_name = metadata.get("projection_name")
        if projection_name:
            return str(projection_name)
    return ""


def _edge_signature(edge: Any) -> str:
    return "|".join(
        [
            str(getattr(edge, "from_node_id", "")),
            str(getattr(edge, "to_node_id", "")),
            str(getattr(edge, "edge_type", "")),
            _edge_projection_name(edge),
        ]
    )


def _count_artifact_kinds(nodes: Sequence[Any]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for node in nodes:
        if getattr(node, "node_type", None) != "artifact":
            continue
        kind, _ = _parse_artifact_node_id(str(getattr(node, "node_id", "")))
        key = str(kind or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _find_shortest_path(
    *,
    graph: Any,
    source: str,
    target: str,
    direction: LineageDirection,
) -> Tuple[List[str], List[Tuple[Any, str]]]:
    if source == target:
        return [source], []

    adjacency: Dict[str, List[Tuple[str, Any, str]]] = {}
    for edge in graph.edges:
        if direction in {"downstream", "both"}:
            adjacency.setdefault(edge.from_node_id, []).append((edge.to_node_id, edge, "forward"))
        if direction in {"upstream", "both"}:
            adjacency.setdefault(edge.to_node_id, []).append((edge.from_node_id, edge, "reverse"))

    queue: deque[str] = deque([source])
    parents: Dict[str, Optional[Tuple[str, Any, str]]] = {source: None}

    while queue:
        current = queue.popleft()
        for next_node, edge, traversal in adjacency.get(current, []):
            if next_node in parents:
                continue
            parents[next_node] = (current, edge, traversal)
            if next_node == target:
                queue.clear()
                break
            queue.append(next_node)

    if target not in parents:
        return [], []

    path_nodes: List[str] = []
    path_edges: List[Tuple[Any, str]] = []
    cursor: Optional[str] = target
    while cursor is not None:
        path_nodes.append(cursor)
        parent = parents.get(cursor)
        if parent is None:
            break
        parent_node, edge, traversal = parent
        path_edges.append((edge, traversal))
        cursor = parent_node

    path_nodes.reverse()
    path_edges.reverse()
    return path_nodes, path_edges


def _to_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _bucket_start(ts: datetime, *, bucket_minutes: int) -> datetime:
    ts_utc = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    bucket_seconds = int(bucket_minutes) * 60
    epoch_seconds = int(ts_utc.timestamp())
    floored = (epoch_seconds // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def _compact_edge_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    keep_keys = (
        "db_name",
        "branch",
        "producer_service",
        "producer_run_id",
        "producer_code_sha",
        "run_id",
        "code_sha",
        "schema_version",
        "ontology",
        "projection_name",
        "column_lineage_ref",
        "column_lineage_storage",
        "column_lineage_schema_version",
        "provenance_ref",
        "provenance_storage",
        "provenance_schema_version",
    )
    compact: Dict[str, Any] = {}
    for key in keep_keys:
        value = metadata.get(key)
        if value is not None:
            compact[key] = value
    return compact


def _build_timeline_summary(
    *,
    edges: Sequence[Dict[str, Any]],
    bucket_minutes: int,
) -> Dict[str, Any]:
    edge_type_counts: Dict[str, int] = {}
    projection_counts: Dict[str, int] = {}
    producer_service_counts: Dict[str, int] = {}
    buckets: Dict[datetime, Dict[str, Any]] = {}

    for edge in edges:
        edge_type = str(edge.get("edge_type") or "unknown")
        edge_type_counts[edge_type] = edge_type_counts.get(edge_type, 0) + 1

        projection_name = str(edge.get("projection_name") or "")
        if projection_name:
            projection_counts[projection_name] = projection_counts.get(projection_name, 0) + 1

        metadata = edge.get("metadata")
        if isinstance(metadata, dict):
            producer_service = metadata.get("producer_service")
            if producer_service:
                key = str(producer_service)
                producer_service_counts[key] = producer_service_counts.get(key, 0) + 1

        occurred_at = _to_utc_datetime(edge.get("occurred_at"))
        if occurred_at is None:
            continue
        b_start = _bucket_start(occurred_at, bucket_minutes=bucket_minutes)
        bucket = buckets.get(b_start)
        if bucket is None:
            bucket = {
                "bucket_start": b_start.isoformat(),
                "edge_count": 0,
                "edge_type_counts": {},
            }
            buckets[b_start] = bucket
        bucket["edge_count"] = int(bucket["edge_count"]) + 1
        bucket_edge_counts = bucket["edge_type_counts"]
        bucket_edge_counts[edge_type] = int(bucket_edge_counts.get(edge_type, 0)) + 1

    sorted_buckets = [buckets[k] for k in sorted(buckets.keys())]
    bucket_counts = [int(b["edge_count"]) for b in sorted_buckets]
    baseline = float(median(bucket_counts)) if bucket_counts else 0.0
    threshold = max(5.0, baseline * 3.0)
    spikes: List[Dict[str, Any]] = []
    for bucket in sorted_buckets:
        edge_count = int(bucket["edge_count"])
        if edge_count >= threshold:
            spikes.append(
                {
                    "bucket_start": bucket["bucket_start"],
                    "edge_count": edge_count,
                    "baseline": baseline,
                    "threshold": threshold,
                }
            )

    return {
        "edge_type_counts": dict(sorted(edge_type_counts.items(), key=lambda item: item[1], reverse=True)),
        "projection_counts": dict(sorted(projection_counts.items(), key=lambda item: item[1], reverse=True)),
        "producer_service_counts": dict(sorted(producer_service_counts.items(), key=lambda item: item[1], reverse=True)),
        "buckets": sorted_buckets,
        "spikes": spikes,
        "total_edges": len(edges),
    }


def _extract_column_lineage_refs(
    *,
    edges: Sequence[Dict[str, Any]],
    limit: int = 200,
) -> Tuple[List[Dict[str, Any]], int]:
    refs: Dict[str, Dict[str, Any]] = {}
    for edge in edges:
        metadata = edge.get("metadata") if isinstance(edge.get("metadata"), dict) else {}
        ref = metadata.get("column_lineage_ref") or metadata.get("provenance_ref")
        if not isinstance(ref, str) or not ref.strip():
            continue
        key = ref.strip()
        occurred_at = _to_utc_datetime(edge.get("occurred_at"))
        current = refs.get(key)
        if current is None:
            refs[key] = {
                "ref": key,
                "storage": metadata.get("column_lineage_storage") or metadata.get("provenance_storage"),
                "schema_version": metadata.get("column_lineage_schema_version")
                or metadata.get("provenance_schema_version"),
                "latest_occurred_at": occurred_at.isoformat() if occurred_at else None,
                "edge_count": 1,
            }
            continue

        current["edge_count"] = int(current.get("edge_count") or 0) + 1
        current_ts = _to_utc_datetime(current.get("latest_occurred_at"))
        if current_ts is None or (occurred_at and occurred_at > current_ts):
            current["latest_occurred_at"] = occurred_at.isoformat() if occurred_at else None

    values = list(refs.values())
    values.sort(key=lambda item: str(item.get("latest_occurred_at") or ""), reverse=True)
    return values[: max(1, int(limit))], len(refs)


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_objectify_mapping_spec_ref(ref: str) -> Tuple[str, Optional[int]]:
    raw = str(ref or "").strip()
    if not raw:
        return "", None
    prefix = "objectify_mapping_spec:"
    if not raw.startswith(prefix):
        return raw, None
    payload = raw[len(prefix) :].strip()
    if not payload:
        return "", None
    spec_id, sep, version_token = payload.rpartition(":v")
    if sep and spec_id:
        return spec_id.strip(), _coerce_int(version_token.strip())
    return payload, None


def _extract_column_lineage_ref_entry(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(metadata, dict):
        return None

    raw_ref = metadata.get("column_lineage_ref") or metadata.get("provenance_ref")
    spec_id_raw = metadata.get("mapping_spec_id")
    spec_version_raw = metadata.get("mapping_spec_version")

    spec_id: Optional[str] = None
    spec_version: Optional[int] = None
    ref: Optional[str] = None

    if isinstance(raw_ref, str) and raw_ref.strip():
        ref = raw_ref.strip()
        parsed_spec_id, parsed_spec_version = _parse_objectify_mapping_spec_ref(ref)
        if parsed_spec_id:
            spec_id = parsed_spec_id
            spec_version = parsed_spec_version

    if spec_id is None and isinstance(spec_id_raw, str) and spec_id_raw.strip():
        spec_id = spec_id_raw.strip()
    if spec_version is None:
        spec_version = _coerce_int(spec_version_raw)

    if not spec_id:
        return None
    if ref is None:
        if spec_version is None:
            ref = f"objectify_mapping_spec:{spec_id}"
        else:
            ref = f"objectify_mapping_spec:{spec_id}:v{spec_version}"

    target_class_id = metadata.get("target_class_id")
    return {
        "ref": ref,
        "mapping_spec_id": spec_id,
        "mapping_spec_version": spec_version,
        "target_class_id": str(target_class_id).strip() if isinstance(target_class_id, str) and target_class_id.strip() else None,
        "storage": metadata.get("column_lineage_storage") or metadata.get("provenance_storage"),
        "schema_version": metadata.get("column_lineage_schema_version") or metadata.get("provenance_schema_version"),
    }


def _normalize_mapping_pair(mapping: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    if not isinstance(mapping, dict):
        return None
    source_field = mapping.get("source_field")
    if source_field is None:
        source_field = mapping.get("sourceField")
    target_field = mapping.get("target_field")
    if target_field is None:
        target_field = mapping.get("targetField")
    source = str(source_field or "").strip()
    target = str(target_field or "").strip()
    if not source or not target:
        return None
    return source, target


def _extract_column_lineage_pairs_from_metadata(metadata: Dict[str, Any]) -> List[Tuple[str, str]]:
    if not isinstance(metadata, dict):
        return []
    raw_pairs = metadata.get("column_lineage_pairs")
    if not isinstance(raw_pairs, list):
        return []
    pairs: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for raw_pair in raw_pairs:
        pair = _normalize_mapping_pair(raw_pair if isinstance(raw_pair, dict) else {})
        if pair is None:
            continue
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def _freshness_status(
    *,
    last_occurred_at: Optional[datetime],
    as_of: datetime,
    freshness_slo_minutes: int,
) -> Tuple[str, Optional[float]]:
    if last_occurred_at is None:
        return "no_data", None

    observed = last_occurred_at if last_occurred_at.tzinfo else last_occurred_at.replace(tzinfo=timezone.utc)
    age_minutes = max(0.0, (as_of - observed).total_seconds() / 60.0)
    if age_minutes <= float(freshness_slo_minutes):
        return "healthy", age_minutes
    if age_minutes <= float(freshness_slo_minutes) * 3.0:
        return "warning", age_minutes
    return "critical", age_minutes


def _status_rank(status: str) -> int:
    order = {"critical": 0, "warning": 1, "no_data": 2, "healthy": 3}
    return order.get(status, 4)


def _normalize_scope(
    *,
    db_name: Optional[str],
    branch: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    normalized_db = validate_db_name(db_name) if db_name else None
    normalized_branch = validate_branch_name(branch) if branch else None
    return normalized_db, normalized_branch


def _normalize_window(
    *,
    since: Optional[datetime],
    until: Optional[datetime],
    default_hours: int,
) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    since_ts = since or (now - timedelta(hours=int(default_hours)))
    until_ts = until or now
    since_ts = since_ts if since_ts.tzinfo else since_ts.replace(tzinfo=timezone.utc)
    until_ts = until_ts if until_ts.tzinfo else until_ts.replace(tzinfo=timezone.utc)
    if since_ts > until_ts:
        raise ValueError("since must be less than or equal to until")
    return since_ts, until_ts


def _extract_impacted_artifacts_from_edges(
    *,
    edges: Sequence[Dict[str, Any]],
    limit: int,
) -> Tuple[List[Dict[str, Any]], int]:
    impacted_artifacts_map: Dict[str, Dict[str, Any]] = {}
    for edge in edges:
        to_node_id = str(edge.get("to_node_id") or "")
        if not to_node_id.startswith("artifact:"):
            continue
        kind, _ = _parse_artifact_node_id(to_node_id)
        occurred_at = _to_utc_datetime(edge.get("occurred_at"))
        current = impacted_artifacts_map.get(to_node_id)
        candidate = {
            "node_id": to_node_id,
            "kind": kind,
            "label": None,
            "last_occurred_at": occurred_at.isoformat() if occurred_at else None,
        }
        if current is None:
            impacted_artifacts_map[to_node_id] = candidate
            continue
        current_ts = _to_utc_datetime(current.get("last_occurred_at"))
        if (current_ts is None) or (occurred_at and occurred_at > current_ts):
            impacted_artifacts_map[to_node_id] = candidate

    impacted_artifacts = list(impacted_artifacts_map.values())
    impacted_artifacts.sort(key=lambda item: str(item.get("last_occurred_at") or ""), reverse=True)
    return impacted_artifacts[:max(1, int(limit))], len(impacted_artifacts_map)


def _summarize_run_rows(
    *,
    run_rows: Sequence[Dict[str, Any]],
    as_of: datetime,
    freshness_slo_minutes: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    runs: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {"healthy": 0, "warning": 0, "critical": 0, "no_data": 0}
    for row in run_rows:
        last_occurred_at = _to_utc_datetime(row.get("last_occurred_at"))
        first_occurred_at = _to_utc_datetime(row.get("first_occurred_at"))
        status_name, age_minutes = _freshness_status(
            last_occurred_at=last_occurred_at,
            as_of=as_of,
            freshness_slo_minutes=int(freshness_slo_minutes),
        )
        counts[status_name] = counts.get(status_name, 0) + 1
        runs.append(
            {
                "run_id": row.get("run_id"),
                "status": status_name,
                "age_minutes": age_minutes,
                "first_occurred_at": first_occurred_at.isoformat() if first_occurred_at else None,
                "last_occurred_at": last_occurred_at.isoformat() if last_occurred_at else None,
                "edge_count": int(row.get("edge_count") or 0),
                "impacted_artifact_count": int(row.get("impacted_artifact_count") or 0),
                "impacted_projection_count": int(row.get("impacted_projection_count") or 0),
            }
        )
    runs.sort(
        key=lambda item: (
            _status_rank(str(item.get("status"))),
            -float(item.get("age_minutes") or 0.0),
            str(item.get("run_id") or ""),
        )
    )
    return runs, counts


def _edge_cause_payload(edge: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(edge, dict):
        return None
    metadata = edge.get("metadata") if isinstance(edge.get("metadata"), dict) else {}
    occurred_at = _to_utc_datetime(edge.get("occurred_at"))
    run_id = edge.get("run_id") or metadata.get("producer_run_id") or metadata.get("run_id")
    code_sha = edge.get("code_sha") or metadata.get("producer_code_sha") or metadata.get("code_sha")
    return {
        "event_id": edge.get("event_id"),
        "from_node_id": edge.get("from_node_id"),
        "edge_type": edge.get("edge_type"),
        "projection_name": edge.get("projection_name"),
        "occurred_at": occurred_at.isoformat() if occurred_at else None,
        "run_id": str(run_id) if run_id is not None else None,
        "producer_service": metadata.get("producer_service"),
        "producer_code_sha": str(code_sha) if code_sha is not None else None,
    }


def _upstream_gap_minutes(
    *,
    last_occurred_at: Optional[datetime],
    latest_upstream_occurred_at: Optional[datetime],
) -> Optional[float]:
    if last_occurred_at is None or latest_upstream_occurred_at is None:
        return None
    observed = last_occurred_at if last_occurred_at.tzinfo else last_occurred_at.replace(tzinfo=timezone.utc)
    upstream = (
        latest_upstream_occurred_at
        if latest_upstream_occurred_at.tzinfo
        else latest_upstream_occurred_at.replace(tzinfo=timezone.utc)
    )
    if upstream <= observed:
        return 0.0
    return max(0.0, (upstream - observed).total_seconds() / 60.0)


def _event_node_id_from_latest_writer(latest_writer: Any) -> Optional[str]:
    if not isinstance(latest_writer, dict):
        return None
    from_node_id = latest_writer.get("from_node_id")
    if isinstance(from_node_id, str) and from_node_id.startswith("event:"):
        return from_node_id
    event_id = latest_writer.get("event_id")
    if isinstance(event_id, str) and event_id.strip():
        return f"event:{event_id.strip()}"
    return None


def _projection_name_from_latest_writer(latest_writer: Any) -> Optional[str]:
    if not isinstance(latest_writer, dict):
        return None
    projection_name = latest_writer.get("projection_name")
    if isinstance(projection_name, str) and projection_name.strip():
        return projection_name.strip()
    return None


def _writer_code_sha(latest_writer: Any) -> Optional[str]:
    if not isinstance(latest_writer, dict):
        return None
    code_sha = latest_writer.get("producer_code_sha")
    if isinstance(code_sha, str) and code_sha.strip():
        return code_sha.strip()
    return None


def _out_of_date_scope(
    *,
    status_name: str,
    last_occurred_at: Optional[datetime],
    parent_latest_occurred_at: Optional[datetime],
    latest_upstream_occurred_at: Optional[datetime],
) -> str:
    """
    Foundry-style stale scope classification.

    - parent: direct parent aggregate has newer emitted event
    - ancestor: no direct-parent drift, but some upstream producer is newer
    - none: freshness issue without detectable upstream drift (or healthy/no_data)
    """
    if status_name in {"healthy", "no_data"}:
        return "none"
    if last_occurred_at is None:
        return "none"
    if parent_latest_occurred_at is not None and parent_latest_occurred_at > last_occurred_at:
        return "parent"
    if latest_upstream_occurred_at is not None and latest_upstream_occurred_at > last_occurred_at:
        return "ancestor"
    return "none"


def _staleness_reason_with_scope(
    *,
    status_name: str,
    out_of_date_scope: str,
) -> str:
    if status_name == "healthy":
        return "healthy"
    if status_name == "no_data":
        return "no_lineage_data"
    if out_of_date_scope == "parent":
        return "parent_has_newer_events"
    if out_of_date_scope == "ancestor":
        return "ancestor_has_newer_events"
    return "freshness_slo_breached"


def _update_type(
    *,
    status_name: str,
    out_of_date_scope: str,
    last_occurred_at: Optional[datetime],
    latest_projection_occurred_at: Optional[datetime],
    current_code_sha: Optional[str],
    latest_projection_code_sha: Optional[str],
) -> str:
    """
    Foundry-style update type hint:
    - data: upstream data is newer
    - logic: projection writer code changed and newer writes exist
    - none: no actionable update type signal
    """
    if status_name in {"healthy", "no_data"}:
        return "none"
    if (
        last_occurred_at is not None
        and latest_projection_occurred_at is not None
        and latest_projection_occurred_at > last_occurred_at
        and current_code_sha
        and latest_projection_code_sha
        and current_code_sha != latest_projection_code_sha
    ):
        return "logic"
    if out_of_date_scope in {"parent", "ancestor"}:
        return "data"
    return "none"


def _lineage_lookup_batch_size() -> int:
    return max(1, int(get_settings().performance.lineage_latest_edges_max_ids))


def _chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(values), max(1, int(size))):
        chunk = [v for v in values[i : i + size] if isinstance(v, str) and v]
        if chunk:
            yield chunk


async def _get_latest_edges_to_batched(
    *,
    lineage_store: Any,
    to_node_ids: Sequence[str],
    edge_type: Optional[str] = None,
    db_name: Optional[str] = None,
    branch: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    ids = [str(v).strip() for v in to_node_ids if isinstance(v, str) and str(v).strip()]
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}
    batch_size = _lineage_lookup_batch_size()
    merged: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunked(ids, batch_size):
        partial = await lineage_store.get_latest_edges_to(
            to_node_ids=chunk,
            edge_type=edge_type,
            db_name=db_name,
            branch=branch,
        )
        merged.update(partial)
    return merged


async def _get_latest_edges_from_batched(
    *,
    lineage_store: Any,
    from_node_ids: Sequence[str],
    edge_type: Optional[str] = None,
    db_name: Optional[str] = None,
    branch: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    ids = [str(v).strip() for v in from_node_ids if isinstance(v, str) and str(v).strip()]
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}
    batch_size = _lineage_lookup_batch_size()
    merged: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunked(ids, batch_size):
        partial = await lineage_store.get_latest_edges_from(
            from_node_ids=chunk,
            edge_type=edge_type,
            db_name=db_name,
            branch=branch,
        )
        merged.update(partial)
    return merged


async def _get_latest_edges_for_projections_batched(
    *,
    lineage_store: Any,
    projection_names: Sequence[str],
    db_name: Optional[str] = None,
    branch: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    names = [str(v).strip() for v in projection_names if isinstance(v, str) and str(v).strip()]
    names = list(dict.fromkeys(names))
    if not names:
        return {}
    batch_size = _lineage_lookup_batch_size()
    merged: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunked(names, batch_size):
        partial = await lineage_store.get_latest_edges_for_projections(
            projection_names=chunk,
            db_name=db_name,
            branch=branch,
        )
        merged.update(partial)
    return merged


async def _enrich_artifacts_with_latest_writer(
    *,
    lineage_store: Any,
    artifacts: Sequence[Dict[str, Any]],
    db_name: Optional[str] = None,
    branch: Optional[str] = None,
) -> int:
    artifact_node_ids = [str(item.get("node_id") or "") for item in artifacts if item.get("node_id")]
    artifact_causes = await _get_latest_edges_to_batched(
        lineage_store=lineage_store,
        to_node_ids=artifact_node_ids,
        db_name=db_name,
        branch=branch,
    )
    with_cause_count = 0
    for item in artifacts:
        node_id = str(item.get("node_id") or "")
        latest_writer = _edge_cause_payload(artifact_causes.get(node_id))
        item["latest_writer"] = latest_writer
        if isinstance(latest_writer, dict):
            with_cause_count += 1
    return with_cause_count


def _artifact_latest_writer_state(*, run_id: Optional[str], latest_writer: Any) -> str:
    if not isinstance(latest_writer, dict):
        return "unknown"
    writer_run_id = latest_writer.get("run_id")
    if not isinstance(writer_run_id, str) or not writer_run_id.strip():
        return "unknown"
    if isinstance(run_id, str) and run_id and writer_run_id == run_id:
        return "current"
    return "superseded"


@router.get("/graph")
@trace_endpoint("bff.lineage.get_lineage_graph")
async def get_lineage_graph(
    root: str = Query(..., description="Root node id or event id (e.g. event:<uuid> or <uuid>)"),
    db_name: Optional[str] = Query(None, description="Optional database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    as_of: Optional[datetime] = Query(None, description="Optional as-of timestamp (ISO-8601)"),
    direction: LineageDirection = Query("both", description="Traversal direction"),
    max_depth: int = Query(5, ge=0, le=50),
    max_nodes: int = Query(500, ge=1, le=20000),
    max_edges: int = Query(2000, ge=1, le=50000),
    *,
    lineage_store: LineageStoreDep,
):
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        graph = await lineage_store.get_graph(
            root=root,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
            branch=branch,
            as_of=as_of,
        )
        return ApiResponse.success(
            message="Lineage graph fetched",
            data={"graph": graph.model_dump(mode="json")},
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/path")
@trace_endpoint("bff.lineage.get_lineage_path")
async def get_lineage_path(
    source: str = Query(..., description="Path source node id or event id"),
    target: str = Query(..., description="Path target node id or event id"),
    db_name: Optional[str] = Query(None, description="Optional database scope"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    as_of: Optional[datetime] = Query(None, description="Optional as-of timestamp (ISO-8601)"),
    direction: LineageDirection = Query("downstream", description="Traversal direction for path discovery"),
    max_depth: int = Query(20, ge=0, le=50),
    max_nodes: int = Query(5000, ge=1, le=20000),
    max_edges: int = Query(15000, ge=1, le=50000),
    *,
    lineage_store: LineageStoreDep,
):
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)

        graph = await lineage_store.get_graph(
            root=source,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
            branch=branch,
            as_of=as_of,
        )
        source_node = graph.root
        target_node = lineage_store.normalize_root(target)
        path_node_ids, path_edges = _find_shortest_path(
            graph=graph,
            source=source_node,
            target=target_node,
            direction=direction,
        )

        nodes_by_id = {node.node_id: node for node in graph.nodes}
        path_nodes: List[Dict[str, Any]] = []
        for node_id in path_node_ids:
            node = nodes_by_id.get(node_id)
            if node is not None:
                path_nodes.append(node.model_dump(mode="json"))
                continue
            path_nodes.append(
                {
                    "node_id": node_id,
                    "node_type": "unknown",
                    "label": None,
                    "created_at": None,
                    "recorded_at": None,
                    "metadata": {},
                }
            )

        path_edge_payload: List[Dict[str, Any]] = []
        for edge, traversal in path_edges:
            edge_payload = edge.model_dump(mode="json")
            edge_payload["traversal_direction"] = traversal
            path_edge_payload.append(edge_payload)

        found = len(path_node_ids) > 0
        warnings = list(graph.warnings)
        if not found:
            warnings.append("path_not_found")

        return ApiResponse.success(
            message="Lineage path fetched",
            data={
                "source": source_node,
                "target": target_node,
                "direction": direction,
                "found": found,
                "hops": max(0, len(path_node_ids) - 1),
                "nodes": path_nodes,
                "edges": path_edge_payload,
                "warnings": warnings,
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/impact")
@trace_endpoint("bff.lineage.get_lineage_impact")
async def get_lineage_impact(
    root: str = Query(..., description="Root node id or event id"),
    db_name: Optional[str] = Query(None, description="Optional database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    as_of: Optional[datetime] = Query(None, description="Optional as-of timestamp (ISO-8601)"),
    direction: LineageDirection = Query("downstream", description="Impact direction (usually downstream)"),
    max_depth: int = Query(10, ge=0, le=50),
    artifact_kind: Optional[str] = Query(None, description="Filter by artifact kind (es|s3|graph|...)"),
    max_nodes: int = Query(2000, ge=1, le=20000),
    max_edges: int = Query(5000, ge=1, le=50000),
    *,
    lineage_store: LineageStoreDep,
):
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        graph = await lineage_store.get_graph(
            root=root,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
            branch=branch,
            as_of=as_of,
        )

        artifacts: list[Dict[str, Any]] = []
        counts: Dict[str, int] = {}
        for node in graph.nodes:
            if node.node_type != "artifact":
                continue
            kind, _ = _parse_artifact_node_id(node.node_id)
            if artifact_kind and kind != artifact_kind:
                continue
            artifacts.append(
                {
                    "node_id": node.node_id,
                    "kind": kind,
                    "label": node.label,
                    "metadata": node.metadata,
                }
            )
            counts[str(kind)] = counts.get(str(kind), 0) + 1

        actions = _suggest_remediation_actions(artifacts=artifacts)

        return ApiResponse.success(
            message="Lineage impact fetched",
            data={
                "root": graph.root,
                "direction": graph.direction,
                "artifacts": artifacts,
                "counts": counts,
                "recommended_actions": actions,
                "warnings": graph.warnings,
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/diff")
@trace_endpoint("bff.lineage.get_lineage_diff")
async def get_lineage_diff(
    root: str = Query(..., description="Root node id or event id"),
    from_as_of: datetime = Query(..., description="Baseline as-of timestamp (ISO-8601)"),
    to_as_of: Optional[datetime] = Query(None, description="Target as-of timestamp (ISO-8601), defaults to now"),
    db_name: Optional[str] = Query(None, description="Optional database scope"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    direction: LineageDirection = Query("downstream", description="Diff traversal direction"),
    max_depth: int = Query(10, ge=0, le=50),
    max_nodes: int = Query(5000, ge=1, le=20000),
    max_edges: int = Query(15000, ge=1, le=50000),
    *,
    lineage_store: LineageStoreDep,
):
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        if from_as_of.tzinfo is None:
            from_as_of = from_as_of.replace(tzinfo=timezone.utc)
        if to_as_of is None:
            to_as_of = datetime.now(timezone.utc)
        elif to_as_of.tzinfo is None:
            to_as_of = to_as_of.replace(tzinfo=timezone.utc)
        if from_as_of > to_as_of:
            raise ValueError("from_as_of must be less than or equal to to_as_of")

        baseline_graph = await lineage_store.get_graph(
            root=root,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
            branch=branch,
            as_of=from_as_of,
        )
        target_graph = await lineage_store.get_graph(
            root=root,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
            branch=branch,
            as_of=to_as_of,
        )

        baseline_nodes = {node.node_id: node for node in baseline_graph.nodes}
        target_nodes = {node.node_id: node for node in target_graph.nodes}

        baseline_edges = {_edge_signature(edge): edge for edge in baseline_graph.edges}
        target_edges = {_edge_signature(edge): edge for edge in target_graph.edges}

        added_node_ids = sorted(set(target_nodes.keys()) - set(baseline_nodes.keys()))
        removed_node_ids = sorted(set(baseline_nodes.keys()) - set(target_nodes.keys()))
        added_edge_ids = sorted(set(target_edges.keys()) - set(baseline_edges.keys()))
        removed_edge_ids = sorted(set(baseline_edges.keys()) - set(target_edges.keys()))

        added_nodes = [target_nodes[node_id] for node_id in added_node_ids]
        removed_nodes = [baseline_nodes[node_id] for node_id in removed_node_ids]
        added_edges = [target_edges[edge_id] for edge_id in added_edge_ids]
        removed_edges = [baseline_edges[edge_id] for edge_id in removed_edge_ids]

        added_artifacts: List[Dict[str, Any]] = []
        for node in added_nodes:
            if node.node_type != "artifact":
                continue
            kind, _ = _parse_artifact_node_id(node.node_id)
            added_artifacts.append(
                {
                    "node_id": node.node_id,
                    "kind": kind,
                    "label": node.label,
                    "metadata": node.metadata,
                }
            )

        return ApiResponse.success(
            message="Lineage diff fetched",
            data={
                "root": target_graph.root,
                "direction": direction,
                "from_as_of": from_as_of.isoformat(),
                "to_as_of": to_as_of.isoformat(),
                "added_nodes": [node.model_dump(mode="json") for node in added_nodes],
                "removed_nodes": [node.model_dump(mode="json") for node in removed_nodes],
                "added_edges": [edge.model_dump(mode="json") for edge in added_edges],
                "removed_edges": [edge.model_dump(mode="json") for edge in removed_edges],
                "added_artifact_counts": _count_artifact_kinds(added_nodes),
                "removed_artifact_counts": _count_artifact_kinds(removed_nodes),
                "recommended_actions": _suggest_remediation_actions(artifacts=added_artifacts),
                "counts": {
                    "added_nodes": len(added_nodes),
                    "removed_nodes": len(removed_nodes),
                    "added_edges": len(added_edges),
                    "removed_edges": len(removed_edges),
                },
                "warnings": sorted(set(baseline_graph.warnings + target_graph.warnings)),
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/metrics")
@trace_endpoint("bff.lineage.get_lineage_metrics")
async def get_lineage_metrics(
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    window_minutes: int = Query(60, ge=1, le=24 * 60, description="Time window for missing ratio estimate"),
    *,
    lineage_store: LineageStoreDep,
    audit_store: AuditLogStoreDep,
):
    """
    Operational lineage metrics.

    - `lineage_lag_seconds`: age of oldest missing-lineage item in the backfill queue
    - `missing_lineage_ratio_estimate`: based on EVENT_APPENDED audit count vs recorded lineage edges
    """
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        now = datetime.now(timezone.utc)
        since = now - timedelta(minutes=int(window_minutes))

        partition_key = f"db:{db_name}" if db_name else None
        events_appended = await audit_store.count_logs(
            partition_key=partition_key,
            action="EVENT_APPENDED",
            since=since,
            until=now,
        )
        lineage_recorded = await lineage_store.count_edges(
            edge_type=EDGE_AGGREGATE_EMITTED_EVENT,
            db_name=db_name,
            branch=branch,
            since=since,
            until=now,
        )
        missing_est = max(0, int(events_appended) - int(lineage_recorded))
        missing_ratio = (missing_est / events_appended) if events_appended else 0.0

        backfill = await lineage_store.get_backfill_metrics(db_name=db_name, branch=branch)

        return ApiResponse.success(
            message="Lineage metrics fetched",
            data={
                "db_name": db_name,
                "branch": branch,
                "window_minutes": int(window_minutes),
                "events_appended": int(events_appended),
                "lineage_edges_recorded": int(lineage_recorded),
                "missing_lineage_estimate": missing_est,
                "missing_lineage_ratio_estimate": missing_ratio,
                "backfill_queue": backfill,
                "scope_warning": (
                    "events_appended is db-scoped; branch-specific missing ratio is approximate"
                    if branch
                    else None
                ),
                "timestamp": now.isoformat(),
            },
        ).to_dict()
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/runs")
@trace_endpoint("bff.lineage.get_lineage_runs")
async def get_lineage_runs(
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    since: Optional[datetime] = Query(None, description="Window start (ISO-8601), default now-24h"),
    until: Optional[datetime] = Query(None, description="Window end (ISO-8601), default now"),
    edge_type: Optional[str] = Query(None, description="Optional lineage edge type filter"),
    run_limit: int = Query(200, ge=1, le=5000, description="Max run/build summaries returned"),
    freshness_slo_minutes: int = Query(120, ge=1, le=7 * 24 * 60, description="Freshness SLO in minutes"),
    include_impact_preview: bool = Query(False, description="Include top impacted artifacts preview per run"),
    impact_preview_runs_limit: int = Query(20, ge=1, le=200, description="Number of runs to enrich with impact preview"),
    impact_preview_artifacts_limit: int = Query(20, ge=1, le=200, description="Max impacted artifacts per run preview"),
    impact_preview_edge_limit: int = Query(2000, ge=1, le=20000, description="Edges loaded per run for preview"),
    *,
    lineage_store: LineageStoreDep,
):
    """
    Foundry-style build/run timeline:
    - run windows (first/last activity)
    - per-run blast-radius stats
    - optional impacted artifact previews
    """
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        since_ts, until_ts = _normalize_window(since=since, until=until, default_hours=24)

        run_rows = await lineage_store.list_run_summaries(
            db_name=db_name,
            branch=branch,
            edge_type=edge_type,
            since=since_ts,
            until=until_ts,
            limit=run_limit,
        )
        runs, status_counts = _summarize_run_rows(
            run_rows=run_rows,
            as_of=until_ts,
            freshness_slo_minutes=int(freshness_slo_minutes),
        )

        warnings: List[str] = []
        if len(run_rows) >= int(run_limit):
            warnings.append(f"run_limit_reached(limit={int(run_limit)})")

        if include_impact_preview and runs:
            preview_limit = min(len(runs), int(impact_preview_runs_limit))
            for run in runs[:preview_limit]:
                run_id = str(run.get("run_id") or "")
                if not run_id:
                    continue
                run_edges = await lineage_store.list_edges(
                    db_name=db_name,
                    branch=branch,
                    run_id=run_id,
                    since=since_ts,
                    until=until_ts,
                    limit=impact_preview_edge_limit,
                )
                impacted_artifacts, impacted_artifact_count = _extract_impacted_artifacts_from_edges(
                    edges=run_edges,
                    limit=impact_preview_artifacts_limit,
                )
                impacted_artifact_with_cause_count = await _enrich_artifacts_with_latest_writer(
                    lineage_store=lineage_store,
                    artifacts=impacted_artifacts,
                    db_name=db_name,
                    branch=branch,
                )
                latest_writer_state_counts: Dict[str, int] = {"current": 0, "superseded": 0, "unknown": 0}
                for item in impacted_artifacts:
                    state = _artifact_latest_writer_state(run_id=run_id, latest_writer=item.get("latest_writer"))
                    item["latest_writer_state"] = state
                    latest_writer_state_counts[state] = latest_writer_state_counts.get(state, 0) + 1
                run["impact_preview"] = {
                    "impacted_artifact_count": impacted_artifact_count,
                    "impacted_artifacts": impacted_artifacts,
                    "impacted_artifact_with_cause_count": impacted_artifact_with_cause_count,
                    "latest_writer_state_counts": dict(
                        sorted(latest_writer_state_counts.items(), key=lambda item: item[0])
                    ),
                }
                if len(run_edges) >= int(impact_preview_edge_limit):
                    warnings.append(
                        f"impact_preview_edge_limit_reached(run_id={run_id},limit={int(impact_preview_edge_limit)})"
                    )

        return ApiResponse.success(
            message="Lineage runs fetched",
            data={
                "db_name": db_name,
                "branch": branch,
                "edge_type": edge_type,
                "since": since_ts.isoformat(),
                "until": until_ts.isoformat(),
                "freshness_slo_minutes": int(freshness_slo_minutes),
                "run_limit": int(run_limit),
                "run_count": len(runs),
                "status_counts": status_counts,
                "runs": runs,
                "warnings": sorted(set(warnings)),
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/run-impact")
@trace_endpoint("bff.lineage.get_lineage_run_impact")
async def get_lineage_run_impact(
    run_id: str = Query(..., min_length=1, description="Producer/runtime run id"),
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    since: Optional[datetime] = Query(None, description="Window start (ISO-8601), default now-24h"),
    until: Optional[datetime] = Query(None, description="Window end (ISO-8601), default now"),
    event_limit: int = Query(5000, ge=1, le=20000, description="Max lineage edges loaded"),
    artifact_preview_limit: int = Query(200, ge=1, le=2000, description="Max impacted artifacts returned"),
    *,
    lineage_store: LineageStoreDep,
):
    """
    Foundry-style run/build impact diagnostics.
    Answers: "this run touched what, when, and with what blast radius?"
    """
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        since_ts, until_ts = _normalize_window(since=since, until=until, default_hours=24)

        edges = await lineage_store.list_edges(
            db_name=db_name,
            branch=branch,
            run_id=run_id,
            since=since_ts,
            until=until_ts,
            limit=event_limit,
        )
        summary = _build_timeline_summary(edges=edges, bucket_minutes=15)
        column_lineage_refs, column_lineage_ref_count = _extract_column_lineage_refs(edges=edges, limit=200)

        root_nodes = sorted({str(edge.get("from_node_id")) for edge in edges if edge.get("from_node_id")})
        impacted_artifacts, impacted_artifact_count = _extract_impacted_artifacts_from_edges(
            edges=edges,
            limit=artifact_preview_limit,
        )
        impacted_artifact_with_cause_count = await _enrich_artifacts_with_latest_writer(
            lineage_store=lineage_store,
            artifacts=impacted_artifacts,
            db_name=db_name,
            branch=branch,
        )
        latest_writer_state_counts: Dict[str, int] = {"current": 0, "superseded": 0, "unknown": 0}
        for item in impacted_artifacts:
            state = _artifact_latest_writer_state(run_id=run_id, latest_writer=item.get("latest_writer"))
            item["latest_writer_state"] = state
            latest_writer_state_counts[state] = latest_writer_state_counts.get(state, 0) + 1
        remediation = _suggest_remediation_actions(
            artifacts=[
                {"kind": a.get("kind"), "node_id": a.get("node_id"), "label": a.get("label")}
                for a in impacted_artifacts
            ]
        )

        warnings: List[str] = []
        if len(edges) >= int(event_limit):
            warnings.append(f"event_limit_reached(limit={int(event_limit)})")
        if column_lineage_ref_count > len(column_lineage_refs):
            warnings.append(f"column_lineage_ref_preview_truncated(limit={len(column_lineage_refs)})")

        return ApiResponse.success(
            message="Lineage run impact fetched",
            data={
                "run_id": run_id,
                "db_name": db_name,
                "branch": branch,
                "since": since_ts.isoformat(),
                "until": until_ts.isoformat(),
                "edges_loaded": len(edges),
                "root_nodes": root_nodes,
                "root_node_count": len(root_nodes),
                "impacted_artifacts": impacted_artifacts,
                "impacted_artifact_count": impacted_artifact_count,
                "impacted_artifact_with_cause_count": impacted_artifact_with_cause_count,
                "latest_writer_state_counts": dict(sorted(latest_writer_state_counts.items(), key=lambda item: item[0])),
                "column_lineage_ref_count": column_lineage_ref_count,
                "column_lineage_refs": column_lineage_refs,
                "recommended_actions": remediation,
                "summary": summary,
                "warnings": warnings,
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/timeline")
@trace_endpoint("bff.lineage.get_lineage_timeline")
async def get_lineage_timeline(
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    since: Optional[datetime] = Query(None, description="Window start (ISO-8601), default now-24h"),
    until: Optional[datetime] = Query(None, description="Window end (ISO-8601), default now"),
    edge_type: Optional[str] = Query(None, description="Optional lineage edge type filter"),
    projection_name: Optional[str] = Query(None, description="Optional projection filter"),
    bucket_minutes: int = Query(15, ge=1, le=24 * 60, description="Bucket size in minutes"),
    event_limit: int = Query(5000, ge=1, le=20000, description="Max events loaded from lineage store"),
    event_preview_limit: int = Query(200, ge=1, le=2000, description="Max event rows returned in payload"),
    *,
    lineage_store: LineageStoreDep,
):
    """
    Foundry-style lineage timeline for operational debugging:
    - when lineage events spiked
    - which edge/projection/service dominated
    """
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        since_ts, until_ts = _normalize_window(since=since, until=until, default_hours=24)

        edges = await lineage_store.list_edges(
            edge_type=edge_type,
            projection_name=projection_name,
            db_name=db_name,
            branch=branch,
            since=since_ts,
            until=until_ts,
            limit=event_limit,
        )
        summary = _build_timeline_summary(edges=edges, bucket_minutes=bucket_minutes)
        column_lineage_refs, column_lineage_ref_count = _extract_column_lineage_refs(edges=edges, limit=200)

        preview_rows: List[Dict[str, Any]] = []
        for edge in edges[:event_preview_limit]:
            occurred_at = _to_utc_datetime(edge.get("occurred_at"))
            recorded_at = _to_utc_datetime(edge.get("recorded_at"))
            preview_rows.append(
                {
                    "edge_id": edge.get("edge_id"),
                    "from_node_id": edge.get("from_node_id"),
                    "to_node_id": edge.get("to_node_id"),
                    "edge_type": edge.get("edge_type"),
                    "projection_name": edge.get("projection_name"),
                    "occurred_at": occurred_at.isoformat() if occurred_at else None,
                    "recorded_at": recorded_at.isoformat() if recorded_at else None,
                    "metadata": _compact_edge_metadata(edge.get("metadata") if isinstance(edge, dict) else {}),
                }
            )

        warnings: List[str] = []
        if len(edges) >= int(event_limit):
            warnings.append(f"event_limit_reached(limit={int(event_limit)})")
        if column_lineage_ref_count > len(column_lineage_refs):
            warnings.append(f"column_lineage_ref_preview_truncated(limit={len(column_lineage_refs)})")

        return ApiResponse.success(
            message="Lineage timeline fetched",
            data={
                "db_name": db_name,
                "branch": branch,
                "edge_type": edge_type,
                "projection_name": projection_name,
                "since": since_ts.isoformat(),
                "until": until_ts.isoformat(),
                "bucket_minutes": int(bucket_minutes),
                "event_limit": int(event_limit),
                "event_preview_limit": int(event_preview_limit),
                "event_count_loaded": len(edges),
                "column_lineage_ref_count": column_lineage_ref_count,
                "column_lineage_refs": column_lineage_refs,
                "events": preview_rows,
                "summary": summary,
                "warnings": warnings,
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/column-lineage")
@trace_endpoint("bff.lineage.get_lineage_column_lineage")
async def get_lineage_column_lineage(
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    run_id: Optional[str] = Query(None, description="Optional run id filter"),
    source_field: Optional[str] = Query(None, description="Optional source column filter"),
    target_field: Optional[str] = Query(None, description="Optional target field filter"),
    target_class_id: Optional[str] = Query(None, description="Optional target class filter"),
    since: Optional[datetime] = Query(None, description="Window start (ISO-8601), default now-24h"),
    until: Optional[datetime] = Query(None, description="Window end (ISO-8601), default now"),
    edge_limit: int = Query(5000, ge=1, le=20000, description="Max lineage edges loaded"),
    pair_limit: int = Query(2000, ge=1, le=10000, description="Max column lineage pairs returned"),
    *,
    lineage_store: LineageStoreDep,
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    """
    Resolve column-level lineage from mapping-spec references embedded in lineage edges.

    This provides a Foundry-style "which source columns mapped to which target fields"
    view without exploding the graph into per-column nodes.
    """
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        since_ts, until_ts = _normalize_window(since=since, until=until, default_hours=24)
        source_field_filter = str(source_field or "").strip() or None
        target_field_filter = str(target_field or "").strip() or None
        target_class_filter = str(target_class_id or "").strip() or None
        run_id_filter = str(run_id or "").strip() or None

        edges = await lineage_store.list_edges(
            db_name=db_name,
            branch=branch,
            run_id=run_id_filter,
            since=since_ts,
            until=until_ts,
            limit=edge_limit,
        )

        ref_usage_by_key: Dict[str, Dict[str, Any]] = {}
        inline_pair_rows_by_key: Dict[str, Dict[str, Any]] = {}
        for edge in edges:
            metadata = edge.get("metadata") if isinstance(edge.get("metadata"), dict) else {}
            ref_entry = _extract_column_lineage_ref_entry(metadata)
            inline_pairs = _extract_column_lineage_pairs_from_metadata(metadata)
            edge_occurred_at = _to_utc_datetime(edge.get("occurred_at"))
            edge_type = str(edge.get("edge_type") or "").strip()
            target_class_value = str(metadata.get("target_class_id") or "").strip()
            dataset_id_value = str(metadata.get("dataset_id") or "").strip()
            dataset_branch_value = str(metadata.get("dataset_branch") or metadata.get("branch") or "").strip()
            inline_mapping_spec_id = (
                str((ref_entry or {}).get("mapping_spec_id") or metadata.get("mapping_spec_id") or "").strip()
            )
            inline_mapping_spec_version = _coerce_int(
                (ref_entry or {}).get("mapping_spec_version") if ref_entry is not None else metadata.get("mapping_spec_version")
            )
            inline_ref_value = str((ref_entry or {}).get("ref") or "").strip()
            for source_col, target_col in inline_pairs:
                if source_field_filter and source_col != source_field_filter:
                    continue
                if target_field_filter and target_col != target_field_filter:
                    continue
                if target_class_filter and target_class_value != target_class_filter:
                    continue
                pair_key = "|".join([source_col, target_col, target_class_value])
                inline_row = inline_pair_rows_by_key.get(pair_key)
                if inline_row is None:
                    inline_row = {
                        "source_field": source_col,
                        "target_field": target_col,
                        "target_class_id": target_class_value or None,
                        "dataset_id": dataset_id_value or None,
                        "dataset_branch": dataset_branch_value or None,
                        "edge_count": 0,
                        "latest_occurred_at": None,
                        "mapping_spec_ids": set(),
                        "mapping_spec_versions": set(),
                        "refs": set(),
                        "edge_types": set(),
                    }
                    inline_pair_rows_by_key[pair_key] = inline_row
                inline_row["edge_count"] = int(inline_row.get("edge_count") or 0) + 1
                if inline_mapping_spec_id:
                    inline_row["mapping_spec_ids"].add(inline_mapping_spec_id)
                if inline_mapping_spec_version is not None:
                    inline_row["mapping_spec_versions"].add(inline_mapping_spec_version)
                if inline_ref_value:
                    inline_row["refs"].add(inline_ref_value)
                if edge_type:
                    inline_row["edge_types"].add(edge_type)
                current_latest = inline_row.get("latest_occurred_at")
                if edge_occurred_at and (current_latest is None or edge_occurred_at > current_latest):
                    inline_row["latest_occurred_at"] = edge_occurred_at

            if not isinstance(ref_entry, dict):
                continue

            mapping_spec_id = str(ref_entry.get("mapping_spec_id") or "").strip()
            mapping_spec_version = _coerce_int(ref_entry.get("mapping_spec_version"))
            if not mapping_spec_id:
                continue
            key = f"{mapping_spec_id}|{'' if mapping_spec_version is None else mapping_spec_version}"
            usage = ref_usage_by_key.get(key)
            if usage is None:
                usage = {
                    "key": key,
                    "ref": ref_entry.get("ref"),
                    "mapping_spec_id": mapping_spec_id,
                    "mapping_spec_version": mapping_spec_version,
                    "target_class_id": ref_entry.get("target_class_id"),
                    "storage": ref_entry.get("storage"),
                    "schema_version": ref_entry.get("schema_version"),
                    "edge_count": 0,
                    "edge_types": set(),
                    "latest_occurred_at": None,
                }
                ref_usage_by_key[key] = usage

            usage["edge_count"] = int(usage.get("edge_count") or 0) + 1
            edge_type = str(edge.get("edge_type") or "").strip()
            if edge_type:
                usage["edge_types"].add(edge_type)
            occurred_at = _to_utc_datetime(edge.get("occurred_at"))
            latest_occurred_at = usage.get("latest_occurred_at")
            if occurred_at and (latest_occurred_at is None or occurred_at > latest_occurred_at):
                usage["latest_occurred_at"] = occurred_at

        resolution_by_key: Dict[str, Dict[str, Any]] = {}
        pair_rows_by_key: Dict[str, Dict[str, Any]] = {}
        for key, usage in ref_usage_by_key.items():
            mapping_spec_id = str(usage.get("mapping_spec_id") or "").strip()
            expected_version = _coerce_int(usage.get("mapping_spec_version"))
            try:
                mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
            except Exception as exc:
                resolution_by_key[key] = {"status": "unresolved", "reason": f"lookup_error:{exc}"}
                continue

            if mapping_spec is None:
                resolution_by_key[key] = {"status": "unresolved", "reason": "mapping_spec_not_found"}
                continue

            actual_version = _coerce_int(getattr(mapping_spec, "version", None))
            if expected_version is not None and actual_version is not None and expected_version != actual_version:
                resolution_by_key[key] = {
                    "status": "unresolved",
                    "reason": f"version_mismatch(expected={expected_version},actual={actual_version})",
                }
                continue

            mappings = getattr(mapping_spec, "mappings", None)
            if not isinstance(mappings, list):
                resolution_by_key[key] = {"status": "unresolved", "reason": "invalid_mapping_spec_mappings"}
                continue

            resolution_by_key[key] = {"status": "resolved"}

            resolved_target_class_id = str(usage.get("target_class_id") or "").strip() or str(
                getattr(mapping_spec, "target_class_id", "") or ""
            ).strip()
            resolved_dataset_id = str(getattr(mapping_spec, "dataset_id", "") or "").strip()
            resolved_dataset_branch = str(getattr(mapping_spec, "dataset_branch", "") or "").strip()

            for mapping in mappings:
                pair = _normalize_mapping_pair(mapping if isinstance(mapping, dict) else {})
                if pair is None:
                    continue
                source_col, target_col = pair
                if source_field_filter and source_col != source_field_filter:
                    continue
                if target_field_filter and target_col != target_field_filter:
                    continue
                if target_class_filter and resolved_target_class_id != target_class_filter:
                    continue

                pair_key = "|".join(
                    [
                        source_col,
                        target_col,
                        resolved_target_class_id,
                    ]
                )
                pair_row = pair_rows_by_key.get(pair_key)
                if pair_row is None:
                    pair_row = {
                        "source_field": source_col,
                        "target_field": target_col,
                        "target_class_id": resolved_target_class_id or None,
                        "dataset_id": resolved_dataset_id or None,
                        "dataset_branch": resolved_dataset_branch or None,
                        "edge_count": 0,
                        "latest_occurred_at": None,
                        "mapping_spec_ids": set(),
                        "mapping_spec_versions": set(),
                        "refs": set(),
                        "edge_types": set(),
                    }
                    pair_rows_by_key[pair_key] = pair_row

                pair_row["edge_count"] = int(pair_row.get("edge_count") or 0) + int(usage.get("edge_count") or 0)
                if mapping_spec_id:
                    pair_row["mapping_spec_ids"].add(mapping_spec_id)
                if actual_version is not None:
                    pair_row["mapping_spec_versions"].add(actual_version)
                ref_value = str(usage.get("ref") or "").strip()
                if ref_value:
                    pair_row["refs"].add(ref_value)
                pair_row["edge_types"].update(set(usage.get("edge_types") or set()))
                latest_occurred_at = usage.get("latest_occurred_at")
                current_latest = pair_row.get("latest_occurred_at")
                if latest_occurred_at and (current_latest is None or latest_occurred_at > current_latest):
                    pair_row["latest_occurred_at"] = latest_occurred_at

        for pair_key, inline_row in inline_pair_rows_by_key.items():
            pair_row = pair_rows_by_key.get(pair_key)
            if pair_row is None:
                pair_rows_by_key[pair_key] = inline_row
                continue
            pair_row["edge_count"] = int(pair_row.get("edge_count") or 0) + int(inline_row.get("edge_count") or 0)
            pair_row["mapping_spec_ids"].update(set(inline_row.get("mapping_spec_ids") or set()))
            pair_row["mapping_spec_versions"].update(set(inline_row.get("mapping_spec_versions") or set()))
            pair_row["refs"].update(set(inline_row.get("refs") or set()))
            pair_row["edge_types"].update(set(inline_row.get("edge_types") or set()))
            current_latest = pair_row.get("latest_occurred_at")
            inline_latest = inline_row.get("latest_occurred_at")
            if inline_latest and (current_latest is None or inline_latest > current_latest):
                pair_row["latest_occurred_at"] = inline_latest

        refs_payload: List[Dict[str, Any]] = []
        for usage in ref_usage_by_key.values():
            key = str(usage.get("key") or "")
            resolution = resolution_by_key.get(key) or {"status": "unresolved", "reason": "not_resolved"}
            latest_occurred_at = usage.get("latest_occurred_at")
            refs_payload.append(
                {
                    "ref": usage.get("ref"),
                    "mapping_spec_id": usage.get("mapping_spec_id"),
                    "mapping_spec_version": usage.get("mapping_spec_version"),
                    "target_class_id": usage.get("target_class_id"),
                    "storage": usage.get("storage"),
                    "schema_version": usage.get("schema_version"),
                    "edge_count": int(usage.get("edge_count") or 0),
                    "edge_types": sorted(str(value) for value in (usage.get("edge_types") or set()) if str(value)),
                    "latest_occurred_at": latest_occurred_at.isoformat() if latest_occurred_at else None,
                    "status": resolution.get("status"),
                    "reason": resolution.get("reason"),
                }
            )
        refs_payload.sort(key=lambda item: str(item.get("latest_occurred_at") or ""), reverse=True)

        pair_rows: List[Dict[str, Any]] = []
        for row in pair_rows_by_key.values():
            latest_occurred_at = row.get("latest_occurred_at")
            pair_rows.append(
                {
                    "source_field": row.get("source_field"),
                    "target_field": row.get("target_field"),
                    "target_class_id": row.get("target_class_id"),
                    "dataset_id": row.get("dataset_id"),
                    "dataset_branch": row.get("dataset_branch"),
                    "edge_count": int(row.get("edge_count") or 0),
                    "latest_occurred_at": latest_occurred_at.isoformat() if latest_occurred_at else None,
                    "mapping_spec_ids": sorted(str(value) for value in (row.get("mapping_spec_ids") or set()) if str(value)),
                    "mapping_spec_versions": sorted(int(value) for value in (row.get("mapping_spec_versions") or set())),
                    "refs": sorted(str(value) for value in (row.get("refs") or set()) if str(value)),
                    "edge_types": sorted(str(value) for value in (row.get("edge_types") or set()) if str(value)),
                }
            )
        pair_rows.sort(
            key=lambda item: (
                -int(item.get("edge_count") or 0),
                str(item.get("latest_occurred_at") or ""),
                str(item.get("source_field") or ""),
                str(item.get("target_field") or ""),
            )
        )

        pair_count_total = len(pair_rows)
        pair_rows = pair_rows[: int(pair_limit)]
        warnings: List[str] = []
        if len(edges) >= int(edge_limit):
            warnings.append(f"edge_limit_reached(limit={int(edge_limit)})")
        if pair_count_total > len(pair_rows):
            warnings.append(f"pair_preview_truncated(limit={len(pair_rows)})")

        resolved_ref_count = len([row for row in refs_payload if row.get("status") == "resolved"])
        unresolved_ref_count = len(refs_payload) - resolved_ref_count

        unique_source_fields = sorted(
            {
                str(row.get("source_field") or "").strip()
                for row in pair_rows
                if str(row.get("source_field") or "").strip()
            }
        )
        unique_target_fields = sorted(
            {
                str(row.get("target_field") or "").strip()
                for row in pair_rows
                if str(row.get("target_field") or "").strip()
            }
        )
        unique_target_classes = sorted(
            {
                str(row.get("target_class_id") or "").strip()
                for row in pair_rows
                if str(row.get("target_class_id") or "").strip()
            }
        )

        return ApiResponse.success(
            message="Lineage column-level mapping fetched",
            data={
                "db_name": db_name,
                "branch": branch,
                "run_id": run_id_filter,
                "since": since_ts.isoformat(),
                "until": until_ts.isoformat(),
                "source_field_filter": source_field_filter,
                "target_field_filter": target_field_filter,
                "target_class_id_filter": target_class_filter,
                "edge_count_loaded": len(edges),
                "column_lineage_ref_count": len(refs_payload),
                "resolved_ref_count": resolved_ref_count,
                "unresolved_ref_count": unresolved_ref_count,
                "pair_count": pair_count_total,
                "pairs": pair_rows,
                "refs": refs_payload,
                "summary": {
                    "unique_source_field_count": len(unique_source_fields),
                    "unique_target_field_count": len(unique_target_fields),
                    "unique_target_class_count": len(unique_target_classes),
                    "unique_source_fields": unique_source_fields,
                    "unique_target_fields": unique_target_fields,
                    "unique_target_classes": unique_target_classes,
                },
                "warnings": warnings,
            },
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e



@router.get("/out-of-date")
@trace_endpoint("bff.lineage.get_lineage_out_of_date")
async def get_lineage_out_of_date(
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
    branch: Optional[str] = Query(None, description="Optional branch scope"),
    artifact_kind: Optional[str] = Query(None, description="Optional artifact kind filter (es|graph|s3|...)"),
    as_of: Optional[datetime] = Query(None, description="As-of timestamp for freshness evaluation"),
    freshness_slo_minutes: int = Query(120, ge=1, le=7 * 24 * 60, description="Freshness SLO in minutes"),
    artifact_limit: int = Query(5000, ge=1, le=20000, description="Max artifact rows to evaluate"),
    stale_preview_limit: int = Query(200, ge=1, le=2000, description="Max stale artifacts returned"),
    projection_limit: int = Query(1000, ge=1, le=5000, description="Max projection rows to evaluate"),
    projection_preview_limit: int = Query(200, ge=1, le=2000, description="Max projections returned"),
    *,
    lineage_store: LineageStoreDep,
):
    """
    Foundry-style out-of-date diagnostics:
    - identify stale artifacts/projections by latest lineage write time
    - provide actionable blast-radius remediation hints
    """
    try:
        db_name, branch = _normalize_scope(db_name=db_name, branch=branch)
        service = LineageOutOfDateService(lineage_store=lineage_store)
        payload = await service.analyze(
            db_name=db_name,
            branch=branch,
            artifact_kind=artifact_kind,
            as_of=as_of,
            freshness_slo_minutes=freshness_slo_minutes,
            artifact_limit=artifact_limit,
            stale_preview_limit=stale_preview_limit,
            projection_limit=projection_limit,
            projection_preview_limit=projection_preview_limit,
        )
        return ApiResponse.success(
            message="Lineage out-of-date diagnostics fetched",
            data=payload,
        ).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e
