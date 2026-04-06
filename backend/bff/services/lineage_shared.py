from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

from shared.config.settings import get_settings


class LineageEdgeLookupStore(Protocol):
    async def get_latest_edges_to(
        self,
        *,
        to_node_ids: Sequence[str],
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]: ...

    async def get_latest_edges_from(
        self,
        *,
        from_node_ids: Sequence[str],
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]: ...

    async def get_latest_edges_for_projections(
        self,
        *,
        projection_names: Sequence[str],
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]: ...


def _parse_artifact_node_id(node_id: str) -> Tuple[Optional[str], str]:
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
    lineage_store: LineageEdgeLookupStore,
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
    lineage_store: LineageEdgeLookupStore,
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
    lineage_store: LineageEdgeLookupStore,
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


async def _enrich_artifacts_with_latest_writer(
    *,
    lineage_store: LineageEdgeLookupStore,
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
    if status_name in {"healthy", "no_data"}:
        return "none"
    if last_occurred_at is None:
        return "none"
    if parent_latest_occurred_at is not None and parent_latest_occurred_at > last_occurred_at:
        return "parent"
    if latest_upstream_occurred_at is not None and latest_upstream_occurred_at > last_occurred_at:
        return "ancestor"
    return "none"
