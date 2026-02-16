from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

from shared.config.settings import get_settings
from shared.models.lineage_edge_types import EDGE_AGGREGATE_EMITTED_EVENT


class LineageOutOfDateStore(Protocol):
    async def list_edges(
        self,
        *,
        edge_type: Optional[str] = None,
        projection_name: Optional[str] = None,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        run_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]: ...

    async def list_artifact_latest_writes(
        self,
        *,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        artifact_kind: Optional[str] = None,
        as_of: Optional[datetime] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]: ...

    async def list_projection_latest_writes(
        self,
        *,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        as_of: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]: ...

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
    lineage_store: LineageOutOfDateStore,
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
    lineage_store: LineageOutOfDateStore,
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
    lineage_store: LineageOutOfDateStore,
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
    lineage_store: LineageOutOfDateStore,
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


class LineageOutOfDateService:
    """Service class for `/lineage/out-of-date` diagnostics."""

    def __init__(self, *, lineage_store: LineageOutOfDateStore) -> None:
        self._lineage_store = lineage_store

    async def analyze(
        self,
        *,
        db_name: Optional[str],
        branch: Optional[str],
        artifact_kind: Optional[str],
        as_of: Optional[datetime],
        freshness_slo_minutes: int,
        artifact_limit: int,
        stale_preview_limit: int,
        projection_limit: int,
        projection_preview_limit: int,
    ) -> Dict[str, Any]:
        as_of_ts = as_of or datetime.now(timezone.utc)
        as_of_ts = as_of_ts if as_of_ts.tzinfo else as_of_ts.replace(tzinfo=timezone.utc)
        latest_upstream_occurred_at = await self._latest_upstream_occurred_at(
            db_name=db_name,
            branch=branch,
            as_of=as_of_ts,
        )
        artifacts, artifact_rows_count, artifact_counts = await self._collect_artifacts(
            db_name=db_name,
            branch=branch,
            artifact_kind=artifact_kind,
            as_of=as_of_ts,
            freshness_slo_minutes=freshness_slo_minutes,
            artifact_limit=artifact_limit,
            latest_upstream_occurred_at=latest_upstream_occurred_at,
        )
        projections, projection_rows_count, projection_counts = await self._collect_projections(
            db_name=db_name,
            branch=branch,
            as_of=as_of_ts,
            freshness_slo_minutes=freshness_slo_minutes,
            projection_limit=projection_limit,
            latest_upstream_occurred_at=latest_upstream_occurred_at,
        )
        stale_artifacts_all = [item for item in artifacts if item.get("status") != "healthy"]
        stale_projections_all = [item for item in projections if item.get("status") != "healthy"]
        await _enrich_artifacts_with_latest_writer(
            lineage_store=self._lineage_store,
            artifacts=stale_artifacts_all,
            db_name=db_name,
            branch=branch,
        )
        out_of_date_scope_counts, update_type_counts = await self._annotate_scope_and_update_type(
            stale_artifacts_all=stale_artifacts_all,
            stale_projections_all=stale_projections_all,
            latest_upstream_occurred_at=latest_upstream_occurred_at,
            db_name=db_name,
            branch=branch,
        )
        stale_artifacts = stale_artifacts_all[:stale_preview_limit]
        stale_projections = stale_projections_all[:projection_preview_limit]
        remediation_actions = _suggest_remediation_actions(
            artifacts=[
                {
                    "kind": item.get("kind"),
                    "node_id": item.get("node_id"),
                    "label": item.get("label"),
                }
                for item in stale_artifacts
            ]
        )
        staleness_reason_counts = self._staleness_reason_counts(artifacts=artifacts, projections=projections)
        warnings = self._warnings(
            artifact_rows_count=artifact_rows_count,
            artifact_limit=artifact_limit,
            projection_rows_count=projection_rows_count,
            projection_limit=projection_limit,
            stale_artifact_count=len(stale_artifacts_all),
            stale_preview_limit=stale_preview_limit,
            stale_projection_count=len(stale_projections_all),
            projection_preview_limit=projection_preview_limit,
        )
        return {
            "db_name": db_name,
            "branch": branch,
            "artifact_kind": artifact_kind,
            "as_of": as_of_ts.isoformat(),
            "upstream_latest_event_at": latest_upstream_occurred_at.isoformat() if latest_upstream_occurred_at else None,
            "freshness_slo_minutes": int(freshness_slo_minutes),
            "artifact_counts": artifact_counts,
            "projection_counts": projection_counts,
            "staleness_reason_counts": dict(sorted(staleness_reason_counts.items(), key=lambda item: item[0])),
            "out_of_date_scope_counts": dict(sorted(out_of_date_scope_counts.items(), key=lambda item: item[0])),
            "update_type_counts": dict(sorted(update_type_counts.items(), key=lambda item: item[0])),
            "stale_artifacts": stale_artifacts,
            "stale_projections": stale_projections,
            "recommended_actions": remediation_actions,
            "summary": {
                "artifacts_evaluated": artifact_rows_count,
                "projections_evaluated": projection_rows_count,
                "stale_artifact_count": len(stale_artifacts_all),
                "stale_projection_count": len(stale_projections_all),
                "stale_artifact_with_cause_count": len(
                    [item for item in stale_artifacts if isinstance(item.get("latest_writer"), dict)]
                ),
                "stale_projection_with_cause_count": len(
                    [item for item in stale_projections if isinstance(item.get("latest_writer"), dict)]
                ),
                "stale_data_update_count": int(update_type_counts.get("data", 0)),
                "stale_logic_update_count": int(update_type_counts.get("logic", 0)),
            },
            "warnings": warnings,
        }

    async def _latest_upstream_occurred_at(
        self,
        *,
        db_name: Optional[str],
        branch: Optional[str],
        as_of: datetime,
    ) -> Optional[datetime]:
        latest_upstream_edges = await self._lineage_store.list_edges(
            edge_type=EDGE_AGGREGATE_EMITTED_EVENT,
            db_name=db_name,
            branch=branch,
            until=as_of,
            limit=1,
        )
        if not latest_upstream_edges:
            return None
        return _to_utc_datetime(latest_upstream_edges[0].get("occurred_at"))

    async def _collect_artifacts(
        self,
        *,
        db_name: Optional[str],
        branch: Optional[str],
        artifact_kind: Optional[str],
        as_of: datetime,
        freshness_slo_minutes: int,
        artifact_limit: int,
        latest_upstream_occurred_at: Optional[datetime],
    ) -> Tuple[List[Dict[str, Any]], int, Dict[str, int]]:
        artifact_rows = await self._lineage_store.list_artifact_latest_writes(
            db_name=db_name,
            branch=branch,
            artifact_kind=artifact_kind,
            as_of=as_of,
            limit=artifact_limit,
        )
        artifacts: List[Dict[str, Any]] = []
        counts: Dict[str, int] = {"healthy": 0, "warning": 0, "critical": 0, "no_data": 0}
        for row in artifact_rows:
            last_occurred_at = _to_utc_datetime(row.get("last_occurred_at"))
            status_name, age_minutes = _freshness_status(
                last_occurred_at=last_occurred_at,
                as_of=as_of,
                freshness_slo_minutes=int(freshness_slo_minutes),
            )
            counts[status_name] = counts.get(status_name, 0) + 1
            artifacts.append(
                {
                    "node_id": row.get("node_id"),
                    "label": row.get("label"),
                    "kind": row.get("artifact_kind"),
                    "status": status_name,
                    "staleness_reason": _staleness_reason_with_scope(status_name=status_name, out_of_date_scope="none"),
                    "update_type": "none",
                    "age_minutes": age_minutes,
                    "upstream_gap_minutes": _upstream_gap_minutes(
                        last_occurred_at=last_occurred_at,
                        latest_upstream_occurred_at=latest_upstream_occurred_at,
                    ),
                    "upstream_latest_event_at": (
                        latest_upstream_occurred_at.isoformat() if latest_upstream_occurred_at else None
                    ),
                    "last_occurred_at": last_occurred_at.isoformat() if last_occurred_at else None,
                    "write_count": int(row.get("write_count") or 0),
                    "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
                }
            )
        artifacts.sort(
            key=lambda item: (
                _status_rank(str(item.get("status"))),
                -float(item.get("age_minutes") or 0.0),
            )
        )
        return artifacts, len(artifact_rows), counts

    async def _collect_projections(
        self,
        *,
        db_name: Optional[str],
        branch: Optional[str],
        as_of: datetime,
        freshness_slo_minutes: int,
        projection_limit: int,
        latest_upstream_occurred_at: Optional[datetime],
    ) -> Tuple[List[Dict[str, Any]], int, Dict[str, int]]:
        projection_rows = await self._lineage_store.list_projection_latest_writes(
            db_name=db_name,
            branch=branch,
            as_of=as_of,
            limit=projection_limit,
        )
        projections: List[Dict[str, Any]] = []
        counts: Dict[str, int] = {"healthy": 0, "warning": 0, "critical": 0, "no_data": 0}
        for row in projection_rows:
            last_occurred_at = _to_utc_datetime(row.get("last_occurred_at"))
            status_name, age_minutes = _freshness_status(
                last_occurred_at=last_occurred_at,
                as_of=as_of,
                freshness_slo_minutes=int(freshness_slo_minutes),
            )
            counts[status_name] = counts.get(status_name, 0) + 1
            projections.append(
                {
                    "projection_name": row.get("projection_name"),
                    "status": status_name,
                    "staleness_reason": _staleness_reason_with_scope(status_name=status_name, out_of_date_scope="none"),
                    "update_type": "none",
                    "age_minutes": age_minutes,
                    "upstream_gap_minutes": _upstream_gap_minutes(
                        last_occurred_at=last_occurred_at,
                        latest_upstream_occurred_at=latest_upstream_occurred_at,
                    ),
                    "upstream_latest_event_at": (
                        latest_upstream_occurred_at.isoformat() if latest_upstream_occurred_at else None
                    ),
                    "last_occurred_at": last_occurred_at.isoformat() if last_occurred_at else None,
                    "edge_count": int(row.get("edge_count") or 0),
                }
            )
        projections.sort(
            key=lambda item: (
                _status_rank(str(item.get("status"))),
                -float(item.get("age_minutes") or 0.0),
            )
        )
        return projections, len(projection_rows), counts

    async def _annotate_scope_and_update_type(
        self,
        *,
        stale_artifacts_all: List[Dict[str, Any]],
        stale_projections_all: List[Dict[str, Any]],
        latest_upstream_occurred_at: Optional[datetime],
        db_name: Optional[str],
        branch: Optional[str],
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        stale_projection_names = [
            str(item.get("projection_name") or "") for item in stale_projections_all if item.get("projection_name")
        ]
        stale_artifact_projection_names = [
            projection_name
            for projection_name in (
                _projection_name_from_latest_writer(item.get("latest_writer")) for item in stale_artifacts_all
            )
            if projection_name
        ]
        projection_names_for_lookup = list(dict.fromkeys([*stale_projection_names, *stale_artifact_projection_names]))
        projection_latest_writers = await _get_latest_edges_for_projections_batched(
            lineage_store=self._lineage_store,
            projection_names=projection_names_for_lookup,
            db_name=db_name,
            branch=branch,
        )
        for item in stale_projections_all:
            projection_name = str(item.get("projection_name") or "")
            item["latest_writer"] = _edge_cause_payload(projection_latest_writers.get(projection_name))

        stale_items_for_scope: List[Dict[str, Any]] = [*stale_artifacts_all, *stale_projections_all]
        writer_event_node_ids = [
            event_node_id
            for event_node_id in (
                _event_node_id_from_latest_writer(item.get("latest_writer")) for item in stale_items_for_scope
            )
            if event_node_id
        ]
        writer_event_node_ids = list(dict.fromkeys(writer_event_node_ids))
        parent_edges_by_event = await _get_latest_edges_to_batched(
            lineage_store=self._lineage_store,
            to_node_ids=writer_event_node_ids,
            edge_type=EDGE_AGGREGATE_EMITTED_EVENT,
            db_name=db_name,
            branch=branch,
        )
        aggregate_node_ids = [
            str(edge.get("from_node_id") or "")
            for edge in parent_edges_by_event.values()
            if isinstance(edge, dict) and str(edge.get("from_node_id") or "").startswith("agg:")
        ]
        aggregate_node_ids = list(dict.fromkeys([node_id for node_id in aggregate_node_ids if node_id]))
        parent_latest_edges_by_aggregate = await _get_latest_edges_from_batched(
            lineage_store=self._lineage_store,
            from_node_ids=aggregate_node_ids,
            edge_type=EDGE_AGGREGATE_EMITTED_EVENT,
            db_name=db_name,
            branch=branch,
        )

        out_of_date_scope_counts: Dict[str, int] = {"parent": 0, "ancestor": 0, "none": 0}
        update_type_counts: Dict[str, int] = {"data": 0, "logic": 0, "none": 0}
        for item in stale_items_for_scope:
            status_name = str(item.get("status") or "")
            last_occurred_at = _to_utc_datetime(item.get("last_occurred_at"))
            writer_event_node_id = _event_node_id_from_latest_writer(item.get("latest_writer"))
            projection_name = _projection_name_from_latest_writer(item.get("latest_writer"))
            current_code_sha = _writer_code_sha(item.get("latest_writer"))
            parent_aggregate_node_id: Optional[str] = None
            parent_latest_occurred_at: Optional[datetime] = None

            if writer_event_node_id:
                parent_edge = parent_edges_by_event.get(writer_event_node_id)
                if isinstance(parent_edge, dict):
                    candidate_parent_node_id = str(parent_edge.get("from_node_id") or "")
                    if candidate_parent_node_id.startswith("agg:"):
                        parent_aggregate_node_id = candidate_parent_node_id
                        parent_latest_edge = parent_latest_edges_by_aggregate.get(parent_aggregate_node_id)
                        if isinstance(parent_latest_edge, dict):
                            parent_latest_occurred_at = _to_utc_datetime(parent_latest_edge.get("occurred_at"))

            out_of_date_scope = _out_of_date_scope(
                status_name=status_name,
                last_occurred_at=last_occurred_at,
                parent_latest_occurred_at=parent_latest_occurred_at,
                latest_upstream_occurred_at=latest_upstream_occurred_at,
            )
            out_of_date_scope_counts[out_of_date_scope] = out_of_date_scope_counts.get(out_of_date_scope, 0) + 1
            item["out_of_date_scope"] = out_of_date_scope
            item["writer_event_node_id"] = writer_event_node_id
            item["parent_aggregate_node_id"] = parent_aggregate_node_id
            item["parent_latest_event_at"] = parent_latest_occurred_at.isoformat() if parent_latest_occurred_at else None
            item["parent_gap_minutes"] = _upstream_gap_minutes(
                last_occurred_at=last_occurred_at,
                latest_upstream_occurred_at=parent_latest_occurred_at,
            )
            item["staleness_reason"] = _staleness_reason_with_scope(
                status_name=status_name,
                out_of_date_scope=out_of_date_scope,
            )

            latest_projection_occurred_at: Optional[datetime] = None
            latest_projection_code_sha: Optional[str] = None
            if projection_name:
                latest_projection_edge = projection_latest_writers.get(projection_name)
                if isinstance(latest_projection_edge, dict):
                    latest_projection_payload = _edge_cause_payload(latest_projection_edge)
                    latest_projection_occurred_at = _to_utc_datetime(latest_projection_edge.get("occurred_at"))
                    latest_projection_code_sha = _writer_code_sha(latest_projection_payload)
                    item["latest_projection_event_at"] = (
                        latest_projection_occurred_at.isoformat() if latest_projection_occurred_at else None
                    )
                    item["latest_projection_code_sha"] = latest_projection_code_sha
                    item["latest_projection_gap_minutes"] = _upstream_gap_minutes(
                        last_occurred_at=last_occurred_at,
                        latest_upstream_occurred_at=latest_projection_occurred_at,
                    )

            update_type = _update_type(
                status_name=status_name,
                out_of_date_scope=out_of_date_scope,
                last_occurred_at=last_occurred_at,
                latest_projection_occurred_at=latest_projection_occurred_at,
                current_code_sha=current_code_sha,
                latest_projection_code_sha=latest_projection_code_sha,
            )
            item["update_type"] = update_type
            update_type_counts[update_type] = update_type_counts.get(update_type, 0) + 1
        return out_of_date_scope_counts, update_type_counts

    @staticmethod
    def _staleness_reason_counts(
        *,
        artifacts: Sequence[Dict[str, Any]],
        projections: Sequence[Dict[str, Any]],
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for item in [*artifacts, *projections]:
            reason = str(item.get("staleness_reason") or "")
            if not reason:
                continue
            counts[reason] = counts.get(reason, 0) + 1
        return counts

    @staticmethod
    def _warnings(
        *,
        artifact_rows_count: int,
        artifact_limit: int,
        projection_rows_count: int,
        projection_limit: int,
        stale_artifact_count: int,
        stale_preview_limit: int,
        stale_projection_count: int,
        projection_preview_limit: int,
    ) -> List[str]:
        warnings: List[str] = []
        if artifact_rows_count >= int(artifact_limit):
            warnings.append(f"artifact_limit_reached(limit={int(artifact_limit)})")
        if projection_rows_count >= int(projection_limit):
            warnings.append(f"projection_limit_reached(limit={int(projection_limit)})")
        if stale_artifact_count > int(stale_preview_limit):
            warnings.append(f"stale_artifact_preview_truncated(limit={int(stale_preview_limit)})")
        if stale_projection_count > int(projection_preview_limit):
            warnings.append(f"stale_projection_preview_truncated(limit={int(projection_preview_limit)})")
        return warnings
