"""
Lineage (provenance) query router for BFF.

Exposes first-class, queryable lineage graphs to the frontend via BFF.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, status

from shared.dependencies.providers import LineageStoreDep
from shared.dependencies.providers import AuditLogStoreDep
from shared.models.lineage import LineageDirection
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import validate_db_name

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
        elif kind == "terminus":
            actions.append(
                {
                    "action": "REPLAY_TO_TARGET",
                    "target_kind": "terminus",
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


@router.get("/graph")
async def get_lineage_graph(
    root: str = Query(..., description="Root node id or event id (e.g. event:<uuid> or <uuid>)"),
    db_name: Optional[str] = Query(None, description="Optional database scope (recommended)"),
    direction: LineageDirection = Query("both", description="Traversal direction"),
    max_depth: int = Query(5, ge=0, le=50),
    max_nodes: int = Query(500, ge=1, le=20000),
    max_edges: int = Query(2000, ge=1, le=50000),
    *,
    lineage_store: LineageStoreDep,
):
    try:
        if db_name:
            db_name = validate_db_name(db_name)
        graph = await lineage_store.get_graph(
            root=root,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
        )
        return ApiResponse.success(
            message="Lineage graph fetched",
            data={"graph": graph.model_dump(mode="json")},
        ).to_dict()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@router.get("/impact")
async def get_lineage_impact(
    root: str = Query(..., description="Root node id or event id"),
    db_name: Optional[str] = Query(None, description="Optional database scope (recommended)"),
    direction: LineageDirection = Query("downstream", description="Impact direction (usually downstream)"),
    max_depth: int = Query(10, ge=0, le=50),
    artifact_kind: Optional[str] = Query(None, description="Filter by artifact kind (es|s3|terminus|...)"),
    max_nodes: int = Query(2000, ge=1, le=20000),
    max_edges: int = Query(5000, ge=1, le=50000),
    *,
    lineage_store: LineageStoreDep,
):
    try:
        if db_name:
            db_name = validate_db_name(db_name)
        graph = await lineage_store.get_graph(
            root=root,
            direction=direction,
            max_depth=max_depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            db_name=db_name,
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@router.get("/metrics")
async def get_lineage_metrics(
    db_name: Optional[str] = Query(None, description="Database scope (recommended)"),
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
        if db_name:
            db_name = validate_db_name(db_name)
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
            edge_type="aggregate_emitted_event",
            db_name=db_name,
            since=since,
            until=now,
        )
        missing_est = max(0, int(events_appended) - int(lineage_recorded))
        missing_ratio = (missing_est / events_appended) if events_appended else 0.0

        backfill = await lineage_store.get_backfill_metrics(db_name=db_name)

        return ApiResponse.success(
            message="Lineage metrics fetched",
            data={
                "db_name": db_name,
                "window_minutes": int(window_minutes),
                "events_appended": int(events_appended),
                "lineage_edges_recorded": int(lineage_recorded),
                "missing_lineage_estimate": missing_est,
                "missing_lineage_ratio_estimate": missing_ratio,
                "backfill_queue": backfill,
                "timestamp": now.isoformat(),
            },
        ).to_dict()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e
