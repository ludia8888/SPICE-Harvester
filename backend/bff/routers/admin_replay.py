"""Admin replay endpoints (BFF).

Replays instance command history from the instance snapshot bucket and stores
results in Redis for later inspection. Composed by `bff.routers.admin`.

This router is intentionally thin: business logic lives in
`bff.services.admin_replay_service` (Facade), and request/response schemas live
in `bff.schemas.admin_replay_requests`.
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query

from bff.dependencies import get_storage_service
from bff.schemas.admin_replay_requests import ReplayInstanceStateRequest, ReplayInstanceStateResponse
from bff.services import admin_replay_service
from shared.dependencies.providers import AuditLogStoreDep, LineageStoreDep, RedisServiceDep
from shared.dependencies.providers import BackgroundTaskManagerDep
from shared.models.lineage import LineageDirection
from shared.services.storage.storage_service import StorageService

router = APIRouter(tags=["Admin Operations"])


@router.post("/replay-instance-state", response_model=ReplayInstanceStateResponse)
@trace_endpoint("bff.admin.replay_instance_state")
async def replay_instance_state(
    request: ReplayInstanceStateRequest,
    background_tasks: BackgroundTasks,
    storage_service: StorageService = Depends(get_storage_service),
    *,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
) -> ReplayInstanceStateResponse:
    return await admin_replay_service.start_replay_instance_state(
        request=request,
        background_tasks=background_tasks,
        storage_service=storage_service,
        task_manager=task_manager,
        redis_service=redis_service,
    )


@router.get("/replay-instance-state/{task_id}/result")
@trace_endpoint("bff.admin.get_replay_result")
async def get_replay_result(
    task_id: str,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
) -> Dict[str, Any]:
    return await admin_replay_service.get_replay_result(
        task_id=task_id,
        task_manager=task_manager,
        redis_service=redis_service,
    )


@router.get("/replay-instance-state/{task_id}/trace")
@trace_endpoint("bff.admin.get_replay_trace")
async def get_replay_trace(
    task_id: str,
    command_id: Optional[str] = Query(
        None,
        description="Command/event id to trace (defaults to last command in the replayed history)",
    ),
    include_audit: bool = Query(True, description="Include audit logs for the selected command"),
    audit_limit: int = Query(200, ge=1, le=1000),
    include_lineage: bool = Query(True, description="Include lineage graph for the selected command"),
    lineage_direction: LineageDirection = Query("both", description="Lineage traversal direction"),
    lineage_max_depth: int = Query(5, ge=0, le=50),
    lineage_max_nodes: int = Query(500, ge=1, le=20000),
    lineage_max_edges: int = Query(2000, ge=1, le=50000),
    timeline_limit: int = Query(200, ge=1, le=2000, description="Max command history items to include"),
    *,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    lineage_store: LineageStoreDep,
) -> Dict[str, Any]:
    return await admin_replay_service.get_replay_trace(
        task_id=task_id,
        command_id=command_id,
        include_audit=include_audit,
        audit_limit=audit_limit,
        include_lineage=include_lineage,
        lineage_direction=lineage_direction,
        lineage_max_depth=lineage_max_depth,
        lineage_max_nodes=lineage_max_nodes,
        lineage_max_edges=lineage_max_edges,
        timeline_limit=timeline_limit,
        task_manager=task_manager,
        redis_service=redis_service,
        audit_store=audit_store,
        lineage_store=lineage_store,
    )


@router.post("/cleanup-old-replays")
@trace_endpoint("bff.admin.cleanup_old_replay_results")
async def cleanup_old_replay_results(
    older_than_hours: int = Query(24, ge=1, le=168, description="Delete results older than N hours"),
    *,
    redis_service: RedisServiceDep,
) -> Dict[str, Any]:
    return await admin_replay_service.cleanup_old_replay_results(
        older_than_hours=older_than_hours,
        redis_service=redis_service,
    )
