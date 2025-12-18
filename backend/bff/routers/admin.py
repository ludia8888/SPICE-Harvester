"""
Admin API Router

Provides administrative endpoints for system management,
including instance state replay and other long-running operations.

This router addresses Anti-pattern 14 by using BackgroundTasks
and BackgroundTaskManager for all long-running operations.
"""

import json
import hmac
import logging
import os
from typing import Dict, Any, Optional, Literal
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from bff.dependencies import get_oms_client, get_storage_service, get_elasticsearch_service
from bff.services.oms_client import OMSClient
from shared.services.event_store import EventStore
from shared.config.settings import settings
from shared.config.search_config import (
    get_instances_index_name,
    get_ontologies_index_name,
    DEFAULT_INDEX_SETTINGS,
)
from shared.dependencies.providers import AuditLogStoreDep, LineageStoreDep
from shared.models.lineage import LineageDirection
from shared.services.storage_service import StorageService
from shared.services.background_task_manager import (
    BackgroundTaskManager,
    create_background_task_manager,
)
from shared.services.redis_service import RedisService, create_redis_service
from shared.services.elasticsearch_service import ElasticsearchService
from shared.dependencies import get_container, ServiceContainer
from shared.models.background_task import TaskStatus
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets
from shared.utils.ontology_version import split_ref_commit

logger = logging.getLogger(__name__)

def _get_configured_admin_token() -> Optional[str]:
    for key in ("BFF_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _extract_presented_admin_token(request: Request) -> Optional[str]:
    raw = (request.headers.get("X-Admin-Token") or "").strip()
    if raw:
        return raw
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    return None


async def require_admin(request: Request) -> None:
    """
    Minimal admin guard for operational endpoints.

    Contract:
    - Requires a shared secret token in `X-Admin-Token` or `Authorization: Bearer ...`.
    - If token is not configured, admin endpoints are effectively disabled.
    """
    expected = _get_configured_admin_token()
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin endpoints are disabled (set BFF_ADMIN_TOKEN to enable)",
        )

    presented = _extract_presented_admin_token(request)
    if not presented or not hmac.compare_digest(presented, expected):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin authorization failed")

    actor = (request.headers.get("X-Admin-Actor") or "admin").strip() or "admin"
    request.state.admin_actor = actor


router = APIRouter(prefix="/admin", tags=["Admin Operations"], dependencies=[Depends(require_admin)])


# Request/Response Models
class ReplayInstanceStateRequest(BaseModel):
    """Request model for instance state replay."""
    db_name: str = Field(..., description="Database name")
    class_id: str = Field(..., description="Class ID")
    instance_id: str = Field(..., description="Instance ID")
    store_result: bool = Field(default=True, description="Store result in Redis")
    result_ttl: int = Field(default=3600, description="Result TTL in seconds")


class ReplayInstanceStateResponse(BaseModel):
    """Response model for instance state replay."""
    task_id: str = Field(..., description="Background task ID")
    status: str = Field(..., description="Task status")
    message: str = Field(..., description="Status message")
    status_url: str = Field(..., description="URL to check task status")


class RecomputeProjectionRequest(BaseModel):
    """Request model for projection recompute (Versioning + Recompute)."""

    db_name: str = Field(..., description="Database name")
    projection: Literal["instances", "ontologies"] = Field(
        ...,
        description="Projection to rebuild (instances|ontologies)",
    )
    branch: str = Field(default="main", description="Branch scope (default: main)")
    from_ts: datetime = Field(..., description="Start timestamp (ISO8601; timezone-aware recommended)")
    to_ts: Optional[datetime] = Field(default=None, description="End timestamp (default: now)")
    promote: bool = Field(
        default=False,
        description="Promote the rebuilt index to the base index name (alias swap).",
    )
    allow_delete_base_index: bool = Field(
        default=False,
        description="If base index exists as a concrete index (not alias), allow deleting it to create an alias.",
    )
    max_events: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional safety limit for number of events to process",
    )


class RecomputeProjectionResponse(BaseModel):
    """Response model for projection recompute."""

    task_id: str = Field(..., description="Background task ID")
    status: str = Field(..., description="Task status")
    message: str = Field(..., description="Status message")
    status_url: str = Field(..., description="URL to check task status")


# Dependency injection
async def get_task_manager(
    container: ServiceContainer = Depends(get_container)
) -> BackgroundTaskManager:
    """Get BackgroundTaskManager from container."""
    if container.has(BackgroundTaskManager) and container.is_created(BackgroundTaskManager):
        return await container.get(BackgroundTaskManager)

    if not container.has(RedisService):
        container.register_singleton(RedisService, create_redis_service)
    redis_service = await container.get(RedisService)

    task_manager = create_background_task_manager(redis_service)
    container.register_instance(BackgroundTaskManager, task_manager)
    return task_manager


async def get_redis_service(
    container: ServiceContainer = Depends(get_container)
) -> RedisService:
    """Get RedisService from container."""
    if not container.has(RedisService):
        container.register_singleton(RedisService, create_redis_service)
    return await container.get(RedisService)


# API Endpoints
@router.post("/replay-instance-state", response_model=ReplayInstanceStateResponse)
async def replay_instance_state(
    request: ReplayInstanceStateRequest,
    background_tasks: BackgroundTasks,
    storage_service: StorageService = Depends(get_storage_service),
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service)
) -> ReplayInstanceStateResponse:
    """
    Replay instance state from event store.
    
    This endpoint reconstructs the complete state of an instance by
    replaying all commands from the event store. This is a potentially
    long-running operation that runs in the background.
    
    The reconstructed state includes:
    - Current data state
    - Complete command history
    - Deletion information (if deleted)
    - Version information
    
    Results are stored in Redis with configurable TTL.
    """
    # Generate task ID
    task_id = str(uuid4())
    
    # Create background task with BackgroundTaskManager
    background_task_id = await task_manager.create_task(
        _replay_instance_state_task,
        task_id=task_id,
        request=request,
        storage_service=storage_service,
        redis_service=redis_service,
        task_name=f"Replay instance state: {request.instance_id}",
        task_type="instance_state_replay",
        metadata={
            "db_name": request.db_name,
            "class_id": request.class_id,
            "instance_id": request.instance_id,
            "requested_at": datetime.now(timezone.utc).isoformat()
        }
    )
    
    # Also add to FastAPI background tasks for redundancy
    background_tasks.add_task(
        _monitor_replay_task,
        task_id=background_task_id,
        task_manager=task_manager
    )
    
    return ReplayInstanceStateResponse(
        task_id=background_task_id,
        status="accepted",
        message=f"Instance state replay started for {request.instance_id}",
        status_url=f"/api/v1/tasks/{background_task_id}"
    )


@router.get("/replay-instance-state/{task_id}/result")
async def get_replay_result(
    task_id: str,
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service)
) -> Dict[str, Any]:
    """
    Get the result of instance state replay.
    
    Returns the reconstructed instance state if the task is completed.
    """
    # Check task status
    task = await task_manager.get_task_status(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found"
        )
    
    if not task.is_complete:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task {task_id} is not complete. Current status: {task.status.value}"
        )
    
    if task.status == TaskStatus.FAILED:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": task.result.error if task.result else "Unknown error",
            "message": "Instance state replay failed"
        }
    
    # Get result from Redis
    result_key = f"replay_result:{task_id}"
    result = await redis_service.get_json(result_key)
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Result for task {task_id} not found or expired"
        )
    
    return {
        "task_id": task_id,
        "status": "completed",
        "instance_state": result.get("instance_state"),
        "is_deleted": result.get("is_deleted", False),
        "deletion_info": result.get("deletion_info"),
        "command_count": result.get("command_count", 0),
        "replayed_at": result.get("replayed_at")
    }


@router.get("/replay-instance-state/{task_id}/trace")
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
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service),
    audit_store: AuditLogStoreDep,
    lineage_store: LineageStoreDep,
) -> Dict[str, Any]:
    """
    Trace a replayed instance state into Audit + Lineage.

    This is an "admin time machine": it does not roll back the system,
    but it reconstructs the instance state and links each change to:
    - structured audit logs (who/what/when)
    - lineage graph (which artifacts were written as side-effects)
    """
    # Ensure task exists and is complete
    task = await task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found")
    if not task.is_complete:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task {task_id} is not complete. Current status: {task.status.value}",
        )
    if task.status == TaskStatus.FAILED:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": task.result.error if task.result else "Unknown error",
            "message": "Instance state replay failed",
        }

    # Load replay output
    result_key = f"replay_result:{task_id}"
    result = await redis_service.get_json(result_key)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Result for task {task_id} not found or expired",
        )

    instance_state = result.get("instance_state")
    meta = instance_state.get("_metadata", {}) if isinstance(instance_state, dict) else {}
    history = meta.get("command_history") if isinstance(meta, dict) else None
    command_history = history if isinstance(history, list) else []

    # Resolve target identifiers (best-effort)
    db_name = (task.metadata or {}).get("db_name") if isinstance(task.metadata, dict) else None
    class_id = (task.metadata or {}).get("class_id") if isinstance(task.metadata, dict) else None
    instance_id = (task.metadata or {}).get("instance_id") if isinstance(task.metadata, dict) else None
    if isinstance(instance_state, dict):
        db_name = db_name or instance_state.get("db_name")
        class_id = class_id or instance_state.get("class_id")
        instance_id = instance_id or instance_state.get("instance_id")

    # Select which command to trace (default: last)
    selected = None
    if command_history:
        if command_id:
            for entry in command_history:
                if str(entry.get("command_id") or "") == command_id:
                    selected = entry
                    break
            if not selected:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"command_id '{command_id}' not found in replay history",
                )
        else:
            selected = command_history[-1]

    selected_command_id = str((selected or {}).get("command_id") or "") if selected else ""

    # Build timeline with direct drill-down links
    timeline: list[Dict[str, Any]] = []
    for entry in command_history[-int(timeline_limit) :]:
        cid = str(entry.get("command_id") or "")
        timeline.append(
            {
                "command_id": cid or None,
                "command_type": entry.get("command_type"),
                "timestamp": entry.get("timestamp"),
                "file": entry.get("file"),
                "links": {
                    "audit": f"/api/v1/audit/logs?partition_key=db:{db_name}&command_id={cid}" if (db_name and cid) else None,
                    "lineage_graph": f"/api/v1/lineage/graph?db_name={db_name}&root={cid}&direction=both" if (db_name and cid) else None,
                    "lineage_impact": f"/api/v1/lineage/impact?db_name={db_name}&root={cid}&direction=downstream" if (db_name and cid) else None,
                },
            }
        )

    audit: Optional[Dict[str, Any]] = None
    audit_error: Optional[str] = None
    if include_audit and db_name and selected_command_id:
        try:
            items = await audit_store.list_logs(
                partition_key=f"db:{db_name}",
                command_id=selected_command_id,
                limit=audit_limit,
                offset=0,
            )
            if not items:
                # Some producers may only set event_id; best-effort fallback.
                items = await audit_store.list_logs(
                    partition_key=f"db:{db_name}",
                    event_id=selected_command_id,
                    limit=audit_limit,
                    offset=0,
                )
            audit = {
                "count": len(items),
                "items": [item.model_dump(mode="json") for item in items],
            }
        except Exception as e:
            audit_error = str(e)

    lineage: Optional[Dict[str, Any]] = None
    lineage_error: Optional[str] = None
    if include_lineage and db_name and selected_command_id:
        try:
            graph = await lineage_store.get_graph(
                root=selected_command_id,
                db_name=str(db_name),
                direction=lineage_direction,
                max_depth=lineage_max_depth,
                max_nodes=lineage_max_nodes,
                max_edges=lineage_max_edges,
            )
            lineage = graph.model_dump(mode="json")
        except Exception as e:
            lineage_error = str(e)

    return {
        "task_id": task_id,
        "status": "completed",
        "target": {"db_name": db_name, "class_id": class_id, "instance_id": instance_id},
        "replay": {
            "instance_state": instance_state,
            "is_deleted": result.get("is_deleted", False),
            "deletion_info": result.get("deletion_info"),
            "command_count": result.get("command_count", 0),
            "replayed_at": result.get("replayed_at"),
        },
        "timeline": timeline,
        "selected": {
            "command_id": selected_command_id or None,
            "command_type": (selected or {}).get("command_type") if selected else None,
            "timestamp": (selected or {}).get("timestamp") if selected else None,
            "file": (selected or {}).get("file") if selected else None,
            "audit": audit,
            "audit_error": audit_error,
            "lineage": lineage,
            "lineage_error": lineage_error,
        },
    }


@router.post(
    "/recompute-projection",
    response_model=RecomputeProjectionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@rate_limit(**RateLimitPresets.STRICT)
async def recompute_projection(
    http_request: Request,
    request: RecomputeProjectionRequest,
    background_tasks: BackgroundTasks,
    *,
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    audit_store: AuditLogStoreDep,
    lineage_store: LineageStoreDep,
) -> RecomputeProjectionResponse:
    """
    Recompute (re-materialize) an Elasticsearch projection from the Event Store.

    This is the "Versioning + Recompute" path:
    - We do NOT roll back ontology state in-place.
    - We rebuild read models by replaying immutable domain events.
    """
    task_id = str(uuid4())
    requested_by = getattr(http_request.state, "admin_actor", None)
    request_ip = getattr(getattr(http_request, "client", None), "host", None)

    background_task_id = await task_manager.create_task(
        _recompute_projection_task,
        task_id=task_id,
        request=request,
        elasticsearch_service=elasticsearch_service,
        redis_service=redis_service,
        audit_store=audit_store,
        lineage_store=lineage_store,
        requested_by=requested_by,
        request_ip=request_ip,
        task_name=f"Recompute projection: {request.projection} ({request.db_name})",
        task_type="projection_recompute",
        metadata={
            "db_name": request.db_name,
            "branch": request.branch,
            "projection": request.projection,
            "requested_by": requested_by,
            "request_ip": request_ip,
            "requested_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    background_tasks.add_task(_monitor_admin_task, task_id=background_task_id, task_manager=task_manager)

    return RecomputeProjectionResponse(
        task_id=background_task_id,
        status="accepted",
        message=f"Projection recompute task started: {request.projection}",
        status_url=f"/api/v1/tasks/{background_task_id}",
    )


@router.get("/recompute-projection/{task_id}/result")
async def get_recompute_projection_result(
    task_id: str,
    *,
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service),
) -> Dict[str, Any]:
    """
    Get the result of a projection recompute task.
    """
    task = await task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found")
    if not task.is_complete:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task {task_id} is not complete. Current status: {task.status.value}",
        )
    if task.status == TaskStatus.FAILED:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": task.result.error if task.result else "Unknown error",
            "message": "Projection recompute failed",
        }

    result_key = f"recompute_result:{task_id}"
    result = await redis_service.get_json(result_key)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Result for task {task_id} not found or expired",
        )
    return result


# Background task implementations
async def _replay_instance_state_task(
    task_id: str,
    request: ReplayInstanceStateRequest,
    storage_service: StorageService,
    redis_service: RedisService
) -> None:
    """
    Background task to replay instance state.
    
    This function demonstrates proper background task implementation
    without fire-and-forget patterns.
    """
    try:
        logger.info(f"Starting instance state replay for {request.instance_id}")
        
        # Reconstruct from the same S3 bucket used by the instance worker snapshots.
        # (See `INSTANCE_BUCKET` / `settings.storage.instance_bucket`; default: `instance-events`)
        bucket = settings.storage.instance_bucket
        
        # Get all command files for the instance
        command_files = await storage_service.get_all_commands_for_instance(
            bucket=bucket,
            db_name=request.db_name,
            class_id=request.class_id,
            instance_id=request.instance_id
        )
        
        if not command_files:
            # No commands found
            result = {
                "instance_state": None,
                "is_deleted": False,
                "deletion_info": None,
                "command_count": 0,
                "replayed_at": datetime.now(timezone.utc).isoformat(),
                "message": "No commands found for instance"
            }
        else:
            # Replay instance state
            instance_state = await storage_service.replay_instance_state(
                bucket=bucket,
                command_files=command_files
            )
            
            # Check if instance is deleted
            is_deleted = storage_service.is_instance_deleted(instance_state)
            deletion_info = storage_service.get_deletion_info(instance_state) if is_deleted else None
            
            result = {
                "instance_state": instance_state,
                "is_deleted": is_deleted,
                "deletion_info": deletion_info,
                "command_count": len(command_files),
                "replayed_at": datetime.now(timezone.utc).isoformat()
            }
        
        # Store result in Redis if requested
        if request.store_result:
            result_key = f"replay_result:{task_id}"
            await redis_service.set_json(
                key=result_key,
                value=result,
                ttl=request.result_ttl
            )
            logger.info(f"Stored replay result for task {task_id} with TTL {request.result_ttl}s")
        
        logger.info(f"Completed instance state replay for {request.instance_id}")
        
    except Exception as e:
        logger.error(f"Failed to replay instance state for {request.instance_id}: {e}")
        raise


def _normalize_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_projection_mapping(*, projection: str) -> Dict[str, Any]:
    filename = "instances_mapping.json" if projection == "instances" else "ontologies_mapping.json"
    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    mapping_path = os.path.join(backend_dir, "projection_worker", "mappings", filename)
    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)


async def _ensure_es_connected(es: ElasticsearchService) -> None:
    if getattr(es, "_client", None) is None:
        await es.connect()


async def _monitor_replay_task(
    task_id: str,
    task_manager: BackgroundTaskManager
) -> None:
    """
    Monitor the replay task and log completion.
    
    This demonstrates how to track task completion without blocking.
    """
    import asyncio
    
    # Wait a moment for task to start
    await asyncio.sleep(1)
    
    # Check task status periodically
    max_checks = 60  # Check for up to 5 minutes
    check_interval = 5  # Check every 5 seconds
    
    for i in range(max_checks):
        task = await task_manager.get_task_status(task_id)
        
        if task and task.is_complete:
            if task.status == TaskStatus.COMPLETED:
                logger.info(f"Replay task {task_id} completed successfully")
            else:
                logger.error(f"Replay task {task_id} failed: {task.result.error if task.result else 'Unknown error'}")
            break
        
        await asyncio.sleep(check_interval)
    else:
        logger.warning(f"Replay task {task_id} did not complete within monitoring window")


async def _monitor_admin_task(task_id: str, task_manager: BackgroundTaskManager) -> None:
    """
    Generic monitor for admin background tasks.
    """
    import asyncio

    await asyncio.sleep(1)

    max_checks = 60
    check_interval = 5

    for _ in range(max_checks):
        task = await task_manager.get_task_status(task_id)
        if task and task.is_complete:
            if task.status == TaskStatus.COMPLETED:
                logger.info(f"Admin task {task_id} completed successfully")
            else:
                logger.error(f"Admin task {task_id} failed: {task.result.error if task.result else 'Unknown error'}")
            return
        await asyncio.sleep(check_interval)

    logger.warning(f"Admin task {task_id} did not complete within monitoring window")


async def _recompute_projection_task(
    task_id: str,
    request: RecomputeProjectionRequest,
    elasticsearch_service: ElasticsearchService,
    redis_service: RedisService,
    audit_store,
    lineage_store,
    requested_by: Optional[str] = None,
    request_ip: Optional[str] = None,
) -> None:
    """
    Background task: replay domain events from S3/MinIO Event Store to rebuild an ES projection.
    """
    started_at = datetime.now(timezone.utc)

    db_name = validate_db_name(request.db_name)
    branch = validate_branch_name(request.branch or "main")
    projection = request.projection
    from_dt = _normalize_dt(request.from_ts)
    to_dt = _normalize_dt(request.to_ts) if request.to_ts else datetime.now(timezone.utc)

    if to_dt < from_dt:
        raise ValueError("to_ts must be >= from_ts")

    await _ensure_es_connected(elasticsearch_service)

    # Resolve index names (base + versioned rebuild target)
    version_suffix = f"v{int(started_at.timestamp())}"
    if projection == "instances":
        base_index = get_instances_index_name(db_name, branch=branch)
        new_index = get_instances_index_name(db_name, version=version_suffix, branch=branch)
        event_types = ["INSTANCE_CREATED", "INSTANCE_UPDATED", "INSTANCE_DELETED"]
    else:
        base_index = get_ontologies_index_name(db_name, branch=branch)
        new_index = get_ontologies_index_name(db_name, version=version_suffix, branch=branch)
        event_types = ["ONTOLOGY_CLASS_CREATED", "ONTOLOGY_CLASS_UPDATED", "ONTOLOGY_CLASS_DELETED"]

    # Audit: start
    try:
        await audit_store.log(
            partition_key=f"db:{db_name}",
            actor="bff_admin",
            action="RECOMPUTE_PROJECTION_STARTED",
            status="success",
            resource_type="es_index",
            resource_id=new_index,
            metadata={
                "db_name": db_name,
                "branch": branch,
                "projection": projection,
                "base_index": base_index,
                "new_index": new_index,
                "from_ts": from_dt.isoformat(),
                "to_ts": to_dt.isoformat(),
                "promote": bool(request.promote),
                "requested_by": requested_by,
                "request_ip": request_ip,
            },
            occurred_at=started_at,
        )
    except Exception:
        pass

    mapping = _load_projection_mapping(projection=projection)
    settings_payload = dict(mapping.get("settings", {}) or {})
    settings_payload.update(DEFAULT_INDEX_SETTINGS)

    # Create fresh target index (delete if it already exists)
    if await elasticsearch_service.index_exists(new_index):
        await elasticsearch_service.delete_index(new_index)

    await elasticsearch_service.create_index(
        new_index,
        mappings=mapping.get("mappings"),
        settings=settings_payload,
    )

    # Rebuild by replaying events
    event_store = EventStore()
    await event_store.connect()

    processed = 0
    indexed = 0
    deleted = 0
    skipped = 0

    created_at_cache: Dict[str, str] = {}

    async for envelope in event_store.replay_events(from_dt, to_dt, event_types=event_types):
        if request.max_events is not None and processed >= int(request.max_events):
            break

        processed += 1

        data = envelope.data if isinstance(envelope.data, dict) else {}
        if str(data.get("db_name") or "") != db_name:
            skipped += 1
            continue
        if str(data.get("branch") or "main") != branch:
            skipped += 1
            continue

        meta = envelope.metadata if isinstance(envelope.metadata, dict) else {}
        ontology_ref, ontology_commit = split_ref_commit(meta.get("ontology"))

        seq = envelope.sequence_number
        event_ts = envelope.occurred_at
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=timezone.utc)

        if projection == "instances":
            instance_id = data.get("instance_id")
            class_id = data.get("class_id")
            if not instance_id or not class_id:
                skipped += 1
                continue

            doc_id = str(instance_id)
            created_at = created_at_cache.get(doc_id)
            if envelope.event_type == "INSTANCE_CREATED":
                created_at = created_at or event_ts.isoformat()
                created_at_cache.setdefault(doc_id, created_at)

            if envelope.event_type == "INSTANCE_UPDATED":
                if created_at is None:
                    existing = await elasticsearch_service.get_document(new_index, doc_id)
                    created_at = (existing or {}).get("created_at") or event_ts.isoformat()
                    created_at_cache[doc_id] = created_at

            if envelope.event_type in {"INSTANCE_CREATED", "INSTANCE_UPDATED"}:
                doc = {
                    "instance_id": doc_id,
                    "class_id": str(class_id),
                    "class_label": str(class_id),
                    "properties": [],
                    "data": data,
                    "event_id": str(envelope.event_id),
                    "event_sequence": seq,
                    "event_timestamp": event_ts.isoformat(),
                    "version": int(seq or 1),
                    "db_name": db_name,
                    "branch": branch,
                    "ontology_ref": ontology_ref,
                    "ontology_commit": ontology_commit,
                    "created_at": created_at or event_ts.isoformat(),
                    "updated_at": event_ts.isoformat(),
                }
                await elasticsearch_service.index_document(
                    new_index,
                    doc,
                    doc_id=doc_id,
                    refresh=False,
                    version=int(seq) if seq is not None else None,
                    version_type="external_gte" if seq is not None else None,
                )
                indexed += 1
                if lineage_store:
                    try:
                        await lineage_store.record_link(
                            from_node_id=lineage_store.node_event(str(envelope.event_id)),
                            to_node_id=lineage_store.node_artifact("es", new_index, doc_id),
                            edge_type="event_materialized_es_document",
                            occurred_at=event_ts,
                            db_name=db_name,
                            edge_metadata={
                                "projection_name": "recompute",
                                "db_name": db_name,
                                "index": new_index,
                                "doc_id": doc_id,
                                "sequence_number": seq,
                                "ontology_ref": ontology_ref,
                                "ontology_commit": ontology_commit,
                            },
                        )
                    except Exception:
                        pass
                continue

            if envelope.event_type == "INSTANCE_DELETED":
                if branch != "main":
                    tombstone_doc = {
                        "instance_id": doc_id,
                        "class_id": str(class_id),
                        "db_name": db_name,
                        "branch": branch,
                        "ontology_ref": ontology_ref,
                        "ontology_commit": ontology_commit,
                        "deleted": True,
                        "deleted_at": event_ts.isoformat(),
                        "event_id": str(envelope.event_id),
                        "event_sequence": seq,
                        "event_timestamp": event_ts.isoformat(),
                        "version": int(seq or 1),
                        "updated_at": event_ts.isoformat(),
                    }
                    await elasticsearch_service.index_document(
                        new_index,
                        tombstone_doc,
                        doc_id=doc_id,
                        refresh=False,
                        version=int(seq) if seq is not None else None,
                        version_type="external_gte" if seq is not None else None,
                    )
                    indexed += 1
                else:
                    await elasticsearch_service.delete_document(
                        new_index,
                        doc_id,
                        refresh=False,
                        version=int(seq) if seq is not None else None,
                        version_type="external_gte" if seq is not None else None,
                    )
                    deleted += 1
                continue

            skipped += 1
            continue

        # projection == "ontologies"
        class_id = data.get("class_id") or data.get("id")
        if not class_id:
            skipped += 1
            continue
        doc_id = str(class_id)

        created_at = created_at_cache.get(doc_id)
        if envelope.event_type == "ONTOLOGY_CLASS_CREATED":
            created_at = created_at or event_ts.isoformat()
            created_at_cache.setdefault(doc_id, created_at)
        if envelope.event_type == "ONTOLOGY_CLASS_UPDATED":
            if created_at is None:
                existing = await elasticsearch_service.get_document(new_index, doc_id)
                created_at = (existing or {}).get("created_at") or event_ts.isoformat()
                created_at_cache[doc_id] = created_at

        if envelope.event_type in {"ONTOLOGY_CLASS_CREATED", "ONTOLOGY_CLASS_UPDATED"}:
            doc = {
                "class_id": doc_id,
                "label": data.get("label"),
                "description": data.get("description"),
                "properties": data.get("properties", []),
                "relationships": data.get("relationships", []),
                "parent_classes": data.get("parent_classes", []),
                "child_classes": data.get("child_classes", []),
                "db_name": db_name,
                "branch": branch,
                "ontology_ref": ontology_ref,
                "ontology_commit": ontology_commit,
                "version": int(seq or 1),
                "event_id": str(envelope.event_id),
                "event_sequence": seq,
                "event_timestamp": event_ts.isoformat(),
                "created_at": created_at or event_ts.isoformat(),
                "updated_at": event_ts.isoformat(),
            }
            await elasticsearch_service.index_document(
                new_index,
                doc,
                doc_id=doc_id,
                refresh=False,
                version=int(seq) if seq is not None else None,
                version_type="external_gte" if seq is not None else None,
            )
            indexed += 1
            if lineage_store:
                try:
                    await lineage_store.record_link(
                        from_node_id=lineage_store.node_event(str(envelope.event_id)),
                        to_node_id=lineage_store.node_artifact("es", new_index, doc_id),
                        edge_type="event_materialized_es_document",
                        occurred_at=event_ts,
                        db_name=db_name,
                        edge_metadata={
                            "projection_name": "recompute",
                            "db_name": db_name,
                            "index": new_index,
                            "doc_id": doc_id,
                            "sequence_number": seq,
                            "ontology_ref": ontology_ref,
                            "ontology_commit": ontology_commit,
                        },
                    )
                except Exception:
                    pass
            continue

        if envelope.event_type == "ONTOLOGY_CLASS_DELETED":
            if branch != "main":
                tombstone_doc = {
                    "class_id": doc_id,
                    "db_name": db_name,
                    "branch": branch,
                    "ontology_ref": ontology_ref,
                    "ontology_commit": ontology_commit,
                    "deleted": True,
                    "deleted_at": event_ts.isoformat(),
                    "event_id": str(envelope.event_id),
                    "event_sequence": seq,
                    "event_timestamp": event_ts.isoformat(),
                    "version": int(seq or 1),
                    "updated_at": event_ts.isoformat(),
                }
                await elasticsearch_service.index_document(
                    new_index,
                    tombstone_doc,
                    doc_id=doc_id,
                    refresh=False,
                    version=int(seq) if seq is not None else None,
                    version_type="external_gte" if seq is not None else None,
                )
                indexed += 1
            else:
                await elasticsearch_service.delete_document(
                    new_index,
                    doc_id,
                    refresh=False,
                    version=int(seq) if seq is not None else None,
                    version_type="external_gte" if seq is not None else None,
                )
                deleted += 1
            continue

        skipped += 1

    await elasticsearch_service.refresh_index(new_index)

    promoted = False
    promote_error: Optional[str] = None
    if request.promote:
        try:
            alias_exists = False
            try:
                alias_exists = await elasticsearch_service.client.indices.exists_alias(name=base_index)
            except Exception:
                alias_exists = False

            if alias_exists:
                current = await elasticsearch_service.client.indices.get_alias(name=base_index)
                current_indices = list(current.keys())
                actions = [{"remove": {"index": idx, "alias": base_index}} for idx in current_indices]
                actions.append({"add": {"index": new_index, "alias": base_index}})
                await elasticsearch_service.update_aliases(actions)
                promoted = True
            else:
                base_exists = await elasticsearch_service.index_exists(base_index)
                if base_exists and not request.allow_delete_base_index:
                    raise RuntimeError(
                        "Base index exists as a concrete index; set allow_delete_base_index=true to convert it to an alias"
                    )
                if base_exists:
                    await elasticsearch_service.delete_index(base_index)
                await elasticsearch_service.create_alias(index=new_index, alias=base_index)
                promoted = True
        except Exception as e:
            promote_error = str(e)

    completed_at = datetime.now(timezone.utc)
    result = {
        "task_id": task_id,
        "status": "completed",
        "db_name": db_name,
        "branch": branch,
        "projection": projection,
        "base_index": base_index,
        "new_index": new_index,
        "from_ts": from_dt.isoformat(),
        "to_ts": to_dt.isoformat(),
        "processed_events": processed,
        "indexed_docs": indexed,
        "deleted_docs": deleted,
        "skipped_events": skipped,
        "promoted": promoted,
        "promote_error": promote_error,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "next_steps": (
            None
            if promoted
            else {
                "note": "Index rebuilt but not promoted. You can re-run with promote=true, or manually switch alias/index in Elasticsearch.",
                "base_index": base_index,
                "new_index": new_index,
            }
        ),
    }

    await redis_service.set_json(key=f"recompute_result:{task_id}", value=result, ttl=3600)

    try:
        await audit_store.log(
            partition_key=f"db:{db_name}",
            actor="bff_admin",
            action="RECOMPUTE_PROJECTION_COMPLETED",
            status="success",
            resource_type="es_index",
            resource_id=new_index,
            metadata=result,
            occurred_at=completed_at,
        )
    except Exception:
        pass


@router.post("/cleanup-old-replays")
async def cleanup_old_replay_results(
    older_than_hours: int = Query(24, ge=1, le=168, description="Delete results older than N hours"),
    redis_service: RedisService = Depends(get_redis_service)
) -> Dict[str, Any]:
    """
    Clean up old replay results from Redis.
    
    This helps manage Redis memory usage by removing expired results.
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=int(older_than_hours))

        # Scan for replay result keys
        pattern = "replay_result:*"
        keys = await redis_service.scan_keys(pattern)
        
        deleted_count = 0
        skipped_count = 0
        unreadable_count = 0
        for key in keys:
            payload = await redis_service.get_json(key)
            if not payload:
                # Key exists but payload missing/unreadable; delete best-effort to reduce clutter.
                if await redis_service.delete(key):
                    deleted_count += 1
                continue

            replayed_at_raw = payload.get("replayed_at")
            if not replayed_at_raw:
                skipped_count += 1
                continue

            try:
                replayed_at = datetime.fromisoformat(str(replayed_at_raw))
                if replayed_at.tzinfo is None:
                    replayed_at = replayed_at.replace(tzinfo=timezone.utc)
                replayed_at = replayed_at.astimezone(timezone.utc)
            except Exception:
                unreadable_count += 1
                continue

            if replayed_at < cutoff:
                if await redis_service.delete(key):
                    deleted_count += 1
        
        return {
            "message": f"Cleaned up {deleted_count} old replay results",
            "scanned_keys": len(keys),
            "deleted_keys": deleted_count,
            "skipped_keys": skipped_count,
            "unreadable_keys": unreadable_count,
            "cutoff": cutoff.isoformat(),
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup old replay results: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cleanup failed: {str(e)}"
        )


@router.get("/system-health")
async def get_system_health(
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service)
) -> Dict[str, Any]:
    """
    Get overall system health including background task metrics.
    
    This endpoint provides visibility into the health of background
    task processing, helping identify potential issues.
    """
    # Get task metrics
    task_metrics = await task_manager.get_task_metrics()
    
    # Get Redis connection status
    redis_healthy = await redis_service.ping()
    
    # Determine overall health
    issues = []
    
    if not redis_healthy:
        issues.append("Redis connection unhealthy")
    
    if task_metrics.success_rate < 90:
        issues.append(f"Low task success rate: {task_metrics.success_rate:.1f}%")
    
    if task_metrics.processing_tasks > 20:
        issues.append(f"High number of processing tasks: {task_metrics.processing_tasks}")
    
    health_status = "healthy" if not issues else "degraded"
    
    return {
        "status": health_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "issues": issues,
        "components": {
            "redis": "healthy" if redis_healthy else "unhealthy",
            "background_tasks": {
                "status": "healthy" if task_metrics.success_rate >= 90 else "degraded",
                "metrics": {
                    "total_tasks": task_metrics.total_tasks,
                    "active_tasks": task_metrics.active_tasks,
                    "success_rate": task_metrics.success_rate,
                    "average_duration_seconds": task_metrics.average_duration
                }
            }
        }
    }
