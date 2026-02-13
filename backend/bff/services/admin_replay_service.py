"""Admin replay service (BFF).

Extracted from `bff.routers.admin_replay` to keep routers thin and to centralize
instance-state replay and trace enrichment (Facade pattern).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Sequence
from uuid import uuid4

from fastapi import BackgroundTasks
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.admin_task_monitor import monitor_admin_task
from bff.schemas.admin_replay_requests import ReplayInstanceStateRequest, ReplayInstanceStateResponse
from shared.config.settings import get_settings
from shared.models.background_task import BackgroundTask, TaskStatus
from shared.models.lineage import LineageDirection
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.core.background_task_manager import BackgroundTaskManager
from shared.services.registries.lineage_store import LineageStore
from shared.services.storage.redis_service import RedisService
from shared.services.storage.storage_service import StorageService
from shared.observability.tracing import trace_external_call, trace_db_operation

logger = logging.getLogger(__name__)


@trace_db_operation("bff.admin_replay.start_replay_instance_state")
async def start_replay_instance_state(
    *,
    request: ReplayInstanceStateRequest,
    background_tasks: BackgroundTasks,
    storage_service: StorageService,
    task_manager: BackgroundTaskManager,
    redis_service: RedisService,
) -> ReplayInstanceStateResponse:
    task_id = str(uuid4())

    background_task_id = await task_manager.create_task(
        replay_instance_state_task,
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
            "requested_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    background_tasks.add_task(monitor_admin_task, task_id=background_task_id, task_manager=task_manager)

    return ReplayInstanceStateResponse(
        task_id=background_task_id,
        status="accepted",
        message=f"Instance state replay started for {request.instance_id}",
        status_url=f"/api/v1/tasks/{background_task_id}",
    )


@trace_db_operation("bff.admin_replay.get_replay_result")
async def get_replay_result(*, task_id: str, task_manager: BackgroundTaskManager, redis_service: RedisService) -> Dict[str, Any]:
    task = await _require_completed_task(task_id=task_id, task_manager=task_manager)

    if task.status == TaskStatus.FAILED:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": task.result.error if task.result else "Unknown error",
            "message": "Instance state replay failed",
        }

    payload = await _get_replay_payload(task_id=task_id, redis_service=redis_service)

    return {
        "task_id": task_id,
        "status": "completed",
        "instance_state": payload.get("instance_state"),
        "is_deleted": payload.get("is_deleted", False),
        "deletion_info": payload.get("deletion_info"),
        "command_count": payload.get("command_count", 0),
        "replayed_at": payload.get("replayed_at"),
    }


@trace_db_operation("bff.admin_replay.get_replay_trace")
async def get_replay_trace(
    *,
    task_id: str,
    command_id: Optional[str],
    include_audit: bool,
    audit_limit: int,
    include_lineage: bool,
    lineage_direction: LineageDirection,
    lineage_max_depth: int,
    lineage_max_nodes: int,
    lineage_max_edges: int,
    timeline_limit: int,
    task_manager: BackgroundTaskManager,
    redis_service: RedisService,
    audit_store: AuditLogStore,
    lineage_store: LineageStore,
) -> Dict[str, Any]:
    task = await _require_completed_task(task_id=task_id, task_manager=task_manager)
    if task.status == TaskStatus.FAILED:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": task.result.error if task.result else "Unknown error",
            "message": "Instance state replay failed",
        }

    payload = await _get_replay_payload(task_id=task_id, redis_service=redis_service)
    instance_state = payload.get("instance_state")

    command_history = _extract_command_history(instance_state)
    selected = _select_command(command_history=command_history, command_id=command_id)
    selected_command_id = str((selected or {}).get("command_id") or "") if selected else ""

    target = _resolve_target(task=task, instance_state=instance_state)
    db_name = target.get("db_name")
    class_id = target.get("class_id")
    instance_id = target.get("instance_id")

    timeline = _build_timeline(command_history=command_history, timeline_limit=timeline_limit, db_name=db_name)

    audit: Optional[Dict[str, Any]] = None
    audit_error: Optional[str] = None
    if include_audit and db_name and selected_command_id:
        try:
            audit_items = await _load_audit_logs(
                audit_store=audit_store,
                db_name=str(db_name),
                command_id=selected_command_id,
                limit=audit_limit,
            )
            audit = {"count": len(audit_items), "items": [item.model_dump(mode="json") for item in audit_items]}
        except Exception as exc:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/admin_replay_service.py:146", exc_info=True)
            audit_error = str(exc)

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
        except Exception as exc:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/admin_replay_service.py:162", exc_info=True)
            lineage_error = str(exc)

    return {
        "task_id": task_id,
        "status": "completed",
        "target": {"db_name": db_name, "class_id": class_id, "instance_id": instance_id},
        "replay": {
            "instance_state": instance_state,
            "is_deleted": payload.get("is_deleted", False),
            "deletion_info": payload.get("deletion_info"),
            "command_count": payload.get("command_count", 0),
            "replayed_at": payload.get("replayed_at"),
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


@trace_db_operation("bff.admin_replay.cleanup_old_replay_results")
async def cleanup_old_replay_results(*, older_than_hours: int, redis_service: RedisService) -> Dict[str, Any]:
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=int(older_than_hours))

        pattern = "replay_result:*"
        keys = await redis_service.scan_keys(pattern)

        deleted_count = 0
        skipped_count = 0
        unreadable_count = 0
        for key in keys:
            payload = await redis_service.get_json(key)
            if not payload:
                if await redis_service.delete(key):
                    deleted_count += 1
                continue

            replayed_at_raw = payload.get("replayed_at")
            if not replayed_at_raw:
                skipped_count += 1
                continue

            replayed_at = _parse_iso_datetime(str(replayed_at_raw))
            if replayed_at is None:
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
    except Exception as exc:
        logger.error("Failed to cleanup old replay results: %s", exc, exc_info=True)
        raise classified_http_exception(500, f"Cleanup failed: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.admin_replay.replay_instance_state_task")
async def replay_instance_state_task(
    task_id: str,
    request: ReplayInstanceStateRequest,
    storage_service: StorageService,
    redis_service: RedisService,
) -> None:
    try:
        logger.info("Starting instance state replay for %s", request.instance_id)

        bucket = get_settings().storage.instance_bucket

        command_files = await storage_service.get_all_commands_for_instance(
            bucket=bucket,
            db_name=request.db_name,
            class_id=request.class_id,
            instance_id=request.instance_id,
        )

        if not command_files:
            result = {
                "instance_state": None,
                "is_deleted": False,
                "deletion_info": None,
                "command_count": 0,
                "replayed_at": datetime.now(timezone.utc).isoformat(),
                "message": "No commands found for instance",
            }
        else:
            instance_state = await storage_service.replay_instance_state(bucket=bucket, command_files=command_files)

            is_deleted = storage_service.is_instance_deleted(instance_state)
            deletion_info = storage_service.get_deletion_info(instance_state) if is_deleted else None

            result = {
                "instance_state": instance_state,
                "is_deleted": is_deleted,
                "deletion_info": deletion_info,
                "command_count": len(command_files),
                "replayed_at": datetime.now(timezone.utc).isoformat(),
            }

        if request.store_result:
            result_key = f"replay_result:{task_id}"
            await redis_service.set_json(key=result_key, value=result, ttl=request.result_ttl)
            logger.info("Stored replay result for task %s with TTL %ss", task_id, request.result_ttl)

        logger.info("Completed instance state replay for %s", request.instance_id)
    except Exception as exc:
        logger.error("Failed to replay instance state for %s: %s", request.instance_id, exc, exc_info=True)
        raise


async def _require_completed_task(*, task_id: str, task_manager: BackgroundTaskManager) -> BackgroundTask:
    task = await task_manager.get_task_status(task_id)

    if not task:
        raise classified_http_exception(404, f"Task {task_id} not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    if not task.is_complete:
        raise classified_http_exception(400, f"Task {task_id} is not complete. Current status: {task.status.value}", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    return task


async def _get_replay_payload(*, task_id: str, redis_service: RedisService) -> Dict[str, Any]:
    result_key = f"replay_result:{task_id}"
    payload = await redis_service.get_json(result_key)

    if not payload:
        raise classified_http_exception(404, f"Result for task {task_id} not found or expired", code=ErrorCode.RESOURCE_NOT_FOUND)

    if not isinstance(payload, dict):
        raise classified_http_exception(500, f"Invalid replay result payload for task {task_id}", code=ErrorCode.INTERNAL_ERROR)

    return payload


def _extract_command_history(instance_state: Any) -> Sequence[Dict[str, Any]]:
    if not isinstance(instance_state, dict):
        return []

    meta = instance_state.get("_metadata", {})
    if not isinstance(meta, dict):
        return []

    history = meta.get("command_history")
    if not isinstance(history, list):
        return []

    return [item for item in history if isinstance(item, dict)]


def _select_command(*, command_history: Sequence[Dict[str, Any]], command_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not command_history:
        return None

    if command_id:
        for entry in command_history:
            if str(entry.get("command_id") or "") == command_id:
                return entry
        raise classified_http_exception(404, f"command_id '{command_id}' not found in replay history", code=ErrorCode.RESOURCE_NOT_FOUND)

    return command_history[-1]


def _resolve_target(*, task: BackgroundTask, instance_state: Any) -> Dict[str, Optional[str]]:
    db_name = (task.metadata or {}).get("db_name") if isinstance(task.metadata, dict) else None
    class_id = (task.metadata or {}).get("class_id") if isinstance(task.metadata, dict) else None
    instance_id = (task.metadata or {}).get("instance_id") if isinstance(task.metadata, dict) else None

    if isinstance(instance_state, dict):
        db_name = db_name or instance_state.get("db_name")
        class_id = class_id or instance_state.get("class_id")
        instance_id = instance_id or instance_state.get("instance_id")

    return {"db_name": db_name, "class_id": class_id, "instance_id": instance_id}


def _build_timeline(
    *, command_history: Sequence[Dict[str, Any]], timeline_limit: int, db_name: Optional[str]
) -> list[Dict[str, Any]]:
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
                    "audit": f"/api/v1/audit/logs?partition_key=db:{db_name}&command_id={cid}"
                    if (db_name and cid)
                    else None,
                    "lineage_graph": f"/api/v1/lineage/graph?db_name={db_name}&root={cid}&direction=both"
                    if (db_name and cid)
                    else None,
                    "lineage_impact": f"/api/v1/lineage/impact?db_name={db_name}&root={cid}&direction=downstream"
                    if (db_name and cid)
                    else None,
                },
            }
        )
    return timeline


async def _load_audit_logs(
    *,
    audit_store: AuditLogStore,
    db_name: str,
    command_id: str,
    limit: int,
) -> list[Any]:
    items = await audit_store.list_logs(
        partition_key=f"db:{db_name}",
        command_id=command_id,
        limit=limit,
        offset=0,
    )
    if items:
        return items

    return await audit_store.list_logs(
        partition_key=f"db:{db_name}",
        event_id=command_id,
        limit=limit,
        offset=0,
    )


def _parse_iso_datetime(raw: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/admin_replay_service.py:412", exc_info=True)
        return None
