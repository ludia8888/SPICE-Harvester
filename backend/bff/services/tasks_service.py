"""
Background task domain logic (BFF).

Extracted from `bff.routers.tasks` to keep routers thin and to centralize
response-shaping behind a small service facade.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import HTTPException, status

from bff.schemas.tasks_requests import TaskListResponse, TaskMetricsResponse, TaskStatusResponse
from shared.models.background_task import BackgroundTask, TaskStatus


def _to_status_response(task: BackgroundTask) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=task.task_id,
        task_name=task.task_name,
        task_type=task.task_type,
        status=task.status,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        duration=task.duration,
        progress=task.progress.model_dump(mode="json") if task.progress else None,
        result=task.result.model_dump(mode="json") if task.result else None,
    )


async def get_task_status(*, task_id: str, task_manager: Any) -> TaskStatusResponse:
    task = await task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found")
    return _to_status_response(task)


async def list_tasks(
    *,
    status_filter: Optional[TaskStatus],
    task_type: Optional[str],
    limit: int,
    task_manager: Any,
) -> TaskListResponse:
    tasks = await task_manager.get_all_tasks(status=status_filter, task_type=task_type, limit=limit)
    responses = [_to_status_response(task) for task in tasks]
    return TaskListResponse(tasks=responses, total=len(responses))


async def cancel_task(*, task_id: str, task_manager: Any) -> Dict[str, Any]:
    success = await task_manager.cancel_task(task_id)
    if not success:
        task = await task_manager.get_task_status(task_id)
        if not task:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found")
        return {"message": f"Task {task_id} is already {task.status.value}", "cancelled": False}
    return {"message": f"Task {task_id} cancelled successfully", "cancelled": True}


async def get_task_metrics(*, task_manager: Any) -> TaskMetricsResponse:
    metrics = await task_manager.get_task_metrics()
    return TaskMetricsResponse(metrics=metrics, timestamp=datetime.now(timezone.utc))


async def get_task_result(*, task_id: str, task_manager: Any) -> Dict[str, Any]:
    task = await task_manager.get_task_status(task_id)

    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found")

    if not task.is_complete:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task {task_id} is not complete. Current status: {task.status.value}",
        )

    if not task.result:
        return {
            "task_id": task_id,
            "status": task.status.value,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "result": None,
            "message": "Task completed but no result data available",
        }

    return {
        "task_id": task_id,
        "status": task.status.value,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "success": task.result.success,
        "data": task.result.data,
        "error": task.result.error,
        "message": task.result.message,
        "warnings": task.result.warnings,
    }

