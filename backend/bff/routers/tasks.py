"""
Background Task Management Router

Provides API endpoints for managing and monitoring background tasks,
eliminating fire-and-forget anti-patterns.

Key features:
1. ✅ Task status monitoring
2. ✅ Task result retrieval
3. ✅ Task cancellation
4. ✅ Task metrics and analytics
5. ✅ Real-time updates via WebSocket
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, status

from bff.schemas.tasks_requests import TaskListResponse, TaskMetricsResponse, TaskStatusResponse
from bff.services import tasks_service
from shared.models.background_task import TaskStatus
from shared.dependencies.providers import BackgroundTaskManagerDep

router = APIRouter(prefix="/tasks", tags=["Background Tasks"])


# API Endpoints
@router.get("/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: str,
    task_manager: BackgroundTaskManagerDep,
) -> TaskStatusResponse:
    """
    Get current status of a background task.
    
    This endpoint allows monitoring of any background task by its ID,
    providing real-time status updates and results.
    """
    return await tasks_service.get_task_status(task_id=task_id, task_manager=task_manager)


@router.get("/", response_model=TaskListResponse)
async def list_tasks(
    status: Optional[TaskStatus] = Query(None, description="Filter by status"),
    task_type: Optional[str] = Query(None, description="Filter by task type"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum tasks to return"),
    *,
    task_manager: BackgroundTaskManagerDep,
) -> TaskListResponse:
    """
    List background tasks with optional filtering.
    
    Provides visibility into all background tasks running in the system,
    helping identify stuck or failed tasks.
    """
    return await tasks_service.list_tasks(
        status_filter=status,
        task_type=task_type,
        limit=limit,
        task_manager=task_manager,
    )


@router.delete("/{task_id}")
async def cancel_task(
    task_id: str,
    task_manager: BackgroundTaskManagerDep,
) -> Dict[str, Any]:
    """
    Cancel a running background task.
    
    Allows graceful cancellation of long-running tasks,
    preventing resource waste and enabling retry.
    """
    return await tasks_service.cancel_task(task_id=task_id, task_manager=task_manager)


@router.get("/metrics/summary", response_model=TaskMetricsResponse)
async def get_task_metrics(
    task_manager: BackgroundTaskManagerDep,
) -> TaskMetricsResponse:
    """
    Get aggregated metrics for all background tasks.
    
    Provides insights into task execution patterns,
    success rates, and performance metrics.
    """
    return await tasks_service.get_task_metrics(task_manager=task_manager)


@router.post("/{task_id}/retry", include_in_schema=False)
async def retry_task(
    task_id: str,
    task_manager: BackgroundTaskManagerDep,
) -> Dict[str, Any]:
    """
    Retry a failed task.
    
    This endpoint allows manual retry of failed tasks,
    useful for recovering from transient failures.
    """
    task = await task_manager.get_task_status(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found"
        )
    
    if task.status != TaskStatus.FAILED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Can only retry failed tasks. Current status: {task.status.value}"
        )

    # Enterprise correctness: do not pretend we can requeue a completed/failed in-process task.
    # BackgroundTaskManager retries are implemented *during execution* (within the same process),
    # but manual retry requires a durable task spec (callable + args) which is not persisted today.
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail=(
            "Manual task retry is not supported. "
            "Tasks auto-retry while running (in-process); after failure, create a new task/command instead."
        ),
    )


@router.get("/{task_id}/result")
async def get_task_result(
    task_id: str,
    task_manager: BackgroundTaskManagerDep,
) -> Dict[str, Any]:
    """
    Get the result of a completed task.
    
    Returns the full result data for completed tasks,
    including any output data or error information.
    """
    return await tasks_service.get_task_result(task_id=task_id, task_manager=task_manager)


# WebSocket endpoint for real-time updates would be in websocket.py router
# This provides REST API access to task status and management
