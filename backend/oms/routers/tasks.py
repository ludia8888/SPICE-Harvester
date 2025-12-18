"""
OMS Background Task Management Router

Provides task management endpoints for OMS service,
supporting proper tracking of background operations.

This router is simpler than BFF's version as it focuses on
internal task management without user-facing features.
"""

import logging
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status

from oms.dependencies import get_redis_service
from shared.services.redis_service import RedisService
from shared.models.background_task import TaskStatus, BackgroundTask
from shared.services.background_task_manager import BackgroundTaskManager
from shared.dependencies import get_container, ServiceContainer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["Task Management"])


# Dependency injection
async def get_task_manager(
    container: ServiceContainer = Depends(get_container)
) -> BackgroundTaskManager:
    """Get BackgroundTaskManager from container."""
    if not container.has(BackgroundTaskManager):
        # Register BackgroundTaskManager if not already registered
        def create_task_manager(container: ServiceContainer) -> BackgroundTaskManager:
            from shared.services.background_task_manager import create_background_task_manager
            redis_service = container.get_sync('RedisService')
            return create_background_task_manager(redis_service)
        
        container.register_singleton(BackgroundTaskManager, create_task_manager)
    
    try:
        return await container.get(BackgroundTaskManager)
    except Exception as e:
        logger.error(f"Failed to get BackgroundTaskManager: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Background task service unavailable"
        )


@router.get("/internal/status/{task_id}")
async def get_internal_task_status(
    task_id: str,
    task_manager: BackgroundTaskManager = Depends(get_task_manager)
) -> Dict[str, Any]:
    """
    Get internal task status for monitoring.
    
    This is used by internal services to check task status.
    """
    task = await task_manager.get_task_status(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found"
        )
    
    return {
        "task_id": task.task_id,
        "status": task.status.value,
        "task_type": task.task_type,
        "created_at": task.created_at.isoformat(),
        "is_complete": task.is_complete,
        "is_successful": task.is_successful,
        "progress": task.progress.model_dump(mode="json") if task.progress else None
    }


@router.get("/internal/active")
async def get_active_tasks(
    task_manager: BackgroundTaskManager = Depends(get_task_manager)
) -> Dict[str, Any]:
    """
    Get all active (running) tasks.
    
    Used for monitoring system load and identifying stuck tasks.
    """
    # Get processing and retrying tasks
    processing_tasks = await task_manager.get_all_tasks(
        status=TaskStatus.PROCESSING,
        limit=100
    )
    
    retrying_tasks = await task_manager.get_all_tasks(
        status=TaskStatus.RETRYING,
        limit=100
    )
    
    active_tasks = processing_tasks + retrying_tasks
    
    return {
        "active_count": len(active_tasks),
        "processing_count": len(processing_tasks),
        "retrying_count": len(retrying_tasks),
        "tasks": [
            {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "task_type": task.task_type,
                "status": task.status.value,
                "started_at": task.started_at.isoformat() if task.started_at else None,
                "duration_seconds": task.duration
            }
            for task in active_tasks
        ]
    }


@router.post("/internal/cleanup")
async def cleanup_old_tasks(
    older_than_days: int = Query(7, ge=1, le=30, description="Delete tasks older than N days"),
    task_manager: BackgroundTaskManager = Depends(get_task_manager),
    redis_service: RedisService = Depends(get_redis_service)
) -> Dict[str, Any]:
    """
    Clean up old completed tasks.
    
    Removes task records older than specified days to prevent
    Redis memory bloat.
    """
    from datetime import datetime, timedelta, timezone
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    
    all_tasks = await task_manager.get_all_tasks(limit=1000)
    
    deleted_count = 0
    for task in all_tasks:
        if task.is_complete and task.created_at < cutoff_date:
            # Delete task record
            key = f"background_task:{task.task_id}"
            if await redis_service.delete(key):
                deleted_count += 1
    
    return {
        "message": f"Cleaned up {deleted_count} old tasks",
        "cutoff_date": cutoff_date.isoformat(),
        "older_than_days": older_than_days
    }


@router.get("/internal/health")
async def task_service_health(
    task_manager: BackgroundTaskManager = Depends(get_task_manager)
) -> Dict[str, Any]:
    """
    Get task service health status.
    
    Provides health metrics for the background task system.
    """
    metrics = await task_manager.get_task_metrics()
    
    # Determine health status
    health_status = "healthy"
    issues = []
    
    # Check for high failure rate
    if metrics.success_rate < 80:
        health_status = "warning"
        issues.append(f"High failure rate: {metrics.success_rate:.1f}%")
    
    # Check for stuck tasks
    if metrics.processing_tasks > 10:
        health_status = "warning"
        issues.append(f"Many processing tasks: {metrics.processing_tasks}")
    
    # Check for many retrying tasks
    if metrics.retrying_tasks > 5:
        health_status = "warning"
        issues.append(f"Many retrying tasks: {metrics.retrying_tasks}")
    
    return {
        "status": health_status,
        "issues": issues,
        "metrics": {
            "total_tasks": metrics.total_tasks,
            "active_tasks": metrics.active_tasks,
            "completed_tasks": metrics.completed_tasks,
            "failed_tasks": metrics.failed_tasks,
            "success_rate": metrics.success_rate,
            "average_duration_seconds": metrics.average_duration
        }
    }
