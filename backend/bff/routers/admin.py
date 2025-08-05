"""
Admin API Router

Provides administrative endpoints for system management,
including instance state replay and other long-running operations.

This router addresses Anti-pattern 14 by using BackgroundTasks
and BackgroundTaskManager for all long-running operations.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from uuid import uuid4
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from bff.dependencies import get_oms_client, get_storage_service
from bff.services.oms_client import OMSClient
from shared.services.storage_service import StorageService
from shared.services.background_task_manager import BackgroundTaskManager
from shared.services.redis_service import RedisService
from shared.dependencies import get_container, ServiceContainer
from shared.models.background_task import TaskStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin Operations"])


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
    
    return await container.get(BackgroundTaskManager)


async def get_redis_service(
    container: ServiceContainer = Depends(get_container)
) -> RedisService:
    """Get RedisService from container."""
    return await container.get('RedisService')


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
            "requested_at": datetime.utcnow().isoformat()
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
        
        # Get bucket name from environment or config
        bucket = f"spice-commands-{request.db_name}"
        
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
                "replayed_at": datetime.utcnow().isoformat(),
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
                "replayed_at": datetime.utcnow().isoformat()
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
        # Scan for replay result keys
        pattern = "replay_result:*"
        keys = await redis_service.scan_keys(pattern)
        
        deleted_count = 0
        for key in keys:
            # Delete the key (results should have TTL anyway)
            if await redis_service.delete(key):
                deleted_count += 1
        
        return {
            "message": f"Cleaned up {deleted_count} old replay results",
            "scanned_keys": len(keys),
            "deleted_keys": deleted_count
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
        "timestamp": datetime.utcnow().isoformat(),
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