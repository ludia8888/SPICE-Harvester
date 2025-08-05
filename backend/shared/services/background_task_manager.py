"""
Background Task Manager Service

Provides centralized management for all background tasks in the system,
eliminating fire-and-forget anti-patterns and ensuring proper tracking.

Key features:
1. ✅ Task lifecycle management with proper error handling
2. ✅ Task status tracking and persistence
3. ✅ Automatic failure detection and retry logic
4. ✅ Task result storage and retrieval
5. ✅ Real-time progress updates via WebSocket
6. ✅ Task cancellation support
7. ✅ Dead task detection and cleanup

This service addresses Anti-pattern 14 by ensuring no background task
runs without proper monitoring and error handling.
"""

import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from uuid import UUID, uuid4

from shared.services.redis_service import RedisService
from shared.services.websocket_service import WebSocketNotificationService
from shared.models.background_task import (
    BackgroundTask,
    TaskStatus,
    TaskResult,
    TaskProgress,
    TaskMetrics
)

logger = logging.getLogger(__name__)


class TaskPriority(str, Enum):
    """Task execution priority levels."""
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"


class BackgroundTaskManager:
    """
    Centralized background task management service.
    
    This service provides a robust alternative to fire-and-forget patterns,
    ensuring all background tasks are properly tracked, monitored, and handled.
    """
    
    def __init__(
        self,
        redis_service: RedisService,
        websocket_service: Optional[WebSocketNotificationService] = None
    ):
        """
        Initialize the background task manager.
        
        Args:
            redis_service: Redis service for task persistence
            websocket_service: Optional WebSocket service for real-time updates
        """
        self.redis = redis_service
        self.websocket = websocket_service
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._task_callbacks: Dict[str, List[Callable]] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown = False
        
        # Configuration
        self.task_ttl = 86400  # 24 hours
        self.result_ttl = 604800  # 7 days
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.cleanup_interval = 300  # 5 minutes
        self.dead_task_threshold = 600  # 10 minutes
        
    async def start(self) -> None:
        """Start the background task manager."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_dead_tasks())
            self._cleanup_task.add_done_callback(self._handle_cleanup_task_done)
        logger.info("Background task manager started")
        
    async def stop(self) -> None:
        """Stop the background task manager and cancel all tasks."""
        self._shutdown = True
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
                
        # Cancel all running tasks
        for task_id, task in list(self._running_tasks.items()):
            await self.cancel_task(task_id)
            
        logger.info("Background task manager stopped")
        
    async def create_task(
        self,
        func: Callable,
        *args,
        task_name: Optional[str] = None,
        task_type: Optional[str] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        Create and track a new background task.
        
        This is the primary method to replace asyncio.create_task() calls,
        ensuring proper tracking and error handling.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            task_name: Human-readable task name
            task_type: Task categorization (e.g., "bulk_create", "replay")
            priority: Task execution priority
            metadata: Additional task metadata
            **kwargs: Keyword arguments for the function
            
        Returns:
            Task ID for tracking
            
        Example:
            task_id = await task_manager.create_task(
                heavy_computation,
                data_id,
                task_name="Process large dataset",
                task_type="data_processing",
                priority=TaskPriority.HIGH
            )
        """
        task_id = str(uuid4())
        
        # Create task record
        task_record = BackgroundTask(
            task_id=task_id,
            task_name=task_name or func.__name__,
            task_type=task_type or "generic",
            status=TaskStatus.PENDING,
            priority=priority,
            created_at=datetime.now(timezone.utc),
            metadata=metadata or {},
            retry_count=0,
            max_retries=self.max_retries
        )
        
        # Save task record
        await self._save_task(task_record)
        
        # Create asyncio task with wrapper
        asyncio_task = asyncio.create_task(
            self._execute_task(task_id, func, args, kwargs)
        )
        
        # Track the task
        self._running_tasks[task_id] = asyncio_task
        
        # Add done callback for cleanup
        asyncio_task.add_done_callback(
            lambda t: asyncio.create_task(self._handle_task_done(task_id, t))
        )
        
        logger.info(f"Created background task {task_id}: {task_name}")
        
        # Send real-time notification
        await self._notify_task_created(task_record)
        
        return task_id
        
    async def run_with_tracking(
        self,
        task_id: str,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict = None
    ) -> None:
        """
        Run a function with full tracking (for use with FastAPI BackgroundTasks).
        
        This method is designed to be used with FastAPI's BackgroundTasks:
        
        background_tasks.add_task(
            task_manager.run_with_tracking,
            task_id=task_id,
            func=heavy_function,
            args=(arg1, arg2)
        )
        """
        kwargs = kwargs or {}
        await self._execute_task(task_id, func, args, kwargs)
        
    async def _execute_task(
        self,
        task_id: str,
        func: Callable,
        args: Tuple,
        kwargs: Dict
    ) -> Any:
        """Execute a task with full error handling and status tracking."""
        task = await self._get_task(task_id)
        if not task:
            # Create task record if not exists (for run_with_tracking)
            task = BackgroundTask(
                task_id=task_id,
                task_name=func.__name__,
                task_type="direct_execution",
                status=TaskStatus.PENDING,
                created_at=datetime.now(timezone.utc),
                metadata={},
                retry_count=0,
                max_retries=self.max_retries
            )
            await self._save_task(task)
            
        try:
            # Update status to processing
            task.status = TaskStatus.PROCESSING
            task.started_at = datetime.now(timezone.utc)
            await self._save_task(task)
            await self._notify_task_status_changed(task)
            
            # Execute the function
            logger.info(f"Executing task {task_id}: {task.task_name}")
            result = await func(*args, **kwargs)
            
            # Update status to completed
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc)
            task.result = TaskResult(
                success=True,
                data=result,
                message="Task completed successfully"
            )
            await self._save_task(task)
            await self._notify_task_completed(task)
            
            logger.info(f"Task {task_id} completed successfully")
            return result
            
        except asyncio.CancelledError:
            # Task was cancelled
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now(timezone.utc)
            task.result = TaskResult(
                success=False,
                error="Task was cancelled",
                message="Task execution was cancelled by user or system"
            )
            await self._save_task(task)
            await self._notify_task_cancelled(task)
            logger.info(f"Task {task_id} was cancelled")
            raise
            
        except Exception as e:
            # Task failed
            error_traceback = traceback.format_exc()
            logger.error(f"Task {task_id} failed: {str(e)}\n{error_traceback}")
            
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(timezone.utc)
            task.result = TaskResult(
                success=False,
                error=str(e),
                error_type=type(e).__name__,
                error_traceback=error_traceback,
                message=f"Task failed: {str(e)}"
            )
            task.retry_count += 1
            
            # Check if we should retry
            if task.retry_count < task.max_retries:
                task.status = TaskStatus.RETRYING
                task.next_retry_at = datetime.now(timezone.utc) + timedelta(
                    seconds=self.retry_delay * task.retry_count
                )
                await self._save_task(task)
                await self._notify_task_retrying(task)
                
                # Schedule retry
                logger.info(f"Scheduling retry {task.retry_count}/{task.max_retries} for task {task_id}")
                await asyncio.sleep(self.retry_delay * task.retry_count)
                return await self._execute_task(task_id, func, args, kwargs)
            else:
                # Max retries reached
                await self._save_task(task)
                await self._notify_task_failed(task)
                
            raise
            
    async def update_progress(
        self,
        task_id: str,
        current: int,
        total: int,
        message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update task progress for long-running operations.
        
        This should be called periodically from within the task function
        to provide progress updates.
        
        Args:
            task_id: Task identifier
            current: Current progress value
            total: Total expected value
            message: Optional progress message
            metadata: Optional additional metadata
            
        Example:
            async def process_items(items, task_id):
                for i, item in enumerate(items):
                    await process_item(item)
                    await task_manager.update_progress(
                        task_id, i + 1, len(items),
                        f"Processing item {i + 1} of {len(items)}"
                    )
        """
        task = await self._get_task(task_id)
        if not task:
            logger.warning(f"Cannot update progress for unknown task {task_id}")
            return
            
        task.progress = TaskProgress(
            current=current,
            total=total,
            percentage=(current / total * 100) if total > 0 else 0,
            message=message,
            updated_at=datetime.now(timezone.utc)
        )
        
        if metadata:
            task.metadata.update(metadata)
            
        await self._save_task(task)
        await self._notify_task_progress(task)
        
    async def get_task_status(self, task_id: str) -> Optional[BackgroundTask]:
        """Get current task status and details."""
        return await self._get_task(task_id)
        
    async def get_all_tasks(
        self,
        status: Optional[TaskStatus] = None,
        task_type: Optional[str] = None,
        limit: int = 100
    ) -> List[BackgroundTask]:
        """
        Get all tasks matching the criteria.
        
        Args:
            status: Filter by task status
            task_type: Filter by task type
            limit: Maximum number of tasks to return
            
        Returns:
            List of matching tasks
        """
        # Get all task keys
        pattern = "background_task:*"
        task_keys = await self.redis.scan_keys(pattern)
        
        tasks = []
        for key in task_keys[:limit]:
            task_data = await self.redis.get_json(key)
            if task_data:
                task = BackgroundTask(**task_data)
                
                # Apply filters
                if status and task.status != status:
                    continue
                if task_type and task.task_type != task_type:
                    continue
                    
                tasks.append(task)
                
        # Sort by created_at descending
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        
        return tasks
        
    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a running task.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if task was cancelled, False if not found or already completed
        """
        # Check if task is running
        if task_id in self._running_tasks:
            asyncio_task = self._running_tasks[task_id]
            if not asyncio_task.done():
                asyncio_task.cancel()
                logger.info(f"Cancelled task {task_id}")
                return True
                
        # Update task status if it exists
        task = await self._get_task(task_id)
        if task and task.status in [TaskStatus.PENDING, TaskStatus.PROCESSING, TaskStatus.RETRYING]:
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now(timezone.utc)
            task.result = TaskResult(
                success=False,
                error="Cancelled by user",
                message="Task was cancelled before completion"
            )
            await self._save_task(task)
            await self._notify_task_cancelled(task)
            return True
            
        return False
        
    async def add_task_callback(
        self,
        task_id: str,
        callback: Callable[[BackgroundTask], None]
    ) -> None:
        """
        Add a callback to be called when task completes.
        
        This provides a way to handle task completion without polling.
        
        Args:
            task_id: Task identifier
            callback: Async function to call with task result
        """
        if task_id not in self._task_callbacks:
            self._task_callbacks[task_id] = []
        self._task_callbacks[task_id].append(callback)
        
    async def get_task_metrics(self) -> TaskMetrics:
        """Get overall task execution metrics."""
        all_tasks = await self.get_all_tasks(limit=1000)
        
        metrics = TaskMetrics(
            total_tasks=len(all_tasks),
            pending_tasks=sum(1 for t in all_tasks if t.status == TaskStatus.PENDING),
            processing_tasks=sum(1 for t in all_tasks if t.status == TaskStatus.PROCESSING),
            completed_tasks=sum(1 for t in all_tasks if t.status == TaskStatus.COMPLETED),
            failed_tasks=sum(1 for t in all_tasks if t.status == TaskStatus.FAILED),
            cancelled_tasks=sum(1 for t in all_tasks if t.status == TaskStatus.CANCELLED),
            retrying_tasks=sum(1 for t in all_tasks if t.status == TaskStatus.RETRYING),
            average_duration=self._calculate_average_duration(all_tasks),
            success_rate=self._calculate_success_rate(all_tasks)
        )
        
        return metrics
        
    # Private methods
    
    async def _save_task(self, task: BackgroundTask) -> None:
        """Save task to Redis."""
        key = f"background_task:{task.task_id}"
        await self.redis.set_json(key, task.dict(), ex=self.task_ttl)
        
        # Also save result separately with longer TTL if completed
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            result_key = f"task_result:{task.task_id}"
            await self.redis.set_json(
                result_key,
                task.result.dict() if task.result else {},
                ex=self.result_ttl
            )
            
    async def _get_task(self, task_id: str) -> Optional[BackgroundTask]:
        """Get task from Redis."""
        key = f"background_task:{task_id}"
        task_data = await self.redis.get_json(key)
        if task_data:
            return BackgroundTask(**task_data)
        return None
        
    async def _handle_task_done(self, task_id: str, asyncio_task: asyncio.Task) -> None:
        """Handle task completion callback."""
        # Remove from running tasks
        self._running_tasks.pop(task_id, None)
        
        # Execute callbacks
        if task_id in self._task_callbacks:
            task = await self._get_task(task_id)
            if task:
                for callback in self._task_callbacks[task_id]:
                    try:
                        await callback(task)
                    except Exception as e:
                        logger.error(f"Error in task callback for {task_id}: {e}")
                        
            # Clean up callbacks
            del self._task_callbacks[task_id]
            
    def _handle_cleanup_task_done(self, task: asyncio.Task) -> None:
        """Handle cleanup task completion."""
        try:
            task.result()
        except asyncio.CancelledError:
            logger.info("Cleanup task was cancelled")
        except Exception as e:
            logger.error(f"Cleanup task failed: {e}")
            
        # Restart cleanup task if not shutting down
        if not self._shutdown:
            self._cleanup_task = asyncio.create_task(self._cleanup_dead_tasks())
            self._cleanup_task.add_done_callback(self._handle_cleanup_task_done)
            
    async def _cleanup_dead_tasks(self) -> None:
        """Periodically clean up dead tasks."""
        while not self._shutdown:
            try:
                await asyncio.sleep(self.cleanup_interval)
                
                # Find potentially dead tasks
                processing_tasks = await self.get_all_tasks(status=TaskStatus.PROCESSING)
                
                for task in processing_tasks:
                    if task.started_at:
                        age = (datetime.now(timezone.utc) - task.started_at).total_seconds()
                        
                        # Check if task is dead (no update for threshold period)
                        if age > self.dead_task_threshold:
                            # Check if task is still in running tasks
                            if task.task_id not in self._running_tasks:
                                logger.warning(f"Found dead task {task.task_id}, marking as failed")
                                
                                task.status = TaskStatus.FAILED
                                task.completed_at = datetime.now(timezone.utc)
                                task.result = TaskResult(
                                    success=False,
                                    error="Task died unexpectedly",
                                    message="Task stopped updating and was marked as dead"
                                )
                                await self._save_task(task)
                                await self._notify_task_failed(task)
                                
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                
    def _calculate_average_duration(self, tasks: List[BackgroundTask]) -> float:
        """Calculate average task duration in seconds."""
        durations = []
        for task in tasks:
            if task.started_at and task.completed_at:
                duration = (task.completed_at - task.started_at).total_seconds()
                durations.append(duration)
                
        return sum(durations) / len(durations) if durations else 0.0
        
    def _calculate_success_rate(self, tasks: List[BackgroundTask]) -> float:
        """Calculate task success rate as percentage."""
        completed_tasks = [t for t in tasks if t.status in [
            TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED
        ]]
        
        if not completed_tasks:
            return 100.0
            
        successful = sum(1 for t in completed_tasks if t.status == TaskStatus.COMPLETED)
        return (successful / len(completed_tasks)) * 100
        
    # Notification methods
    
    async def _notify_task_created(self, task: BackgroundTask) -> None:
        """Notify about task creation."""
        if self.websocket:
            await self.websocket.notify_task_update({
                "event": "task_created",
                "task_id": task.task_id,
                "task_name": task.task_name,
                "task_type": task.task_type,
                "status": task.status
            })
            
    async def _notify_task_status_changed(self, task: BackgroundTask) -> None:
        """Notify about task status change."""
        if self.websocket:
            await self.websocket.notify_task_update({
                "event": "task_status_changed",
                "task_id": task.task_id,
                "status": task.status
            })
            
    async def _notify_task_progress(self, task: BackgroundTask) -> None:
        """Notify about task progress update."""
        if self.websocket and task.progress:
            await self.websocket.notify_task_update({
                "event": "task_progress",
                "task_id": task.task_id,
                "progress": task.progress.dict()
            })
            
    async def _notify_task_completed(self, task: BackgroundTask) -> None:
        """Notify about task completion."""
        if self.websocket:
            await self.websocket.notify_task_update({
                "event": "task_completed",
                "task_id": task.task_id,
                "result": task.result.dict() if task.result else None
            })
            
    async def _notify_task_failed(self, task: BackgroundTask) -> None:
        """Notify about task failure."""
        if self.websocket:
            await self.websocket.notify_task_update({
                "event": "task_failed",
                "task_id": task.task_id,
                "error": task.result.error if task.result else "Unknown error",
                "retry_count": task.retry_count
            })
            
    async def _notify_task_cancelled(self, task: BackgroundTask) -> None:
        """Notify about task cancellation."""
        if self.websocket:
            await self.websocket.notify_task_update({
                "event": "task_cancelled",
                "task_id": task.task_id
            })
            
    async def _notify_task_retrying(self, task: BackgroundTask) -> None:
        """Notify about task retry."""
        if self.websocket:
            await self.websocket.notify_task_update({
                "event": "task_retrying",
                "task_id": task.task_id,
                "retry_count": task.retry_count,
                "max_retries": task.max_retries,
                "next_retry_at": task.next_retry_at.isoformat() if task.next_retry_at else None
            })


# Factory function for creating BackgroundTaskManager
def create_background_task_manager(
    redis_service: RedisService,
    websocket_service: Optional[WebSocketNotificationService] = None
) -> BackgroundTaskManager:
    """
    Create a BackgroundTaskManager instance.
    
    Args:
        redis_service: Redis service for persistence
        websocket_service: Optional WebSocket service for notifications
        
    Returns:
        BackgroundTaskManager instance
    """
    return BackgroundTaskManager(redis_service, websocket_service)