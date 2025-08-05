"""
Background Task Models

Data models for background task management system.
These models support comprehensive task tracking and eliminate
fire-and-forget anti-patterns.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Background task execution status."""
    PENDING = "PENDING"          # Task created but not started
    PROCESSING = "PROCESSING"    # Task is currently running
    COMPLETED = "COMPLETED"      # Task finished successfully
    FAILED = "FAILED"           # Task failed after all retries
    CANCELLED = "CANCELLED"     # Task was cancelled
    RETRYING = "RETRYING"       # Task failed and will retry


class TaskProgress(BaseModel):
    """Task progress information for long-running operations."""
    current: int = Field(..., description="Current progress value")
    total: int = Field(..., description="Total expected value")
    percentage: float = Field(..., description="Progress percentage (0-100)")
    message: Optional[str] = Field(None, description="Progress status message")
    updated_at: datetime = Field(..., description="Last progress update time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TaskResult(BaseModel):
    """Task execution result."""
    success: bool = Field(..., description="Whether task succeeded")
    data: Optional[Any] = Field(None, description="Task result data (if successful)")
    error: Optional[str] = Field(None, description="Error message (if failed)")
    error_type: Optional[str] = Field(None, description="Error type/class name")
    error_traceback: Optional[str] = Field(None, description="Full error traceback")
    message: Optional[str] = Field(None, description="Human-readable result message")
    warnings: List[str] = Field(default_factory=list, description="Non-fatal warnings")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BackgroundTask(BaseModel):
    """
    Complete background task representation.
    
    This model tracks all aspects of a background task's lifecycle,
    ensuring no task runs without proper monitoring.
    """
    # Basic identification
    task_id: str = Field(..., description="Unique task identifier")
    task_name: str = Field(..., description="Human-readable task name")
    task_type: str = Field(..., description="Task categorization")
    
    # Status tracking
    status: TaskStatus = Field(..., description="Current task status")
    priority: str = Field(default="NORMAL", description="Task priority")
    
    # Timing information
    created_at: datetime = Field(..., description="Task creation time")
    started_at: Optional[datetime] = Field(None, description="Task start time")
    completed_at: Optional[datetime] = Field(None, description="Task completion time")
    next_retry_at: Optional[datetime] = Field(None, description="Next retry time (if retrying)")
    
    # Progress tracking
    progress: Optional[TaskProgress] = Field(None, description="Task progress information")
    
    # Result storage
    result: Optional[TaskResult] = Field(None, description="Task execution result")
    
    # Retry management
    retry_count: int = Field(default=0, description="Number of retry attempts")
    max_retries: int = Field(default=3, description="Maximum retry attempts allowed")
    
    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional task metadata")
    parent_task_id: Optional[str] = Field(None, description="Parent task ID for nested tasks")
    child_task_ids: List[str] = Field(default_factory=list, description="Child task IDs")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
    @property
    def duration(self) -> Optional[float]:
        """Calculate task duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
        
    @property
    def is_running(self) -> bool:
        """Check if task is currently running."""
        return self.status in [TaskStatus.PROCESSING, TaskStatus.RETRYING]
        
    @property
    def is_complete(self) -> bool:
        """Check if task has completed (successfully or not)."""
        return self.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
        
    @property
    def is_successful(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.COMPLETED and self.result and self.result.success


class TaskMetrics(BaseModel):
    """Aggregated metrics for background tasks."""
    total_tasks: int = Field(..., description="Total number of tasks")
    pending_tasks: int = Field(..., description="Number of pending tasks")
    processing_tasks: int = Field(..., description="Number of currently processing tasks")
    completed_tasks: int = Field(..., description="Number of completed tasks")
    failed_tasks: int = Field(..., description="Number of failed tasks")
    cancelled_tasks: int = Field(..., description="Number of cancelled tasks")
    retrying_tasks: int = Field(..., description="Number of tasks being retried")
    average_duration: float = Field(..., description="Average task duration in seconds")
    success_rate: float = Field(..., description="Task success rate percentage")
    
    @property
    def active_tasks(self) -> int:
        """Get number of active (running) tasks."""
        return self.processing_tasks + self.retrying_tasks
        
    @property
    def finished_tasks(self) -> int:
        """Get number of finished tasks."""
        return self.completed_tasks + self.failed_tasks + self.cancelled_tasks


class TaskFilter(BaseModel):
    """Filter criteria for querying tasks."""
    status: Optional[TaskStatus] = Field(None, description="Filter by status")
    task_type: Optional[str] = Field(None, description="Filter by task type")
    priority: Optional[str] = Field(None, description="Filter by priority")
    created_after: Optional[datetime] = Field(None, description="Filter by creation time")
    created_before: Optional[datetime] = Field(None, description="Filter by creation time")
    parent_task_id: Optional[str] = Field(None, description="Filter by parent task")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TaskUpdateNotification(BaseModel):
    """Real-time task update notification model."""
    event: str = Field(..., description="Event type (created, progress, completed, etc.)")
    task_id: str = Field(..., description="Task identifier")
    task_name: Optional[str] = Field(None, description="Task name")
    task_type: Optional[str] = Field(None, description="Task type")
    status: Optional[TaskStatus] = Field(None, description="Current status")
    progress: Optional[Dict[str, Any]] = Field(None, description="Progress information")
    result: Optional[Dict[str, Any]] = Field(None, description="Task result")
    error: Optional[str] = Field(None, description="Error message")
    retry_count: Optional[int] = Field(None, description="Retry count")
    max_retries: Optional[int] = Field(None, description="Maximum retries")
    next_retry_at: Optional[str] = Field(None, description="Next retry time")
    timestamp: datetime = Field(default_factory=lambda: datetime.utcnow(), description="Notification timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }