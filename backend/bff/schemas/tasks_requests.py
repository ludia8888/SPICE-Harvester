"""
Background task request/response schemas (BFF).

Keeping these models outside routers reduces router bloat and supports router
composition.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.models.background_task import TaskMetrics, TaskStatus


class TaskStatusResponse(BaseModel):
    """Task status response model."""

    task_id: str = Field(..., description="Task identifier")
    task_name: str = Field(..., description="Task name")
    task_type: str = Field(..., description="Task type")
    status: TaskStatus = Field(..., description="Current status")
    created_at: datetime = Field(..., description="Creation time")
    started_at: Optional[datetime] = Field(None, description="Start time")
    completed_at: Optional[datetime] = Field(None, description="Completion time")
    duration: Optional[float] = Field(None, description="Duration in seconds")
    progress: Optional[Dict[str, Any]] = Field(None, description="Progress information")
    result: Optional[Dict[str, Any]] = Field(None, description="Task result")


class TaskListResponse(BaseModel):
    """Task list response model."""

    tasks: List[TaskStatusResponse] = Field(..., description="List of tasks")
    total: int = Field(..., description="Total number of tasks")


class TaskMetricsResponse(BaseModel):
    """Task metrics response model."""

    metrics: TaskMetrics = Field(..., description="Task execution metrics")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Metrics timestamp",
    )

