"""Admin replay request/response schemas (BFF)."""

from __future__ import annotations

from pydantic import BaseModel, Field


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
