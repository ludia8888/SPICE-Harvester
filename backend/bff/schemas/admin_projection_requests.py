"""Admin projection request/response schemas (BFF)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


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

