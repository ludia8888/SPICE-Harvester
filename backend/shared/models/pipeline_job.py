"""
Pipeline job message payload.

Shared between BFF and pipeline worker for Spark/Flink execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class PipelineJob(BaseModel):
    job_id: str = Field(..., description="Unique pipeline job id")
    pipeline_id: str = Field(..., description="Pipeline aggregate id")
    db_name: str = Field(..., description="Database name")
    pipeline_type: str = Field(default="batch", description="batch | streaming")
    definition_json: Dict[str, Any] = Field(default_factory=dict)
    definition_hash: Optional[str] = Field(default=None, description="Stable hash of definition_json used for preview/deploy equivalence")
    definition_commit_id: Optional[str] = Field(default=None, description="lakeFS commit id for the pipeline definition")
    node_id: Optional[str] = Field(default=None, description="Output node id override")
    output_dataset_name: str = Field(..., description="Output dataset name")
    mode: str = Field(default="deploy", description="deploy | preview | build")
    schedule_interval_seconds: Optional[int] = Field(default=None)
    schedule_cron: Optional[str] = Field(default=None)
    preview_limit: Optional[int] = Field(default=None, description="Preview row limit")
    branch: Optional[str] = Field(default=None, description="Pipeline branch")
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
