"""
Objectify job payload shared between BFF and objectify worker.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, model_validator


class ObjectifyJob(BaseModel):
    job_id: str = Field(..., description="Unique objectify job id")
    db_name: str = Field(..., description="Database name")
    dataset_id: str = Field(..., description="Dataset id")
    dataset_version_id: Optional[str] = Field(default=None, description="Dataset version id")
    artifact_id: Optional[str] = Field(default=None, description="Pipeline artifact id")
    artifact_output_name: Optional[str] = Field(default=None, description="Artifact output name")
    dedupe_key: Optional[str] = Field(default=None, description="Stable deduplication key for the job")
    dataset_branch: str = Field(default="main", description="Dataset branch")
    artifact_key: Optional[str] = Field(
        default=None,
        description="s3:// artifact key for the dataset version or artifact output",
    )
    mapping_spec_id: str = Field(..., description="Mapping spec id")
    mapping_spec_version: int = Field(..., description="Mapping spec version")
    target_class_id: str = Field(..., description="Ontology class id")
    ontology_branch: Optional[str] = Field(default=None, description="Ontology branch")
    max_rows: Optional[int] = Field(default=None, description="Optional row cap")
    batch_size: Optional[int] = Field(default=None, description="Bulk-create batch size")
    allow_partial: bool = Field(default=False, description="Allow partial import when rows fail validation")
    options: Dict[str, Any] = Field(default_factory=dict)
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def _validate_inputs(self) -> "ObjectifyJob":
        has_version = bool(self.dataset_version_id)
        has_artifact = bool(self.artifact_id)
        if has_version == has_artifact:
            raise ValueError("Exactly one of dataset_version_id or artifact_id is required")
        if has_artifact and not self.artifact_output_name:
            raise ValueError("artifact_output_name is required when artifact_id is set")
        return self
