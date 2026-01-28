"""
Pipeline job message payload.

Shared between BFF and pipeline worker for Spark/Flink execution.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, model_validator


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
    dedupe_key: Optional[str] = Field(default=None, description="Idempotency key for job deduplication")
    idempotency_key: Optional[str] = Field(default=None, description="Client-provided idempotency key")
    sequence_number: Optional[int] = Field(default=None, description="Sequence number for aggregate ordering")

    @model_validator(mode="after")
    def _compute_dedupe_key(self) -> "PipelineJob":
        """Auto-compute dedupe_key if not provided."""
        if self.dedupe_key:
            return self

        # Build composite dedupe key from stable fields
        parts = [
            f"pipeline:{self.pipeline_id}",
            f"mode:{self.mode}",
            f"branch:{self.branch or 'main'}",
        ]
        if self.definition_hash:
            parts.append(f"hash:{self.definition_hash}")
        if self.idempotency_key:
            parts.append(f"idem:{self.idempotency_key}")
        if self.node_id:
            parts.append(f"node:{self.node_id}")

        key_str = "|".join(parts)
        # Use SHA256 hash for fixed-length key
        object.__setattr__(
            self, "dedupe_key", hashlib.sha256(key_str.encode()).hexdigest()[:32]
        )
        return self

    @staticmethod
    def build_dedupe_key(
        pipeline_id: str,
        mode: str,
        branch: Optional[str] = None,
        definition_hash: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> str:
        """Build dedupe key from components (for external use)."""
        parts = [
            f"pipeline:{pipeline_id}",
            f"mode:{mode}",
            f"branch:{branch or 'main'}",
        ]
        if definition_hash:
            parts.append(f"hash:{definition_hash}")
        if idempotency_key:
            parts.append(f"idem:{idempotency_key}")
        if node_id:
            parts.append(f"node:{node_id}")
        key_str = "|".join(parts)
        return hashlib.sha256(key_str.encode()).hexdigest()[:32]
