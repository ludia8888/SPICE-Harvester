from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from shared.models.pipeline_plan import PipelinePlanDataScope


class PipelineOutputBinding(BaseModel):
    dataset_id: str = Field(..., min_length=1, max_length=200)
    dataset_version_id: Optional[str] = Field(default=None, max_length=200)
    dataset_branch: Optional[str] = Field(default=None, max_length=200)
    artifact_output_name: Optional[str] = Field(default=None, max_length=200)
    output_kind: Optional[str] = Field(default=None, max_length=20)
    target_class_id: Optional[str] = Field(default=None, max_length=200)
    source_class_id: Optional[str] = Field(default=None, max_length=200)
    link_type_id: Optional[str] = Field(default=None, max_length=200)
    predicate: Optional[str] = Field(default=None, max_length=200)
    cardinality: Optional[str] = Field(default=None, max_length=40)
    source_key_column: Optional[str] = Field(default=None, max_length=200)
    target_key_column: Optional[str] = Field(default=None, max_length=200)
    relationship_spec_type: Optional[str] = Field(default=None, max_length=40)


class PipelineAgentRunRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: PipelinePlanDataScope
    answers: Optional[Dict[str, Any]] = Field(default=None)
    planner_hints: Optional[Dict[str, Any]] = Field(default=None)
    output_bindings: Optional[Dict[str, PipelineOutputBinding]] = Field(default=None)
    preview_node_id: Optional[str] = Field(default=None, max_length=200)
    preview_limit: int = Field(default=200, ge=1, le=500)
    max_repairs: int = Field(default=2, ge=0, le=5)
    max_cleansing: int = Field(default=1, ge=0, le=5)
    apply_specs: bool = Field(default=False)
    auto_sync: bool = Field(default=True)
    ontology_branch: Optional[str] = Field(default=None, max_length=200)
    dangling_policy: str = Field(default="FAIL", max_length=20)
    dedupe_policy: str = Field(default="DEDUP", max_length=20)
