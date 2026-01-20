"""
Pipeline plan schema for LLM-generated pipeline definitions.

LLM outputs a typed plan (definition JSON + outputs) and the server
validates/preview-simulates before any execution.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class PipelinePlanOutputKind(str, Enum):
    object = "object"
    link = "link"
    unknown = "unknown"


class PipelinePlanDataScope(BaseModel):
    db_name: Optional[str] = None
    branch: Optional[str] = None
    dataset_ids: List[str] = Field(default_factory=list)
    dataset_version_ids: List[str] = Field(default_factory=list)
    pipeline_id: Optional[str] = None


class PipelinePlanOutput(BaseModel):
    output_name: str = Field(..., min_length=1, max_length=200)
    output_kind: PipelinePlanOutputKind = Field(default=PipelinePlanOutputKind.unknown)
    target_class_id: Optional[str] = Field(default=None, max_length=200)
    link_type_id: Optional[str] = Field(default=None, max_length=200)
    notes: Optional[str] = Field(default=None, max_length=2000)


class PipelinePlan(BaseModel):
    plan_id: Optional[str] = None
    goal: str = Field(..., min_length=1, max_length=2000)
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    data_scope: PipelinePlanDataScope = Field(default_factory=PipelinePlanDataScope)
    definition_json: Dict[str, Any] = Field(default_factory=dict)
    outputs: List[PipelinePlanOutput] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    @field_validator("definition_json")
    @classmethod
    def _validate_definition(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(value, dict) or not value:
            raise ValueError("definition_json is required")
        return value
