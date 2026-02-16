"""
Pipeline plan schema for LLM-generated pipeline definitions.

LLM outputs a typed plan (definition JSON + outputs) and the server
validates/preview-simulates before any execution.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from uuid import UUID

from shared.models.pipeline_task_spec import PipelineTaskSpec
from shared.services.pipeline.output_plugins import (
    OUTPUT_KIND_DATASET,
    OUTPUT_KIND_GEOTEMPORAL,
    OUTPUT_KIND_MEDIA,
    OUTPUT_KIND_ONTOLOGY,
    OUTPUT_KIND_VIRTUAL,
    normalize_output_kind,
)
import logging


class PipelinePlanOutputKind(str, Enum):
    dataset = OUTPUT_KIND_DATASET
    geotemporal = OUTPUT_KIND_GEOTEMPORAL
    media = OUTPUT_KIND_MEDIA
    virtual = OUTPUT_KIND_VIRTUAL
    ontology = OUTPUT_KIND_ONTOLOGY


class PipelinePlanDataScope(BaseModel):
    db_name: Optional[str] = None
    branch: Optional[str] = None
    dataset_ids: List[str] = Field(default_factory=list)
    dataset_version_ids: List[str] = Field(default_factory=list)
    pipeline_id: Optional[str] = None


class PipelinePlanOutput(BaseModel):
    output_name: str = Field(..., min_length=1, max_length=200)
    output_kind: PipelinePlanOutputKind = Field(default=PipelinePlanOutputKind.dataset)
    output_metadata: Dict[str, Any] = Field(default_factory=dict)
    target_class_id: Optional[str] = Field(default=None, max_length=200)
    source_class_id: Optional[str] = Field(default=None, max_length=200)
    link_type_id: Optional[str] = Field(default=None, max_length=200)
    predicate: Optional[str] = Field(default=None, max_length=200)
    cardinality: Optional[str] = Field(default=None, max_length=40)
    source_key_column: Optional[str] = Field(default=None, max_length=200)
    target_key_column: Optional[str] = Field(default=None, max_length=200)
    relationship_spec_type: Optional[str] = Field(default=None, max_length=40)
    notes: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("output_kind", mode="before")
    @classmethod
    def _normalize_output_kind(cls, value: Any) -> Any:
        return normalize_output_kind(value)


class PipelinePlanAssociation(BaseModel):
    association_id: Optional[str] = Field(default=None, max_length=200)
    left_dataset_id: Optional[str] = Field(default=None, max_length=200)
    right_dataset_id: Optional[str] = Field(default=None, max_length=200)
    left_dataset_name: Optional[str] = Field(default=None, max_length=200)
    right_dataset_name: Optional[str] = Field(default=None, max_length=200)
    left_keys: Optional[List[str]] = None
    right_keys: Optional[List[str]] = None
    join_type: Optional[str] = Field(default="inner", max_length=40)
    cardinality_hint: Optional[str] = Field(default=None, max_length=40)
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    notes: Optional[str] = Field(default=None, max_length=1000)

    @model_validator(mode="before")
    @classmethod
    def _normalize_fields(cls, data: Any):  # noqa: ANN001
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        assoc_id = normalized.get("association_id") or normalized.get("id")
        if assoc_id:
            normalized["association_id"] = str(assoc_id).strip()
        left_ref = normalized.get("left_dataset_id") or normalized.get("left_dataset") or normalized.get("from")
        right_ref = normalized.get("right_dataset_id") or normalized.get("right_dataset") or normalized.get("to")
        if left_ref and not normalized.get("left_dataset_id"):
            left_text = str(left_ref).strip()
            try:
                UUID(left_text)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at shared/models/pipeline_plan.py:76", exc_info=True)
                normalized["left_dataset_name"] = normalized.get("left_dataset_name") or left_text
            else:
                normalized["left_dataset_id"] = left_text
        if right_ref and not normalized.get("right_dataset_id"):
            right_text = str(right_ref).strip()
            try:
                UUID(right_text)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at shared/models/pipeline_plan.py:84", exc_info=True)
                normalized["right_dataset_name"] = normalized.get("right_dataset_name") or right_text
            else:
                normalized["right_dataset_id"] = right_text
        left_name = normalized.get("left_dataset_name") or normalized.get("left_name")
        right_name = normalized.get("right_dataset_name") or normalized.get("right_name")
        if left_name:
            normalized["left_dataset_name"] = str(left_name).strip()
        if right_name:
            normalized["right_dataset_name"] = str(right_name).strip()

        def _normalize_keys(value: Any) -> List[str]:
            if value is None:
                return []
            if isinstance(value, list):
                items = value
            else:
                items = [value]
            output: List[str] = []
            for item in items:
                if item is None:
                    continue
                raw = str(item).strip()
                if not raw:
                    continue
                parts = [part.strip() for part in raw.replace("+", ",").split(",") if part.strip()]
                output.extend(parts if parts else [raw])
            return [item for item in output if item]

        left_keys = normalized.get("left_keys") or normalized.get("left_columns") or normalized.get("left_on")
        right_keys = normalized.get("right_keys") or normalized.get("right_columns") or normalized.get("right_on")
        on_keys = normalized.get("on")
        if not left_keys and not right_keys and on_keys:
            left_keys = on_keys
            right_keys = on_keys
        left_keys = _normalize_keys(left_keys)
        right_keys = _normalize_keys(right_keys)
        if not left_keys:
            left_single = normalized.get("left_column")
            left_keys = _normalize_keys(left_single)
        if not right_keys:
            right_single = normalized.get("right_column")
            right_keys = _normalize_keys(right_single)
        if left_keys:
            normalized["left_keys"] = left_keys
        if right_keys:
            normalized["right_keys"] = right_keys
        join_type = normalized.get("join_type") or normalized.get("joinType")
        if join_type:
            normalized["join_type"] = str(join_type).strip().lower()
        return normalized


class PipelinePlan(BaseModel):
    plan_id: Optional[str] = None
    goal: str = Field(..., min_length=1, max_length=2000)
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    data_scope: PipelinePlanDataScope = Field(default_factory=PipelinePlanDataScope)
    definition_json: Dict[str, Any] = Field(default_factory=dict)
    outputs: List[PipelinePlanOutput] = Field(default_factory=list)
    associations: List[PipelinePlanAssociation] = Field(default_factory=list)
    # Control-plane scope contract (prevents overreach). Optional for backwards compatibility.
    task_spec: Optional[PipelineTaskSpec] = None
    warnings: List[str] = Field(default_factory=list)

    @field_validator("definition_json")
    @classmethod
    def _validate_definition(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(value, dict) or not value:
            raise ValueError("definition_json is required")
        return value
