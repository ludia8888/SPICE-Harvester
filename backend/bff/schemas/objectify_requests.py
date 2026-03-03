"""
Objectify-related request schemas (BFF).

These schemas are shared across objectify routers. Keeping them in one module
reduces router bloat and supports router composition (Composite pattern).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MappingSpecField(BaseModel):
    source_field: str
    target_field: str


class CreateMappingSpecRequest(BaseModel):
    dataset_id: str
    dataset_branch: Optional[str] = Field(default=None, description="Dataset branch (default: dataset branch)")
    artifact_output_name: Optional[str] = Field(
        default=None, description="Artifact output name this mapping spec applies to (default: dataset name)"
    )
    schema_hash: Optional[str] = Field(default=None, description="Schema hash for the bound output")
    backing_datasource_id: Optional[str] = Field(default=None, description="Backing datasource id")
    backing_datasource_version_id: Optional[str] = Field(default=None, description="Backing datasource version id")
    target_class_id: str
    mappings: List[MappingSpecField]
    target_field_types: Optional[Dict[str, str]] = Field(default=None, description="Optional target field types")
    status: str = Field(default="ACTIVE")
    auto_sync: bool = Field(default=True)
    options: Optional[Dict[str, Any]] = Field(default=None)


class TriggerObjectifyRequest(BaseModel):
    mapping_spec_id: Optional[str] = Field(default=None)
    target_class_id: Optional[str] = Field(
        default=None,
        description="OMS ontology class id. When provided without mapping_spec_id, "
        "resolves property_mappings from OMS backing_source (Foundry-style).",
    )
    dataset_version_id: Optional[str] = Field(default=None)
    artifact_id: Optional[str] = Field(default=None)
    artifact_output_name: Optional[str] = Field(default=None)
    batch_size: Optional[int] = Field(default=None)
    max_rows: Optional[int] = Field(default=None)
    allow_partial: bool = Field(default=False)
    options: Optional[Dict[str, Any]] = Field(default=None)


class RunObjectifyDAGRequest(BaseModel):
    """Topologically enqueue objectify jobs based on mapping-spec relationship dependencies."""

    class_ids: List[str] = Field(..., min_length=1, description="Target ontology class ids to objectify")
    branch: str = Field(default="main", description="Ontology branch to use for all jobs")
    include_dependencies: bool = Field(
        default=True,
        description="If true, automatically include transitive dependencies referenced via relationships",
    )
    max_depth: int = Field(default=10, ge=0, le=50, description="Max dependency expansion depth")
    dry_run: bool = Field(default=False, description="If true, only returns the computed plan (no jobs queued)")
    options: Optional[Dict[str, Any]] = Field(default=None, description="Options merged into each job options")


class DetectRelationshipsRequest(BaseModel):
    confidence_threshold: float = Field(default=0.6, ge=0.0, le=1.0)
    include_sample_analysis: bool = Field(default=True)


class DetectRelationshipsResponse(BaseModel):
    patterns: List[Dict[str, Any]] = Field(default_factory=list)
    suggestions: List[Dict[str, Any]] = Field(default_factory=list)


class TriggerIncrementalRequest(BaseModel):
    execution_mode: str = Field(default="incremental", pattern="^(full|incremental|delta)$")
    watermark_column: Optional[str] = None
    force_full_refresh: bool = Field(default=False)
    max_rows: Optional[int] = None
    batch_size: Optional[int] = None
