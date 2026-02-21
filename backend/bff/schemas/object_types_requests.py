"""Object type contract request schemas (BFF)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ObjectTypeContractRequest(BaseModel):
    class_id: str = Field(..., description="Ontology class id")
    backing_dataset_id: Optional[str] = Field(default=None, description="Dataset id backing the object type")
    backing_datasource_id: Optional[str] = Field(default=None, description="Backing datasource id")
    backing_datasource_version_id: Optional[str] = Field(default=None, description="Backing datasource version id")
    backing_sources: Optional[List[Dict[str, Any]]] = Field(default=None, description="Optional multi-source backing definition")
    dataset_version_id: Optional[str] = Field(default=None, description="Dataset version id")
    schema_hash: Optional[str] = Field(default=None, description="Schema hash override")
    pk_spec: Dict[str, Any] = Field(default_factory=dict, description="Key spec (primary/title/unique/nullability)")
    mapping_spec_id: Optional[str] = Field(default=None, description="Mapping spec id for property mapping")
    mapping_spec_version: Optional[int] = Field(default=None, description="Mapping spec version")
    status: str = Field(default="ACTIVE", description="Contract status")
    auto_generate_mapping: bool = Field(default=False, description="Auto-generate property mapping")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ObjectTypeContractUpdate(BaseModel):
    backing_dataset_id: Optional[str] = None
    backing_datasource_id: Optional[str] = None
    backing_datasource_version_id: Optional[str] = None
    backing_sources: Optional[List[Dict[str, Any]]] = None
    dataset_version_id: Optional[str] = None
    schema_hash: Optional[str] = None
    pk_spec: Optional[Dict[str, Any]] = None
    mapping_spec_id: Optional[str] = None
    mapping_spec_version: Optional[int] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    migration: Optional[Dict[str, Any]] = None
