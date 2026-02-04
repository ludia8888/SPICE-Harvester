"""Link type request schemas (BFF)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ForeignKeyRelationshipSpec(BaseModel):
    type: str = Field("foreign_key")
    source_dataset_id: Optional[str] = Field(default=None)
    source_dataset_version_id: Optional[str] = Field(default=None)
    source_pk_fields: Optional[List[str]] = Field(default=None)
    fk_column: str
    target_pk_field: Optional[str] = Field(default=None)
    dangling_policy: str = Field(default="FAIL")
    auto_sync: bool = Field(default=True)


class JoinTableRelationshipSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = Field("join_table")
    join_dataset_id: Optional[str] = Field(default=None)
    join_dataset_version_id: Optional[str] = Field(default=None)
    join_dataset_name: Optional[str] = Field(default=None)
    join_dataset_branch: Optional[str] = Field(default=None)
    auto_create: bool = Field(default=False, alias="autoCreate")
    source_key_column: str
    target_key_column: str
    dedupe_policy: str = Field(default="DEDUP")
    dangling_policy: str = Field(default="FAIL")
    auto_sync: bool = Field(default=True)


class ObjectBackedRelationshipSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = Field("object_backed")
    relationship_object_type: str
    relationship_dataset_id: Optional[str] = Field(default=None)
    relationship_dataset_version_id: Optional[str] = Field(default=None)
    source_key_column: str
    target_key_column: str
    dedupe_policy: str = Field(default="DEDUP")
    dangling_policy: str = Field(default="FAIL")
    auto_sync: bool = Field(default=True)


class LinkTypeRequest(BaseModel):
    id: str = Field(..., description="Link type id")
    label: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    from_ref: str = Field(..., alias="from")
    to_ref: str = Field(..., alias="to")
    predicate: str
    cardinality: str
    status: str = Field(default="ACTIVE")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    relationship_spec: Dict[str, Any] = Field(default_factory=dict)
    trigger_index: bool = Field(default=True)


class LinkTypeUpdateRequest(BaseModel):
    label: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    relationship_spec: Optional[Dict[str, Any]] = None
    trigger_index: bool = Field(default=True)


class LinkEditRequest(BaseModel):
    source_instance_id: str
    target_instance_id: str
    edit_type: str = Field(..., description="ADD or REMOVE")
    branch: Optional[str] = Field(default="main")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

