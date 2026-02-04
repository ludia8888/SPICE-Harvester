"""Governance request schemas (BFF).

These schemas are shared across governance endpoints.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class CreateBackingDatasourceRequest(BaseModel):
    dataset_id: str
    name: Optional[str] = None
    description: Optional[str] = None


class CreateBackingDatasourceVersionRequest(BaseModel):
    dataset_version_id: str
    schema_hash: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class CreateKeySpecRequest(BaseModel):
    dataset_id: str
    dataset_version_id: Optional[str] = None
    primary_key: List[str] = Field(default_factory=list)
    title_key: List[str] = Field(default_factory=list)
    unique_keys: List[List[str]] = Field(default_factory=list)
    nullable_fields: List[str] = Field(default_factory=list)
    required_fields: List[str] = Field(default_factory=list)


class GatePolicyRequest(BaseModel):
    scope: str
    name: str
    description: Optional[str] = None
    rules: Dict[str, Any] = Field(default_factory=dict)
    status: Literal["ACTIVE", "INACTIVE"] | str = Field(default="ACTIVE")


class AccessPolicyRequest(BaseModel):
    db_name: str
    scope: str = Field(default="data_access")
    subject_type: str
    subject_id: str
    policy: Dict[str, Any] = Field(default_factory=dict)
    status: Literal["ACTIVE", "INACTIVE"] | str = Field(default="ACTIVE")

