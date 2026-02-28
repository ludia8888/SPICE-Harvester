"""Database access (RBAC) request schemas (BFF)."""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class DatabaseAccessEntryRequest(BaseModel):
    principal_type: str = Field(default="user", description="Principal type (e.g., user, service)")
    principal_id: str = Field(..., description="Principal identifier (e.g., JWT subject)")
    principal_name: Optional[str] = Field(default=None, description="Display name for auditing")
    role: str = Field(..., description="Database role (Owner/Editor/Viewer/DomainModeler/DataEngineer/Security)")


class UpsertDatabaseAccessRequest(BaseModel):
    entries: List[DatabaseAccessEntryRequest] = Field(default_factory=list)

