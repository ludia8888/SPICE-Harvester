"""Ontology extension request schemas (shared canonical definition).

These models back the "ontology extensions" resource endpoints in both BFF and OMS.
Canonical location to avoid divergence between services.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class OntologyResourceRequest(BaseModel):
    id: Optional[str] = Field(None, description="Resource id (optional; auto-generated from label if omitted)")
    label: Any = Field(..., description="Resource label")
    description: Optional[Any] = Field(None, description="Resource description")
    spec: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class OntologyDeploymentRecordRequest(BaseModel):
    target_branch: str = Field("main", description="Deployment target branch")
    ontology_commit_id: Optional[str] = Field(
        None,
        description="Deployed ontology commit id (defaults to branch:<target_branch>)",
    )
    snapshot_rid: Optional[str] = Field(None, description="Optional snapshot resource identifier")
    deployed_by: Optional[str] = Field(None, description="Actor id for deployment audit")
    metadata: Dict[str, Any] = Field(default_factory=dict)
