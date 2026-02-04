"""Ontology extension request schemas (BFF).

These models back the "ontology extensions" endpoints that handle ontology
resources (shared-properties, value-types, etc.) and governance flows
(proposals, approvals, deploy).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class OntologyResourceRequest(BaseModel):
    id: Optional[str] = Field(None, description="Resource id (optional)")
    label: Any = Field(..., description="Resource label")
    description: Optional[Any] = Field(None, description="Resource description")
    spec: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class OntologyProposalRequest(BaseModel):
    source_branch: str = Field(..., description="Source branch name")
    target_branch: str = Field("main", description="Target branch name")
    title: str = Field(..., description="Proposal title")
    description: Optional[str] = Field(None, description="Proposal description")
    author: str = Field("system", description="Proposal author")


class OntologyApproveRequest(BaseModel):
    merge_message: Optional[str] = Field(None, description="Merge message override")
    author: str = Field("system", description="Approval author")


class OntologyDeployRequest(BaseModel):
    proposal_id: str = Field(..., description="Proposal (pull request) id")
    ontology_commit_id: str = Field(..., description="Approved ontology commit id")
    merge_message: Optional[str] = Field(None, description="Merge message override")
    author: str = Field("system", description="Deploy author")
    definition_hash: Optional[str] = Field(None, description="Ontology definition hash")
