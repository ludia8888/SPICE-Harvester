"""Context7 request schemas (BFF)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    """Context7 search request."""

    query: str = Field(..., description="Search query string")
    limit: int = Field(10, ge=1, le=100, description="Maximum number of results")
    filters: Optional[Dict[str, Any]] = Field(None, description="Search filters")


class KnowledgeRequest(BaseModel):
    """Request to add knowledge to Context7."""

    title: str = Field(..., description="Knowledge title")
    content: str = Field(..., description="Knowledge content")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")


class EntityLinkRequest(BaseModel):
    """Request to create entity relationship."""

    source_id: str = Field(..., description="Source entity ID")
    target_id: str = Field(..., description="Target entity ID")
    relationship: str = Field(..., description="Relationship type")
    properties: Optional[Dict[str, Any]] = Field(None, description="Relationship properties")


class OntologyAnalysisRequest(BaseModel):
    """Request to analyze ontology with Context7."""

    ontology_id: str = Field(..., description="Ontology ID")
    db_name: str = Field(..., description="Database name")
    branch: str = Field("main", description="Ontology branch")
    include_relationships: bool = Field(True, description="Include relationship analysis")
    include_suggestions: bool = Field(True, description="Include improvement suggestions")
