"""
AI/LLM API models (domain-neutral).

These are used by the BFF AI router to expose LLM-assisted endpoints while
keeping deterministic core APIs unchanged (see docs/LLM_INTEGRATION.md).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from shared.models.graph_query import GraphQueryRequest
from shared.models.ontology import QueryInput


class AIQueryMode(str, Enum):
    auto = "auto"
    label_query = "label_query"
    graph_query = "graph_query"


class AIQueryTool(str, Enum):
    label_query = "label_query"
    graph_query = "graph_query"
    unsupported = "unsupported"


class AIQueryPlan(BaseModel):
    """
    LLM-produced query plan.

    Safety contract:
    - The plan is *only* a recommendation. Server validates and enforces caps.
    - Output must be JSON (schema-validated).
    """

    tool: AIQueryTool
    interpretation: str = Field(..., description="Human-readable interpretation of the question")
    confidence: float = Field(..., ge=0.0, le=1.0)
    query: Optional[QueryInput] = None
    graph_query: Optional[GraphQueryRequest] = None
    warnings: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_shape(self) -> "AIQueryPlan":
        if self.tool == AIQueryTool.label_query and not self.query:
            raise ValueError("tool=label_query requires 'query'")
        if self.tool == AIQueryTool.graph_query and not self.graph_query:
            raise ValueError("tool=graph_query requires 'graph_query'")
        if self.tool == AIQueryTool.unsupported and (self.query or self.graph_query):
            raise ValueError("tool=unsupported must not include executable queries")
        return self


class AIQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    branch: str = Field(default="main", description="Branch for graph queries (default: main)")
    mode: AIQueryMode = Field(default=AIQueryMode.auto)
    limit: int = Field(default=50, ge=1, le=500)
    include_provenance: bool = Field(default=True)
    include_documents: bool = Field(default=True)


class AIAnswer(BaseModel):
    answer: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    rationale: Optional[str] = None
    follow_ups: List[str] = Field(default_factory=list)


class AIQueryResponse(BaseModel):
    answer: AIAnswer
    plan: AIQueryPlan
    execution: Dict[str, Any]
    llm: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)

