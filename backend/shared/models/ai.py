"""
AI/LLM API models (domain-neutral).

These are used by the BFF AI router to expose LLM-assisted endpoints while
keeping deterministic core APIs unchanged (see docs/LLM_INTEGRATION.md).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator
from pydantic import ConfigDict

from shared.models.graph_query import GraphQueryRequest
from shared.models.ontology import QueryInput


class AIQueryMode(str, Enum):
    auto = "auto"
    label_query = "label_query"
    graph_query = "graph_query"
    dataset_list = "dataset_list"


class AIQueryTool(str, Enum):
    label_query = "label_query"
    graph_query = "graph_query"
    dataset_list = "dataset_list"
    unsupported = "unsupported"


class AIIntentType(str, Enum):
    greeting = "greeting"
    small_talk = "small_talk"
    help = "help"
    data_query = "data_query"
    plan_request = "plan_request"
    unknown = "unknown"


class AIIntentRoute(str, Enum):
    chat = "chat"
    query = "query"
    plan = "plan"
    pipeline = "pipeline"


class AIIntentRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    db_name: Optional[str] = Field(default=None, description="Selected project/db name (if any)")
    project_name: Optional[str] = Field(default=None)
    pipeline_name: Optional[str] = Field(default=None)
    language: Optional[str] = Field(default=None, description="Preferred language hint (optional)")
    context: Optional[Dict[str, Any]] = Field(default=None, description="Additional context hints")
    session_id: Optional[str] = Field(default=None, description="Conversation session id (UUID)")


class AIIntentDraft(BaseModel):
    model_config = ConfigDict(extra="ignore")

    intent: Optional[AIIntentType] = None
    route: Optional[AIIntentRoute] = None
    confidence: Optional[float] = None
    requires_clarification: Optional[bool] = None
    clarifying_question: Optional[str] = None
    reply: Optional[str] = None
    missing_fields: Optional[List[str]] = None


class AIIntentResponse(BaseModel):
    intent: AIIntentType
    route: AIIntentRoute
    confidence: float = Field(..., ge=0.0, le=1.0)
    requires_clarification: bool = Field(default=False)
    clarifying_question: Optional[str] = None
    reply: Optional[str] = None
    missing_fields: List[str] = Field(default_factory=list)
    llm: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _validate_reply(self) -> "AIIntentResponse":
        if self.requires_clarification and not (self.clarifying_question or "").strip():
            raise ValueError("clarifying_question required when requires_clarification is true")
        return self


class AIQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    branch: str = Field(default="main", description="Branch for graph queries (default: main)")
    mode: AIQueryMode = Field(default=AIQueryMode.auto)
    limit: int = Field(default=50, ge=1, le=500)
    include_provenance: bool = Field(default=True)
    include_documents: bool = Field(default=True)
    session_id: Optional[str] = Field(default=None, description="Conversation session id (UUID)")


class AIAnswer(BaseModel):
    answer: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    rationale: Optional[str] = None
    follow_ups: List[str] = Field(default_factory=list)


class DatasetListQuery(BaseModel):
    name_contains: Optional[str] = Field(default=None, description="Substring to match dataset/file name")
    source_type: Optional[str] = Field(default=None, description="Dataset source type filter (optional)")
    limit: int = Field(default=50, ge=1, le=500)


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
    dataset_query: Optional[DatasetListQuery] = None
    warnings: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_shape(self) -> "AIQueryPlan":
        if self.tool == AIQueryTool.label_query and not self.query:
            raise ValueError("tool=label_query requires 'query'")
        if self.tool == AIQueryTool.graph_query and not self.graph_query:
            raise ValueError("tool=graph_query requires 'graph_query'")
        if self.tool == AIQueryTool.dataset_list and self.dataset_query is None:
            self.dataset_query = DatasetListQuery()
        if self.tool == AIQueryTool.unsupported and (self.query or self.graph_query or self.dataset_query):
            raise ValueError("tool=unsupported must not include executable queries")
        return self


class AIQueryResponse(BaseModel):
    answer: AIAnswer
    plan: AIQueryPlan
    execution: Dict[str, Any]
    llm: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
