from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class AgentToolCall(BaseModel):
    service: Literal["bff"] = Field(..., description="Target service (BFF only)")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        "POST", description="HTTP method"
    )
    path: str = Field(..., description="HTTP path (e.g. /api/v1/pipelines)")
    query: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    body: Optional[Dict[str, Any]] = Field(default=None, description="JSON body")
    headers: Dict[str, str] = Field(default_factory=dict, description="Extra headers")
    base_url: Optional[str] = Field(default=None, description="Base URL for custom service")
    data_scope: Dict[str, Any] = Field(default_factory=dict, description="Data scope hints")
    description: Optional[str] = Field(default=None, description="Human-readable step purpose")


class AgentRunRequest(BaseModel):
    goal: str = Field(..., description="Run goal or instruction")
    steps: List[AgentToolCall] = Field(default_factory=list, description="Ordered tool calls")
    context: Dict[str, Any] = Field(default_factory=dict, description="Shared context metadata")
    dry_run: bool = Field(default=False, description="If true, do not execute tool calls")
    request_id: Optional[str] = Field(default=None, description="Client request id")


class AgentRunResponse(BaseModel):
    run_id: str
    status: str
    created_at: datetime
    steps_count: int


class AgentRunSummary(BaseModel):
    run_id: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    steps_total: int = 0
    steps_completed: int = 0
    failed_step: Optional[int] = None
    events: List[Dict[str, Any]] = Field(default_factory=list)
