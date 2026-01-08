"""
Agent plan schema for LangGraph planner + approval gate.

The plan is a JSON contract produced by a planner and validated by the server.
Execution should only happen after allowlist/policy/approval checks.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


_TOOL_ID_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"


class AgentPlanRiskLevel(str, Enum):
    read = "read"
    write = "write"
    admin = "admin"
    destructive = "destructive"


class AgentPlanDataScope(BaseModel):
    db_name: Optional[str] = None
    branch: Optional[str] = None
    dataset_id: Optional[str] = None
    dataset_version_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    object_type: Optional[str] = None
    class_id: Optional[str] = None
    link_type: Optional[str] = None
    mapping_spec_version: Optional[int] = None


class AgentPlanStep(BaseModel):
    step_id: str = Field(..., min_length=1, max_length=200)
    tool_id: str = Field(..., min_length=1, max_length=200)
    method: Optional[str] = Field(default=None, description="Optional HTTP method hint")
    path_params: Dict[str, Any] = Field(default_factory=dict)
    query: Dict[str, Any] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    requires_approval: bool = Field(default=False)
    idempotency_key: Optional[str] = None
    data_scope: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None
    expected_output: Optional[str] = None

    @field_validator("tool_id")
    @classmethod
    def _validate_tool_id(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("tool_id is required")
        for ch in stripped:
            if ch not in _TOOL_ID_ALLOWED:
                raise ValueError("tool_id must match [A-Za-z0-9._-]+")
        return stripped

    @field_validator("method")
    @classmethod
    def _normalize_method(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError("method must be one of GET, POST, PUT, PATCH, DELETE")
        return normalized

    @model_validator(mode="after")
    def _validate_step(self) -> "AgentPlanStep":
        if self.requires_approval and not self.idempotency_key:
            raise ValueError("idempotency_key is required when requires_approval=true")
        if self.method in {"POST", "PUT", "PATCH", "DELETE"} and not self.idempotency_key:
            raise ValueError("idempotency_key is required for write methods")
        return self


class AgentPlan(BaseModel):
    plan_id: Optional[str] = None
    goal: str = Field(..., min_length=1, max_length=2000)
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    risk_level: AgentPlanRiskLevel = Field(default=AgentPlanRiskLevel.read)
    requires_approval: bool = Field(default=False)
    data_scope: AgentPlanDataScope = Field(default_factory=AgentPlanDataScope)
    steps: List[AgentPlanStep] = Field(default_factory=list)
    policy_notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_plan(self) -> "AgentPlan":
        if not self.steps:
            raise ValueError("plan requires at least one step")
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("step_id values must be unique")
        if any(step.requires_approval for step in self.steps) and not self.requires_approval:
            raise ValueError("requires_approval must be true when any step requires approval")
        if self.risk_level in {AgentPlanRiskLevel.write, AgentPlanRiskLevel.admin, AgentPlanRiskLevel.destructive}:
            if not self.requires_approval:
                raise ValueError("requires_approval must be true for write/admin/destructive plans")
        return self


def validate_agent_plan(payload: Dict[str, Any]) -> AgentPlan:
    """
    Validate raw plan payload and return the parsed model.

    This is intentionally a thin wrapper around Pydantic validation so
    callers can capture ValueError/ValidationError and respond consistently.
    """

    return AgentPlan.model_validate(payload)
