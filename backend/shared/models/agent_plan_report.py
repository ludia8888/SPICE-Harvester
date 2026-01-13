"""
Agent plan compilation/validation report models.

These models support the "reject + server patch proposal" workflow:
- Keep the system transparent (no silent normalization for safety-critical fixes).
- Still provide actionable, machine-readable fixes for clients/agents.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class PlanDiagnosticSeverity(str, Enum):
    error = "error"
    warning = "warning"


class PlanPatchOp(BaseModel):
    """
    RFC6902-like JSON patch operation.

    We keep the surface small and validate aggressively because patches are
    applied to control-plane artifacts.
    """

    op: str = Field(..., description="add|remove|replace|move|copy|test")
    path: str = Field(..., min_length=1, max_length=500, description="JSON Pointer path")
    from_path: Optional[str] = Field(default=None, alias="from", max_length=500)
    value: Optional[Any] = Field(default=None)

    @field_validator("op")
    @classmethod
    def _validate_op(cls, value: str) -> str:
        op = str(value or "").strip().lower()
        allowed = {"add", "remove", "replace", "move", "copy", "test"}
        if op not in allowed:
            raise ValueError(f"op must be one of {sorted(allowed)}")
        return op

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        path = str(value or "").strip()
        if not path.startswith("/"):
            raise ValueError("path must be a JSON Pointer starting with '/'")
        return path

    @model_validator(mode="after")
    def _validate_shape(self) -> "PlanPatchOp":
        if self.op in {"move", "copy"} and not self.from_path:
            raise ValueError("from is required for move/copy operations")
        if self.op in {"add", "replace", "test"} and self.value is None:
            raise ValueError("value is required for add/replace/test operations")
        if self.op in {"remove"} and self.value is not None:
            raise ValueError("value must be omitted for remove operations")
        return self


class PlanPatchProposal(BaseModel):
    patch_id: str = Field(..., min_length=1, max_length=100)
    title: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(default=None, max_length=2000)
    operations: List[PlanPatchOp] = Field(default_factory=list, max_length=50)
    auto_applicable: bool = Field(
        default=False,
        description="If true, server can apply safely without additional user-provided values (still requires explicit acceptance).",
    )


class PlanDiagnostic(BaseModel):
    code: str = Field(..., min_length=1, max_length=100)
    severity: PlanDiagnosticSeverity = Field(...)
    message: str = Field(..., min_length=1, max_length=2000)
    step_id: Optional[str] = Field(default=None, max_length=200)
    tool_id: Optional[str] = Field(default=None, max_length=200)
    field: Optional[str] = Field(default=None, max_length=200)
    details: Optional[Dict[str, Any]] = None
    fix_hint: Optional[str] = Field(default=None, max_length=2000)
    patch_id: Optional[str] = Field(default=None, max_length=100)


class PlanRequiredControl(str, Enum):
    simulate_first = "simulate_first"
    approval_required = "approval_required"
    idempotency_key_required = "idempotency_key_required"
    artifact_flow_declared = "artifact_flow_declared"
    policy_snapshot_bound = "policy_snapshot_bound"


class PlanPolicySnapshot(BaseModel):
    catalog_fingerprint: str
    tool_allowlist_hash: str
    policy_snapshot_hash: str


class PlanCompilationReport(BaseModel):
    """
    Machine-readable compilation report used by clients and agent UIs.
    """

    plan_id: str
    status: str = Field(..., description="success|clarification_required|error")
    plan_digest: Optional[str] = Field(default=None, description="sha256:... digest of the stored plan JSON")
    policy_snapshot: Optional[PlanPolicySnapshot] = None
    risk_level: Optional[str] = Field(default=None, description="read|write|admin|destructive")
    required_controls: List[PlanRequiredControl] = Field(default_factory=list)
    diagnostics: List[PlanDiagnostic] = Field(default_factory=list)
    patches: List[PlanPatchProposal] = Field(default_factory=list)

