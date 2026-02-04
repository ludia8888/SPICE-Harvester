"""
Action-related request schemas (BFF).

These schemas are shared across action submission + simulation endpoints.
Keeping them in one module reduces router bloat and supports router composition.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class ActionSubmitRequest(BaseModel):
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")


class ActionSimulateScenarioRequest(BaseModel):
    scenario_id: Optional[str] = Field(default=None, description="Optional client-provided scenario identifier")
    conflict_policy: Optional[str] = Field(
        default=None,
        description="Optional conflict_policy override for this scenario (WRITEBACK_WINS|BASE_WINS|FAIL|MANUAL_REVIEW)",
    )


class ActionSimulateStatePatch(BaseModel):
    """Patch-like state override for decision simulation (what-if)."""

    set: Dict[str, Any] = Field(default_factory=dict)
    unset: List[str] = Field(default_factory=list)
    link_add: List[Any] = Field(default_factory=list)
    link_remove: List[Any] = Field(default_factory=list)
    delete: bool = Field(default=False, description="delete is not supported for simulation assumptions")

    @field_validator("delete")
    @classmethod
    def _reject_delete(cls, value: bool) -> bool:
        if value:
            raise ValueError("delete is not supported for simulation assumptions")
        return False


class ActionSimulateObservedBaseOverrides(BaseModel):
    """Override observed_base snapshot fields/links to simulate stale reads."""

    fields: Dict[str, Any] = Field(default_factory=dict)
    links: Dict[str, Any] = Field(default_factory=dict)


class ActionSimulateTargetAssumption(BaseModel):
    class_id: str
    instance_id: str
    base_overrides: Optional[ActionSimulateStatePatch] = None
    observed_base_overrides: Optional[ActionSimulateObservedBaseOverrides] = None


class ActionSimulateAssumptions(BaseModel):
    targets: List[ActionSimulateTargetAssumption] = Field(
        default_factory=list,
        description="Per-target state injections (Level 2 what-if base state assumptions).",
    )


class ActionSimulateRequest(BaseModel):
    input: Dict[str, Any] = Field(default_factory=dict, description="Intent-only action input payload")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id for trace/audit")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")
    base_branch: str = Field("main", description="Base branch for authoritative reads (default: main)")
    overlay_branch: Optional[str] = Field(
        default=None,
        description="Optional overlay branch override (default derived from writeback_target)",
    )
    simulation_id: Optional[str] = Field(default=None, description="Optional simulation id for versioned reruns")
    title: Optional[str] = Field(default=None, max_length=200)
    description: Optional[str] = Field(default=None, max_length=2000)
    scenarios: Optional[List[ActionSimulateScenarioRequest]] = Field(
        default=None,
        description="Optional scenario list (policy comparisons). If omitted, server simulates the effective policy only.",
    )
    include_effects: bool = Field(default=True, description="If true, compute downstream lakeFS/ES overlay effects")
    assumptions: Optional[ActionSimulateAssumptions] = Field(
        default=None,
        description="Optional decision simulation assumptions (Level 2 state injection).",
    )

