"""
Agent plan validation against allowlist + risk policy.

This module enforces tool allowlist constraints and derives plan risk/approval
requirements before execution.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import List, Optional, Tuple

from shared.models.agent_plan import AgentPlan, AgentPlanRiskLevel, AgentPlanStep
from shared.services.agent_tool_registry import AgentToolPolicyRecord, AgentToolRegistry


_RISK_ORDER = {
    AgentPlanRiskLevel.read: 0,
    AgentPlanRiskLevel.write: 1,
    AgentPlanRiskLevel.admin: 2,
    AgentPlanRiskLevel.destructive: 3,
}
_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


@dataclass(frozen=True)
class AgentPlanValidationResult:
    plan: AgentPlan
    errors: List[str]
    warnings: List[str]


def _extract_path_params(path: str) -> List[str]:
    return re.findall(r"{([^}]+)}", path or "")


def _derive_risk_level(method: Optional[str]) -> AgentPlanRiskLevel:
    if not method:
        return AgentPlanRiskLevel.read
    normalized = method.upper()
    if normalized == "DELETE":
        return AgentPlanRiskLevel.destructive
    if normalized in {"POST", "PUT", "PATCH"}:
        return AgentPlanRiskLevel.write
    return AgentPlanRiskLevel.read


def _max_risk(levels: List[AgentPlanRiskLevel]) -> AgentPlanRiskLevel:
    if not levels:
        return AgentPlanRiskLevel.read
    return max(levels, key=lambda level: _RISK_ORDER[level])


def _normalize_step(
    *,
    step: AgentPlanStep,
    policy: AgentToolPolicyRecord,
    errors: List[str],
    warnings: List[str],
) -> Tuple[AgentPlanStep, AgentPlanRiskLevel]:
    method = step.method or policy.method
    if step.method and policy.method and step.method != policy.method:
        errors.append(
            f"tool_id={step.tool_id} method mismatch (plan={step.method}, allowlist={policy.method})"
        )
    risk = _derive_risk_level(method) if not policy.risk_level else AgentPlanRiskLevel(policy.risk_level)
    step_requires_approval = step.requires_approval or policy.requires_approval or risk != AgentPlanRiskLevel.read

    if policy.requires_idempotency_key and not step.idempotency_key:
        errors.append(f"tool_id={step.tool_id} requires idempotency_key")
    if method in _WRITE_METHODS and not step.idempotency_key:
        errors.append(f"tool_id={step.tool_id} write method requires idempotency_key")

    required_params = _extract_path_params(policy.path)
    missing = [param for param in required_params if not step.path_params.get(param)]
    if missing:
        errors.append(f"tool_id={step.tool_id} missing path_params: {missing}")

    if step.requires_approval and not policy.requires_approval and risk == AgentPlanRiskLevel.read:
        warnings.append(f"tool_id={step.tool_id} approval requested for read-only step")

    normalized_step = step.model_copy(
        update={
            "method": method,
            "requires_approval": step_requires_approval,
        }
    )
    return normalized_step, risk


async def validate_agent_plan(
    *,
    plan: AgentPlan,
    tool_registry: AgentToolRegistry,
) -> AgentPlanValidationResult:
    errors: List[str] = []
    warnings: List[str] = []
    normalized_steps: List[AgentPlanStep] = []
    risk_levels: List[AgentPlanRiskLevel] = []

    for step in plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            errors.append(f"tool_id={step.tool_id} not in allowlist")
            continue
        if policy.status.upper() != "ACTIVE":
            errors.append(f"tool_id={step.tool_id} is not ACTIVE")
            continue

        try:
            normalized_step, risk = _normalize_step(
                step=step, policy=policy, errors=errors, warnings=warnings
            )
        except ValueError as exc:
            errors.append(f"tool_id={step.tool_id} invalid risk_level: {exc}")
            continue
        normalized_steps.append(normalized_step)
        risk_levels.append(risk)

    plan_risk = _max_risk(risk_levels)
    plan_requires_approval = plan.requires_approval or any(
        step.requires_approval for step in normalized_steps
    )
    if plan_risk != AgentPlanRiskLevel.read:
        plan_requires_approval = True

    normalized_plan = plan.model_copy(
        update={
            "steps": normalized_steps,
            "risk_level": plan_risk,
            "requires_approval": plan_requires_approval,
        }
    )

    return AgentPlanValidationResult(plan=normalized_plan, errors=errors, warnings=warnings)
