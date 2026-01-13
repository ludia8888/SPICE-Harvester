"""
Agent plan validation against allowlist + risk policy.

This module enforces tool allowlist constraints and derives plan risk/approval
requirements before execution.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from shared.models.agent_plan import AgentPlan, AgentPlanRiskLevel, AgentPlanStep
from shared.models.agent_plan_report import (
    PlanCompilationReport,
    PlanDiagnostic,
    PlanDiagnosticSeverity,
    PlanPatchOp,
    PlanPatchProposal,
    PlanPolicySnapshot,
    PlanRequiredControl,
)
from shared.errors.enterprise_catalog import enterprise_catalog_fingerprint
from shared.services.agent_tool_registry import AgentToolPolicyRecord, AgentToolRegistry
from shared.utils.canonical_json import sha256_canonical_json_prefixed


_RISK_ORDER = {
    AgentPlanRiskLevel.read: 0,
    AgentPlanRiskLevel.write: 1,
    AgentPlanRiskLevel.admin: 2,
    AgentPlanRiskLevel.destructive: 3,
}
_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_ACTION_SUBMIT_SUFFIX = "/actions/{action_type_id}/submit"
_ACTION_SIMULATE_SUFFIX = "/actions/{action_type_id}/simulate"


@dataclass(frozen=True)
class AgentPlanValidationResult:
    plan: AgentPlan
    errors: List[str]
    warnings: List[str]
    compilation_report: PlanCompilationReport


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


def _is_action_submit(policy: AgentToolPolicyRecord) -> bool:
    path = (policy.path or "").rstrip("/")
    return policy.method.upper() == "POST" and path.endswith(_ACTION_SUBMIT_SUFFIX)


def _is_action_simulate(policy: AgentToolPolicyRecord) -> bool:
    path = (policy.path or "").rstrip("/")
    return policy.method.upper() == "POST" and path.endswith(_ACTION_SIMULATE_SUFFIX)


def _enforce_action_simulate_first(
    *,
    steps: List[AgentPlanStep],
    policies: List[AgentToolPolicyRecord],
    errors: List[str],
) -> None:
    """
    Ensure action writeback submit is preceded by simulate for the same db_name + action_type_id.

    This enforces the "always simulate → approve → submit" rule for writeback workflows.
    """

    seen_simulate: set[tuple[str, str]] = set()
    for step, policy in zip(steps, policies):
        if _is_action_simulate(policy):
            db_name = str(step.path_params.get("db_name") or "").strip()
            action_type_id = str(step.path_params.get("action_type_id") or "").strip()
            if db_name and action_type_id:
                seen_simulate.add((db_name, action_type_id))

        if _is_action_submit(policy):
            db_name = str(step.path_params.get("db_name") or "").strip()
            action_type_id = str(step.path_params.get("action_type_id") or "").strip()
            if not db_name or not action_type_id:
                continue
            if (db_name, action_type_id) not in seen_simulate:
                errors.append(
                    f"tool_id={step.tool_id} action submit requires prior simulate step for "
                    f"db_name={db_name} action_type_id={action_type_id}"
                )


def _tool_policy_hash(policies: List[AgentToolPolicyRecord]) -> str:
    payload = [
        {
            "tool_id": p.tool_id,
            "method": p.method,
            "path": p.path,
            "risk_level": p.risk_level,
            "requires_approval": bool(p.requires_approval),
            "requires_idempotency_key": bool(p.requires_idempotency_key),
            "status": p.status,
            "roles": p.roles,
            "max_payload_bytes": p.max_payload_bytes,
        }
        for p in policies
    ]
    return sha256_canonical_json_prefixed(payload)


def _policy_snapshot(*, tool_allowlist_hash: str) -> PlanPolicySnapshot:
    catalog = enterprise_catalog_fingerprint()
    snapshot_hash = sha256_canonical_json_prefixed(
        {"catalog_fingerprint": catalog, "tool_allowlist_hash": tool_allowlist_hash}
    )
    return PlanPolicySnapshot(
        catalog_fingerprint=catalog,
        tool_allowlist_hash=tool_allowlist_hash,
        policy_snapshot_hash=snapshot_hash,
    )


def _find_policy_by_path(policies: List[AgentToolPolicyRecord], *, method: str, path: str) -> Optional[AgentToolPolicyRecord]:
    wanted_method = str(method or "").strip().upper()
    wanted_path = str(path or "").strip().rstrip("/")
    for policy in policies:
        if str(policy.method or "").strip().upper() != wanted_method:
            continue
        if str(policy.path or "").strip().rstrip("/") == wanted_path:
            return policy
    return None


def _ensure_list(value: Any) -> list:
    return list(value) if isinstance(value, list) else []


def _action_simulation_artifacts(simulation_id: str) -> list[str]:
    sid = str(simulation_id or "").strip()
    return [f"action_simulation.id.{sid}", f"action_simulation.effective.{sid}"]


def _derive_required_controls(*, plan: AgentPlan, contains_action_submit: bool) -> list[PlanRequiredControl]:
    controls: list[PlanRequiredControl] = []
    if contains_action_submit:
        controls.append(PlanRequiredControl.simulate_first)
        controls.append(PlanRequiredControl.artifact_flow_declared)
    if plan.requires_approval or plan.risk_level != AgentPlanRiskLevel.read:
        controls.append(PlanRequiredControl.approval_required)
        controls.append(PlanRequiredControl.idempotency_key_required)
    controls.append(PlanRequiredControl.policy_snapshot_bound)
    return controls


async def validate_agent_plan(
    *,
    plan: AgentPlan,
    tool_registry: AgentToolRegistry,
) -> AgentPlanValidationResult:
    errors: List[str] = []
    warnings: List[str] = []
    diagnostics: List[PlanDiagnostic] = []
    patches: List[PlanPatchProposal] = []
    patch_ids: set[str] = set()

    def _add_diag(
        *,
        code: str,
        severity: PlanDiagnosticSeverity,
        message: str,
        step_id: Optional[str] = None,
        tool_id: Optional[str] = None,
        field: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        fix_hint: Optional[str] = None,
        patch_id: Optional[str] = None,
    ) -> None:
        diagnostics.append(
            PlanDiagnostic(
                code=code,
                severity=severity,
                message=message,
                step_id=step_id,
                tool_id=tool_id,
                field=field,
                details=details,
                fix_hint=fix_hint,
                patch_id=patch_id,
            )
        )

    def _add_error(
        *,
        code: str,
        message: str,
        step: AgentPlanStep,
        field: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        fix_hint: Optional[str] = None,
        patch_id: Optional[str] = None,
    ) -> None:
        errors.append(message)
        _add_diag(
            code=code,
            severity=PlanDiagnosticSeverity.error,
            message=message,
            step_id=step.step_id,
            tool_id=step.tool_id,
            field=field,
            details=details,
            fix_hint=fix_hint,
            patch_id=patch_id,
        )

    def _add_warning(
        *,
        code: str,
        message: str,
        step: AgentPlanStep,
        field: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        fix_hint: Optional[str] = None,
        patch_id: Optional[str] = None,
    ) -> None:
        warnings.append(message)
        _add_diag(
            code=code,
            severity=PlanDiagnosticSeverity.warning,
            message=message,
            step_id=step.step_id,
            tool_id=step.tool_id,
            field=field,
            details=details,
            fix_hint=fix_hint,
            patch_id=patch_id,
        )

    def _maybe_add_patch(
        *,
        patch_id: str,
        title: str,
        operations: List[PlanPatchOp],
        description: Optional[str] = None,
        auto_applicable: bool = False,
    ) -> None:
        if patch_id in patch_ids:
            return
        patch_ids.add(patch_id)
        patches.append(
            PlanPatchProposal(
                patch_id=patch_id,
                title=title,
                description=description,
                operations=operations,
                auto_applicable=auto_applicable,
            )
        )

    all_policies = await tool_registry.list_tool_policies(status=None, limit=1000)
    policy_by_tool_id = {p.tool_id: p for p in all_policies if p.tool_id}
    active_policies = [p for p in all_policies if str(p.status or "").strip().upper() == "ACTIVE"]
    allowlist_hash = _tool_policy_hash(active_policies)
    snapshot = _policy_snapshot(tool_allowlist_hash=allowlist_hash)

    normalized_steps: List[AgentPlanStep] = []
    risk_levels: List[AgentPlanRiskLevel] = []

    seen_artifacts: set[str] = set()
    seen_simulate: set[tuple[str, str]] = set()
    simulation_by_key: dict[tuple[str, str], str] = {}
    contains_action_submit = False

    for idx, step in enumerate(plan.steps):
        policy = policy_by_tool_id.get(step.tool_id)
        if not policy:
            msg = f"tool_id={step.tool_id} not in allowlist"
            _add_error(
                code="tool_not_in_allowlist",
                message=msg,
                step=step,
                fix_hint="Choose a tool_id from the allowlist tool registry.",
            )
            risk_levels.append(AgentPlanRiskLevel.admin)
            normalized_steps.append(step.model_copy(update={"requires_approval": True}))
            continue

        if str(policy.status or "").strip().upper() != "ACTIVE":
            msg = f"tool_id={step.tool_id} is not ACTIVE"
            _add_error(
                code="tool_not_active",
                message=msg,
                step=step,
                fix_hint="Activate the tool policy or choose another tool_id.",
            )
            risk_levels.append(AgentPlanRiskLevel.admin)
            normalized_steps.append(step.model_copy(update={"requires_approval": True}))
            continue

        method = step.method or policy.method
        if step.method and policy.method and step.method != policy.method:
            msg = f"tool_id={step.tool_id} method mismatch (plan={step.method}, allowlist={policy.method})"
            patch_id = f"fix_method.{step.step_id}"
            _add_error(
                code="method_mismatch",
                message=msg,
                step=step,
                field="method",
                fix_hint="Align the step HTTP method with the allowlist policy.",
                patch_id=patch_id,
            )
            _maybe_add_patch(
                patch_id=patch_id,
                title=f"Fix method for step {step.step_id}",
                description="Replace step.method with the allowlist policy method.",
                auto_applicable=True,
                operations=[PlanPatchOp(op="replace", path=f"/steps/{idx}/method", value=str(policy.method).strip().upper())],
            )

        try:
            risk = _derive_risk_level(method) if not policy.risk_level else AgentPlanRiskLevel(policy.risk_level)
        except ValueError as exc:
            msg = f"tool_id={step.tool_id} invalid risk_level: {exc}"
            _add_error(code="invalid_risk_level", message=msg, step=step)
            risk = AgentPlanRiskLevel.admin

        step_requires_approval = step.requires_approval or policy.requires_approval or risk != AgentPlanRiskLevel.read

        if policy.requires_idempotency_key and not step.idempotency_key:
            msg = f"tool_id={step.tool_id} requires idempotency_key"
            patch_id = f"add_idempotency.{step.step_id}"
            _add_error(
                code="idempotency_key_required",
                message=msg,
                step=step,
                field="idempotency_key",
                patch_id=patch_id,
            )
            _maybe_add_patch(
                patch_id=patch_id,
                title=f"Add idempotency_key for step {step.step_id}",
                description="Generate an idempotency key for this step.",
                auto_applicable=True,
                operations=[PlanPatchOp(op="add", path=f"/steps/{idx}/idempotency_key", value=str(uuid4()))],
            )

        if str(method or "").strip().upper() in _WRITE_METHODS and not step.idempotency_key:
            msg = f"tool_id={step.tool_id} write method requires idempotency_key"
            patch_id = f"add_idempotency_write.{step.step_id}"
            _add_error(
                code="idempotency_key_required",
                message=msg,
                step=step,
                field="idempotency_key",
                patch_id=patch_id,
            )
            _maybe_add_patch(
                patch_id=patch_id,
                title=f"Add idempotency_key for step {step.step_id}",
                description="Generate an idempotency key for this write step.",
                auto_applicable=True,
                operations=[PlanPatchOp(op="add", path=f"/steps/{idx}/idempotency_key", value=str(uuid4()))],
            )

        required_params = _extract_path_params(policy.path)
        missing = [param for param in required_params if not step.path_params.get(param)]
        if missing:
            msg = f"tool_id={step.tool_id} missing path_params: {missing}"
            patch_id = f"fill_path_params.{step.step_id}"
            _add_error(
                code="missing_path_params",
                message=msg,
                step=step,
                field="path_params",
                details={"missing": missing},
                fix_hint="Provide values for required path_params.",
                patch_id=patch_id,
            )
            ops: List[PlanPatchOp] = []
            for param in missing[:10]:
                ops.append(
                    PlanPatchOp(op="add", path=f"/steps/{idx}/path_params/{param}", value="<REQUIRED>")
                )
            if ops:
                _maybe_add_patch(
                    patch_id=patch_id,
                    title=f"Fill missing path_params for step {step.step_id}",
                    description="Adds placeholders for missing path params (replace <REQUIRED> with real values).",
                    auto_applicable=False,
                    operations=ops,
                )

        if step.requires_approval and not policy.requires_approval and risk == AgentPlanRiskLevel.read:
            msg = f"tool_id={step.tool_id} approval requested for read-only step"
            _add_warning(code="approval_unnecessary", message=msg, step=step, field="requires_approval")

        normalized_step = step.model_copy(
            update={
                "method": str(method or "").strip().upper() or None,
                "requires_approval": step_requires_approval,
            }
        )

        # Dataflow: consumes must have been produced previously.
        for artifact in normalized_step.consumes:
            if artifact not in seen_artifacts:
                msg = f"step_id={normalized_step.step_id} consumes missing artifact: {artifact}"
                _add_error(
                    code="missing_artifact",
                    message=msg,
                    step=normalized_step,
                    field="consumes",
                    details={"artifact": artifact},
                    fix_hint="Add a prior step that produces this artifact or adjust consumes.",
                )

        if _is_action_simulate(policy):
            db_name = str(normalized_step.path_params.get("db_name") or "").strip()
            action_type_id = str(normalized_step.path_params.get("action_type_id") or "").strip()
            if db_name and action_type_id:
                seen_simulate.add((db_name, action_type_id))

            body = normalized_step.body if isinstance(normalized_step.body, dict) else {}
            sim_id = str(body.get("simulation_id") or "").strip()
            if not sim_id:
                patch_id = f"add_simulation_id.{normalized_step.step_id}"
                _add_error(
                    code="simulation_id_required",
                    message=f"tool_id={normalized_step.tool_id} simulate requires body.simulation_id (UUID) for traceable plans",
                    step=normalized_step,
                    field="body.simulation_id",
                    fix_hint="Set simulation_id explicitly so submit can link to the simulation.",
                    patch_id=patch_id,
                )
                generated = str(uuid4())
                ops: List[PlanPatchOp] = []
                if not isinstance(normalized_step.body, dict):
                    ops.append(PlanPatchOp(op="add", path=f"/steps/{idx}/body", value={}))
                ops.append(PlanPatchOp(op="add", path=f"/steps/{idx}/body/simulation_id", value=generated))
                _maybe_add_patch(
                    patch_id=patch_id,
                    title=f"Add simulation_id for step {normalized_step.step_id}",
                    description="Adds a deterministic simulation_id so subsequent submit steps can reference it.",
                    auto_applicable=True,
                    operations=ops,
                )
                sim_id = generated
            else:
                # Track simulation id for submit linking if path params are set.
                if db_name and action_type_id:
                    simulation_by_key[(db_name, action_type_id)] = sim_id

            required_produces = set(_action_simulation_artifacts(sim_id))
            missing_produces = [a for a in required_produces if a not in set(normalized_step.produces)]
            if missing_produces:
                patch_id = f"add_simulation_produces.{normalized_step.step_id}"
                _add_error(
                    code="produces_missing_required_artifacts",
                    message=f"tool_id={normalized_step.tool_id} simulate must declare produces for {sorted(required_produces)}",
                    step=normalized_step,
                    field="produces",
                    fix_hint="Declare produced artifacts so downstream steps can safely consume them.",
                    patch_id=patch_id,
                )
                ops = [PlanPatchOp(op="replace", path=f"/steps/{idx}/produces", value=sorted(required_produces))]
                _maybe_add_patch(
                    patch_id=patch_id,
                    title=f"Declare produces for simulate step {normalized_step.step_id}",
                    description="Sets produces to include the required simulation artifacts.",
                    auto_applicable=True,
                    operations=ops,
                )

        if _is_action_submit(policy):
            contains_action_submit = True
            db_name = str(normalized_step.path_params.get("db_name") or "").strip()
            action_type_id = str(normalized_step.path_params.get("action_type_id") or "").strip()
            key = (db_name, action_type_id) if db_name and action_type_id else None

            if key and key not in seen_simulate:
                msg = (
                    f"tool_id={normalized_step.tool_id} action submit requires prior simulate step for "
                    f"db_name={db_name} action_type_id={action_type_id}"
                )
                patch_id = f"insert_simulate_before.{normalized_step.step_id}"
                _add_error(
                    code="simulate_first_required",
                    message=msg,
                    step=normalized_step,
                    fix_hint="Add an action simulate step before submit.",
                    patch_id=patch_id,
                )
                submit_path = str(policy.path or "").rstrip("/")
                simulate_path = submit_path[: -len(_ACTION_SUBMIT_SUFFIX)] + _ACTION_SIMULATE_SUFFIX if submit_path.endswith(_ACTION_SUBMIT_SUFFIX) else None
                simulate_policy = (
                    _find_policy_by_path(active_policies, method="POST", path=simulate_path) if simulate_path else None
                )
                if simulate_policy and key:
                    generated_sim_id = str(uuid4())
                    simulate_step_id = f"simulate_before_{normalized_step.step_id}"[:200]
                    simulate_step = {
                        "step_id": simulate_step_id,
                        "tool_id": simulate_policy.tool_id,
                        "method": "POST",
                        "path_params": {"db_name": db_name, "action_type_id": action_type_id},
                        "body": {
                            "input": (normalized_step.body or {}).get("input") if isinstance(normalized_step.body, dict) else {},
                            "correlation_id": (normalized_step.body or {}).get("correlation_id") if isinstance(normalized_step.body, dict) else None,
                            "metadata": (normalized_step.body or {}).get("metadata") if isinstance(normalized_step.body, dict) else {},
                            "base_branch": (normalized_step.body or {}).get("base_branch") if isinstance(normalized_step.body, dict) else "main",
                            "overlay_branch": (normalized_step.body or {}).get("overlay_branch") if isinstance(normalized_step.body, dict) else None,
                            "simulation_id": generated_sim_id,
                            "include_effects": True,
                        },
                        "produces": sorted(_action_simulation_artifacts(generated_sim_id)),
                        "consumes": [],
                        "requires_approval": True,
                        "idempotency_key": str(uuid4()),
                        "data_scope": dict(normalized_step.data_scope or {}),
                        "description": "server-suggested: simulate before submit",
                    }
                    submit_body = normalized_step.body if isinstance(normalized_step.body, dict) else {}
                    patched_submit_body = {
                        **submit_body,
                        "metadata": {**(submit_body.get("metadata") if isinstance(submit_body.get("metadata"), dict) else {}), "simulation_id": generated_sim_id},
                    }
                    ops: List[PlanPatchOp] = [
                        PlanPatchOp(op="add", path=f"/steps/{idx}", value=simulate_step),
                        PlanPatchOp(op="replace", path=f"/steps/{idx+1}/body", value=patched_submit_body),
                        PlanPatchOp(op="replace", path=f"/steps/{idx+1}/consumes", value=sorted(_action_simulation_artifacts(generated_sim_id))),
                    ]
                    if not normalized_step.idempotency_key:
                        ops.append(PlanPatchOp(op="add", path=f"/steps/{idx+1}/idempotency_key", value=str(uuid4())))
                    _maybe_add_patch(
                        patch_id=patch_id,
                        title=f"Insert simulate step before {normalized_step.step_id}",
                        description="Inserts a simulate step and links submit to the simulation_id via consumes + metadata.",
                        auto_applicable=True,
                        operations=ops,
                    )

            expected_sim_id = simulation_by_key.get(key) if key else None
            submit_body = normalized_step.body if isinstance(normalized_step.body, dict) else {}
            submit_meta = submit_body.get("metadata") if isinstance(submit_body.get("metadata"), dict) else {}
            declared_sim_id = str(submit_meta.get("simulation_id") or "").strip() or None
            use_sim_id = expected_sim_id or declared_sim_id
            if not use_sim_id:
                patch_id = f"link_simulation_id.{normalized_step.step_id}"
                _add_error(
                    code="simulation_link_required",
                    message=f"tool_id={normalized_step.tool_id} submit requires body.metadata.simulation_id to link to prior simulation",
                    step=normalized_step,
                    field="body.metadata.simulation_id",
                    fix_hint="Copy simulation_id from the prior simulate step (same db_name/action_type_id).",
                    patch_id=patch_id,
                )
            else:
                required_consumes = set(_action_simulation_artifacts(use_sim_id))
                missing_consumes = [a for a in required_consumes if a not in set(normalized_step.consumes)]
                if missing_consumes:
                    patch_id = f"add_submit_consumes.{normalized_step.step_id}"
                    _add_error(
                        code="consumes_missing_required_artifacts",
                        message=f"tool_id={normalized_step.tool_id} submit must declare consumes for {sorted(required_consumes)}",
                        step=normalized_step,
                        field="consumes",
                        fix_hint="Declare consumed artifacts so the compiler can verify simulate→submit dataflow.",
                        patch_id=patch_id,
                    )
                    _maybe_add_patch(
                        patch_id=patch_id,
                        title=f"Declare consumes for submit step {normalized_step.step_id}",
                        description="Sets consumes to include the required simulation artifacts.",
                        auto_applicable=True,
                        operations=[PlanPatchOp(op="replace", path=f"/steps/{idx}/consumes", value=sorted(required_consumes))],
                    )

                if expected_sim_id and declared_sim_id and declared_sim_id != expected_sim_id:
                    patch_id = f"fix_simulation_link.{normalized_step.step_id}"
                    _add_error(
                        code="simulation_link_mismatch",
                        message=f"tool_id={normalized_step.tool_id} submit metadata.simulation_id must match prior simulate (expected={expected_sim_id})",
                        step=normalized_step,
                        field="body.metadata.simulation_id",
                        patch_id=patch_id,
                    )
                    patched_body = {
                        **submit_body,
                        "metadata": {**submit_meta, "simulation_id": expected_sim_id},
                    }
                    _maybe_add_patch(
                        patch_id=patch_id,
                        title=f"Fix simulation_id link for submit step {normalized_step.step_id}",
                        description="Replaces submit metadata.simulation_id with the simulation_id from the prior simulate step.",
                        auto_applicable=True,
                        operations=[PlanPatchOp(op="replace", path=f"/steps/{idx}/body", value=patched_body)],
                    )

        # Update produced artifacts after validations.
        for artifact in normalized_step.produces:
            seen_artifacts.add(artifact)

        normalized_steps.append(normalized_step)
        risk_levels.append(risk)

    plan_risk = _max_risk(risk_levels)
    plan_requires_approval = plan.requires_approval or any(step.requires_approval for step in normalized_steps)
    if plan_risk != AgentPlanRiskLevel.read:
        plan_requires_approval = True

    normalized_plan = plan.model_copy(
        update={
            "steps": normalized_steps,
            "risk_level": plan_risk,
            "requires_approval": plan_requires_approval,
        }
    )

    required_controls = _derive_required_controls(plan=normalized_plan, contains_action_submit=contains_action_submit)
    report_status = "success" if not errors else "clarification_required"
    report = PlanCompilationReport(
        plan_id=str(normalized_plan.plan_id or plan.plan_id or ""),
        status=report_status,
        plan_digest=None,
        policy_snapshot=snapshot,
        risk_level=str(plan_risk.value if hasattr(plan_risk, "value") else plan_risk),
        required_controls=required_controls,
        diagnostics=diagnostics,
        patches=patches,
    )

    return AgentPlanValidationResult(
        plan=normalized_plan,
        errors=errors,
        warnings=warnings,
        compilation_report=report,
    )
