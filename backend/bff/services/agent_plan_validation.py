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

from bff.services.agent_tool_schemas import validate_against_request_schema


_RISK_ORDER = {
    AgentPlanRiskLevel.read: 0,
    AgentPlanRiskLevel.write: 1,
    AgentPlanRiskLevel.admin: 2,
    AgentPlanRiskLevel.destructive: 3,
}
_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_ACTION_SUBMIT_SUFFIX = "/actions/{action_type_id}/submit"
_ACTION_SIMULATE_SUFFIX = "/actions/{action_type_id}/simulate"
_PIPELINE_SIMULATE_DEFINITION_TOOL_ID = "pipelines.simulate_definition"
_PIPELINE_PREVIEW_TOOL_ID = "pipelines.preview"
_PIPELINE_CREATE_TOOL_ID = "pipelines.create"
_PIPELINE_UPDATE_TOOL_ID = "pipelines.update"
_PIPELINE_BUILD_TOOL_ID = "pipelines.build"
_PIPELINE_DEPLOY_TOOL_ID = "pipelines.deploy"

_TEMPLATE_TOKEN_RE = re.compile(r"\$\{([^{}]+)\}")
_TEMPLATE_KEY_ALLOWED_RE = re.compile(r"^[A-Za-z0-9._-]{1,200}$")
_TEMPLATE_STEP_OUTPUT_KEYS = {
    "pipeline_id",
    "dataset_id",
    "dataset_version_id",
    "job_id",
    "artifact_id",
    "ingest_request_id",
    "mapping_spec_id",
    "action_log_id",
    "simulation_id",
    "command_id",
    "deployed_commit_id",
    "pipeline_run_status",
}


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


def _iter_template_tokens(value: Any) -> list[str]:
    tokens: list[str] = []
    if value is None:
        return tokens
    if isinstance(value, str):
        for match in _TEMPLATE_TOKEN_RE.finditer(value):
            token = str(match.group(1) or "").strip()
            if token:
                tokens.append(token)
        return tokens
    if isinstance(value, dict):
        for nested in value.values():
            tokens.extend(_iter_template_tokens(nested))
        return tokens
    if isinstance(value, list):
        for nested in value:
            tokens.extend(_iter_template_tokens(nested))
        return tokens
    return tokens


def _parse_steps_token(token: str) -> Optional[tuple[str, str]]:
    if not token.startswith("steps."):
        return None
    rest = token[len("steps."):]
    parts = rest.split(".")
    if len(parts) < 2:
        return None
    step_id = ".".join(parts[:-1])
    key = parts[-1]
    if not _TEMPLATE_KEY_ALLOWED_RE.match(step_id):
        return None
    if not _TEMPLATE_KEY_ALLOWED_RE.match(key):
        return None
    return step_id, key


def _parse_artifact_token(token: str) -> Optional[str]:
    if not token.startswith("artifacts."):
        return None
    artifact_key = token[len("artifacts."):]
    if not _TEMPLATE_KEY_ALLOWED_RE.match(artifact_key):
        return None
    return artifact_key


def _parse_context_token(token: str) -> Optional[str]:
    if not token.startswith("context."):
        return None
    key = token[len("context."):]
    if not _TEMPLATE_KEY_ALLOWED_RE.match(key):
        return None
    return key


def _derive_required_controls(
    *,
    plan: AgentPlan,
    contains_action_submit: bool,
    contains_pipeline_write: bool,
) -> list[PlanRequiredControl]:
    controls: list[PlanRequiredControl] = []
    if contains_action_submit:
        controls.append(PlanRequiredControl.simulate_first)
        controls.append(PlanRequiredControl.artifact_flow_declared)
    if contains_pipeline_write:
        controls.append(PlanRequiredControl.pipeline_simulate_first)
    if plan.requires_approval or plan.risk_level != AgentPlanRiskLevel.read:
        controls.append(PlanRequiredControl.approval_required)
        controls.append(PlanRequiredControl.idempotency_key_required)
    controls.append(PlanRequiredControl.policy_snapshot_bound)
    return controls


async def validate_agent_plan(
    *,
    plan: AgentPlan,
    tool_registry: AgentToolRegistry,
    allowed_tool_ids: Optional[List[str]] = None,
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

    def _normalize_pipeline_edges(definition: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        edges = definition.get("edges")
        if not isinstance(edges, list):
            return definition, False
        normalized_edges: List[Any] = []
        changed = False
        for edge in edges:
            if not isinstance(edge, dict):
                normalized_edges.append(edge)
                continue
            from_value = edge.get("from")
            to_value = edge.get("to")
            source_value = edge.get("source")
            target_value = edge.get("target")
            if (from_value is None or to_value is None) and (source_value is not None or target_value is not None):
                changed = True
                updated_edge = dict(edge)
                if from_value is None and source_value is not None:
                    updated_edge["from"] = source_value
                if to_value is None and target_value is not None:
                    updated_edge["to"] = target_value
                updated_edge.pop("source", None)
                updated_edge.pop("target", None)
                normalized_edges.append(updated_edge)
                continue
            normalized_edges.append(edge)
        if not changed:
            return definition, False
        updated_definition = dict(definition)
        updated_definition["edges"] = normalized_edges
        return updated_definition, True

    def _normalize_pipeline_nodes(definition: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        nodes = definition.get("nodes")
        if not isinstance(nodes, list):
            return definition, False
        changed = False
        normalized_nodes: List[Any] = []
        for node in nodes:
            if not isinstance(node, dict):
                normalized_nodes.append(node)
                continue
            metadata = node.get("metadata")
            if not isinstance(metadata, dict):
                normalized_nodes.append(node)
                continue
            operation_raw = str(metadata.get("operation") or "").strip()
            operation = operation_raw.lower()
            updated_metadata = metadata
            updated = False
            if not operation:
                if any(
                    key in metadata
                    for key in (
                        "leftKey",
                        "rightKey",
                        "joinKey",
                        "joinType",
                        "allowCrossJoin",
                        "left",
                        "right",
                    )
                ):
                    updated_metadata = dict(updated_metadata)
                    updated_metadata["operation"] = "join"
                    operation = "join"
                    updated = True
                elif any(key in metadata for key in ("aggregates", "aggregations", "groupBy", "groupKeys", "keys")):
                    updated_metadata = dict(updated_metadata)
                    updated_metadata["operation"] = "groupBy"
                    operation = "groupby"
                    updated = True
                elif any(key in metadata for key in ("columns", "fields")):
                    updated_metadata = dict(updated_metadata)
                    updated_metadata["operation"] = "select"
                    operation = "select"
                    updated = True
            if operation in {"groupby", "aggregate"}:
                if not updated_metadata.get("groupBy"):
                    for key in ("groupKeys", "group_by", "keys"):
                        if isinstance(updated_metadata.get(key), list):
                            updated_metadata = dict(updated_metadata)
                            updated_metadata["groupBy"] = updated_metadata.get(key)
                            updated = True
                            break
                source_aggregates = None
                if isinstance(updated_metadata.get("aggregates"), list):
                    source_aggregates = updated_metadata.get("aggregates")
                elif isinstance(updated_metadata.get("aggregations"), list):
                    source_aggregates = updated_metadata.get("aggregations")
                if source_aggregates is not None:
                    normalized_aggregates = []
                    for agg in source_aggregates:
                        if not isinstance(agg, dict):
                            continue
                        column = agg.get("column") or agg.get("field")
                        op = agg.get("op") or agg.get("func")
                        alias = agg.get("alias") or agg.get("as")
                        if not column or not op:
                            continue
                        entry = {"column": column, "op": op}
                        if alias:
                            entry["alias"] = alias
                        normalized_aggregates.append(entry)
                    if normalized_aggregates:
                        updated_metadata = dict(updated_metadata)
                        updated_metadata["aggregates"] = normalized_aggregates
                        updated_metadata.pop("aggregations", None)
                        updated = True
            if operation in {"select", "drop", "sort", "dedupe", "explode"}:
                columns = updated_metadata.get("columns") if isinstance(updated_metadata.get("columns"), list) else []
                if not columns and isinstance(updated_metadata.get("fields"), list):
                    updated_metadata = dict(updated_metadata)
                    updated_metadata["columns"] = updated_metadata.get("fields")
                    updated_metadata.pop("fields", None)
                    updated = True
            if updated:
                updated_node = dict(node)
                updated_node["metadata"] = updated_metadata
                normalized_nodes.append(updated_node)
                changed = True
            else:
                normalized_nodes.append(node)
        if not changed:
            return definition, False
        updated_definition = dict(definition)
        updated_definition["nodes"] = normalized_nodes
        return updated_definition, True

    all_policies = await tool_registry.list_tool_policies(status=None, limit=1000)
    policy_by_tool_id = {p.tool_id: p for p in all_policies if p.tool_id}
    active_policies = [p for p in all_policies if str(p.status or "").strip().upper() == "ACTIVE"]
    allowlist_hash = _tool_policy_hash(active_policies)
    snapshot = _policy_snapshot(tool_allowlist_hash=allowlist_hash)
    allowed_tool_set = (
        {str(tool_id).strip() for tool_id in (allowed_tool_ids or []) if str(tool_id).strip()}
        if allowed_tool_ids is not None
        else None
    )

    normalized_steps: List[AgentPlanStep] = []
    risk_levels: List[AgentPlanRiskLevel] = []

    seen_artifacts: set[str] = set()
    seen_simulate: set[tuple[str, str]] = set()
    simulation_by_key: dict[tuple[str, str], str] = {}
    contains_action_submit = False
    seen_pipeline_simulate_definition = False
    previewed_pipeline_ids: set[str] = set()
    contains_pipeline_write = False
    step_id_to_index = {step.step_id: idx for idx, step in enumerate(plan.steps)}

    for idx, step in enumerate(plan.steps):
        if allowed_tool_set is not None and step.tool_id not in allowed_tool_set:
            msg = f"tool_id={step.tool_id} not enabled for this session"
            _add_error(
                code="tool_not_enabled",
                message=msg,
                step=step,
                fix_hint="Enable the tool_id for this session or re-plan using an enabled tool.",
            )
            risk_levels.append(AgentPlanRiskLevel.admin)
            normalized_steps.append(step.model_copy(update={"requires_approval": True}))
            continue
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

        if str(policy.tool_type or "").strip().lower() == "clarification":
            msg = f"tool_id={step.tool_id} is not allowed in agent plans (clarification tools are server-driven)"
            _add_error(
                code="tool_not_allowed",
                message=msg,
                step=step,
                fix_hint="Remove clarification steps; the server will request clarifications directly.",
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

        if normalized_step.tool_id in {
            _PIPELINE_SIMULATE_DEFINITION_TOOL_ID,
            _PIPELINE_CREATE_TOOL_ID,
            _PIPELINE_UPDATE_TOOL_ID,
        }:
            body_obj = normalized_step.body if isinstance(normalized_step.body, dict) else {}
            definition_json = None
            definition_key = None
            if isinstance(body_obj.get("definition_json"), dict):
                definition_json = body_obj.get("definition_json")
                definition_key = "definition_json"
            elif isinstance(body_obj.get("definitionJson"), dict):
                definition_json = body_obj.get("definitionJson")
                definition_key = "definitionJson"
            graph_obj = body_obj.get("graph") if isinstance(body_obj.get("graph"), dict) else None
            unwrap_body: Optional[Dict[str, Any]] = None
            if isinstance(definition_json, dict) and isinstance(definition_json.get("graph"), dict):
                has_nodes = isinstance(definition_json.get("nodes"), list) or isinstance(definition_json.get("edges"), list)
                if not has_nodes:
                    graph_payload = definition_json.get("graph")
                    candidate_body = dict(body_obj)
                    if not str(candidate_body.get("db_name") or "").strip():
                        db_value = candidate_body.get("dbName") or definition_json.get("db_name") or definition_json.get("dbName")
                        if db_value:
                            candidate_body["db_name"] = db_value
                    if not str(candidate_body.get("branch") or "").strip():
                        branch_value = definition_json.get("branch")
                        if branch_value:
                            candidate_body["branch"] = branch_value
                    if normalized_step.tool_id in {_PIPELINE_CREATE_TOOL_ID, _PIPELINE_UPDATE_TOOL_ID}:
                        if not str(candidate_body.get("name") or "").strip():
                            name_value = definition_json.get("name")
                            if name_value:
                                candidate_body["name"] = name_value
                        if not str(candidate_body.get("description") or "").strip():
                            desc_value = definition_json.get("description")
                            if desc_value:
                                candidate_body["description"] = desc_value
                    candidate_body["definition_json"] = graph_payload
                    unwrap_body = candidate_body
                    definition_json = graph_payload
                    definition_key = "definition_json"
            if unwrap_body is not None:
                patch_id = f"unwrap_definition_graph.{normalized_step.step_id}"
                _add_error(
                    code="pipeline_definition_wrapped",
                    message=f"tool_id={normalized_step.tool_id} definition_json.graph should be body.definition_json",
                    step=normalized_step,
                    field="body.definition_json.graph",
                    fix_hint="Move definition_json.graph into body.definition_json and lift db_name/branch/name.",
                    patch_id=patch_id,
                )
                _maybe_add_patch(
                    patch_id=patch_id,
                    title=f"Unwrap definition_json.graph for step {normalized_step.step_id}",
                    description="Moves definition_json.graph into body.definition_json and lifts common fields to body.",
                    auto_applicable=True,
                    operations=[PlanPatchOp(op="replace", path=f"/steps/{idx}/body", value=unwrap_body)],
                )
            if isinstance(definition_json, dict):
                normalized_definition, edges_changed = _normalize_pipeline_edges(definition_json)
                if edges_changed:
                    field_name = f"body.{definition_key}.edges" if definition_key else "body.definition_json.edges"
                    _add_warning(
                        code="pipeline_edge_fields_normalized",
                        message=f"tool_id={normalized_step.tool_id} pipeline edges normalized to from/to",
                        step=normalized_step,
                        field=field_name,
                        fix_hint="Use edges[].from/to in future plans.",
                    )
                normalized_definition, nodes_changed = _normalize_pipeline_nodes(normalized_definition)
                if nodes_changed:
                    field_name = f"body.{definition_key}" if definition_key else "body.definition_json"
                    _add_warning(
                        code="pipeline_node_metadata_normalized",
                        message=f"tool_id={normalized_step.tool_id} pipeline node metadata normalized",
                        step=normalized_step,
                        field=field_name,
                        fix_hint="Use groupBy/aggregates/columns keys in future plans.",
                    )
                if edges_changed or nodes_changed:
                    updated_body = dict(body_obj)
                    key = definition_key or "definition_json"
                    updated_body[key] = normalized_definition
                    normalized_step = normalized_step.model_copy(update={"body": updated_body})
                    body_obj = updated_body
                definition_json = normalized_definition

            if normalized_step.tool_id == _PIPELINE_SIMULATE_DEFINITION_TOOL_ID:
                updated_body = dict(body_obj)
                if not str(updated_body.get("db_name") or "").strip():
                    db_value = str(updated_body.get("dbName") or getattr(plan.data_scope, "db_name", "") or "").strip()
                    if db_value:
                        updated_body["db_name"] = db_value
                        _add_warning(
                            code="pipeline_db_name_normalized",
                            message=f"tool_id={normalized_step.tool_id} missing db_name; filled from plan data_scope",
                            step=normalized_step,
                            field="body.db_name",
                            fix_hint="Include body.db_name in future plans.",
                        )
                if not str(updated_body.get("branch") or "").strip():
                    branch_value = str(updated_body.get("branch") or getattr(plan.data_scope, "branch", "") or "").strip()
                    if branch_value:
                        updated_body["branch"] = branch_value
                        _add_warning(
                            code="pipeline_branch_normalized",
                            message=f"tool_id={normalized_step.tool_id} missing branch; filled from plan data_scope",
                            step=normalized_step,
                            field="body.branch",
                            fix_hint="Include body.branch in future plans.",
                        )
                if updated_body != body_obj:
                    normalized_step = normalized_step.model_copy(update={"body": updated_body})
                    body_obj = updated_body
            if unwrap_body is None:
                if not str(body_obj.get("db_name") or "").strip():
                    db_value = body_obj.get("dbName")
                    if isinstance(definition_json, dict):
                        db_value = db_value or definition_json.get("db_name") or definition_json.get("dbName")
                    if db_value:
                        patch_id = f"lift_db_name.{normalized_step.step_id}"
                        _add_error(
                            code="pipeline_db_name_missing",
                            message=f"tool_id={normalized_step.tool_id} missing body.db_name (found in definition_json)",
                            step=normalized_step,
                            field="body.db_name",
                            fix_hint="Set body.db_name from definition_json.",
                            patch_id=patch_id,
                        )
                        _maybe_add_patch(
                            patch_id=patch_id,
                            title=f"Lift db_name for step {normalized_step.step_id}",
                            description="Adds body.db_name from definition_json/dbName.",
                            auto_applicable=True,
                            operations=[PlanPatchOp(op="add", path=f"/steps/{idx}/body/db_name", value=db_value)],
                        )
            elif isinstance(graph_obj, dict):
                graph_obj, _ = _normalize_pipeline_edges(graph_obj)
            if definition_json is None and graph_obj is not None:
                patch_id = f"move_graph_to_definition.{normalized_step.step_id}"
                _add_error(
                    code="definition_json_required",
                    message=f"tool_id={normalized_step.tool_id} requires body.definition_json (found body.graph)",
                    step=normalized_step,
                    field="body.definition_json",
                    fix_hint="Move body.graph into body.definition_json.",
                    patch_id=patch_id,
                )
                patched_body = {**body_obj, "definition_json": graph_obj}
                patched_body.pop("graph", None)
                _maybe_add_patch(
                    patch_id=patch_id,
                    title=f"Move graph to definition_json for step {normalized_step.step_id}",
                    description="Rewrites body.graph into body.definition_json for pipeline definition endpoints.",
                    auto_applicable=True,
                    operations=[PlanPatchOp(op="replace", path=f"/steps/{idx}/body", value=patched_body)],
                )

        if normalized_step.tool_id == _PIPELINE_CREATE_TOOL_ID:
            body_obj = normalized_step.body if isinstance(normalized_step.body, dict) else {}
            location_raw = str(body_obj.get("location") or "").strip()
            if not location_raw:
                db_name_for_location = str(body_obj.get("db_name") or getattr(plan.data_scope, "db_name", "") or "").strip()
                if not db_name_for_location:
                    db_name_for_location = "default"
                default_location = f"/projects/{db_name_for_location}/pipelines"
                patch_id = f"add_location.{normalized_step.step_id}"
                _add_error(
                    code="pipeline_location_required",
                    message=f"tool_id={normalized_step.tool_id} missing required field: body.location",
                    step=normalized_step,
                    field="body.location",
                    fix_hint="Provide a pipeline location (e.g. /projects/<db_name>/pipelines).",
                    patch_id=patch_id,
                )
                _maybe_add_patch(
                    patch_id=patch_id,
                    title=f"Add location for step {normalized_step.step_id}",
                    description="Adds a default pipeline location derived from db_name.",
                    auto_applicable=True,
                    operations=[
                        PlanPatchOp(op="add", path=f"/steps/{idx}/body/location", value=default_location),
                    ],
                )

        schema_errors = validate_against_request_schema(
            method=str(method or policy.method or "").strip().upper(),
            path=str(policy.path or "").strip(),
            body=normalized_step.body,
            query=normalized_step.query or {},
        )
        for schema_error in schema_errors[:20]:
            _add_error(
                code="request_schema_mismatch",
                message=f"tool_id={step.tool_id} schema validation failed: {schema_error}",
                step=normalized_step,
                field="body",
                fix_hint="Fix step.body/query to match the tool input schema.",
            )

        for artifact in normalized_step.produces:
            key = str(artifact or "").strip()
            if not _TEMPLATE_KEY_ALLOWED_RE.match(key):
                _add_error(
                    code="invalid_artifact_key",
                    message=f"step_id={normalized_step.step_id} has invalid artifact key in produces: {artifact}",
                    step=normalized_step,
                    field="produces",
                    fix_hint="Artifact keys must match [A-Za-z0-9._-] (1..200 chars).",
                )

        for artifact in normalized_step.consumes:
            key = str(artifact or "").strip()
            if not _TEMPLATE_KEY_ALLOWED_RE.match(key):
                _add_error(
                    code="invalid_artifact_key",
                    message=f"step_id={normalized_step.step_id} has invalid artifact key in consumes: {artifact}",
                    step=normalized_step,
                    field="consumes",
                    fix_hint="Artifact keys must match [A-Za-z0-9._-] (1..200 chars).",
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

        # Runtime template tokens: validate references are well-formed and point to earlier steps/artifacts.
        template_tokens: list[str] = []
        for source in (
            normalized_step.path_params,
            normalized_step.query,
            normalized_step.body,
            normalized_step.data_scope,
        ):
            template_tokens.extend(_iter_template_tokens(source))

        for token in template_tokens:
            parsed_steps = _parse_steps_token(token)
            if parsed_steps:
                ref_step_id, ref_key = parsed_steps
                ref_index = step_id_to_index.get(ref_step_id)
                if ref_index is None:
                    _add_error(
                        code="template_step_unknown",
                        message=f"step_id={normalized_step.step_id} references unknown template step: {ref_step_id}",
                        step=normalized_step,
                        field="template",
                        fix_hint="Reference an earlier step_id using ${steps.<step_id>.<field>}.",
                    )
                elif ref_index >= idx:
                    _add_error(
                        code="template_step_order",
                        message=f"step_id={normalized_step.step_id} references a future step in template: {ref_step_id}",
                        step=normalized_step,
                        field="template",
                        fix_hint="Templates may only reference outputs from earlier steps.",
                    )
                else:
                    if ref_key not in _TEMPLATE_STEP_OUTPUT_KEYS:
                        _add_warning(
                            code="template_step_key_unknown",
                            message=f"step_id={normalized_step.step_id} references non-standard step output key: {ref_key}",
                            step=normalized_step,
                            field="template",
                            fix_hint=f"Prefer one of: {sorted(_TEMPLATE_STEP_OUTPUT_KEYS)}",
                        )
                continue

            artifact_key = _parse_artifact_token(token)
            if artifact_key:
                if artifact_key not in set(normalized_step.consumes):
                    patch_id = f"add_consumes_for_artifact.{normalized_step.step_id}.{artifact_key}"[:100]
                    _add_error(
                        code="template_artifact_missing_consumes",
                        message=f"step_id={normalized_step.step_id} uses artifact template {artifact_key} but does not declare it in consumes",
                        step=normalized_step,
                        field="consumes",
                        fix_hint="Add the referenced artifact to consumes for explicit dataflow.",
                        patch_id=patch_id,
                    )
                    merged = sorted(set(normalized_step.consumes) | {artifact_key})
                    _maybe_add_patch(
                        patch_id=patch_id,
                        title=f"Declare consumes for {artifact_key} on step {normalized_step.step_id}",
                        description="Adds the referenced artifact to consumes so the plan explicitly declares dependencies.",
                        auto_applicable=True,
                        operations=[PlanPatchOp(op="replace", path=f"/steps/{idx}/consumes", value=merged)],
                    )
                continue

            context_key = _parse_context_token(token)
            if context_key:
                _add_warning(
                    code="template_context_reference",
                    message=f"step_id={normalized_step.step_id} references runtime context key: {context_key}",
                    step=normalized_step,
                    field="template",
                    fix_hint="Ensure this context key is provided at execution time.",
                )
                continue

            _add_error(
                code="template_token_unsupported",
                message=f"step_id={normalized_step.step_id} contains unsupported template token: {token}",
                step=normalized_step,
                field="template",
                fix_hint="Supported: ${steps.<step_id>.<field>}, ${artifacts.<artifact_key>}, ${context.<key>}.",
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

        # Pipeline Builder: enforce simulate/preview before write operations.
        if normalized_step.tool_id == _PIPELINE_SIMULATE_DEFINITION_TOOL_ID:
            seen_pipeline_simulate_definition = True

        if normalized_step.tool_id == _PIPELINE_PREVIEW_TOOL_ID:
            pipeline_id_value = str(normalized_step.path_params.get("pipeline_id") or "").strip()
            if pipeline_id_value:
                previewed_pipeline_ids.add(pipeline_id_value)

        if normalized_step.tool_id == _PIPELINE_CREATE_TOOL_ID:
            contains_pipeline_write = True
            if not seen_pipeline_simulate_definition:
                patch_id = f"insert_pipeline_simulate_before.{normalized_step.step_id}"
                _add_error(
                    code="pipeline_simulate_first_required",
                    message=f"tool_id={normalized_step.tool_id} requires prior pipelines.simulate_definition",
                    step=normalized_step,
                    fix_hint="Add a pipelines.simulate_definition step before creating the pipeline.",
                    patch_id=patch_id,
                )
                simulate_policy = policy_by_tool_id.get(_PIPELINE_SIMULATE_DEFINITION_TOOL_ID)
                if simulate_policy and str(simulate_policy.status or "").strip().upper() == "ACTIVE":
                    body = normalized_step.body if isinstance(normalized_step.body, dict) else {}
                    definition_json = body.get("definition_json") if isinstance(body.get("definition_json"), dict) else None
                    if definition_json is None and isinstance(body.get("definitionJson"), dict):
                        definition_json = body.get("definitionJson")
                    db_name = str(body.get("db_name") or body.get("dbName") or plan.data_scope.db_name or "").strip()
                    branch = str(body.get("branch") or plan.data_scope.branch or "main").strip() or "main"
                    if db_name and isinstance(definition_json, dict) and definition_json:
                        simulate_step_id = f"simulate_definition_before_{normalized_step.step_id}"[:200]
                        simulate_step = {
                            "step_id": simulate_step_id,
                            "tool_id": simulate_policy.tool_id,
                            "method": "POST",
                            "path_params": {},
                            "body": {
                                "db_name": db_name,
                                "branch": branch,
                                "definition_json": definition_json,
                                "limit": 200,
                            },
                            "produces": [],
                            "consumes": [],
                            "requires_approval": False,
                            "idempotency_key": str(uuid4()),
                            "data_scope": dict(normalized_step.data_scope or {}),
                            "description": "server-suggested: simulate definition before create",
                        }
                        _maybe_add_patch(
                            patch_id=patch_id,
                            title=f"Insert simulate_definition before {normalized_step.step_id}",
                            description="Adds a pipelines.simulate_definition step to validate the definition before creating the pipeline.",
                            auto_applicable=True,
                            operations=[PlanPatchOp(op="add", path=f"/steps/{idx}", value=simulate_step)],
                        )

        if normalized_step.tool_id == _PIPELINE_UPDATE_TOOL_ID:
            body = normalized_step.body if isinstance(normalized_step.body, dict) else {}
            has_definition_update = any(
                isinstance(body.get(key), dict) and body.get(key)
                for key in ("definition_json", "definitionJson")
            )
            if has_definition_update:
                contains_pipeline_write = True
                if not seen_pipeline_simulate_definition:
                    patch_id = f"insert_pipeline_simulate_before.{normalized_step.step_id}"
                    _add_error(
                        code="pipeline_simulate_first_required",
                        message=f"tool_id={normalized_step.tool_id} definition update requires prior pipelines.simulate_definition",
                        step=normalized_step,
                        fix_hint="Add a pipelines.simulate_definition step before updating the pipeline definition.",
                        patch_id=patch_id,
                    )
                    simulate_policy = policy_by_tool_id.get(_PIPELINE_SIMULATE_DEFINITION_TOOL_ID)
                    if simulate_policy and str(simulate_policy.status or "").strip().upper() == "ACTIVE":
                        definition_json = body.get("definition_json") if isinstance(body.get("definition_json"), dict) else None
                        if definition_json is None and isinstance(body.get("definitionJson"), dict):
                            definition_json = body.get("definitionJson")
                        db_name = str(body.get("db_name") or body.get("dbName") or plan.data_scope.db_name or "").strip()
                        branch = str(body.get("branch") or plan.data_scope.branch or "main").strip() or "main"
                        if db_name and isinstance(definition_json, dict) and definition_json:
                            simulate_step_id = f"simulate_definition_before_{normalized_step.step_id}"[:200]
                            simulate_step = {
                                "step_id": simulate_step_id,
                                "tool_id": simulate_policy.tool_id,
                                "method": "POST",
                                "path_params": {},
                                "body": {
                                    "db_name": db_name,
                                    "branch": branch,
                                    "definition_json": definition_json,
                                    "limit": 200,
                                },
                                "produces": [],
                                "consumes": [],
                                "requires_approval": False,
                                "idempotency_key": str(uuid4()),
                                "data_scope": dict(normalized_step.data_scope or {}),
                                "description": "server-suggested: simulate definition before update",
                            }
                            _maybe_add_patch(
                                patch_id=patch_id,
                                title=f"Insert simulate_definition before {normalized_step.step_id}",
                                description="Adds a pipelines.simulate_definition step to validate the updated definition before applying it.",
                                auto_applicable=True,
                                operations=[PlanPatchOp(op="add", path=f"/steps/{idx}", value=simulate_step)],
                            )

        if normalized_step.tool_id in {_PIPELINE_BUILD_TOOL_ID, _PIPELINE_DEPLOY_TOOL_ID}:
            contains_pipeline_write = True
            pipeline_id_value = str(normalized_step.path_params.get("pipeline_id") or "").strip()
            if pipeline_id_value and pipeline_id_value not in previewed_pipeline_ids and not seen_pipeline_simulate_definition:
                patch_id = f"insert_pipeline_preview_before.{normalized_step.step_id}"
                _add_error(
                    code="pipeline_preview_first_required",
                    message=f"tool_id={normalized_step.tool_id} requires prior pipelines.preview for pipeline_id={pipeline_id_value}",
                    step=normalized_step,
                    fix_hint="Add a pipelines.preview step before build/deploy.",
                    patch_id=patch_id,
                )
                preview_policy = policy_by_tool_id.get(_PIPELINE_PREVIEW_TOOL_ID)
                if preview_policy and str(preview_policy.status or "").strip().upper() == "ACTIVE":
                    preview_step_id = f"preview_before_{normalized_step.step_id}"[:200]
                    preview_step = {
                        "step_id": preview_step_id,
                        "tool_id": preview_policy.tool_id,
                        "method": "POST",
                        "path_params": {"pipeline_id": pipeline_id_value},
                        "body": {"limit": 200},
                        "produces": [],
                        "consumes": [],
                        "requires_approval": False,
                        "idempotency_key": str(uuid4()),
                        "data_scope": dict(normalized_step.data_scope or {}),
                        "description": "server-suggested: preview before build/deploy",
                    }
                    _maybe_add_patch(
                        patch_id=patch_id,
                        title=f"Insert preview before {normalized_step.step_id}",
                        description="Adds a pipelines.preview step to validate pipeline inputs before build/deploy.",
                        auto_applicable=True,
                        operations=[PlanPatchOp(op="add", path=f"/steps/{idx}", value=preview_step)],
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

    required_controls = _derive_required_controls(
        plan=normalized_plan,
        contains_action_submit=contains_action_submit,
        contains_pipeline_write=contains_pipeline_write,
    )
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
