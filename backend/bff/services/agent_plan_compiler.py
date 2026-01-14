"""
LLM-native agent plan compiler (write-capable planner, enterprise-safe).

This module implements the "Planner + Clarifier loop" described in:
- docs/LLM_NATIVE_CONTROL_PLANE.md

Notes:
- The LLM never executes tools; it only proposes a typed plan object.
- The server validates/normalizes the plan using the allowlist tool registry.
- If validation fails, we ask clarifying questions (or return a safe fallback).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from bff.services.agent_plan_validation import validate_agent_plan
from shared.models.agent_plan import AgentPlan, AgentPlanDataScope
from shared.models.agent_plan_report import PlanCompilationReport
from shared.services.agent_tool_registry import AgentToolPolicyRecord, AgentToolRegistry
from shared.services.audit_log_store import AuditLogStore
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.redis_service import RedisService
from shared.utils.json_patch import JsonPatchError, apply_json_patch


class AgentClarificationQuestion(BaseModel):
    id: str = Field(..., min_length=1, max_length=100)
    question: str = Field(..., min_length=1, max_length=2000)
    required: bool = Field(default=True)
    type: str = Field(default="string", description="string|enum|boolean|number|object")
    options: Optional[List[str]] = None
    default: Optional[Any] = None


class AgentClarificationPayload(BaseModel):
    questions: List[AgentClarificationQuestion] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class AgentPlanDraftEnvelope(BaseModel):
    plan: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class AgentPlanCompileResult:
    status: str  # success|clarification_required|error
    plan_id: str
    plan: Optional[AgentPlan]
    validation_errors: List[str]
    validation_warnings: List[str]
    questions: List[AgentClarificationQuestion]
    compilation_report: Optional[PlanCompilationReport] = None
    llm_meta: Optional[LLMCallMeta] = None
    planner_confidence: Optional[float] = None
    planner_notes: Optional[List[str]] = None


def _policy_summary(policy: AgentToolPolicyRecord) -> Dict[str, Any]:
    return {
        "tool_id": policy.tool_id,
        "method": policy.method,
        "path": policy.path,
        "risk_level": policy.risk_level,
        "requires_approval": bool(policy.requires_approval),
        "requires_idempotency_key": bool(policy.requires_idempotency_key),
        "roles": policy.roles,
        "max_payload_bytes": policy.max_payload_bytes,
        "status": policy.status,
    }


def _build_tool_catalog_prompt(policies: List[AgentToolPolicyRecord], *, max_tools: int = 120) -> str:
    trimmed = policies[: max(0, int(max_tools))]
    catalog = [_policy_summary(p) for p in trimmed]
    return json.dumps(catalog, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _build_plan_system_prompt() -> str:
    return (
        "You are a STRICT enterprise automation planner for SPICE-Harvester.\n"
        "You MUST output a single JSON object only (no markdown, no commentary).\n"
        "You MUST plan using ONLY tool_id values from the provided Tool Catalog.\n"
        "Do NOT invent tool_ids. Do NOT call any admin/debug endpoints.\n"
        "\n"
        "You are NOT executing anything. You are producing a plan draft that the server will validate.\n"
        "\n"
        "Hard safety rules:\n"
        "- Any write method (POST/PUT/PATCH/DELETE) MUST include idempotency_key.\n"
        "- Any write-capable plan MUST set requires_approval=true.\n"
        "- For action writeback submit, ALWAYS include a simulate step before submit for the same db_name + action_type_id.\n"
        "- For Pipeline Builder definition changes, include pipelines.simulate_definition before pipelines.create or pipelines.update (when sending definition_json).\n"
        "- For Pipeline Builder execution, include pipelines.preview before pipelines.build or pipelines.deploy (or use pipelines.simulate_definition if you are supplying definition_json).\n"
        "- For multi-step plans, use stable step_id values matching [A-Za-z0-9._-]+.\n"
        "- You MAY reference prior step outputs using templates like ${steps.<step_id>.pipeline_id}.\n"
        "  Supported keys: pipeline_id, dataset_id, dataset_version_id, job_id, artifact_id, ingest_request_id, mapping_spec_id, action_log_id, simulation_id, command_id, deployed_commit_id, pipeline_run_status.\n"
        "- For complex JSON handoff between steps, declare produces/consumes artifacts and reference them with ${artifacts.<artifact_key>}.\n"
        "- Prefer server-generated IDs; do NOT invent UUIDs unless needed for idempotency_key or simulation_id.\n"
        "- Keep steps minimal; ask clarifying questions via notes/warnings only if needed.\n"
        "- When context includes dataset samples/schema, add integration/cleansing recommendations in notes (join keys, casts, dedupe strategy).\n"
        "\n"
        "Output schema:\n"
        "{\n"
        '  \"plan\": {AgentPlan JSON},\n'
        '  \"confidence\": number (0..1),\n'
        '  \"notes\": string[],\n'
        '  \"warnings\": string[]\n'
        "}\n"
    )


def _build_plan_user_prompt(
    *,
    goal: str,
    data_scope: Optional[AgentPlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    tool_catalog_json: str,
) -> str:
    scope_payload = data_scope.model_dump(mode="json") if data_scope is not None else {}
    answers_payload = answers or {}
    context_payload = context_pack or {}
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope hints (optional):\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Clarification answers (if any):\n{json.dumps(answers_payload, ensure_ascii=False)}\n\n"
        f"Operational Memory context pack (safe summary):\n{json.dumps(context_payload, ensure_ascii=False)}\n\n"
        "Tool Catalog (use tool_id ONLY from this list):\n"
        f"{tool_catalog_json}\n"
    )


def _build_clarifier_system_prompt() -> str:
    return (
        "You are a STRICT clarification generator for SPICE-Harvester.\n"
        "You MUST output a single JSON object only (no markdown, no commentary).\n"
        "Your job is to ask the MINIMUM set of questions needed to fix server validation errors.\n"
        "Do NOT propose a plan. Do NOT include any tool calls.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        '  \"questions\": [{\"id\": string, \"question\": string, \"required\": boolean, \"type\": string, \"options\": string[]|null, \"default\": any|null}],\n'
        '  \"assumptions\": string[],\n'
        '  \"warnings\": string[]\n'
        "}\n"
    )


def _build_clarifier_user_prompt(
    *,
    goal: str,
    draft_plan: Dict[str, Any],
    validation_errors: List[str],
    validation_warnings: List[str],
    data_scope: Optional[AgentPlanDataScope],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    scope_payload = data_scope.model_dump(mode="json") if data_scope is not None else {}
    context_payload = context_pack or {}
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope hints:\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Operational Memory context pack (safe summary):\n{json.dumps(context_payload, ensure_ascii=False)}\n\n"
        "Draft plan (may be invalid):\n"
        f"{json.dumps(draft_plan or {}, ensure_ascii=False)}\n\n"
        f"Server validation errors:\n{json.dumps(validation_errors or [], ensure_ascii=False)}\n\n"
        f"Server validation warnings:\n{json.dumps(validation_warnings or [], ensure_ascii=False)}\n"
    )


def _fallback_questions_from_errors(errors: List[str]) -> List[AgentClarificationQuestion]:
    questions: List[AgentClarificationQuestion] = []
    missing_params = [e for e in errors if "missing path_params" in e]
    if missing_params:
        questions.append(
            AgentClarificationQuestion(
                id="missing_path_params",
                question="실행에 필요한 path_params(예: db_name, action_type_id 등)가 누락되었습니다. 어떤 값으로 채울까요?",
                required=True,
                type="object",
            )
        )
    simulate_required = [e for e in errors if "requires prior simulate" in e.lower()]
    if simulate_required:
        questions.append(
            AgentClarificationQuestion(
                id="simulate_first",
                question="writeback submit 전에 simulate(미리보기)를 먼저 실행해야 합니다. 시뮬레이션을 먼저 수행할까요?",
                required=True,
                type="boolean",
                default=True,
            )
        )
    if not questions:
        questions.append(
            AgentClarificationQuestion(
                id="clarify",
                question="요청이 모호합니다. 대상 범위(db/branch/대상 객체)와 원하는 결과를 더 구체적으로 알려주세요.",
                required=True,
                type="string",
            )
        )
    return questions


async def compile_agent_plan(
    *,
    goal: str,
    data_scope: Optional[AgentPlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]] = None,
    actor: str,
    allowed_tool_ids: Optional[List[str]] = None,
    selected_model: Optional[str] = None,
    tool_registry: AgentToolRegistry,
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
) -> AgentPlanCompileResult:
    goal = str(goal or "").strip()
    if not goal:
        return AgentPlanCompileResult(
            status="error",
            plan_id=str(uuid4()),
            plan=None,
            validation_errors=["goal is required"],
            validation_warnings=[],
            questions=[],
        )

    policies = await tool_registry.list_tool_policies(status="ACTIVE", limit=200)
    allowed_set = (
        {str(tool_id).strip() for tool_id in (allowed_tool_ids or []) if str(tool_id).strip()}
        if allowed_tool_ids is not None
        else None
    )
    if allowed_set is not None:
        policies = [p for p in policies if p.tool_id in allowed_set]
    if not policies:
        return AgentPlanCompileResult(
            status="error",
            plan_id=str(uuid4()),
            plan=None,
            validation_errors=[
                "No enabled agent tool policies found for this session (configure allowlist or enable tools)."
            ],
            validation_warnings=[],
            questions=[],
        )

    plan_id = str(uuid4())
    tool_catalog_json = _build_tool_catalog_prompt(policies)

    llm_meta: Optional[LLMCallMeta] = None
    draft: Optional[AgentPlanDraftEnvelope] = None
    draft_plan_obj: Dict[str, Any] = {}

    try:
        draft, llm_meta = await llm_gateway.complete_json(
            task="AGENT_PLAN_COMPILE_V1",
            system_prompt=_build_plan_system_prompt(),
            user_prompt=_build_plan_user_prompt(
                goal=goal,
                data_scope=data_scope,
                answers=answers,
                context_pack=context_pack,
                tool_catalog_json=tool_catalog_json,
            ),
            response_model=AgentPlanDraftEnvelope,
            model=selected_model,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"agent_plan:{plan_id}",
            audit_actor=actor,
            audit_resource_id=plan_id,
            audit_metadata={"kind": "agent_plan_compile", "schema": "AgentPlan"},
        )
        draft_plan_obj = dict(draft.plan or {})
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        questions = _fallback_questions_from_errors([str(exc)])
        return AgentPlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[str(exc)],
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
        )

    try:
        plan = AgentPlan.model_validate(draft_plan_obj)
    except ValidationError as exc:
        errors = [str(e.get("msg") or e) for e in exc.errors()]
        questions = _fallback_questions_from_errors(errors)
        return AgentPlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=errors,
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
        )

    plan = plan.model_copy(
        update={
            "plan_id": plan.plan_id or plan_id,
            "created_by": plan.created_by or actor,
        }
    )

    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    auto_applied_patches: list[str] = []

    if validation.errors:
        candidate_plan = validation.plan
        for _ in range(5):
            report = validation.compilation_report
            auto_patches = [
                patch
                for patch in (report.patches or [])
                if patch.auto_applicable and patch.operations and patch.patch_id not in auto_applied_patches
            ]
            if not auto_patches:
                break
            patch = auto_patches[0]
            ops = [op.model_dump(mode="json", by_alias=True) for op in (patch.operations or [])]
            try:
                patched_obj = apply_json_patch(candidate_plan.model_dump(mode="json"), ops)
                candidate_plan = AgentPlan.model_validate(patched_obj)
            except (JsonPatchError, ValidationError):
                break
            auto_applied_patches.append(str(patch.patch_id))
            validation = await validate_agent_plan(
                plan=candidate_plan,
                tool_registry=tool_registry,
                allowed_tool_ids=allowed_tool_ids,
            )
            if not validation.errors:
                break

    if validation.errors:
        questions: List[AgentClarificationQuestion] = []
        clarification_meta: Optional[LLMCallMeta] = None
        try:
            clarification, clarification_meta = await llm_gateway.complete_json(
                task="AGENT_PLAN_CLARIFY_V1",
                system_prompt=_build_clarifier_system_prompt(),
                user_prompt=_build_clarifier_user_prompt(
                    goal=goal,
                    draft_plan=validation.plan.model_dump(mode="json"),
                    validation_errors=validation.errors,
                    validation_warnings=validation.warnings,
                    data_scope=data_scope,
                    context_pack=context_pack,
                ),
                response_model=AgentClarificationPayload,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"agent_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={"kind": "agent_plan_clarify"},
            )
            questions = list(clarification.questions or [])
            llm_meta = clarification_meta or llm_meta
        except Exception:
            questions = _fallback_questions_from_errors(validation.errors)

        return AgentPlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=validation.plan,
            validation_errors=validation.errors,
            validation_warnings=(
                list(validation.warnings or [])
                + ([f"server_auto_applied_patches: {auto_applied_patches}"] if auto_applied_patches else [])
            ),
            questions=questions,
            compilation_report=validation.compilation_report,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
        )

    return AgentPlanCompileResult(
        status="success",
        plan_id=plan_id,
        plan=validation.plan,
        validation_errors=[],
        validation_warnings=(
            list(validation.warnings or [])
            + ([f"server_auto_applied_patches: {auto_applied_patches}"] if auto_applied_patches else [])
        ),
        questions=[],
        compilation_report=validation.compilation_report,
        llm_meta=llm_meta,
        planner_confidence=float(draft.confidence) if draft is not None else None,
        planner_notes=draft.notes if draft is not None else None,
    )
