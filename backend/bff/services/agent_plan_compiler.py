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
from bff.services.agent_tool_schemas import (
    get_query_params,
    get_request_body_required,
    get_request_body_schema,
    get_response_schema,
    schema_hint,
)
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
from shared.services.llm_quota import LLMQuotaExceededError, enforce_llm_quota
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
    body_schema = policy.input_schema or get_request_body_schema(method=policy.method, path=policy.path)
    response_schema = policy.output_schema or get_response_schema(method=policy.method, path=policy.path)
    query_params = get_query_params(method=policy.method, path=policy.path)
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
        "schema": {
            "request_body_required": bool(get_request_body_required(method=policy.method, path=policy.path)) or bool(policy.input_schema),
            "request_body": schema_hint(body_schema),
            "query_params": [
                {
                    "name": p.get("name"),
                    "required": bool(p.get("required") or False),
                    "schema": schema_hint(p.get("schema")),
                }
                for p in query_params
            ],
            "response_body": schema_hint(response_schema),
        },
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
        "Never use clarification.* tools; clarifications are handled by the server.\n"
        "\n"
        "You are NOT executing anything. You are producing a plan draft that the server will validate.\n"
        "\n"
        "Hard safety rules:\n"
        "- Any write method (POST/PUT/PATCH/DELETE) MUST include idempotency_key.\n"
        "- idempotency_key MUST be a fresh UUID (uuid4) per step; never reuse across steps or plans.\n"
        "- Any write-capable plan MUST set requires_approval=true.\n"
        "- The plan MUST include at least one step.\n"
        "- For action writeback submit, ALWAYS include a simulate step before submit for the same db_name + action_type_id.\n"
        "- For Pipeline Builder definition changes, include pipelines.simulate_definition before pipelines.create or pipelines.update (when sending definition_json).\n"
        "- For Pipeline Builder execution, include pipelines.preview before pipelines.build or pipelines.deploy (or use pipelines.simulate_definition if you are supplying definition_json).\n"
        "- If a pipeline definition_json is required, include a VALID graph (nodes/edges) that passes server validation: at least one output node, transform nodes with required operation metadata, and joins with leftKey/rightKey (or joinKey).\n"
        "- Keep pipeline definition_json MINIMAL: nodes should only include id, type, metadata; edges only from/to.\n"
        "- Do NOT include UI/layout fields (position, x/y), labels, names, descriptions, or extra properties unless required for validation.\n"
        "- Use short, stable node ids (e.g., in_orders, join_orders_items, out_canonical).\n"
        "- For pipeline endpoints, the request body field is definition_json (NOT graph).\n"
        "- For pipelines.simulate_definition, body MUST include db_name, branch, and definition_json.\n"
        "- If data_scope.pipeline_id is provided, prefer pipelines.update with that pipeline_id (avoid pipelines.create).\n"
        "- Pipeline edges MUST use from/to keys (NOT source/target).\n"
        "- Use input nodes for datasets when dataset ids are provided; include metadata.datasetId and metadata.datasetName.\n"
        "- For each input dataset, insert a cast transform node immediately after the input.\n"
        "  Use casts for ALL columns from the provided schema (including xsd:string) to normalize trim/empty-to-null.\n"
        "- If output dataset name starts with 'canonical_', include primaryKeys and any foreignKeys in the output metadata.\n"
        "  foreignKeys format: [{columns:[\"col\"], reference:{datasetName|datasetId:\"...\", columns:[\"col\"], branch:\"main\"}, allow_nulls:true}].\n"
        "- Keep graphs concise but executable; prefer a linear join chain over a sprawling DAG.\n"
        "- Pipeline transform metadata schema (use exact keys):\n"
        "  - join: {operation:\"join\", leftKey:\"col\", rightKey:\"col\", joinType:\"inner|left|right|full|cross\", allowCrossJoin:false}\n"
        "  - groupBy: {operation:\"groupBy\", groupBy:[\"col\"], aggregates:[{\"column\":\"col\",\"op\":\"sum|count|avg|min|max\",\"alias\":\"name\"}]}\n"
        "  - select/drop/sort/dedupe/explode: {operation:\"select|drop|sort|dedupe|explode\", columns:[\"col\"]}\n"
        "  - rename: {operation:\"rename\", rename:{\"old\":\"new\"}}\n"
        "  - cast: {operation:\"cast\", casts:[{\"column\":\"col\",\"type\":\"xsd:string|xsd:integer|xsd:decimal|xsd:date|xsd:dateTime|xsd:boolean\"}]}\n"
        "- For multi-step plans, use stable step_id values (prefer snake_case; avoid dots to keep ${steps.<step_id>.*} references unambiguous).\n"
        "- You MAY reference prior step outputs using templates like ${steps.<step_id>.pipeline_id}.\n"
        "  Supported keys: pipeline_id, dataset_id, dataset_version_id, job_id, artifact_id, ingest_request_id, mapping_spec_id, action_log_id, simulation_id, command_id, deployed_commit_id, pipeline_run_status.\n"
        "- For complex JSON handoff between steps, declare produces/consumes artifacts and reference them with ${artifacts.<artifact_key>}.\n"
        "- Prefer server-generated IDs; do NOT invent UUIDs unless needed for idempotency_key or simulation_id.\n"
        "- Keep steps minimal; ask clarifying questions via notes/warnings only if needed.\n"
        "- When context includes dataset samples/schema, add integration/cleansing recommendations in notes (join keys, casts, dedupe strategy).\n"
        "- Use ONLY step fields: step_id, tool_id, method, path_params, query, body, produces, consumes, requires_approval, idempotency_key.\n"
        "- Do NOT use args/params/inputs fields.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        '  \"plan\": {AgentPlan JSON},\n'
        '  \"confidence\": number (0..1),\n'
        '  \"notes\": string[],\n'
        '  \"warnings\": string[]\n'
        "}\n"
        "\n"
        "AgentPlan JSON (minimum fields):\n"
        "{\n"
        '  \"goal\": string,\n'
        '  \"requires_approval\": boolean,\n'
        '  \"risk_level\": \"read|write|admin|destructive\",\n'
        '  \"data_scope\": {\"db_name\": string, \"branch\": string},\n'
        '  \"steps\": [\n'
        "    {\n"
        '      \"step_id\": string,\n'
        '      \"tool_id\": string,\n'
        '      \"method\": \"GET|POST|PUT|PATCH|DELETE\",\n'
        '      \"path_params\": {},\n'
        '      \"query\": {},\n'
        '      \"body\": {},\n'
        '      \"produces\": [],\n'
        '      \"consumes\": [],\n'
        '      \"requires_approval\": boolean,\n'
        '      \"idempotency_key\": string\n'
        "    }\n"
        "  ],\n"
        '  \"policy_notes\": [],\n'
        '  \"warnings\": []\n'
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
        "Tool Catalog (use tool_id ONLY from this list):\n"
        f"{tool_catalog_json}\n\n"
        f"Data scope hints (optional):\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Clarification answers (if any):\n{json.dumps(answers_payload, ensure_ascii=False)}\n\n"
        f"Operational Memory context pack (safe summary):\n{json.dumps(context_payload, ensure_ascii=False)}\n"
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
    tenant_id: Optional[str] = None,
    user_id: Optional[str] = None,
    data_policies: Optional[Dict[str, Any]] = None,
    allowed_tool_ids: Optional[List[str]] = None,
    selected_model: Optional[str] = None,
    allowed_models: Optional[List[str]] = None,
    use_native_tool_calling: bool = False,
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
    policies = [p for p in policies if str(p.tool_type or "").strip().lower() != "clarification"]
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

    system_prompt = _build_plan_system_prompt()
    user_prompt = _build_plan_user_prompt(
        goal=goal,
        data_scope=data_scope,
        answers=answers,
        context_pack=context_pack,
        tool_catalog_json=tool_catalog_json,
    )
    if data_policies and redis_service:
        model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
        if model_for_quota:
            await enforce_llm_quota(
                redis_service=redis_service,
                tenant_id=tenant_id,
                user_id=user_id,
                model_id=model_for_quota,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                data_policies=data_policies,
            )

    try:
        draft, llm_meta = await llm_gateway.complete_json(
            task="AGENT_PLAN_COMPILE_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=AgentPlanDraftEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            use_native_tool_calling=bool(use_native_tool_calling),
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"agent_plan:{plan_id}",
            audit_actor=actor,
            audit_resource_id=plan_id,
            audit_metadata={"kind": "agent_plan_compile", "schema": "AgentPlan", "tenant_id": tenant_id, "user_id": user_id},
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
        # Apply server-side "mechanical" patches (idempotency keys, default locations, etc.)
        #
        # Important: patch the original (still Pydantic-valid) plan shape, not the normalized
        # validation.plan, because validation.plan may contain write-method hints that trigger
        # Pydantic idempotency validators before all patches are applied.
        candidate_plan_obj = plan.model_dump(mode="json")
        for _ in range(25):
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
                candidate_plan_obj = apply_json_patch(candidate_plan_obj, ops)
                candidate_plan = AgentPlan.model_validate(candidate_plan_obj)
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
            clarifier_system_prompt = _build_clarifier_system_prompt()
            clarifier_user_prompt = _build_clarifier_user_prompt(
                goal=goal,
                draft_plan=validation.plan.model_dump(mode="json"),
                validation_errors=validation.errors,
                validation_warnings=validation.warnings,
                data_scope=data_scope,
                context_pack=context_pack,
            )
            if data_policies and redis_service:
                model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
                if model_for_quota:
                    await enforce_llm_quota(
                        redis_service=redis_service,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        model_id=model_for_quota,
                        system_prompt=clarifier_system_prompt,
                        user_prompt=clarifier_user_prompt,
                        data_policies=data_policies,
                    )
            clarification, clarification_meta = await llm_gateway.complete_json(
                task="AGENT_PLAN_CLARIFY_V1",
                system_prompt=clarifier_system_prompt,
                user_prompt=clarifier_user_prompt,
                response_model=AgentClarificationPayload,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"agent_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={"kind": "agent_plan_clarify", "tenant_id": tenant_id, "user_id": user_id},
            )
            questions = list(clarification.questions or [])
            llm_meta = clarification_meta or llm_meta
        except LLMQuotaExceededError:
            raise
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
