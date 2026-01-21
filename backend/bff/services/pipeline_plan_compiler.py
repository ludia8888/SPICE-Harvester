"""
Pipeline plan compiler (LLM-native pipeline definition planner).

Generates typed PipelinePlan artifacts and validates them before preview.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError, model_validator

from bff.services.pipeline_plan_validation import validate_pipeline_plan
from shared.models.agent_plan_report import PlanCompilationReport
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanDataScope
from shared.services.audit_log_store import AuditLogStore
from shared.services.dataset_registry import DatasetRegistry
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.llm_quota import LLMQuotaExceededError, enforce_llm_quota
from shared.services.redis_service import RedisService


class PipelineClarificationQuestion(BaseModel):
    id: str = Field(..., min_length=1, max_length=100)
    question: str = Field(..., min_length=1, max_length=2000)
    required: bool = Field(default=True)
    type: str = Field(default="string", description="string|enum|boolean|number|object")
    options: Optional[List[str]] = None
    default: Optional[Any] = None


class PipelineClarificationPayload(BaseModel):
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class PipelinePlanDraftEnvelope(BaseModel):
    plan: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_envelope(cls, data: Any):  # noqa: ANN001
        if not isinstance(data, dict):
            return data
        if "plan" in data:
            return data
        if not any(key in data for key in ("goal", "definition_json", "outputs", "data_scope")):
            return data
        normalized = dict(data)
        plan_payload = {
            key: normalized[key]
            for key in list(normalized.keys())
            if key not in ("confidence", "notes", "warnings", "questions")
        }
        for key in plan_payload.keys():
            normalized.pop(key, None)
        normalized["plan"] = plan_payload
        return normalized


@dataclass(frozen=True)
class PipelinePlanCompileResult:
    status: str  # success|clarification_required|error
    plan_id: str
    plan: Optional[PipelinePlan]
    validation_errors: List[str]
    validation_warnings: List[str]
    questions: List[PipelineClarificationQuestion]
    compilation_report: Optional[PlanCompilationReport] = None
    llm_meta: Optional[LLMCallMeta] = None
    planner_confidence: Optional[float] = None
    planner_notes: Optional[List[str]] = None
    preflight: Optional[Dict[str, Any]] = None


def _build_plan_system_prompt() -> str:
    return (
        "You are a STRICT enterprise pipeline planner for SPICE-Harvester.\n"
        "You MUST output a single JSON object only (no markdown, no commentary).\n"
        "You are NOT executing anything; you are producing a typed pipeline plan.\n"
        "\n"
        "Hard rules:\n"
        "- definition_json must include nodes[] and edges[].\n"
        "- nodes MUST only include {id,type,metadata}; edges MUST only include {from,to}.\n"
        "- Do NOT include UI/layout fields (position, x/y), labels, or extra properties.\n"
        "- Use supported operations only; no UDF.\n"
        "- join requires leftKey/rightKey or leftKeys/rightKeys (or joinKey) and allowCrossJoin must be false.\n"
        "- For composite joins, ALWAYS use leftKeys/rightKeys arrays.\n"
        "- Edge order matters for join: the first incoming edge is the LEFT input, the second is RIGHT.\n"
        "- Ensure join keys align to the left/right input order; do not swap keys without swapping edges.\n"
        "- If you're unsure whether multi-stage transforms are needed (aggregate/window/groupBy), "
        "ask a clarification question in questions and keep the plan minimal.\n"
        "- definition_json MUST include at least one output node with type \"output\".\n"
        "- output nodes MUST use type \"output\" (not write_output) and include metadata.outputName or metadata.datasetName.\n"
        "- output node metadata.outputName MUST match outputs[].output_name for the same dataset.\n"
        "- For canonical outputs, output_name MUST use canonical_obj_<entity> or canonical_lnk_<left>__<right>.\n"
        "- If you can infer primary keys, include output node metadata.pkColumns and metadata.pkSemantics.\n"
        "- If you can infer data quality rules, include definition_json.expectations (not_null/unique).\n"
        "- Keep the graph minimal; prefer a linear join chain.\n"
        "- output_kind must be one of: object, link, unknown.\n"
        "- object outputs MUST include target_class_id.\n"
        "- link outputs MUST include link_type_id, source_class_id, target_class_id, predicate, cardinality,\n"
        "  source_key_column, target_key_column, and relationship_spec_type (join_table or foreign_key).\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"plan\": {PipelinePlan JSON},\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[],\n"
        "  \"questions\": [PipelineClarificationQuestion] (optional)\n"
        "}\n"
        "\n"
        "PipelinePlan JSON:\n"
        "{\n"
        "  \"goal\": string,\n"
        "  \"data_scope\": {\"db_name\": string, \"branch\": string, \"dataset_ids\": []},\n"
        "  \"definition_json\": {\"nodes\": [], \"edges\": []},\n"
        "  \"outputs\": [\n"
        "    {\n"
        "      \"output_name\": string,\n"
        "      \"output_kind\": \"object|link|unknown\",\n"
        "      \"target_class_id\": string?,\n"
        "      \"source_class_id\": string?,\n"
        "      \"link_type_id\": string?,\n"
        "      \"predicate\": string?,\n"
        "      \"cardinality\": string?,\n"
        "      \"source_key_column\": string?,\n"
        "      \"target_key_column\": string?,\n"
        "      \"relationship_spec_type\": \"join_table|foreign_key\"?\n"
        "    }\n"
        "  ],\n"
        "  \"warnings\": []\n"
        "}\n"
        "\n"
        "PipelineClarificationQuestion JSON:\n"
        "{\n"
        "  \"id\": string,\n"
        "  \"question\": string,\n"
        "  \"required\": boolean,\n"
        "  \"type\": \"string|enum|boolean|number|object\",\n"
        "  \"options\": [string]?,\n"
        "  \"default\": any?\n"
        "}\n"
    )


def _build_plan_user_prompt(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
) -> str:
    scope_payload = data_scope.model_dump(mode="json") if data_scope else {}
    answers_payload = answers or {}
    context_payload = context_pack or {}
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Clarification answers (if any):\n{json.dumps(answers_payload, ensure_ascii=False)}\n\n"
        f"Planner hints (join/cleansing guidance):\n{json.dumps(planner_hints or {}, ensure_ascii=False)}\n\n"
        f"Pipeline context pack (safe summary):\n{json.dumps(context_payload, ensure_ascii=False)}\n"
    )


def _build_clarifier_system_prompt() -> str:
    return (
        "You are a strict pipeline plan clarifier.\n"
        "Return ONLY JSON with questions needed to fix validation errors.\n"
        "Do NOT propose a new plan; ask focused questions.\n"
    )


def _build_clarifier_user_prompt(
    *,
    goal: str,
    draft_plan: Dict[str, Any],
    validation_errors: List[str],
    validation_warnings: List[str],
    data_scope: Optional[PipelinePlanDataScope],
    context_pack: Optional[Dict[str, Any]],
    ) -> str:
        scope_payload = data_scope.model_dump(mode="json") if data_scope else {}
        return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Draft plan:\n{json.dumps(draft_plan, ensure_ascii=False)}\n\n"
        f"Validation errors:\n{json.dumps(validation_errors, ensure_ascii=False)}\n\n"
        f"Validation warnings:\n{json.dumps(validation_warnings, ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


def _build_repair_system_prompt() -> str:
    return (
        "You are a STRICT pipeline plan repair agent.\n"
        "Return ONLY JSON following the PipelinePlan output schema.\n"
        "Fix validation/preview errors by adjusting definition_json.\n"
        "Preserve the original data_scope and output intent.\n"
        "Keep changes minimal and deterministic.\n"
    )


def _build_repair_user_prompt(
    *,
    plan: PipelinePlan,
    validation_errors: List[str],
    validation_warnings: List[str],
    preflight: Optional[Dict[str, Any]],
    preview: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Current plan:\n{json.dumps(plan.model_dump(mode='json'), ensure_ascii=False)}\n\n"
        f"Validation errors:\n{json.dumps(validation_errors or [], ensure_ascii=False)}\n\n"
        f"Validation warnings:\n{json.dumps(validation_warnings or [], ensure_ascii=False)}\n\n"
        f"Preflight:\n{json.dumps(preflight or {}, ensure_ascii=False)}\n\n"
        f"Preview:\n{json.dumps(preview or {}, ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


def _fallback_questions_from_errors(errors: List[str]) -> List[PipelineClarificationQuestion]:
    questions: List[PipelineClarificationQuestion] = []
    for idx, err in enumerate(errors[:5]):
        questions.append(
            PipelineClarificationQuestion(
                id=f"q{idx + 1}",
                question=f"Please clarify: {err}",
                required=True,
                type="string",
            )
        )
    if not questions:
        questions.append(
            PipelineClarificationQuestion(
                id="q1",
                question="Which datasets/columns should be used for joins and outputs?",
                required=True,
                type="string",
            )
        )
    return questions


async def compile_pipeline_plan(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    dataset_registry: DatasetRegistry,
) -> PipelinePlanCompileResult:
    plan_id = str(uuid4())
    system_prompt = _build_plan_system_prompt()
    user_prompt = _build_plan_user_prompt(
        goal=goal,
        data_scope=data_scope,
        answers=answers,
        context_pack=context_pack,
        planner_hints=planner_hints,
    )
    llm_meta: Optional[LLMCallMeta] = None

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

    draft = None
    draft_plan_obj: Dict[str, Any] = {}
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            draft, llm_meta = await llm_gateway.complete_json(
                task="PIPELINE_PLAN_COMPILE_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=PipelinePlanDraftEnvelope,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={
                    "kind": "pipeline_plan_compile",
                    "schema": "PipelinePlan",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "attempt": attempt + 1,
                },
            )
            draft_plan_obj = dict(draft.plan or {})
            last_exc = None
            break
        except LLMOutputValidationError as exc:
            last_exc = exc
            if attempt < 1:
                continue
            break
        except (LLMUnavailableError, LLMRequestError) as exc:
            last_exc = exc
            break
    if last_exc is not None:
        questions = _fallback_questions_from_errors([str(last_exc)])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[str(last_exc)],
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
        )

    if draft is not None and draft.questions:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["planner requested clarification"],
            validation_warnings=[],
            questions=list(draft.questions or []),
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence),
            planner_notes=draft.notes if draft is not None else None,
        )

    try:
        plan = PipelinePlan.model_validate(draft_plan_obj)
    except ValidationError as exc:
        errors = [str(e.get("msg") or e) for e in exc.errors()]
        questions = _fallback_questions_from_errors(errors)
        return PipelinePlanCompileResult(
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
            "data_scope": data_scope or plan.data_scope,
        }
    )

    scope_db = str(plan.data_scope.db_name or "").strip()
    scope_branch = str(plan.data_scope.branch or "").strip() or None
    validation = await validate_pipeline_plan(
        plan=plan,
        dataset_registry=dataset_registry,
        db_name=scope_db,
        branch=scope_branch,
        require_output=True,
    )

    if validation.errors:
        questions: List[PipelineClarificationQuestion] = []
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
                task="PIPELINE_PLAN_CLARIFY_V1",
                system_prompt=clarifier_system_prompt,
                user_prompt=clarifier_user_prompt,
                response_model=PipelineClarificationPayload,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={"kind": "pipeline_plan_clarify", "tenant_id": tenant_id, "user_id": user_id},
            )
            questions = list(clarification.questions or [])
            llm_meta = clarification_meta or llm_meta
        except LLMQuotaExceededError:
            raise
        except Exception:
            questions = _fallback_questions_from_errors(validation.errors)

        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=validation.plan,
            validation_errors=validation.errors,
            validation_warnings=list(validation.warnings or []),
            questions=questions,
            compilation_report=validation.compilation_report,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
            preflight=validation.preflight,
        )

    return PipelinePlanCompileResult(
        status="success",
        plan_id=plan_id,
        plan=validation.plan,
        validation_errors=[],
        validation_warnings=list(validation.warnings or []),
        questions=[],
        compilation_report=validation.compilation_report,
        llm_meta=llm_meta,
        planner_confidence=float(draft.confidence) if draft is not None else None,
        planner_notes=draft.notes if draft is not None else None,
        preflight=validation.preflight,
    )


async def repair_pipeline_plan(
    *,
    plan: PipelinePlan,
    validation_errors: List[str],
    validation_warnings: List[str],
    preflight: Optional[Dict[str, Any]],
    preview: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    dataset_registry: DatasetRegistry,
) -> PipelinePlanCompileResult:
    plan_id = str(plan.plan_id or uuid4())
    system_prompt = _build_repair_system_prompt()
    user_prompt = _build_repair_user_prompt(
        plan=plan,
        validation_errors=validation_errors,
        validation_warnings=validation_warnings,
        preflight=preflight,
        preview=preview,
        context_pack=context_pack,
    )
    llm_meta: Optional[LLMCallMeta] = None

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
            task="PIPELINE_PLAN_REPAIR_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelinePlanDraftEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan_id}",
            audit_actor=actor,
            audit_resource_id=plan_id,
            audit_metadata={"kind": "pipeline_plan_repair", "schema": "PipelinePlan", "tenant_id": tenant_id, "user_id": user_id},
        )
        draft_plan_obj = dict(draft.plan or {})
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        questions = _fallback_questions_from_errors([str(exc)])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[str(exc)],
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
        )

    base_plan = plan.model_dump(mode="json")
    if draft_plan_obj:
        merged = dict(base_plan)
        for key, value in draft_plan_obj.items():
            if value is not None:
                merged[key] = value
        draft_plan_obj = merged
    else:
        draft_plan_obj = base_plan

    try:
        repaired_plan = PipelinePlan.model_validate(draft_plan_obj)
    except ValidationError as exc:
        errors = [str(e.get("msg") or e) for e in exc.errors()]
        questions = _fallback_questions_from_errors(errors)
        return PipelinePlanCompileResult(
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

    repaired_plan = repaired_plan.model_copy(
        update={
            "plan_id": repaired_plan.plan_id or plan_id,
            "created_by": repaired_plan.created_by or actor,
            "data_scope": plan.data_scope,
        }
    )

    scope_db = str(repaired_plan.data_scope.db_name or "").strip()
    scope_branch = str(repaired_plan.data_scope.branch or "").strip() or None
    validation = await validate_pipeline_plan(
        plan=repaired_plan,
        dataset_registry=dataset_registry,
        db_name=scope_db,
        branch=scope_branch,
        require_output=True,
    )

    if validation.errors:
        questions = _fallback_questions_from_errors(validation.errors)
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=validation.plan,
            validation_errors=validation.errors,
            validation_warnings=list(validation.warnings or []),
            questions=questions,
            compilation_report=validation.compilation_report,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
            preflight=validation.preflight,
        )

    return PipelinePlanCompileResult(
        status="success",
        plan_id=plan_id,
        plan=validation.plan,
        validation_errors=[],
        validation_warnings=list(validation.warnings or []),
        questions=[],
        compilation_report=validation.compilation_report,
        llm_meta=llm_meta,
        planner_confidence=float(draft.confidence) if draft is not None else None,
        planner_notes=draft.notes if draft is not None else None,
        preflight=validation.preflight,
    )
