"""
Pipeline intent verifier.

Uses LLM to verify that a pipeline plan matches the user's intent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

from bff.services.pipeline_plan_compiler import PipelineClarificationQuestion
from shared.models.pipeline_plan import PipelinePlan
from shared.services.audit_log_store import AuditLogStore
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.llm_quota import enforce_llm_quota
from shared.services.redis_service import RedisService


class PipelineIntentCheckEnvelope(BaseModel):
    status: str = Field(default="pass", max_length=40)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    missing_requirements: List[str] = Field(default_factory=list)
    suggested_actions: List[str] = Field(default_factory=list)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineIntentCheckResult:
    status: str  # pass|needs_revision|clarification_required
    confidence: Optional[float]
    missing_requirements: List[str]
    suggested_actions: List[str]
    questions: List[PipelineClarificationQuestion]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _build_intent_system_prompt() -> str:
    return (
        "You are a STRICT pipeline intent verifier for SPICE-Harvester.\n"
        "Return ONLY JSON. No markdown, no commentary.\n"
        "Evaluate whether the plan definition satisfies the goal.\n"
        "\n"
        "Rules:\n"
        "- Use status=pass only if all goal requirements are satisfied.\n"
        "- Use status=needs_revision if requirements are missing but can be inferred directly from the goal.\n"
        "- Use status=clarification_required only if the goal is ambiguous.\n"
        "- If you return needs_revision, list missing_requirements and suggested_actions.\n"
        "- If you return clarification_required, provide questions.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"status\": \"pass|needs_revision|clarification_required\",\n"
        "  \"confidence\": number (0..1),\n"
        "  \"missing_requirements\": string[],\n"
        "  \"suggested_actions\": string[],\n"
        "  \"questions\": [PipelineClarificationQuestion],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_intent_user_prompt(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    definition_json: Dict[str, Any],
    outputs: List[Dict[str, Any]],
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(data_scope or {}, ensure_ascii=False)}\n\n"
        f"Definition JSON:\n{json.dumps(definition_json or {}, ensure_ascii=False)}\n\n"
        f"Outputs:\n{json.dumps(outputs or [], ensure_ascii=False)}\n\n"
        f"Join plan:\n{json.dumps(join_plan or [], ensure_ascii=False)}\n\n"
        f"Cleansing hints:\n{json.dumps(cleansing_hints or [], ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


async def verify_pipeline_intent(
    *,
    plan: PipelinePlan,
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
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
) -> PipelineIntentCheckResult:
    system_prompt = _build_intent_system_prompt()
    user_prompt = _build_intent_user_prompt(
        goal=str(plan.goal or ""),
        data_scope=plan.data_scope.model_dump(mode="json"),
        definition_json=plan.definition_json,
        outputs=[output.model_dump(mode="json") for output in (plan.outputs or [])],
        join_plan=join_plan,
        cleansing_hints=cleansing_hints,
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
            task="PIPELINE_INTENT_CHECK_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineIntentCheckEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or ""),
            audit_metadata={"kind": "pipeline_intent_check", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        status = str(draft.status or "pass").strip().lower()
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    if status not in {"pass", "needs_revision", "clarification_required"}:
        status = "needs_revision"

    return PipelineIntentCheckResult(
        status=status,
        confidence=float(draft.confidence) if draft is not None else None,
        missing_requirements=list(draft.missing_requirements or []),
        suggested_actions=list(draft.suggested_actions or []),
        questions=list(draft.questions or []),
        warnings=list(draft.warnings or []),
        llm_meta=llm_meta,
    )
