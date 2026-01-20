"""
Pipeline transform agent.

Refines pipeline definition_json using join selections and context pack guidance.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from bff.services.pipeline_plan_validation import validate_pipeline_plan
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
from shared.services.pipeline_transform_spec import SUPPORTED_TRANSFORMS


class PipelineTransformEnvelope(BaseModel):
    definition_json: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineTransformResult:
    plan: PipelinePlan
    validation_errors: List[str]
    validation_warnings: List[str]
    confidence: Optional[float]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _build_transform_system_prompt() -> str:
    return (
        "You are a STRICT pipeline transform agent for SPICE-Harvester.\n"
        "Return ONLY JSON with definition_json. No markdown.\n"
        "Keep nodes/edges minimal and deterministic.\n"
        "Do NOT change plan outputs or data_scope.\n"
        "Preserve pkSemantics/pkColumns/expectations unless you are fixing validation errors.\n"
        "Use only supported operations; no UDF.\n"
        "join requires leftKey/rightKey (or joinKey) and allowCrossJoin must be false.\n"
        "Prefer join/window/aggregate only when needed by the goal; follow join_plan hints.\n"
        "\n"
        f"Supported operations: {', '.join(sorted(SUPPORTED_TRANSFORMS))}\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"definition_json\": {\"nodes\": [], \"edges\": []},\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_transform_user_prompt(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    current_definition: Dict[str, Any],
    outputs: List[Dict[str, Any]],
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(data_scope or {}, ensure_ascii=False)}\n\n"
        f"Current definition_json:\n{json.dumps(current_definition or {}, ensure_ascii=False)}\n\n"
        f"Outputs:\n{json.dumps(outputs or [], ensure_ascii=False)}\n\n"
        f"Join plan:\n{json.dumps(join_plan or [], ensure_ascii=False)}\n\n"
        f"Cleansing hints:\n{json.dumps(cleansing_hints or [], ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


async def apply_transform_plan(
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
    db_name: str,
    branch: Optional[str],
    dataset_registry: Any,
) -> PipelineTransformResult:
    system_prompt = _build_transform_system_prompt()
    user_prompt = _build_transform_user_prompt(
        goal=str(plan.goal or ""),
        data_scope=plan.data_scope.model_dump(mode="json"),
        current_definition=plan.definition_json,
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
            task="PIPELINE_TRANSFORM_PLAN_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineTransformEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id or uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or uuid4()),
            audit_metadata={"kind": "pipeline_transform_plan", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        definition_json = dict(draft.definition_json or {})
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    updated_plan = plan.model_copy(update={"definition_json": definition_json})
    validation = await validate_pipeline_plan(
        plan=updated_plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
        require_output=True,
    )

    return PipelineTransformResult(
        plan=validation.plan,
        validation_errors=list(validation.errors or []),
        validation_warnings=list(validation.warnings or []),
        confidence=float(draft.confidence) if draft is not None else None,
        notes=list(draft.notes or []),
        warnings=list(draft.warnings or []),
        llm_meta=llm_meta,
    )
