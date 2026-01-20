"""
Pipeline output splitter (LLM-guided).

Classifies pipeline outputs as object/link and enriches metadata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from shared.models.pipeline_plan import PipelinePlan, PipelinePlanOutput
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


class PipelineOutputSplitEnvelope(BaseModel):
    outputs: List[PipelinePlanOutput] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineOutputSplitResult:
    outputs: List[PipelinePlanOutput]
    confidence: Optional[float]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _build_split_system_prompt() -> str:
    return (
        "You are a STRICT output classifier for SPICE-Harvester pipeline plans.\n"
        "Return ONLY JSON. No markdown, no commentary.\n"
        "Do NOT change output_name values.\n"
        "\n"
        "Rules:\n"
        "- output_kind must be one of: object, link, unknown.\n"
        "- object outputs MUST include target_class_id.\n"
        "- link outputs MUST include link_type_id, source_class_id, target_class_id, predicate, cardinality,\n"
        "  source_key_column, target_key_column, relationship_spec_type (join_table or foreign_key).\n"
        "- Use output_bindings as ground truth when provided.\n"
        "- If unsure, set output_kind to unknown and add a warning explaining why.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"outputs\": [PipelinePlanOutput JSON],\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_split_user_prompt(
    *,
    goal: str,
    outputs: List[Dict[str, Any]],
    output_bindings: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Plan outputs:\n{json.dumps(outputs or [], ensure_ascii=False)}\n\n"
        f"Output bindings:\n{json.dumps(output_bindings or {}, ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


async def split_pipeline_outputs(
    *,
    plan: PipelinePlan,
    output_bindings: Optional[Dict[str, Any]],
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
) -> PipelineOutputSplitResult:
    system_prompt = _build_split_system_prompt()
    outputs_payload = [output.model_dump(mode="json") for output in (plan.outputs or [])]
    user_prompt = _build_split_user_prompt(
        goal=str(plan.goal or ""),
        outputs=outputs_payload,
        output_bindings=output_bindings,
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
            task="PIPELINE_OUTPUT_SPLIT_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineOutputSplitEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id or uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or uuid4()),
            audit_metadata={"kind": "pipeline_output_split", "schema": "PipelinePlanOutput", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError):
        raise

    try:
        output_by_name = {output.output_name: output for output in (draft.outputs or [])}
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    resolved: List[PipelinePlanOutput] = []
    for output in plan.outputs or []:
        candidate = output_by_name.get(output.output_name)
        resolved.append(candidate if candidate is not None else output)

    return PipelineOutputSplitResult(
        outputs=resolved,
        confidence=float(draft.confidence) if draft is not None else None,
        notes=list(draft.notes or []),
        warnings=list(draft.warnings or []),
        llm_meta=llm_meta,
    )
