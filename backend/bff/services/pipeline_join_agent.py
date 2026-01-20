"""
Pipeline join key agent.

Selects join keys from context pack candidates using LLM guidance.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

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


class PipelineJoinSelection(BaseModel):
    left_dataset_id: str = Field(..., min_length=1, max_length=200)
    right_dataset_id: str = Field(..., min_length=1, max_length=200)
    left_column: str = Field(..., min_length=1, max_length=200)
    right_column: str = Field(..., min_length=1, max_length=200)
    join_type: str = Field(default="inner", max_length=40)
    cardinality: Optional[str] = Field(default=None, max_length=40)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reason: Optional[str] = Field(default=None, max_length=500)


class PipelineJoinEnvelope(BaseModel):
    joins: List[PipelineJoinSelection] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineJoinResult:
    joins: List[PipelineJoinSelection]
    confidence: Optional[float]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _build_join_system_prompt() -> str:
    return (
        "You are a STRICT join-key selector for SPICE-Harvester.\n"
        "Return ONLY JSON. No markdown, no commentary.\n"
        "Choose join keys ONLY from the provided join candidates.\n"
        "Do NOT invent dataset ids or columns.\n"
        "Keep the join chain minimal to satisfy the goal.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"joins\": [PipelineJoinSelection],\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_join_user_prompt(
    *,
    goal: str,
    context_pack: Dict[str, Any],
    max_joins: int,
) -> str:
    suggestions = {}
    if isinstance(context_pack, dict):
        suggestions = context_pack.get("integration_suggestions") or {}
    payload = {
        "join_key_candidates": suggestions.get("join_key_candidates") if isinstance(suggestions, dict) else None,
        "foreign_key_candidates": suggestions.get("foreign_key_candidates") if isinstance(suggestions, dict) else None,
    }
    return (
        f"Goal:\n{goal}\n\n"
        f"Max joins:\n{max_joins}\n\n"
        f"Join candidates:\n{json.dumps(payload, ensure_ascii=False)}\n"
    )


async def select_join_keys(
    *,
    goal: str,
    context_pack: Dict[str, Any],
    max_joins: int,
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
) -> PipelineJoinResult:
    system_prompt = _build_join_system_prompt()
    user_prompt = _build_join_user_prompt(goal=goal, context_pack=context_pack, max_joins=max_joins)
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
            task="PIPELINE_JOIN_KEYS_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineJoinEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_join:{uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(uuid4()),
            audit_metadata={"kind": "pipeline_join_keys", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        joins = list(draft.joins or [])[: max(0, int(max_joins))]
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    return PipelineJoinResult(
        joins=joins,
        confidence=float(draft.confidence) if draft is not None else None,
        notes=list(draft.notes or []),
        warnings=list(draft.warnings or []),
        llm_meta=llm_meta,
    )
