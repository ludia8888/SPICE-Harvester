"""
Pipeline task spec agent.

Classifies the user's request into a strict task scope that the rest of the
pipeline agent must obey (avoid overreach).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import ValidationError

from shared.models.pipeline_plan import PipelinePlanDataScope
from shared.models.pipeline_task_spec import PipelineTaskScope, PipelineTaskSpec
from shared.services.pipeline_task_spec_policy import clamp_task_spec
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


@dataclass(frozen=True)
class PipelineTaskSpecResult:
    status: str  # success|clarification_required
    task_spec: Optional[PipelineTaskSpec]
    questions: list[dict[str, Any]]
    llm_meta: Optional[LLMCallMeta] = None


def _build_task_spec_system_prompt() -> str:
    return (
        "You are a STRICT pipeline task classifier for SPICE-Harvester.\n"
        "Return ONLY JSON (no markdown, no commentary).\n"
        "Your job is to prevent overreach: only authorize the minimal scope needed.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"scope\": \"report_only\"|\"pipeline\",\n"
        "  \"intent\": \"null_check\"|\"profile\"|\"cleanse\"|\"integrate\"|\"prepare_mapping\"|\"unknown\",\n"
        "  \"allow_join\": boolean,\n"
        "  \"allow_cleansing\": boolean,\n"
        "  \"allow_advanced_transforms\": boolean,\n"
        "  \"allow_specs\": boolean,\n"
        "  \"allow_write\": false,\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[],\n"
        "  \"questions\": [{id,question,required,type,options?,default?}] (optional)\n"
        "}\n"
        "\n"
        "Rules:\n"
        "- If the user asks only for checking/reporting (e.g. null check / data quality report), set scope=report_only.\n"
        "- If the user asks to transform/join/cleanse/map/prepare outputs, set scope=pipeline.\n"
        "- If required information is missing (e.g. which dataset, which columns), ask questions instead of guessing.\n"
        "- allow_write MUST always be false.\n"
        "- allow_join MUST be true only when the goal requires combining multiple datasets.\n"
        "- allow_advanced_transforms MUST be true when the goal requires aggregation/ranking/window operations\n"
        "  (group by, aggregate/sum/avg/count, top-N, rank/row_number, window/over/partition, pivot).\n"
        "- allow_specs MUST be true only when the user explicitly asks for ontology/canonical mapping preparation.\n"
    )


def _build_task_spec_user_prompt(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    scope_payload = data_scope.model_dump(mode="json") if data_scope else {}
    selected = []
    if isinstance(context_pack, dict):
        raw = context_pack.get("selected_datasets")
        if isinstance(raw, list):
            for item in raw[:12]:
                if not isinstance(item, dict):
                    continue
                selected.append(
                    {
                        "dataset_id": item.get("dataset_id"),
                        "name": item.get("name"),
                        "branch": item.get("branch"),
                        "column_count": len(item.get("columns") or []) if isinstance(item.get("columns"), list) else None,
                        "columns_preview": [
                            col.get("name")
                            for col in (item.get("columns") or [])[:20]
                            if isinstance(col, dict) and col.get("name")
                        ],
                    }
                )
    payload = {
        "goal": goal,
        "data_scope": scope_payload,
        "selected_datasets": selected,
    }
    return json.dumps(payload, ensure_ascii=False)


def _clamp_task_spec(*, spec: PipelineTaskSpec, data_scope: Optional[PipelinePlanDataScope]) -> PipelineTaskSpec:
    dataset_count = len(list(data_scope.dataset_ids or [])) if data_scope else 0
    # Keep this adapter for callers in this module; centralize logic in shared.
    return clamp_task_spec(spec=spec, dataset_count=dataset_count)


async def infer_pipeline_task_spec(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    context_pack: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[list[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
) -> PipelineTaskSpecResult:
    task_id = str(uuid4())
    system_prompt = _build_task_spec_system_prompt()
    user_prompt = _build_task_spec_user_prompt(goal=goal, data_scope=data_scope, context_pack=context_pack)
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
        spec, llm_meta = await llm_gateway.complete_json(
            task="PIPELINE_TASK_SPEC_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineTaskSpec,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key="pipeline:task_spec",
            audit_actor=actor,
            audit_resource_id=f"pipeline:task_spec:{task_id}",
            audit_metadata={"kind": "pipeline_task_spec", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        # No rule-based question generation; bubble up so callers can decide the UX (retry/backoff/etc).
        raise exc

    try:
        spec_model = PipelineTaskSpec.model_validate(spec.model_dump(mode="json"))
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    spec_model = _clamp_task_spec(spec=spec_model, data_scope=data_scope)

    questions = [q for q in (spec_model.questions or []) if isinstance(q, dict) and str(q.get("question") or "").strip()]
    status = "clarification_required" if questions else "success"
    return PipelineTaskSpecResult(
        status=status,
        task_spec=spec_model,
        questions=questions,
        llm_meta=llm_meta,
    )
