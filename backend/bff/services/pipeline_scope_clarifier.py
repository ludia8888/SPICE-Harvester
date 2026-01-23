"""
Pipeline scope clarifier.

Generates user-facing clarification questions when the current TaskSpec
forbids the next required action (e.g., intent requires revision but joins
are disabled). This must be LLM-generated (no hard-coded question text).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from bff.services.pipeline_plan_compiler import PipelineClarificationQuestion
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


class PipelineScopeClarificationEnvelope(BaseModel):
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineScopeClarificationResult:
    status: str  # success|clarification_required
    questions: List[PipelineClarificationQuestion]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _build_scope_clarifier_system_prompt() -> str:
    return (
        "You are a STRICT scope clarifier for SPICE-Harvester.\n"
        "Return ONLY JSON. No markdown, no commentary.\n"
        "Your job is to ask the minimal set of questions required to proceed.\n"
        "\n"
        "Rules:\n"
        "- Write questions in the same language as the user's goal.\n"
        "- Do NOT propose a pipeline plan. Do NOT include definition_json.\n"
        "- If the current task_spec forbids required actions (join/advanced transforms/cleansing/specs),\n"
        "  ask for explicit permission to expand scope.\n"
        "- Keep questions <= 3. Prefer boolean/enum questions when possible.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"questions\": [{id,question,required,type,options?,default?}],\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_scope_clarifier_user_prompt(
    *,
    goal: str,
    task_spec: Optional[Dict[str, Any]],
    intent_feedback: Dict[str, Any],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    # Keep prompt compact: include only minimal dataset summary.
    selected_summary: List[Dict[str, Any]] = []
    if isinstance(context_pack, dict):
        selected = context_pack.get("selected_datasets")
        if isinstance(selected, list):
            for ds in selected[:8]:
                if not isinstance(ds, dict):
                    continue
                cols = ds.get("columns") if isinstance(ds.get("columns"), list) else []
                selected_summary.append(
                    {
                        "dataset_id": ds.get("dataset_id"),
                        "name": ds.get("name"),
                        "branch": ds.get("branch"),
                        "columns_preview": [
                            col.get("name")
                            for col in cols[:15]
                            if isinstance(col, dict) and col.get("name")
                        ],
                    }
                )

    payload = {
        "goal": goal,
        "current_task_spec": task_spec or {},
        "intent_feedback": intent_feedback or {},
        "selected_datasets": selected_summary,
    }
    return json.dumps(payload, ensure_ascii=False)


async def clarify_pipeline_scope(
    *,
    goal: str,
    task_spec: Optional[Dict[str, Any]],
    intent_feedback: Dict[str, Any],
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
) -> PipelineScopeClarificationResult:
    scope_id = str(uuid4())
    system_prompt = _build_scope_clarifier_system_prompt()
    user_prompt = _build_scope_clarifier_user_prompt(
        goal=str(goal or ""),
        task_spec=task_spec,
        intent_feedback=intent_feedback or {},
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
            task="PIPELINE_SCOPE_CLARIFY_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineScopeClarificationEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key="pipeline:scope_clarify",
            audit_actor=actor,
            audit_resource_id=f"pipeline:scope_clarify:{scope_id}",
            audit_metadata={"kind": "pipeline_scope_clarify", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        # No rule-based question generation; surface the error so callers can decide next step.
        raise exc

    questions = list(draft.questions or [])
    status = "clarification_required" if questions else "success"
    return PipelineScopeClarificationResult(
        status=status,
        questions=questions,
        notes=list(draft.notes or []),
        warnings=list(draft.warnings or []),
        llm_meta=llm_meta,
    )

