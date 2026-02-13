"""
Pipeline plan compiler (single autonomous loop + MCP tools).

Historically, `/api/v1/pipeline-plans/compile` had its own autonomous loop implementation.
That duplication made enterprise improvements (batching, prompt prefix caching, compaction)
hard to roll out consistently.

This module now delegates to the Pipeline Agent loop runtime and adapts the result to the
legacy `PipelinePlanCompileResult` shape for API compatibility.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

from bff.services.pipeline_agent_autonomous_loop import run_pipeline_agent_mcp_autonomous
from bff.services.pipeline_plan_models import PipelineClarificationQuestion, PipelinePlanCompileResult
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanDataScope
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.agent.llm_gateway import LLMCallMeta, LLMGateway
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.storage.redis_service import RedisService
from shared.observability.tracing import trace_external_call
import logging


def _coerce_questions(raw: Any) -> List[PipelineClarificationQuestion]:
    questions: List[PipelineClarificationQuestion] = []
    items = raw if isinstance(raw, list) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            questions.append(PipelineClarificationQuestion.model_validate(item))
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/pipeline_plan_autonomous_compiler.py:36", exc_info=True)
            continue
    return questions


def _coerce_llm_meta(raw: Any) -> Optional[LLMCallMeta]:
    if not isinstance(raw, dict):
        return None
    try:
        provider = str(raw.get("provider") or "").strip()
        model = str(raw.get("model") or "").strip()
        cache_hit = bool(raw.get("cache_hit"))
        latency_ms = int(raw.get("latency_ms") or 0)
        if not provider and not model:
            return None
        return LLMCallMeta(provider=provider, model=model, cache_hit=cache_hit, latency_ms=latency_ms)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/pipeline_plan_autonomous_compiler.py:52", exc_info=True)
        return None


def _coerce_planner_fields(payload: Dict[str, Any]) -> tuple[Optional[float], Optional[List[str]]]:
    planner = payload.get("planner")
    if not isinstance(planner, dict):
        return None, None
    confidence_raw = planner.get("confidence")
    try:
        confidence = float(confidence_raw) if confidence_raw is not None else None
    except (TypeError, ValueError):
        confidence = None
    notes_raw = planner.get("notes")
    notes = [str(n) for n in notes_raw if str(n or "").strip()] if isinstance(notes_raw, list) else None
    return confidence, notes


@trace_external_call("bff.pipeline_plan_compiler.compile_pipeline_plan_mcp_autonomous")
async def compile_pipeline_plan_mcp_autonomous(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
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
    plan_registry: PipelinePlanRegistry,
) -> PipelinePlanCompileResult:
    """
    Compile a pipeline plan using the single autonomous loop runtime.

    Notes:
    - We set planner_hints.require_plan=true so the agent cannot finish with a report-only answer.
    - We run with persist_plan=False to avoid redundant writes; the router persists using the returned plan_id.
    """

    plan_id = str(uuid4())

    if not data_scope or not str(data_scope.db_name or "").strip():
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["data_scope.db_name is required"],
            validation_warnings=[],
            questions=[
                PipelineClarificationQuestion(
                    id="db_name",
                    question="Which database (db_name) should I use?",
                    required=True,
                    type="string",
                )
            ],
        )

    dataset_ids = [str(item).strip() for item in (data_scope.dataset_ids or []) if str(item).strip()]
    if not dataset_ids:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["data_scope.dataset_ids is required"],
            validation_warnings=[],
            questions=[
                PipelineClarificationQuestion(
                    id="dataset_ids",
                    question="Which dataset_ids should I use to satisfy the goal?",
                    required=True,
                    type="string",
                )
            ],
        )

    merged_hints: Dict[str, Any] = dict(planner_hints or {})
    merged_hints.setdefault("require_plan", True)
    merged_hints.setdefault("mode", "pipeline_plans_compile")

    payload = await run_pipeline_agent_mcp_autonomous(
        goal=str(goal or "").strip(),
        data_scope=data_scope,
        answers=answers if isinstance(answers, dict) else None,
        planner_hints=merged_hints,
        task_spec=task_spec if isinstance(task_spec, dict) else None,
        persist_plan=False,
        actor=actor,
        tenant_id=tenant_id,
        user_id=user_id,
        data_policies=data_policies if isinstance(data_policies, dict) else None,
        selected_model=selected_model,
        allowed_models=allowed_models,
        llm_gateway=llm_gateway,
        redis_service=redis_service,
        audit_store=audit_store,
        dataset_registry=dataset_registry,
        plan_registry=plan_registry,
    )

    status_raw = str(payload.get("status") or "").strip()
    payload_plan = payload.get("plan")
    plan: Optional[PipelinePlan] = None
    if isinstance(payload_plan, dict):
        try:
            plan = PipelinePlan.model_validate(payload_plan)
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/pipeline_plan_autonomous_compiler.py:165", exc_info=True)
            plan = None

    questions = _coerce_questions(payload.get("questions"))
    validation_errors = payload.get("validation_errors")
    validation_warnings = payload.get("validation_warnings")
    errors_out = [str(e) for e in (validation_errors or []) if str(e or "").strip()] if isinstance(validation_errors, list) else []
    warnings_out = [str(w) for w in (validation_warnings or []) if str(w or "").strip()] if isinstance(validation_warnings, list) else []

    llm_meta = _coerce_llm_meta(payload.get("llm"))
    planner_confidence, planner_notes = _coerce_planner_fields(payload)
    preflight = payload.get("preflight") if isinstance(payload.get("preflight"), dict) else None

    # Normalize statuses to the legacy compile API surface.
    out_status = "error"
    if status_raw == "clarification_required":
        out_status = "clarification_required"
    elif plan is not None and not errors_out:
        # If we have a validated plan, treat it as success even if the loop exited without "finish".
        out_status = "success"

    out_plan_id = str(payload.get("plan_id") or plan_id).strip() or plan_id

    # If the agent didn't produce a plan for a compile request, surface a clear error.
    if plan is None and out_status != "clarification_required":
        if not errors_out:
            errors_out = ["pipeline agent did not produce a plan"]

    return PipelinePlanCompileResult(
        status=out_status,
        plan_id=out_plan_id,
        plan=plan,
        validation_errors=errors_out,
        validation_warnings=warnings_out,
        questions=questions,
        compilation_report=None,
        llm_meta=llm_meta,
        planner_confidence=planner_confidence,
        planner_notes=planner_notes,
        preflight=preflight,
    )

