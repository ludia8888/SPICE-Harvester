from shared.observability.tracing import trace_endpoint

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from bff.routers.pipeline_plans_deps import get_pipeline_plan_registry
from bff.routers.pipeline_plans_ops import _resolve_actor, _resolve_tenant_id, _resolve_tenant_policy
from bff.schemas.pipeline_plans_requests import PipelinePlanCompileRequest
from bff.services.pipeline_plan_autonomous_compiler import compile_pipeline_plan_mcp_autonomous
from bff.services.pipeline_plan_models import PipelinePlanCompileResult
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.models.responses import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input, sanitize_label_input, validate_db_name
from shared.services.agent.llm_quota import LLMQuotaExceededError
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Plans"])


@router.post("/compile", response_model=ApiResponse)
@trace_endpoint("bff.pipeline_plans.compile_plan")
async def compile_plan(
    body: PipelinePlanCompileRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise classified_http_exception(status.HTTP_503_SERVICE_UNAVAILABLE, "Pipeline planner is disabled", code=ErrorCode.UPSTREAM_UNAVAILABLE)

    raw_payload = body.model_dump(exclude_none=True)
    raw_answers = raw_payload.pop("answers", None)
    payload = sanitize_input(raw_payload)
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    answers = sanitize_label_input(raw_answers) if isinstance(raw_answers, dict) else None
    planner_hints = payload.get("planner_hints") if isinstance(payload.get("planner_hints"), dict) else None
    task_spec = payload.get("task_spec") if isinstance(payload.get("task_spec"), dict) else None
    if not data_scope or not data_scope.db_name:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "data_scope.db_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    db_name = validate_db_name(str(data_scope.db_name))
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

    try:
        result: PipelinePlanCompileResult = await compile_pipeline_plan_mcp_autonomous(
            goal=goal,
            data_scope=data_scope,
            answers=answers,
            planner_hints=planner_hints,
            task_spec=task_spec,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=data_policies,
            selected_model=selected_model,
            allowed_models=allowed_models,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
            dataset_registry=dataset_registry,
            pipeline_registry=pipeline_registry,
            plan_registry=plan_registry,
        )
    except LLMQuotaExceededError as exc:
        raise classified_http_exception(status.HTTP_429_TOO_MANY_REQUESTS, str(exc), code=ErrorCode.RATE_LIMITED) from exc
    except Exception as exc:
        logger.error("Pipeline plan compile failed: %s", exc)
        raise

    compiled_plan = result.plan
    validation_errors = list(result.validation_errors or [])
    validation_warnings = list(result.validation_warnings or [])
    response_status = result.status

    response_data = {
        "status": response_status,
        "plan_id": result.plan_id,
        "plan": compiled_plan.model_dump(mode="json") if compiled_plan else None,
        "validation_errors": validation_errors,
        "validation_warnings": validation_warnings,
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
        "compilation_report": result.compilation_report.model_dump(mode="json") if result.compilation_report else None,
        "preflight": result.preflight,
        "planner": {
            "confidence": result.planner_confidence,
            "notes": result.planner_notes,
        },
        "llm": (
            {
                "provider": result.llm_meta.provider,
                "model": result.llm_meta.model,
                "cache_hit": result.llm_meta.cache_hit,
                "latency_ms": result.llm_meta.latency_ms,
            }
            if result.llm_meta
            else None
        ),
    }

    if compiled_plan:
        status_value = "COMPILED" if response_status == "success" else "DRAFT"
        await plan_registry.upsert_plan(
            plan_id=result.plan_id,
            tenant_id=tenant_id,
            status=status_value,
            goal=str(compiled_plan.goal or ""),
            db_name=str(compiled_plan.data_scope.db_name or "") if compiled_plan.data_scope else None,
            branch=str(compiled_plan.data_scope.branch or "") if compiled_plan.data_scope else None,
            plan=compiled_plan.model_dump(mode="json"),
            created_by=actor,
        )

    return ApiResponse.success(message="Pipeline plan compiled", data=response_data)
