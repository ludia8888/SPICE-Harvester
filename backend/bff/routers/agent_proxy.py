"""
Agent router (BFF).

Provides the Pipeline Agent API for autonomous LLM-driven pipeline creation.
Includes SSE streaming endpoint for real-time UI updates.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from shared.models.pipeline_agent import PipelineAgentRunRequest
from bff.routers.pipeline_plans import (
    PipelinePlanPreviewRequest,
    get_dataset_registry,
    get_pipeline_plan_registry,
    get_pipeline_registry,
    preview_plan,
    _resolve_actor,
    _resolve_tenant_id,
    _resolve_tenant_policy,
)
from bff.services.pipeline_agent_autonomous_loop import (
    run_pipeline_agent_mcp_autonomous,
    run_pipeline_agent_streaming,
    StreamEvent,
)
from shared.config.settings import get_settings
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.models.responses import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["Agent"])


@router.post("/pipeline-runs")
async def create_pipeline_run(
    request: Request,
    body: PipelineAgentRunRequest,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    """
    Pipeline agent runs are handled in the BFF as a single autonomous loop + MCP tools.

    This is the primary API for natural language pipeline creation.
    """
    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    data_scope = body.data_scope
    db_name = validate_db_name(str(data_scope.db_name or "").strip())
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    payload = await run_pipeline_agent_mcp_autonomous(
        goal=str(body.goal or "").strip(),
        data_scope=data_scope,
        resume_plan_id=str(body.plan_id).strip() if body.plan_id else None,
        answers=body.answers if isinstance(body.answers, dict) else None,
        planner_hints=body.planner_hints if isinstance(body.planner_hints, dict) else None,
        task_spec=body.task_spec if isinstance(body.task_spec, dict) else None,
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
        plan_registry=plan_registry,
    )

    # Best-effort preview to match existing UI expectations. Keep it lightweight by default.
    plan_id = str(payload.get("plan_id") or "").strip()
    if plan_id and payload.get("plan") and payload.get("preview") is None:
        preview_node_id = str(getattr(body, "preview_node_id", None) or "").strip() or None
        limit = max(1, min(int(getattr(body, "preview_limit", 200) or 200), 200))
        try:
            preview_resp = await preview_plan(
                plan_id=plan_id,
                body=PipelinePlanPreviewRequest(
                    node_id=preview_node_id,
                    limit=limit,
                    include_run_tables=False,
                    run_table_limit=limit,
                ),
                request=request,
                dataset_registry=dataset_registry,
                pipeline_registry=pipeline_registry,
                plan_registry=plan_registry,
            )
            data = getattr(preview_resp, "data", None)
            if isinstance(data, dict) and isinstance(data.get("preview"), dict):
                payload["preview"] = data.get("preview")
        except Exception as exc:
            logger.warning("Pipeline agent preview failed plan_id=%s err=%s", plan_id, exc)

    status_value = str(payload.get("status") or "failed").strip().lower()
    agent_message = str(payload.get("message") or "").strip()
    if status_value == "clarification_required":
        return ApiResponse.warning(message=agent_message or "Pipeline agent needs clarification", data=payload)
    if status_value == "partial":
        return ApiResponse.partial(message=agent_message or "Pipeline agent completed with warnings", data=payload)
    if status_value != "success":
        return ApiResponse.warning(message=agent_message or "Pipeline agent failed", data=payload)
    return ApiResponse.success(message=agent_message or "Pipeline agent completed", data=payload)


@router.post("/pipeline-runs/stream")
async def stream_pipeline_run(
    request: Request,
    body: PipelineAgentRunRequest,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> StreamingResponse:
    """
    SSE 스트리밍 버전의 Pipeline Agent API.

    도구 호출마다 실시간 이벤트를 전송하여 UI에서 노드를 동적으로 생성할 수 있습니다.

    이벤트 타입:
    - start: 에이전트 시작
    - tool_start: 도구 호출 시작
    - tool_end: 도구 호출 완료
    - plan_update: 플랜 업데이트 (노드/엣지 추가)
    - clarification: 추가 정보 필요
    - error: 에러 발생
    - complete: 완료
    """
    if not bool(get_settings().pipeline_plan.llm_enabled):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline planner is disabled")

    data_scope = body.data_scope
    db_name = validate_db_name(str(data_scope.db_name or "").strip())
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    tenant_id = _resolve_tenant_id(request)
    actor = _resolve_actor(request)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "").strip() or None

    selected_model, allowed_models, data_policies = await _resolve_tenant_policy(request)

    async def event_generator():
        """SSE 이벤트 생성기"""
        try:
            async for event in run_pipeline_agent_streaming(
                goal=str(body.goal or "").strip(),
                data_scope=data_scope,
                resume_plan_id=str(body.plan_id).strip() if body.plan_id else None,
                answers=body.answers if isinstance(body.answers, dict) else None,
                planner_hints=body.planner_hints if isinstance(body.planner_hints, dict) else None,
                task_spec=body.task_spec if isinstance(body.task_spec, dict) else None,
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
                plan_registry=plan_registry,
            ):
                # SSE 형식으로 이벤트 전송
                event_data = json.dumps(event.data, ensure_ascii=False)
                yield f"event: {event.event_type}\ndata: {event_data}\n\n"

        except Exception as exc:
            logger.error("SSE streaming error: %s", exc)
            error_data = json.dumps({"error": str(exc)}, ensure_ascii=False)
            yield f"event: error\ndata: {error_data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Nginx 버퍼링 비활성화
        },
    )
