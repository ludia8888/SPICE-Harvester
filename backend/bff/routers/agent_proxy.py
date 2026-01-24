"""
Agent proxy router (BFF).

Ensures the Agent service is only reachable through the BFF.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

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
from bff.services.pipeline_agent_autonomous_loop import run_pipeline_agent_mcp_autonomous
from shared.config.settings import build_client_ssl_config, get_settings
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.models.responses import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import validate_db_name
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["Agent"])

_FORWARD_HEADER_ALLOWLIST = {
    "accept",
    "accept-language",
    "authorization",
    "content-type",
    "user-agent",
    "x-admin-token",
    "x-delegated-authorization",
    "x-actor",
    "x-actor-type",
    "x-org-id",
    "x-principal-id",
    "x-principal-type",
    "x-request-id",
    "x-tenant-id",
    "x-user",
    "x-user-id",
    "x-user-roles",
    "x-user-type",
}
_CALLER_HEADER = "x-spice-caller"
_BLOCKED_CALLER = "agent"

_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


def _forward_headers(request: Request) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in request.headers.items():
        if not value:
            continue
        if key.lower() in _FORWARD_HEADER_ALLOWLIST:
            headers[key] = value
    return headers


def _filter_response_headers(headers: httpx.Headers) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in _HOP_BY_HOP_HEADERS:
            continue
        output[key] = value
    return output


async def _proxy_agent_request(request: Request, path: str) -> Response:
    caller = (request.headers.get("X-Spice-Caller") or "").strip().lower()
    if caller == _BLOCKED_CALLER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Agent proxy loop blocked",
        )
    settings = get_settings()
    agent_url = settings.services.agent_base_url
    suffix = path.lstrip("/")
    target_path = "/api/v1/agent"
    if suffix:
        target_path = f"{target_path}/{suffix}"
    url = f"{agent_url}{target_path}"

    timeout_seconds = float(settings.clients.agent_proxy_timeout_seconds)
    ssl_config = build_client_ssl_config(settings)
    headers = _forward_headers(request)
    body = await request.body()

    try:
        async with httpx.AsyncClient(
            timeout=timeout_seconds,
            verify=ssl_config.get("verify", True),
        ) as client:
            response = await client.request(
                request.method,
                url,
                params=request.query_params,
                content=body,
                headers=headers,
            )
    except httpx.RequestError as exc:
        logger.error("Agent proxy request failed: %s %s (%s)", request.method, url, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Agent service request failed",
        ) from exc

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=_filter_response_headers(response.headers),
    )


@router.post("/runs", status_code=status.HTTP_202_ACCEPTED)
async def create_agent_run(request: Request) -> Response:
    return await _proxy_agent_request(request, "runs")


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

    We keep the `/agent/*` prefix for UI compatibility, but do NOT proxy this route
    to the legacy Agent service (tool runner).
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
        answers=body.answers if isinstance(body.answers, dict) else None,
        planner_hints=body.planner_hints if isinstance(body.planner_hints, dict) else None,
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
    if status_value == "clarification_required":
        return ApiResponse.warning(message="Pipeline agent needs clarification", data=payload)
    if status_value == "partial":
        return ApiResponse.partial(message="Pipeline agent completed with warnings", data=payload)
    if status_value != "success":
        return ApiResponse.warning(message="Pipeline agent failed", data=payload)
    return ApiResponse.success(message="Pipeline agent completed", data=payload)


@router.get("/runs/{run_id}")
async def get_agent_run(request: Request, run_id: str) -> Response:
    return await _proxy_agent_request(request, f"runs/{run_id}")


@router.get("/runs/{run_id}/events")
async def list_agent_run_events(request: Request, run_id: str) -> Response:
    return await _proxy_agent_request(request, f"runs/{run_id}/events")
