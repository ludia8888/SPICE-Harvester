"""
Agent plan validation API (BFF).

Validates planner output against allowlist + risk policy.
"""

import logging
import os

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.services.agent_plan_compiler import compile_agent_plan
from bff.services.agent_plan_validation import validate_agent_plan
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.agent_plan import AgentPlan, AgentPlanDataScope
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.agent_registry import AgentRegistry
from shared.services.agent_plan_registry import AgentPlanRegistry
from shared.services.agent_tool_registry import AgentToolRegistry
from shared.config.service_config import ServiceConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-plans", tags=["Agent Plans"])

_AGENT_FORWARD_HEADER_ALLOWLIST = {
    "accept",
    "accept-language",
    "authorization",
    "content-type",
    "user-agent",
    "x-admin-token",
    "x-actor",
    "x-actor-type",
    "x-principal-id",
    "x-principal-type",
    "x-request-id",
    "x-user",
    "x-user-id",
    "x-user-type",
}
_APPROVED_DECISIONS = {"APPROVED", "APPROVE", "ALLOW", "YES"}
_REJECTED_DECISIONS = {"REJECTED", "REJECT", "DENY", "NO"}


def _forward_headers_to_agent(request: Request) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in request.headers.items():
        if not value:
            continue
        if key.lower() in _AGENT_FORWARD_HEADER_ALLOWLIST:
            headers[key] = value
    headers.setdefault("X-Spice-Caller", "bff")
    return headers


def _render_path(template: str, path_params: Dict[str, Any]) -> str:
    path = str(template or "")
    for key, value in (path_params or {}).items():
        token = "{" + str(key) + "}"
        if token in path and value not in (None, ""):
            path = path.replace(token, str(value))
    return path


def _is_action_simulate_path(path: str) -> bool:
    return str(path or "").rstrip("/").endswith("/simulate")


def _is_preview_safe_step(method: str, path: str, policy_risk_level: str) -> bool:
    normalized_method = str(method or "").strip().upper()
    if normalized_method == "GET":
        return True
    if _is_action_simulate_path(path):
        return True
    return str(policy_risk_level or "").strip().lower() == "read"


async def _call_agent_create_run(*, request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
    agent_url = ServiceConfig.get_agent_url().rstrip("/")
    url = f"{agent_url}/api/v1/agent/runs"
    timeout_seconds = float(os.getenv("AGENT_PROXY_TIMEOUT_SECONDS", "30") or "30")
    ssl_config = ServiceConfig.get_client_ssl_config()

    async with httpx.AsyncClient(timeout=timeout_seconds, verify=ssl_config.get("verify", True)) as client:
        resp = await client.post(url, headers=_forward_headers_to_agent(request), json=payload)
    try:
        body = resp.json()
    except Exception:
        body = {"status": "error", "message": (resp.text or "").strip() or "Agent service error"}

    if resp.status_code >= 400:
        raise HTTPException(status_code=int(resp.status_code), detail=body.get("detail") if isinstance(body, dict) else body)
    return body if isinstance(body, dict) else {"status": "success", "data": body}


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


async def get_agent_registry() -> AgentRegistry:
    from bff.main import get_agent_registry as _get_agent_registry

    return await _get_agent_registry()


async def get_agent_plan_registry() -> AgentPlanRegistry:
    from bff.main import get_agent_plan_registry as _get_agent_plan_registry

    return await _get_agent_plan_registry()


class AgentPlanApprovalRequest(BaseModel):
    decision: str = Field(..., min_length=1, max_length=40)
    step_id: str | None = Field(default=None, max_length=200)
    comment: str | None = Field(default=None, max_length=2000)
    metadata: dict | None = Field(default=None)


class AgentPlanCompileRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: AgentPlanDataScope | None = Field(default=None)
    answers: dict | None = Field(default=None, description="Clarification answers from prior compile response")


@router.post("/compile", response_model=ApiResponse)
@rate_limit(**RateLimitPresets.STRICT)
async def compile_plan(
    body: AgentPlanCompileRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    goal = str(payload.get("goal") or "").strip()
    data_scope = body.data_scope
    answers = payload.get("answers")
    actor = (
        request.headers.get("X-User-ID")
        or request.headers.get("X-User")
        or request.headers.get("X-Actor")
        or "system"
    )

    result = await compile_agent_plan(
        goal=goal,
        data_scope=data_scope,
        answers=answers if isinstance(answers, dict) else None,
        actor=str(actor),
        tool_registry=tool_registry,
        llm_gateway=llm,
        redis_service=redis_service,
        audit_store=audit_store,
    )

    response_data = {
        "status": result.status,
        "plan_id": result.plan_id,
        "plan": result.plan.model_dump(mode="json") if result.plan else None,
        "validation_errors": list(result.validation_errors or []),
        "validation_warnings": list(result.validation_warnings or []),
        "questions": [q.model_dump(mode="json") for q in (result.questions or [])],
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

    if result.status == "success":
        if result.plan:
            await plan_registry.upsert_plan(
                plan_id=result.plan_id,
                status="COMPILED",
                goal=str(result.plan.goal or ""),
                risk_level=str(result.plan.risk_level.value if hasattr(result.plan.risk_level, "value") else result.plan.risk_level),
                requires_approval=bool(result.plan.requires_approval),
                plan=result.plan.model_dump(mode="json"),
                created_by=str(actor),
            )
        return ApiResponse.success(message="Agent plan compiled", data=response_data)
    if result.status == "clarification_required":
        return ApiResponse.warning(message="Clarification required", data=response_data)

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"errors": result.validation_errors, "warnings": result.validation_warnings},
    )


@router.get("/{plan_id}", response_model=ApiResponse)
async def get_plan(
    plan_id: str,
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    record = await plan_registry.get_plan(plan_id=plan_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    return ApiResponse.success(
        message="Agent plan retrieved",
        data={
            "plan": record.plan,
            "plan_id": record.plan_id,
            "plan_digest": record.plan_digest,
            "status": record.status,
            "created_by": record.created_by,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        },
    )


@router.post("/{plan_id}/preview", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def preview_plan(
    plan_id: str,
    request: Request,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    record = await plan_registry.get_plan(plan_id=plan_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    steps_payload: List[Dict[str, Any]] = []
    for step in validation.plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            break
        path = _render_path(policy.path, step.path_params)
        if not _is_preview_safe_step(step.method or policy.method, path, policy.risk_level):
            break
        headers: Dict[str, str] = {}
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(validation.plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
            }
        )

    if not steps_payload:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No preview-safe steps found in plan")

    run_payload = {
        "goal": validation.plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan_id,
            "risk_level": str(validation.plan.risk_level.value if hasattr(validation.plan.risk_level, "value") else validation.plan.risk_level),
            "plan_snapshot": validation.plan.model_dump(mode="json"),
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    return ApiResponse.accepted(message="Agent plan preview started", data=agent_resp.get("data") if isinstance(agent_resp, dict) else None)


@router.post("/{plan_id}/execute", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def execute_plan(
    plan_id: str,
    request: Request,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    try:
        plan_id = str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    record = await plan_registry.get_plan(plan_id=plan_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    if validation.plan.requires_approval:
        approvals = await agent_registry.list_approvals(plan_id=plan_id)
        if not approvals:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")
        latest = max(approvals, key=lambda a: a.approved_at)
        decision = str(latest.decision or "").strip().upper()
        if decision in _REJECTED_DECISIONS or decision not in _APPROVED_DECISIONS:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")

    steps_payload: List[Dict[str, Any]] = []
    for step in validation.plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not in allowlist: {step.tool_id}")
        path = _render_path(policy.path, step.path_params)
        headers: Dict[str, str] = {}
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(validation.plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
            }
        )

    run_payload = {
        "goal": validation.plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan_id,
            "risk_level": str(validation.plan.risk_level.value if hasattr(validation.plan.risk_level, "value") else validation.plan.risk_level),
            "plan_snapshot": validation.plan.model_dump(mode="json"),
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    return ApiResponse.accepted(message="Agent plan execution started", data=agent_resp.get("data") if isinstance(agent_resp, dict) else None)


@router.post("/validate", response_model=ApiResponse)
async def validate_plan(
    plan: AgentPlan,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    try:
        result = await validate_agent_plan(plan=plan, tool_registry=tool_registry)
        if result.errors:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": result.errors, "warnings": result.warnings},
            )
        message = "Agent plan validated"
        data = {
            "plan": result.plan.model_dump(mode="json"),
            "warnings": result.warnings,
        }
        if result.warnings:
            return ApiResponse.warning(message=message, data=data)
        return ApiResponse.success(message=message, data=data)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to validate agent plan: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/{plan_id}/approvals", response_model=ApiResponse)
async def approve_plan(
    plan_id: str,
    request: Request,
    body: AgentPlanApprovalRequest,
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    try:
        try:
            plan_id = str(UUID(plan_id))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc
        payload = sanitize_input(body.model_dump(exclude_none=True))
        decision = str(payload.get("decision") or "").strip().upper()
        if not decision:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="decision is required")

        approved_by = (
            request.headers.get("X-User-ID")
            or request.headers.get("X-User")
            or request.headers.get("X-Actor")
            or "system"
        )
        record = await agent_registry.create_approval(
            approval_id=str(uuid4()),
            plan_id=plan_id,
            step_id=payload.get("step_id"),
            decision=decision,
            approved_by=str(approved_by),
            approved_at=datetime.now(timezone.utc),
            comment=payload.get("comment"),
            metadata=payload.get("metadata") or {},
        )
        return ApiResponse.created(
            message="Agent plan approval recorded",
            data={
                "approval": {
                    "approval_id": record.approval_id,
                    "plan_id": record.plan_id,
                    "step_id": record.step_id,
                    "decision": record.decision,
                    "approved_by": record.approved_by,
                    "approved_at": record.approved_at.isoformat(),
                    "comment": record.comment,
                    "metadata": record.metadata,
                }
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to record agent plan approval: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
