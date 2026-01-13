"""
Agent session APIs (BFF).

These endpoints provide the enterprise-grade session boundary required by AGENT_PRD:
- session CRUD
- message persistence
- job lifecycle + status queries
"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from bff.services.agent_plan_compiler import compile_agent_plan
from bff.services.agent_plan_validation import validate_agent_plan
from shared.config.service_config import ServiceConfig
from shared.config.settings import get_settings
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep
from shared.models.agent_plan import AgentPlan, AgentPlanDataScope
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.agent_plan_registry import AgentPlanRegistry
from shared.services.agent_registry import AgentRegistry
from shared.services.agent_session_registry import AgentSessionRegistry
from shared.services.agent_tool_registry import AgentToolRegistry
from shared.services.llm_gateway import LLMUnavailableError
from shared.utils.llm_safety import digest_for_audit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-sessions", tags=["Agent Sessions"])

_APPROVED_DECISIONS = {"APPROVED", "APPROVE", "ALLOW", "YES"}
_REJECTED_DECISIONS = {"REJECTED", "REJECT", "DENY", "NO"}


class AgentSessionCreateRequest(BaseModel):
    selected_model: str | None = Field(default=None, max_length=200)
    enabled_tools: list[str] | None = Field(default=None, description="Optional tool_id allowlist for this session")
    metadata: dict | None = Field(default=None)


class AgentSessionMessageRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=10_000)
    data_scope: AgentPlanDataScope | None = Field(default=None)
    execute: bool = Field(default=True, description="If true, compile a plan and start a job immediately")
    answers: dict | None = Field(default=None, description="Clarification answers when re-compiling")


class AgentSessionJobCreateRequest(BaseModel):
    plan_id: str = Field(..., description="Agent plan id (UUID)")


class AgentSessionSummarizeRequest(BaseModel):
    max_messages: int = Field(default=200, ge=1, le=2000)


class AgentSessionRemoveMessagesRequest(BaseModel):
    message_ids: list[str] | None = Field(default=None, description="Explicit message UUIDs to remove")
    start_message_id: str | None = Field(default=None, description="Remove inclusive range (by created_at order)")
    end_message_id: str | None = Field(default=None, description="Remove inclusive range (by created_at order)")
    reason: str | None = Field(default=None, max_length=500)


def _resolve_verified_principal(request: Request) -> tuple[str, str, str]:
    user = getattr(request.state, "user", None)
    if user is None or not getattr(user, "verified", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User JWT required")
    tenant_id = getattr(user, "tenant_id", None) or getattr(user, "org_id", None) or "default"
    user_id = str(getattr(user, "id", "") or "").strip() or "unknown"
    actor = f"{getattr(user, 'type', 'user')}:{user_id}"
    return str(tenant_id), user_id, actor


def _forward_headers_to_agent(request: Request) -> Dict[str, str]:
    allowlist = {
        "accept",
        "accept-language",
        "authorization",
        "content-type",
        "user-agent",
        "x-admin-token",
        "x-delegated-authorization",
        "x-request-id",
        "x-user",
        "x-user-id",
        "x-user-type",
        "x-principal-id",
        "x-principal-type",
        "x-actor",
        "x-actor-type",
    }
    output: Dict[str, str] = {}
    for key, value in request.headers.items():
        if not value:
            continue
        if key.lower() in allowlist:
            output[key] = value
    output.setdefault("X-Spice-Caller", "bff")
    return output


async def _call_agent_create_run(*, request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
    agent_url = ServiceConfig.get_agent_url().rstrip("/")
    url = f"{agent_url}/api/v1/agent/runs"
    timeout_seconds = get_settings().clients.agent_proxy_timeout_seconds
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


async def get_agent_session_registry() -> AgentSessionRegistry:
    from bff.main import get_agent_session_registry as _get_agent_session_registry

    return await _get_agent_session_registry()


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


async def get_agent_plan_registry() -> AgentPlanRegistry:
    from bff.main import get_agent_plan_registry as _get_agent_plan_registry

    return await _get_agent_plan_registry()


async def get_agent_registry() -> AgentRegistry:
    from bff.main import get_agent_registry as _get_agent_registry

    return await _get_agent_registry()


@router.post("", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_session(
    body: AgentSessionCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    payload = sanitize_input(body.model_dump(exclude_none=True))
    record = await sessions.create_session(
        session_id=str(uuid4()),
        tenant_id=tenant_id,
        created_by=user_id,
        status="ACTIVE",
        selected_model=(payload.get("selected_model") or None),
        enabled_tools=payload.get("enabled_tools") or None,
        metadata=payload.get("metadata") or {},
        started_at=datetime.now(timezone.utc),
    )
    return ApiResponse.created(
        message="Agent session created",
        data={
            "session": {
                "session_id": record.session_id,
                "tenant_id": record.tenant_id,
                "created_by": record.created_by,
                "status": record.status,
                "selected_model": record.selected_model,
                "enabled_tools": record.enabled_tools,
                "started_at": record.started_at.isoformat(),
                "terminated_at": record.terminated_at.isoformat() if record.terminated_at else None,
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
                "metadata": record.metadata,
            }
        },
    )


@router.get("", response_model=ApiResponse)
async def list_sessions(
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    records = await sessions.list_sessions(tenant_id=tenant_id, created_by=user_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="Agent sessions fetched",
        data={
            "count": len(records),
            "sessions": [
                {
                    "session_id": r.session_id,
                    "status": r.status,
                    "selected_model": r.selected_model,
                    "enabled_tools": r.enabled_tools,
                    "started_at": r.started_at.isoformat(),
                    "terminated_at": r.terminated_at.isoformat() if r.terminated_at else None,
                    "created_at": r.created_at.isoformat(),
                    "updated_at": r.updated_at.isoformat(),
                }
                for r in records
            ],
        },
    )


@router.get("/{session_id}", response_model=ApiResponse)
async def get_session(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    include_messages: bool = Query(False),
    messages_limit: int = Query(200, ge=1, le=500),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    messages_payload = None
    if include_messages:
        messages = await sessions.list_messages(session_id=session_id, tenant_id=tenant_id, limit=messages_limit)
        messages_payload = [
            {
                "message_id": m.message_id,
                "role": m.role,
                "content": m.content,
                "content_digest": m.content_digest,
                "token_count": m.token_count,
                "cost_estimate": m.cost_estimate,
                "latency_ms": m.latency_ms,
                "metadata": m.metadata,
                "created_at": m.created_at.isoformat(),
            }
            for m in messages
        ]

    return ApiResponse.success(
        message="Agent session fetched",
        data={
            "session": {
                "session_id": record.session_id,
                "tenant_id": record.tenant_id,
                "created_by": record.created_by,
                "status": record.status,
                "selected_model": record.selected_model,
                "enabled_tools": record.enabled_tools,
                "summary": record.summary,
                "metadata": record.metadata,
                "started_at": record.started_at.isoformat(),
                "terminated_at": record.terminated_at.isoformat() if record.terminated_at else None,
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
            },
            "messages": messages_payload,
        },
    )


@router.delete("/{session_id}", response_model=ApiResponse)
async def terminate_session(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.update_session(
        session_id=session_id,
        tenant_id=tenant_id,
        status="TERMINATED",
        terminated_at=datetime.now(timezone.utc),
    )
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    return ApiResponse.success(message="Agent session terminated", data={"session_id": record.session_id, "status": record.status})


@router.post("/{session_id}/summarize", response_model=ApiResponse)
async def summarize_session(
    session_id: str,
    body: AgentSessionSummarizeRequest,
    request: Request,
    llm: LLMGatewayDep,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    max_messages = int(payload.get("max_messages") or 200)
    messages = await sessions.list_messages(
        session_id=session_id,
        tenant_id=tenant_id,
        limit=max_messages,
        offset=0,
        include_removed=False,
    )
    if not messages:
        return ApiResponse.success(message="No messages to summarize", data={"session_id": session_id, "summary": record.summary})

    transcript = "\n".join([f"{m.role}: {m.content}" for m in messages if m.content])

    class _SummaryEnvelope(BaseModel):
        summary: str = Field(..., min_length=1, max_length=10_000)
        key_points: list[str] = Field(default_factory=list)

    system_prompt = (
        "You are a STRICT session summarizer.\n"
        "Return a single JSON object only.\n"
        "Do not include any tool calls.\n"
        "Summarize the conversation for enterprise audit and future context.\n"
        "Focus on: user goal, constraints, decisions, and current progress.\n"
    )
    user_prompt = f"Session transcript:\n{transcript}\n"

    started = datetime.now(timezone.utc)
    try:
        summary_obj, meta = await llm.complete_json(
            task="SESSION_SUMMARY",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=_SummaryEnvelope,
            audit_store=audit_store,
            audit_partition_key=f"agent_session:{session_id}",
            audit_actor=actor,
            audit_resource_id=session_id,
            audit_metadata={"tenant_id": tenant_id, "session_id": session_id},
        )
        latency_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        summary_text = str(summary_obj.summary or "").strip()
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, summary=summary_text)
        await sessions.add_message(
            message_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            role="system",
            content=summary_text,
            content_digest=digest_for_audit({"summary": summary_text}),
            latency_ms=latency_ms,
            metadata={
                "kind": "session_summary",
                "provider": meta.provider,
                "model": meta.model,
                "cache_hit": meta.cache_hit,
            },
            created_at=datetime.now(timezone.utc),
        )
        return ApiResponse.success(
            message="Session summarized",
            data={
                "session_id": session_id,
                "summary": summary_text,
                "key_points": list(summary_obj.key_points or []),
                "llm": meta.__dict__,
            },
        )
    except LLMUnavailableError as exc:
        summary_text = (transcript[:4000] + "…") if len(transcript) > 4000 else transcript
        summary_text = summary_text.strip() or "No content to summarize"
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, summary=summary_text)
        await sessions.add_message(
            message_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            role="system",
            content=summary_text,
            content_digest=digest_for_audit({"summary": summary_text}),
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
            metadata={"kind": "session_summary", "provider": "fallback", "error": str(exc)},
            created_at=datetime.now(timezone.utc),
        )
        return ApiResponse.warning(
            message="LLM unavailable; stored heuristic summary",
            data={"session_id": session_id, "summary": summary_text, "llm_error": str(exc)},
        )
    except Exception as exc:
        try:
            await audit_store.log(
                partition_key=f"agent_session:{session_id}",
                actor=actor,
                action="SESSION_SUMMARY",
                status="failure",
                resource_type="agent_session",
                resource_id=session_id,
                metadata={"tenant_id": tenant_id, "session_id": session_id, "error": str(exc)},
                error=str(exc),
            )
        except Exception:
            pass
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to summarize session") from exc


@router.post("/{session_id}/messages/remove", response_model=ApiResponse)
async def remove_messages(
    session_id: str,
    body: AgentSessionRemoveMessagesRequest,
    request: Request,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    message_ids = payload.get("message_ids")
    start_id = payload.get("start_message_id")
    end_id = payload.get("end_message_id")
    reason = str(payload.get("reason") or "").strip() or None

    selected_ids: list[str] = []
    if message_ids:
        selected_ids = [str(mid) for mid in message_ids]
    elif start_id and end_id:
        try:
            start_uuid = str(UUID(str(start_id)))
            end_uuid = str(UUID(str(end_id)))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start/end_message_id must be UUID") from exc
        all_messages = await sessions.list_messages(
            session_id=session_id,
            tenant_id=tenant_id,
            limit=2000,
            offset=0,
            include_removed=True,
        )
        ids_in_order = [m.message_id for m in all_messages]
        if start_uuid not in ids_in_order or end_uuid not in ids_in_order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="start/end_message_id not found")
        start_idx = ids_in_order.index(start_uuid)
        end_idx = ids_in_order.index(end_uuid)
        lo, hi = sorted((start_idx, end_idx))
        selected_ids = ids_in_order[lo : hi + 1]
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="message_ids or start/end_message_id required")

    normalized_ids: list[str] = []
    for mid in selected_ids:
        try:
            normalized_ids.append(str(UUID(str(mid))))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="message_ids must be UUIDs") from exc

    to_audit = await sessions.get_messages_by_ids(
        session_id=session_id,
        tenant_id=tenant_id,
        message_ids=normalized_ids,
        include_removed=True,
    )
    audit_payload = [{"message_id": m.message_id, "digest": m.content_digest, "role": m.role} for m in to_audit]
    removed_count = await sessions.mark_messages_removed(
        session_id=session_id,
        tenant_id=tenant_id,
        message_ids=normalized_ids,
        removed_by=user_id,
        removed_reason=reason,
        removed_at=datetime.now(timezone.utc),
    )

    with contextlib.suppress(Exception):
        await audit_store.log(
            partition_key=f"agent_session:{session_id}",
            actor=actor,
            action="SESSION_MESSAGES_REMOVE",
            status="success",
            resource_type="agent_session",
            resource_id=session_id,
            metadata={
                "tenant_id": tenant_id,
                "session_id": session_id,
                "removed_by": user_id,
                "reason": reason,
                "removed_count": removed_count,
                "messages": audit_payload,
            },
        )

    return ApiResponse.success(
        message="Messages removed",
        data={"session_id": session_id, "removed_count": removed_count, "message_ids": normalized_ids},
    )


@router.post("/{session_id}/messages", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def post_message(
    session_id: str,
    body: AgentSessionMessageRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    payload = sanitize_input(body.model_dump(exclude_none=True))
    content = str(payload.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="content is required")

    await sessions.add_message(
        message_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        role="user",
        content=content,
        content_digest=digest_for_audit({"content": content}),
        metadata={"kind": "user_message"},
        created_at=datetime.now(timezone.utc),
    )

    if not bool(payload.get("execute", True)):
        return ApiResponse.accepted(message="Message recorded", data={"session_id": session_id})

    compile_started = datetime.now(timezone.utc)
    compile_result = await compile_agent_plan(
        goal=content,
        data_scope=payload.get("data_scope"),
        answers=payload.get("answers"),
        context_pack=None,
        actor=actor,
        tool_registry=tool_registry,
        llm_gateway=llm,
        redis_service=redis_service,
        audit_store=audit_store,
    )
    compile_latency_ms = int((datetime.now(timezone.utc) - compile_started).total_seconds() * 1000)

    if compile_result.status == "clarification_required":
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ACTIVE")
        return ApiResponse.warning(
            message="Clarification required",
            data={
                "session_id": session_id,
                "plan_id": compile_result.plan_id,
                "questions": compile_result.questions,
                "validation_errors": compile_result.validation_errors,
                "llm_meta": compile_result.llm_meta.model_dump(mode="json") if compile_result.llm_meta else None,
            },
        )
    if compile_result.status != "success" or not compile_result.plan:
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ERROR")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to compile agent plan")

    plan = compile_result.plan
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry)
    status_name = "COMPILED" if not validation.errors else "DRAFT"
    await plan_registry.upsert_plan(
        plan_id=plan.plan_id,
        status=status_name,
        goal=str(plan.goal or ""),
        risk_level=str(plan.risk_level.value if hasattr(plan.risk_level, "value") else plan.risk_level),
        requires_approval=bool(plan.requires_approval),
        plan=plan.model_dump(mode="json"),
        created_by=user_id,
    )

    job_id = str(uuid4())
    await sessions.create_job(
        job_id=job_id,
        session_id=session_id,
        tenant_id=tenant_id,
        plan_id=plan.plan_id,
        status="WAITING_APPROVAL" if plan.requires_approval else "PENDING",
        metadata={"kind": "agent_plan_job", "compile_latency_ms": compile_latency_ms},
    )

    if plan.requires_approval:
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="WAITING_APPROVAL")
        return ApiResponse.accepted(
            message="Plan compiled; approval required",
            data={
                "session_id": session_id,
                "job_id": job_id,
                "plan_id": plan.plan_id,
                "status": "WAITING_APPROVAL",
                "validation_warnings": validation.warnings,
                "compilation_report": validation.compilation_report.model_dump(mode="json"),
            },
        )

    # Auto-execute when approval is not required.
    steps_payload: list[dict[str, Any]] = []
    for step in plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not in allowlist: {step.tool_id}")
        rendered_path = str(policy.path or "")
        for key, value in (step.path_params or {}).items():
            token = "{" + str(key) + "}"
            if token in rendered_path and value not in (None, ""):
                rendered_path = rendered_path.replace(token, str(value))
        headers: Dict[str, str] = {}
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": rendered_path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    run_payload = {
        "goal": plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan.plan_id,
            "risk_level": str(plan.risk_level.value if hasattr(plan.risk_level, "value") else plan.risk_level),
            "plan_snapshot": plan.model_dump(mode="json"),
            "session_id": session_id,
            "job_id": job_id,
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    run_data = agent_resp.get("data") if isinstance(agent_resp, dict) else None
    run_id = (run_data or {}).get("run_id") if isinstance(run_data, dict) else None

    await sessions.update_job(job_id=job_id, tenant_id=tenant_id, status="RUNNING", run_id=run_id, error=None)
    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="RUNNING_TOOL")

    return ApiResponse.accepted(
        message="Agent job started",
        data={
            "session_id": session_id,
            "job_id": job_id,
            "plan_id": plan.plan_id,
            "run_id": run_id,
            "agent": run_data,
        },
    )


@router.post("/{session_id}/jobs", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job_from_plan(
    session_id: str,
    body: AgentSessionJobCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    payload = sanitize_input(body.model_dump(exclude_none=True))
    try:
        plan_id = str(UUID(str(payload.get("plan_id"))))
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

    steps_payload: list[dict[str, Any]] = []
    for step in validation.plan.steps:
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not in allowlist: {step.tool_id}")
        rendered_path = str(policy.path or "")
        for key, value in (step.path_params or {}).items():
            token = "{" + str(key) + "}"
            if token in rendered_path and value not in (None, ""):
                rendered_path = rendered_path.replace(token, str(value))
        headers: Dict[str, str] = {}
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": rendered_path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(validation.plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    job_id = str(uuid4())
    await sessions.create_job(
        job_id=job_id,
        session_id=session_id,
        tenant_id=tenant_id,
        plan_id=plan_id,
        status="PENDING",
        metadata={"kind": "agent_plan_job"},
    )

    run_payload = {
        "goal": validation.plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan_id,
            "risk_level": str(validation.plan.risk_level.value if hasattr(validation.plan.risk_level, "value") else validation.plan.risk_level),
            "plan_snapshot": validation.plan.model_dump(mode="json"),
            "session_id": session_id,
            "job_id": job_id,
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    run_data = agent_resp.get("data") if isinstance(agent_resp, dict) else None
    run_id = (run_data or {}).get("run_id") if isinstance(run_data, dict) else None

    await sessions.update_job(job_id=job_id, tenant_id=tenant_id, status="RUNNING", run_id=run_id, error=None)
    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="RUNNING_TOOL")

    return ApiResponse.accepted(
        message="Agent session job started",
        data={"session_id": session_id, "job_id": job_id, "plan_id": plan_id, "run_id": run_id, "agent": run_data},
    )


@router.get("/{session_id}/jobs", response_model=ApiResponse)
async def list_jobs(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc
    jobs = await sessions.list_jobs(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="Agent jobs fetched",
        data={
            "session_id": session_id,
            "count": len(jobs),
            "jobs": [
                {
                    "job_id": j.job_id,
                    "plan_id": j.plan_id,
                    "run_id": j.run_id,
                    "status": j.status,
                    "error": j.error,
                    "created_at": j.created_at.isoformat(),
                    "updated_at": j.updated_at.isoformat(),
                    "finished_at": j.finished_at.isoformat() if j.finished_at else None,
                    "metadata": j.metadata,
                }
                for j in jobs
            ],
        },
    )


@router.get("/{session_id}/jobs/{job_id}", response_model=ApiResponse)
async def get_job(
    session_id: str,
    job_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
        job_uuid = str(UUID(job_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id/job_id must be UUID") from exc

    job = await sessions.get_job(job_id=job_uuid, tenant_id=tenant_id)
    if not job or job.session_id != session_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return ApiResponse.success(
        message="Agent job fetched",
        data={
            "job": {
                "job_id": job.job_id,
                "session_id": job.session_id,
                "plan_id": job.plan_id,
                "run_id": job.run_id,
                "status": job.status,
                "error": job.error,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                "metadata": job.metadata,
            }
        },
    )
