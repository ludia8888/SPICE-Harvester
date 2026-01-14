"""
Agent session APIs (BFF).

These endpoints provide the enterprise-grade session boundary required by AGENT_PRD:
- session CRUD
- message persistence
- job lifecycle + status queries
"""

from __future__ import annotations

import contextlib
import json
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
from shared.services.agent_policy_registry import AgentPolicyRegistry
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


class AgentSessionContextItemCreateRequest(BaseModel):
    item_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="dataset|ontology|function|branch|proposal|pr|document_bundle|file_upload|custom",
    )
    include_mode: str = Field(default="summary", description="full|summary|search")
    ref: dict = Field(default_factory=dict, description="Type-specific reference payload (JSON)")
    token_count: int | None = Field(default=None, ge=0)
    metadata: dict | None = Field(default=None)


class AgentSessionApprovalDecisionRequest(BaseModel):
    decision: str = Field(..., min_length=1, max_length=40)
    comment: str | None = Field(default=None, max_length=2000)
    metadata: dict | None = Field(default=None)


def _approx_token_count(payload: Any) -> int:
    try:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        text = str(payload)
    # Very rough heuristic: ~4 chars/token for English-ish payloads.
    return max(1, int((len(text) + 3) / 4))


def _plan_risk_level(plan: AgentPlan) -> str:
    risk = getattr(plan.risk_level, "value", plan.risk_level)
    return str(risk or "read").strip().lower() or "read"


def _truncate_preview(value: Any, *, max_chars: int = 1200) -> Optional[str]:
    if value is None:
        return None
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        text = str(value)
    text = text.strip()
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)] + "…"


def _build_approval_request_payload(*, plan: AgentPlan, validation: Any) -> Dict[str, Any]:
    compilation_report = getattr(validation, "compilation_report", None)
    required_controls = getattr(compilation_report, "required_controls", None) if compilation_report else None
    diagnostics = getattr(compilation_report, "diagnostics", None) if compilation_report else None
    scope = plan.data_scope.model_dump(mode="json")

    required_controls_values = [str(getattr(c, "value", c)) for c in (required_controls or [])]
    preview_controls = {"simulate_first", "pipeline_simulate_first"}
    preview_suggested = bool(set(required_controls_values) & preview_controls)
    preview_tools = {"actions.simulate", "pipelines.preview", "pipelines.simulate_definition"}
    preview_present = any(step.tool_id in preview_tools or step.tool_id.endswith(".simulate") for step in plan.steps)

    steps_summary: list[dict[str, Any]] = []
    for step in plan.steps:
        steps_summary.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "method": step.method,
                "path_params": step.path_params,
                "query": step.query,
                "body_preview": _truncate_preview(step.body),
                "body_digest": digest_for_audit(step.body) if step.body is not None else None,
                "requires_approval": bool(step.requires_approval),
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    scope_hints: dict[str, Any] = {
        "db_name": scope.get("db_name"),
        "branch": scope.get("branch"),
        "dataset_id": scope.get("dataset_id"),
        "pipeline_id": scope.get("pipeline_id"),
    }
    for step in plan.steps:
        params = step.path_params or {}
        for key in ("db_name", "branch", "dataset_id", "dataset_version_id", "pipeline_id", "action_type_id"):
            value = params.get(key)
            if value not in (None, ""):
                scope_hints.setdefault(key, value)

    payload: Dict[str, Any] = {
        "plan_id": plan.plan_id,
        "goal": plan.goal,
        "risk_level": _plan_risk_level(plan),
        "requires_approval": bool(plan.requires_approval),
        "data_scope": scope,
        "change_scope": scope_hints,
        "rollback": {
            "possible": _plan_risk_level(plan) != "destructive",
            "suggestion": "Prefer branch/proposal isolation and keep idempotency keys for safe replays.",
        },
        "preview": {
            "suggested": preview_suggested,
            "present_in_plan": preview_present,
            "hint": "For risky writes, run simulate/preview first when available.",
        },
        "steps": steps_summary,
        "validation_warnings": list(getattr(validation, "warnings", []) or []),
        "required_controls": required_controls_values,
        "diagnostics": [d.model_dump(mode="json") for d in (diagnostics or [])] if diagnostics else [],
    }
    if compilation_report is not None:
        payload["compilation_report"] = compilation_report.model_dump(mode="json")
    return payload


def _should_auto_approve(*, plan: AgentPlan, policy: Any) -> bool:
    if policy is None:
        return False
    rules = getattr(policy, "auto_approve_rules", None) or {}
    if not isinstance(rules, dict):
        return False
    if not bool(rules.get("enabled")):
        return False

    allow_risks = {str(r).strip().lower() for r in (rules.get("allow_risk_levels") or []) if str(r).strip()}
    if not allow_risks:
        allow_risks = {"write"}

    risk = _plan_risk_level(plan)
    if risk not in allow_risks:
        return False
    if risk in {"admin", "destructive"}:
        return False

    allow_tool_ids = {str(t).strip() for t in (rules.get("allow_tool_ids") or []) if str(t).strip()}
    deny_tool_ids = {str(t).strip() for t in (rules.get("deny_tool_ids") or []) if str(t).strip()}
    if not allow_tool_ids:
        return False

    allow_branches = {str(b).strip() for b in (rules.get("allow_branches") or []) if str(b).strip()}
    if allow_branches:
        branch = str(plan.data_scope.branch or "").strip() or "main"
        if branch not in allow_branches:
            return False

    for step in plan.steps:
        if step.tool_id in deny_tool_ids:
            return False
        if step.tool_id not in allow_tool_ids:
            return False
    return True


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
        "x-org-id",
        "x-tenant-id",
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


async def _start_agent_job_run(
    *,
    request: Request,
    sessions: AgentSessionRegistry,
    tool_registry: AgentToolRegistry,
    tenant_id: str,
    session_id: str,
    job_id: str,
    plan: AgentPlan,
    allowed_tool_ids: Optional[list[str]],
) -> Dict[str, Any]:
    allowed_set = set(allowed_tool_ids or []) if allowed_tool_ids is not None else None
    steps_payload: list[dict[str, Any]] = []
    for step in plan.steps:
        if allowed_set is not None and step.tool_id not in allowed_set:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"tool_id not enabled: {step.tool_id}")
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

    return {"run_id": run_id, "agent": run_data, "payload": run_payload}


async def _best_effort_reconcile_job(
    *,
    job: Any,
    tenant_id: str,
    sessions: AgentSessionRegistry,
    agent_registry: AgentRegistry,
) -> Any:
    run_id = getattr(job, "run_id", None)
    status_value = str(getattr(job, "status", "") or "").strip().upper()
    if not run_id or status_value not in {"RUNNING", "PENDING"}:
        return job
    try:
        run = await agent_registry.get_run(run_id=str(run_id), tenant_id=tenant_id)
    except Exception:
        return job
    if not run:
        return job
    run_status = str(run.status or "").strip().upper()
    if run_status not in {"COMPLETED", "FAILED"}:
        return job
    finished_at = run.finished_at or datetime.now(timezone.utc)
    updated = await sessions.update_job(
        job_id=str(getattr(job, "job_id")),
        tenant_id=tenant_id,
        status="COMPLETED" if run_status == "COMPLETED" else "FAILED",
        finished_at=finished_at,
    )
    return updated or job


async def get_agent_session_registry() -> AgentSessionRegistry:
    from bff.main import get_agent_session_registry as _get_agent_session_registry

    return await _get_agent_session_registry()


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


async def get_agent_plan_registry() -> AgentPlanRegistry:
    from bff.main import get_agent_plan_registry as _get_agent_plan_registry

    return await _get_agent_plan_registry()


async def get_agent_registry() -> AgentRegistry:
    from bff.main import get_agent_registry as _get_agent_registry

    return await _get_agent_registry()


@router.post("/{session_id}/context/items", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def attach_context_item(
    session_id: str,
    body: AgentSessionContextItemCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    item_type = str(payload.get("item_type") or "").strip()
    include_mode = str(payload.get("include_mode") or "summary").strip().lower() or "summary"
    ref = payload.get("ref") if isinstance(payload.get("ref"), dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    token_count = payload.get("token_count")
    if token_count is None:
        token_count = _approx_token_count(ref)

    record = await sessions.add_context_item(
        item_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        item_type=item_type,
        include_mode=include_mode,
        ref=ref,
        token_count=token_count,
        metadata=metadata,
        created_at=datetime.now(timezone.utc),
    )
    return ApiResponse.created(
        message="Context item attached",
        data={
            "context_item": {
                "item_id": record.item_id,
                "session_id": record.session_id,
                "item_type": record.item_type,
                "include_mode": record.include_mode,
                "ref": record.ref,
                "token_count": record.token_count,
                "metadata": record.metadata,
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
            }
        },
    )


@router.get("/{session_id}/context/items", response_model=ApiResponse)
async def list_context_items(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="Context items fetched",
        data={
            "session_id": session_id,
            "count": len(items),
            "context_items": [
                {
                    "item_id": item.item_id,
                    "session_id": item.session_id,
                    "item_type": item.item_type,
                    "include_mode": item.include_mode,
                    "ref": item.ref,
                    "token_count": item.token_count,
                    "metadata": item.metadata,
                    "created_at": item.created_at.isoformat(),
                    "updated_at": item.updated_at.isoformat(),
                }
                for item in items
            ],
        },
    )


@router.delete("/{session_id}/context/items/{item_id}", response_model=ApiResponse)
async def remove_context_item(
    session_id: str,
    item_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
        item_uuid = str(UUID(item_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id/item_id must be UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    removed = await sessions.remove_context_item(session_id=session_id, tenant_id=tenant_id, item_id=item_uuid)
    if removed <= 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Context item not found")
    return ApiResponse.success(message="Context item removed", data={"session_id": session_id, "item_id": item_uuid})


@router.post("", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_session(
    body: AgentSessionCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    payload = sanitize_input(body.model_dump(exclude_none=True))
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)

    allowed_models: set[str] = set()
    if policy:
        allowed_models = {str(m).strip() for m in (policy.allowed_models or []) if str(m).strip()}
        if not allowed_models and policy.default_model:
            allowed_models = {str(policy.default_model).strip()}
    allowed_tools: Optional[set[str]] = None
    if policy:
        allowed_tools_set = {str(t).strip() for t in (policy.allowed_tools or []) if str(t).strip()}
        allowed_tools = allowed_tools_set or None

    requested_selected_model = payload.get("selected_model") if "selected_model" in payload else None
    selected_model = (
        str(requested_selected_model).strip()
        if requested_selected_model is not None
        else (policy.default_model if policy else None)
    )
    if selected_model == "":
        selected_model = None
    if selected_model is None and policy and policy.allowed_models:
        selected_model = policy.allowed_models[0]

    model_restricted = bool(policy and (policy.default_model or policy.allowed_models))
    if model_restricted and selected_model and allowed_models and selected_model not in allowed_models:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Model not allowed for tenant")

    enabled_tools = payload.get("enabled_tools") if "enabled_tools" in payload else None
    if enabled_tools is None and policy is not None and allowed_tools is not None:
        enabled_tools = list(allowed_tools)
    tools_restricted = bool(enabled_tools is not None)

    if enabled_tools is not None:
        enabled_tools = [str(tool_id).strip() for tool_id in enabled_tools if str(tool_id).strip()]
        if allowed_tools is not None and any(tool_id not in allowed_tools for tool_id in enabled_tools):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tool not allowed for tenant")
        for tool_id in enabled_tools:
            tool_policy = await tool_registry.get_tool_policy(tool_id=tool_id)
            if not tool_policy or str(tool_policy.status or "").strip().upper() != "ACTIVE":
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not ACTIVE: {tool_id}")

    metadata = dict(payload.get("metadata") or {})
    metadata.setdefault("policy_source", "tenant" if policy else "none")
    metadata["tools_restricted"] = tools_restricted
    metadata["model_restricted"] = model_restricted

    record = await sessions.create_session(
        session_id=str(uuid4()),
        tenant_id=tenant_id,
        created_by=user_id,
        status="ACTIVE",
        selected_model=selected_model,
        enabled_tools=enabled_tools,
        metadata=metadata,
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
    include_context_items: bool = Query(False),
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

    context_payload = None
    if include_context_items:
        items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
        context_payload = [
            {
                "item_id": item.item_id,
                "item_type": item.item_type,
                "include_mode": item.include_mode,
                "ref": item.ref,
                "token_count": item.token_count,
                "metadata": item.metadata,
                "created_at": item.created_at.isoformat(),
                "updated_at": item.updated_at.isoformat(),
            }
            for item in items
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
            "context_items": context_payload,
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
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    tools_restricted = bool((session_record.metadata or {}).get("tools_restricted"))
    allowed_tool_ids = list(session_record.enabled_tools or []) if tools_restricted else None
    selected_model = session_record.selected_model

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
        token_count=_approx_token_count(content),
        metadata={"kind": "user_message"},
        created_at=datetime.now(timezone.utc),
    )

    if not bool(payload.get("execute", True)):
        return ApiResponse.accepted(message="Message recorded", data={"session_id": session_id})

    attached_items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
    context_pack = {
        "session": {
            "session_id": session_id,
            "summary": session_record.summary,
            "selected_model": session_record.selected_model,
            "enabled_tools": list(session_record.enabled_tools or []),
        },
        "attached_context": [
            {
                "item_id": item.item_id,
                "item_type": item.item_type,
                "include_mode": item.include_mode,
                "ref": item.ref,
                "token_count": item.token_count,
                "metadata": item.metadata,
            }
            for item in attached_items
        ],
    }

    compile_started = datetime.now(timezone.utc)
    compile_result = await compile_agent_plan(
        goal=content,
        data_scope=payload.get("data_scope"),
        answers=payload.get("answers"),
        context_pack=context_pack,
        actor=actor,
        allowed_tool_ids=allowed_tool_ids,
        selected_model=selected_model,
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
                "llm_meta": dict(compile_result.llm_meta.__dict__) if compile_result.llm_meta else None,
            },
        )
    if compile_result.status != "success" or not compile_result.plan:
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ERROR")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to compile agent plan")

    plan = compile_result.plan
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    status_name = "COMPILED" if not validation.errors else "DRAFT"
    await plan_registry.upsert_plan(
        plan_id=plan.plan_id,
        tenant_id=tenant_id,
        status=status_name,
        goal=str(plan.goal or ""),
        risk_level=str(plan.risk_level.value if hasattr(plan.risk_level, "value") else plan.risk_level),
        requires_approval=bool(plan.requires_approval),
        plan=plan.model_dump(mode="json"),
        created_by=user_id,
    )

    job_metadata: dict[str, Any] = {"kind": "agent_plan_job", "compile_latency_ms": compile_latency_ms}
    if compile_result.llm_meta is not None:
        job_metadata["planner_llm"] = dict(compile_result.llm_meta.__dict__)
    if compile_result.planner_confidence is not None:
        job_metadata["planner_confidence"] = float(compile_result.planner_confidence)
    if compile_result.planner_notes:
        job_metadata["planner_notes"] = list(compile_result.planner_notes)

    approval_request_id: str | None = str(uuid4()) if plan.requires_approval else None
    if approval_request_id is not None:
        job_metadata["approval_request_id"] = approval_request_id

    job_id = str(uuid4())
    await sessions.create_job(
        job_id=job_id,
        session_id=session_id,
        tenant_id=tenant_id,
        plan_id=plan.plan_id,
        status="WAITING_APPROVAL" if plan.requires_approval else "PENDING",
        metadata=job_metadata,
    )

    if plan.requires_approval:
        approval_payload = _build_approval_request_payload(plan=plan, validation=validation)
        if approval_request_id is None:
            approval_request_id = str(uuid4())
        approval_record = await agent_registry.create_approval_request(
            approval_request_id=approval_request_id,
            plan_id=plan.plan_id,
            tenant_id=tenant_id,
            session_id=session_id,
            job_id=job_id,
            status="PENDING",
            risk_level=_plan_risk_level(plan),
            requested_by=user_id,
            requested_at=datetime.now(timezone.utc),
            request_payload=approval_payload,
            metadata={"kind": "agent_plan_approval", "source": "agent_session"},
        )

        policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
        if _should_auto_approve(plan=plan, policy=policy):
            decided_at = datetime.now(timezone.utc)
            await agent_registry.decide_approval_request(
                approval_request_id=approval_request_id,
                tenant_id=tenant_id,
                decision="APPROVED",
                decided_by="policy:auto",
                decided_at=decided_at,
                status="APPROVED",
                comment="Auto-approved by tenant policy",
                metadata={"auto": True},
            )
            await agent_registry.create_approval(
                approval_id=str(uuid4()),
                plan_id=plan.plan_id,
                tenant_id=tenant_id,
                step_id=None,
                decision="APPROVED",
                approved_by="policy:auto",
                approved_at=decided_at,
                comment="Auto-approved by tenant policy",
                metadata={"approval_request_id": approval_request_id, "auto": True},
            )
            started = await _start_agent_job_run(
                request=request,
                sessions=sessions,
                tool_registry=tool_registry,
                tenant_id=tenant_id,
                session_id=session_id,
                job_id=job_id,
                plan=plan,
                allowed_tool_ids=allowed_tool_ids,
            )
            return ApiResponse.accepted(
                message="Plan auto-approved; job started",
                data={
                    "session_id": session_id,
                    "job_id": job_id,
                    "plan_id": plan.plan_id,
                    "run_id": started.get("run_id"),
                    "approval_request_id": approval_request_id,
                    "auto_approved": True,
                    "compilation_report": validation.compilation_report.model_dump(mode="json"),
                    "agent": started.get("agent"),
                },
            )

        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="WAITING_APPROVAL")
        return ApiResponse.accepted(
            message="Plan compiled; approval required",
            data={
                "session_id": session_id,
                "job_id": job_id,
                "plan_id": plan.plan_id,
                "status": "WAITING_APPROVAL",
                "approval_request_id": approval_request_id,
                "approval_request": {
                    "approval_request_id": approval_record.approval_request_id,
                    "plan_id": approval_record.plan_id,
                    "status": approval_record.status,
                    "risk_level": approval_record.risk_level,
                    "requested_by": approval_record.requested_by,
                    "requested_at": approval_record.requested_at.isoformat(),
                    "request_payload": approval_record.request_payload,
                    "metadata": approval_record.metadata,
                },
                "validation_warnings": validation.warnings,
                "compilation_report": validation.compilation_report.model_dump(mode="json"),
            },
        )

    # Auto-execute when approval is not required.
    started = await _start_agent_job_run(
        request=request,
        sessions=sessions,
        tool_registry=tool_registry,
        tenant_id=tenant_id,
        session_id=session_id,
        job_id=job_id,
        plan=plan,
        allowed_tool_ids=allowed_tool_ids,
    )
    return ApiResponse.accepted(
        message="Agent job started",
        data={
            "session_id": session_id,
            "job_id": job_id,
            "plan_id": plan.plan_id,
            "run_id": started.get("run_id"),
            "agent": started.get("agent"),
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

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    tools_restricted = bool((session_record.metadata or {}).get("tools_restricted"))
    allowed_tool_ids = list(session_record.enabled_tools or []) if tools_restricted else None

    payload = sanitize_input(body.model_dump(exclude_none=True))
    try:
        plan_id = str(UUID(str(payload.get("plan_id"))))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    if validation.plan.requires_approval:
        approvals = await agent_registry.list_approvals(plan_id=plan_id, tenant_id=tenant_id)
        if not approvals:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")
        latest = max(approvals, key=lambda a: a.approved_at)
        decision = str(latest.decision or "").strip().upper()
        if decision in _REJECTED_DECISIONS or decision not in _APPROVED_DECISIONS:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")

    steps_payload: list[dict[str, Any]] = []
    for step in validation.plan.steps:
        if allowed_tool_ids is not None and step.tool_id not in set(allowed_tool_ids):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"tool_id not enabled: {step.tool_id}")
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
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc
    jobs = await sessions.list_jobs(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    reconciled = []
    for job in jobs:
        reconciled.append(
            await _best_effort_reconcile_job(job=job, tenant_id=tenant_id, sessions=sessions, agent_registry=agent_registry)
        )
    jobs = reconciled
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
    agent_registry: AgentRegistry = Depends(get_agent_registry),
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

    job = await _best_effort_reconcile_job(job=job, tenant_id=tenant_id, sessions=sessions, agent_registry=agent_registry)
    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if session_record and job and getattr(job, "finished_at", None):
        current_status = str(session_record.status or "").strip().upper()
        if current_status != "TERMINATED":
            normalized = str(getattr(job, "status", "") or "").strip().upper()
            try:
                if normalized == "COMPLETED":
                    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="COMPLETED")
                elif normalized in {"FAILED"}:
                    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ERROR")
            except Exception:
                pass

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


@router.get("/{session_id}/events", response_model=ApiResponse)
async def list_events(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    include_messages: bool = Query(True),
    include_jobs: bool = Query(True),
    include_approvals: bool = Query(True),
    include_agent_steps: bool = Query(True),
    limit: int = Query(500, ge=1, le=2000),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    events: list[dict[str, Any]] = []
    token_total = 0
    cost_total = 0.0

    jobs = []
    if include_jobs or include_agent_steps:
        jobs = await sessions.list_jobs(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
        reconciled = []
        for job in jobs:
            reconciled.append(
                await _best_effort_reconcile_job(job=job, tenant_id=tenant_id, sessions=sessions, agent_registry=agent_registry)
            )
        jobs = reconciled

    if include_messages:
        messages = await sessions.list_messages(session_id=session_id, tenant_id=tenant_id, limit=500, offset=0, include_removed=True)
        for msg in messages:
            if msg.token_count:
                token_total += int(msg.token_count)
            if msg.cost_estimate:
                cost_total += float(msg.cost_estimate)
            events.append(
                {
                    "event_id": msg.message_id,
                    "event_type": "SESSION_MESSAGE",
                    "occurred_at": msg.created_at.isoformat(),
                    "data": {
                        "role": msg.role,
                        "content": msg.content,
                        "content_digest": msg.content_digest,
                        "is_removed": msg.is_removed,
                        "token_count": msg.token_count,
                        "cost_estimate": msg.cost_estimate,
                        "latency_ms": msg.latency_ms,
                        "metadata": msg.metadata,
                    },
                }
            )

    if include_jobs:
        for job in jobs:
            events.append(
                {
                    "event_id": job.job_id,
                    "event_type": "SESSION_JOB",
                    "occurred_at": job.created_at.isoformat(),
                    "data": {
                        "job_id": job.job_id,
                        "plan_id": job.plan_id,
                        "run_id": job.run_id,
                        "status": job.status,
                        "error": job.error,
                        "metadata": job.metadata,
                        "created_at": job.created_at.isoformat(),
                        "updated_at": job.updated_at.isoformat(),
                        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                    },
                }
            )

    if include_approvals:
        approvals = await agent_registry.list_approval_requests(
            tenant_id=tenant_id, session_id=session_id, limit=200, offset=0
        )
        for approval in approvals:
            events.append(
                {
                    "event_id": approval.approval_request_id,
                    "event_type": "APPROVAL_REQUESTED",
                    "occurred_at": approval.requested_at.isoformat(),
                    "data": {
                        "approval_request_id": approval.approval_request_id,
                        "plan_id": approval.plan_id,
                        "job_id": approval.job_id,
                        "status": approval.status,
                        "risk_level": approval.risk_level,
                        "requested_by": approval.requested_by,
                        "request_payload": approval.request_payload,
                        "metadata": approval.metadata,
                    },
                }
            )
            if approval.decided_at:
                events.append(
                    {
                        "event_id": f"{approval.approval_request_id}:decision",
                        "event_type": "APPROVAL_DECIDED",
                        "occurred_at": approval.decided_at.isoformat(),
                        "data": {
                            "approval_request_id": approval.approval_request_id,
                            "decision": approval.decision,
                            "decided_by": approval.decided_by,
                            "comment": approval.comment,
                        },
                    }
                )

    if include_agent_steps:
        for job in jobs:
            if not job.run_id:
                continue
            run = None
            with contextlib.suppress(Exception):
                run = await agent_registry.get_run(run_id=job.run_id, tenant_id=tenant_id)
            if run:
                events.append(
                    {
                        "event_id": run.run_id,
                        "event_type": "AGENT_RUN",
                        "occurred_at": run.started_at.isoformat(),
                        "data": {
                            "run_id": run.run_id,
                            "plan_id": run.plan_id,
                            "status": run.status,
                            "risk_level": run.risk_level,
                            "started_at": run.started_at.isoformat(),
                            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                        },
                    }
                )
            with contextlib.suppress(Exception):
                steps = await agent_registry.list_steps(run_id=job.run_id, tenant_id=tenant_id)
                for step in steps:
                    started_at = step.started_at or step.created_at
                    if started_at:
                        events.append(
                            {
                                "event_id": f"{step.run_id}:{step.step_id}:start",
                                "event_type": "AGENT_STEP_STARTED",
                                "occurred_at": started_at.isoformat(),
                                "data": {
                                    "run_id": step.run_id,
                                    "step_id": step.step_id,
                                    "tool_id": step.tool_id,
                                    "status": step.status,
                                    "metadata": step.metadata,
                                },
                            }
                        )
                    if step.finished_at:
                        duration_ms = None
                        if step.started_at:
                            duration_ms = int((step.finished_at - step.started_at).total_seconds() * 1000)
                        events.append(
                            {
                                "event_id": f"{step.run_id}:{step.step_id}:finish",
                                "event_type": "AGENT_STEP_FINISHED",
                                "occurred_at": step.finished_at.isoformat(),
                                "data": {
                                    "run_id": step.run_id,
                                    "step_id": step.step_id,
                                    "tool_id": step.tool_id,
                                    "status": step.status,
                                    "duration_ms": duration_ms,
                                    "output_digest": step.output_digest,
                                    "error": step.error,
                                },
                            }
                        )

    def _sort_key(item: dict[str, Any]) -> tuple[str, str]:
        return (str(item.get("occurred_at") or ""), str(item.get("event_id") or ""))

    events_sorted = sorted(events, key=_sort_key)
    events_sorted = events_sorted[: int(limit)]

    return ApiResponse.success(
        message="Session events fetched",
        data={
            "session_id": session_id,
            "count": len(events_sorted),
            "events": events_sorted,
            "metrics": {
                "message_tokens_total": token_total,
                "message_cost_total": cost_total,
                "jobs_total": len(jobs),
            },
        },
    )


@router.get("/{session_id}/approvals", response_model=ApiResponse)
async def list_approvals(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, _user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    approvals = await agent_registry.list_approval_requests(
        tenant_id=tenant_id,
        session_id=session_id,
        limit=limit,
        offset=offset,
    )
    return ApiResponse.success(
        message="Approval requests fetched",
        data={
            "session_id": session_id,
            "count": len(approvals),
            "approvals": [
                {
                    "approval_request_id": a.approval_request_id,
                    "plan_id": a.plan_id,
                    "job_id": a.job_id,
                    "status": a.status,
                    "risk_level": a.risk_level,
                    "requested_by": a.requested_by,
                    "requested_at": a.requested_at.isoformat(),
                    "decision": a.decision,
                    "decided_by": a.decided_by,
                    "decided_at": a.decided_at.isoformat() if a.decided_at else None,
                    "comment": a.comment,
                    "request_payload": a.request_payload,
                    "metadata": a.metadata,
                }
                for a in approvals
            ],
        },
    )


@router.post("/{session_id}/approvals/{approval_request_id}", response_model=ApiResponse)
async def decide_approval(
    session_id: str,
    approval_request_id: str,
    body: AgentSessionApprovalDecisionRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
        approval_request_id = str(UUID(approval_request_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id/approval_request_id must be UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    approval = await agent_registry.get_approval_request(approval_request_id=approval_request_id, tenant_id=tenant_id)
    if not approval or approval.session_id != session_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    decision = str(payload.get("decision") or "").strip().upper()
    comment = str(payload.get("comment") or "").strip() or None
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    if decision in _APPROVED_DECISIONS:
        status_value = "APPROVED"
    elif decision in _REJECTED_DECISIONS:
        status_value = "REJECTED"
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="decision must be APPROVE or REJECT")

    decided_at = datetime.now(timezone.utc)
    decided = await agent_registry.decide_approval_request(
        approval_request_id=approval_request_id,
        tenant_id=tenant_id,
        decision=status_value,
        decided_by=user_id,
        decided_at=decided_at,
        status=status_value,
        comment=comment,
        metadata=metadata,
    )
    if not decided:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found")

    await agent_registry.create_approval(
        approval_id=str(uuid4()),
        plan_id=approval.plan_id,
        tenant_id=tenant_id,
        step_id=None,
        decision=status_value,
        approved_by=user_id,
        approved_at=decided_at,
        comment=comment,
        metadata={"approval_request_id": approval_request_id, **metadata},
    )

    job_id = approval.job_id
    if status_value == "REJECTED":
        if job_id:
            await sessions.update_job(job_id=job_id, tenant_id=tenant_id, status="REJECTED", error="Rejected by user")
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ACTIVE")
        return ApiResponse.success(
            message="Approval rejected",
            data={"session_id": session_id, "approval_request_id": approval_request_id, "status": status_value},
        )

    if not job_id:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Approval request missing job_id")

    job = await sessions.get_job(job_id=job_id, tenant_id=tenant_id)
    if not job or job.session_id != session_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found for approval")
    if job.run_id:
        return ApiResponse.success(
            message="Job already running",
            data={"session_id": session_id, "job_id": job.job_id, "run_id": job.run_id},
        )

    record = await plan_registry.get_plan(plan_id=approval.plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    tools_restricted = bool((session_record.metadata or {}).get("tools_restricted"))
    allowed_tool_ids = list(session_record.enabled_tools or []) if tools_restricted else None
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    started = await _start_agent_job_run(
        request=request,
        sessions=sessions,
        tool_registry=tool_registry,
        tenant_id=tenant_id,
        session_id=session_id,
        job_id=job_id,
        plan=validation.plan,
        allowed_tool_ids=allowed_tool_ids,
    )
    return ApiResponse.accepted(
        message="Approval recorded; job resumed",
        data={
            "session_id": session_id,
            "job_id": job_id,
            "plan_id": approval.plan_id,
            "run_id": started.get("run_id"),
            "agent": started.get("agent"),
            "approval_request_id": approval_request_id,
            "status": status_value,
        },
    )
