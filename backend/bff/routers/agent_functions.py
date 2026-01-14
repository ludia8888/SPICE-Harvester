"""
Agent function execution APIs (BFF).

Provides TOOL-005 "Function tool" support:
- Admin can register functions (id+version+tags+handler)
- Agent can execute a registered function via an allowlisted tool endpoint
"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.agent_function_registry import AgentFunctionRecord, AgentFunctionRegistry
from shared.utils.json_patch import JsonPatchError, apply_json_patch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-functions", tags=["Agent Functions"])


async def get_agent_function_registry() -> AgentFunctionRegistry:
    from bff.main import get_agent_function_registry as _get_agent_function_registry

    return await _get_agent_function_registry()


def _resolve_verified_principal(request: Request) -> tuple[str, str, str]:
    user = getattr(request.state, "user", None)
    if user is None or not getattr(user, "verified", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User JWT required")
    tenant_id = getattr(user, "tenant_id", None) or getattr(user, "org_id", None) or "default"
    user_id = str(getattr(user, "id", "") or "").strip() or "unknown"
    actor = f"{getattr(user, 'type', 'user')}:{user_id}"
    return str(tenant_id), user_id, actor


def _require_admin_role(request: Request) -> None:
    principal = getattr(request.state, "user", None)
    roles = set(getattr(principal, "roles", ()) or ())
    if "admin" not in roles and "platform_admin" not in roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")


class AgentFunctionUpsertRequest(BaseModel):
    function_id: str = Field(..., min_length=1, max_length=200)
    version: str = Field(default="v1", min_length=1, max_length=50)
    status: str = Field(default="ACTIVE", max_length=20)
    handler: str = Field(..., min_length=1, max_length=200)
    tags: list[str] = Field(default_factory=list)
    roles: list[str] = Field(default_factory=list)
    input_schema: dict = Field(default_factory=dict)
    output_schema: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)


class AgentFunctionExecuteRequest(BaseModel):
    version: str | None = Field(default=None, max_length=50)
    tag: str | None = Field(default=None, max_length=100)
    input: Any = None


def _record_to_payload(rec: AgentFunctionRecord) -> Dict[str, Any]:
    return {
        "function_id": rec.function_id,
        "version": rec.version,
        "status": rec.status,
        "handler": rec.handler,
        "tags": list(rec.tags or []),
        "roles": list(rec.roles or []),
        "input_schema": rec.input_schema,
        "output_schema": rec.output_schema,
        "metadata": rec.metadata,
        "created_at": rec.created_at.isoformat(),
        "updated_at": rec.updated_at.isoformat(),
    }


async def _resolve_function_for_execution(
    *,
    registry: AgentFunctionRegistry,
    function_id: str,
    version: Optional[str],
    tag: Optional[str],
) -> Optional[AgentFunctionRecord]:
    fid = str(function_id or "").strip()
    if not fid:
        return None
    if version:
        return await registry.get_function(function_id=fid, version=str(version).strip(), status="ACTIVE")
    candidates = await registry.list_functions(function_id=fid, status="ACTIVE", limit=50, offset=0)
    if not candidates:
        return None
    if tag:
        wanted = str(tag).strip()
        for item in candidates:
            if wanted in set(item.tags or []):
                return item
    return candidates[0]


def _execute_handler(handler: str, payload: Any) -> Any:
    handler = str(handler or "").strip().lower()
    if handler == "echo":
        return payload
    if handler == "json_patch":
        if not isinstance(payload, dict):
            raise ValueError("json_patch input must be an object")
        base = payload.get("base")
        ops = payload.get("ops")
        if ops is None:
            ops = payload.get("patch")
        if base is None:
            raise ValueError("json_patch requires base")
        if not isinstance(ops, list):
            raise ValueError("json_patch requires ops list")
        try:
            return apply_json_patch(base, ops)
        except JsonPatchError as exc:
            raise ValueError(str(exc)) from exc
    raise ValueError(f"unknown handler: {handler}")


@router.get("", response_model=ApiResponse)
async def list_agent_functions(
    request: Request,
    registry: AgentFunctionRegistry = Depends(get_agent_function_registry),
    function_id: str | None = None,
    status_filter: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> ApiResponse:
    _resolve_verified_principal(request)
    records = await registry.list_functions(
        function_id=function_id,
        status=status_filter,
        limit=int(limit),
        offset=int(offset),
    )
    return ApiResponse.success(
        message="Agent functions fetched",
        data={"count": len(records), "functions": [_record_to_payload(r) for r in records]},
    )


@router.post("", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def upsert_agent_function(
    body: AgentFunctionUpsertRequest,
    request: Request,
    registry: AgentFunctionRegistry = Depends(get_agent_function_registry),
) -> ApiResponse:
    _resolve_verified_principal(request)
    _require_admin_role(request)
    payload = sanitize_input(body.model_dump(exclude_none=True))
    record = await registry.upsert_function(
        function_id=str(payload.get("function_id") or "").strip(),
        version=str(payload.get("version") or "v1").strip() or "v1",
        status=str(payload.get("status") or "ACTIVE").strip().upper() or "ACTIVE",
        handler=str(payload.get("handler") or "").strip(),
        tags=list(payload.get("tags") or []),
        roles=list(payload.get("roles") or []),
        input_schema=dict(payload.get("input_schema") or {}),
        output_schema=dict(payload.get("output_schema") or {}),
        metadata=dict(payload.get("metadata") or {}),
    )
    return ApiResponse.created(message="Agent function upserted", data={"function": _record_to_payload(record)})


@router.post("/{function_id}/execute", response_model=ApiResponse)
async def execute_agent_function(
    function_id: str,
    body: AgentFunctionExecuteRequest,
    request: Request,
    registry: AgentFunctionRegistry = Depends(get_agent_function_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    payload = sanitize_input(body.model_dump(exclude_none=True))
    record = await _resolve_function_for_execution(
        registry=registry,
        function_id=function_id,
        version=payload.get("version"),
        tag=payload.get("tag"),
    )
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Function not found")

    required_roles = set(str(r).strip() for r in (record.roles or []) if str(r).strip())
    if required_roles:
        principal = getattr(request.state, "user", None)
        user_roles = set(getattr(principal, "roles", ()) or ())
        if not user_roles or not (user_roles & required_roles):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")

    started = datetime.now(timezone.utc)
    try:
        result = _execute_handler(record.handler, payload.get("input"))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Agent function execution failed")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Function execution failed") from exc

    latency_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    with contextlib.suppress(Exception):
        logger.info(
            "Agent function executed: %s@%s by %s (%s/%s) in %sms",
            record.function_id,
            record.version,
            actor,
            tenant_id,
            user_id,
            latency_ms,
        )

    return ApiResponse.success(
        message="Function executed",
        data={
            "function_id": record.function_id,
            "version": record.version,
            "handler": record.handler,
            "latency_ms": latency_ms,
            "result": result,
        },
    )

