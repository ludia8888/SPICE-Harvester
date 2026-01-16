"""
Agent tool allowlist admin API (BFF).

Provides CRUD endpoints for tool policies used by agent plan validation.
"""

from __future__ import annotations

import hmac
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from shared.config.settings import get_settings
from shared.models.requests import ApiResponse
from shared.security.auth_utils import extract_presented_token, get_expected_token
from shared.services.agent_tool_registry import AgentToolRegistry, AgentToolPolicyRecord

logger = logging.getLogger(__name__)

_ADMIN_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")

router = APIRouter(prefix="/admin/agent-tools", tags=["Agent Tool Admin"])


async def require_admin(request: Request) -> None:
    settings = get_settings()
    if settings.is_development and settings.auth.dev_master_auth_enabled:
        actor = (request.headers.get("X-Admin-Actor") or str(settings.auth.dev_master_user_id or "dev-admin")).strip()
        request.state.admin_actor = actor or "dev-admin"
        request.state.dev_master_auth = True
        return

    expected = get_expected_token(_ADMIN_TOKEN_ENV_KEYS)
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin endpoints are disabled (set BFF_ADMIN_TOKEN to enable)",
        )
    presented = extract_presented_token(request.headers)
    if not presented or not hmac.compare_digest(presented, expected):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin authorization failed")


class AgentToolPolicyRequest(BaseModel):
    tool_id: str = Field(..., min_length=1, max_length=200)
    method: str = Field(..., min_length=1, max_length=10)
    path: str = Field(..., min_length=1, max_length=500)
    risk_level: str = Field(default="read")
    requires_approval: bool = Field(default=False)
    requires_idempotency_key: bool = Field(default=False)
    status: str = Field(default="ACTIVE")
    roles: List[str] = Field(default_factory=list)
    max_payload_bytes: Optional[int] = Field(default=None, ge=0)
    version: str = Field(default="v1", max_length=50)
    tool_type: str = Field(default="unknown", max_length=100)
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: Optional[float] = Field(default=None, ge=0)
    retry_policy: Dict[str, Any] = Field(default_factory=dict)
    resource_scopes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


def _serialize_policy(record: AgentToolPolicyRecord) -> dict:
    return {
        "tool_id": record.tool_id,
        "method": record.method,
        "path": record.path,
        "risk_level": record.risk_level,
        "requires_approval": record.requires_approval,
        "requires_idempotency_key": record.requires_idempotency_key,
        "status": record.status,
        "roles": record.roles,
        "max_payload_bytes": record.max_payload_bytes,
        "version": record.version,
        "tool_type": record.tool_type,
        "input_schema": record.input_schema,
        "output_schema": record.output_schema,
        "timeout_seconds": record.timeout_seconds,
        "retry_policy": record.retry_policy,
        "resource_scopes": record.resource_scopes,
        "metadata": record.metadata,
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


@router.post("", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def upsert_tool_policy(
    body: AgentToolPolicyRequest,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    try:
        record = await tool_registry.upsert_tool_policy(
            tool_id=body.tool_id.strip(),
            method=body.method.strip().upper(),
            path=body.path.strip(),
            risk_level=body.risk_level.strip().lower(),
            requires_approval=body.requires_approval,
            requires_idempotency_key=body.requires_idempotency_key,
            status=body.status.strip().upper(),
            roles=body.roles,
            max_payload_bytes=body.max_payload_bytes,
            version=body.version.strip() or "v1",
            tool_type=body.tool_type.strip() or "unknown",
            input_schema=body.input_schema,
            output_schema=body.output_schema,
            timeout_seconds=body.timeout_seconds,
            retry_policy=body.retry_policy,
            resource_scopes=body.resource_scopes,
            metadata=body.metadata,
        )
        return ApiResponse.created(
            message="Agent tool policy upserted",
            data={"policy": _serialize_policy(record)},
        )
    except Exception as exc:
        logger.error("Failed to upsert agent tool policy: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def list_tool_policies(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    try:
        records = await tool_registry.list_tool_policies(
            status=status_filter.strip().upper() if status_filter else None
        )
        return ApiResponse.success(
            message="Agent tool policies retrieved",
            data={"policies": [_serialize_policy(record) for record in records]},
        )
    except Exception as exc:
        logger.error("Failed to list agent tool policies: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/{tool_id}", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def get_tool_policy(
    tool_id: str,
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    try:
        record = await tool_registry.get_tool_policy(tool_id=tool_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tool policy not found")
        return ApiResponse.success(
            message="Agent tool policy retrieved",
            data={"policy": _serialize_policy(record)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get agent tool policy: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
