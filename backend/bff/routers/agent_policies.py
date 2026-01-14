"""
Agent policy admin API (BFF).

This is the operator-facing control plane for tenant allowlists (AUTH-005).
"""

from __future__ import annotations

import hmac
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from shared.models.requests import ApiResponse
from shared.security.auth_utils import extract_presented_token, get_expected_token
from shared.services.agent_policy_registry import AgentPolicyRegistry, AgentTenantPolicyRecord

logger = logging.getLogger(__name__)

_ADMIN_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")

router = APIRouter(prefix="/admin/agent-policies", tags=["Agent Policy Admin"])


async def require_admin(request: Request) -> None:
    expected = get_expected_token(_ADMIN_TOKEN_ENV_KEYS)
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin endpoints are disabled (set BFF_ADMIN_TOKEN to enable)",
        )
    presented = extract_presented_token(request.headers)
    if not presented or not hmac.compare_digest(presented, expected):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin authorization failed")


class AgentTenantPolicyRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=200)
    allowed_models: List[str] = Field(default_factory=list)
    allowed_tools: List[str] = Field(default_factory=list)
    default_model: Optional[str] = Field(default=None, max_length=200)
    auto_approve_rules: Dict[str, Any] = Field(default_factory=dict)
    data_policies: Dict[str, Any] = Field(default_factory=dict)


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


def _serialize_policy(record: AgentTenantPolicyRecord) -> dict:
    return {
        "tenant_id": record.tenant_id,
        "allowed_models": list(record.allowed_models or []),
        "allowed_tools": list(record.allowed_tools or []),
        "default_model": record.default_model,
        "auto_approve_rules": record.auto_approve_rules,
        "data_policies": record.data_policies,
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


@router.post("", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def upsert_tenant_policy(
    body: AgentTenantPolicyRequest,
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
) -> ApiResponse:
    try:
        record = await policy_registry.upsert_tenant_policy(
            tenant_id=body.tenant_id.strip(),
            allowed_models=body.allowed_models,
            allowed_tools=body.allowed_tools,
            default_model=body.default_model,
            auto_approve_rules=body.auto_approve_rules,
            data_policies=body.data_policies,
        )
        return ApiResponse.created(
            message="Agent tenant policy upserted",
            data={"policy": _serialize_policy(record)},
        )
    except Exception as exc:
        logger.error("Failed to upsert agent tenant policy: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def list_tenant_policies(
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    try:
        records = await policy_registry.list_tenant_policies(limit=limit, offset=offset)
        return ApiResponse.success(
            message="Agent tenant policies retrieved",
            data={"count": len(records), "policies": [_serialize_policy(r) for r in records]},
        )
    except Exception as exc:
        logger.error("Failed to list agent tenant policies: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/{tenant_id}", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def get_tenant_policy(
    tenant_id: str,
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
) -> ApiResponse:
    record = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant policy not found")
    return ApiResponse.success(message="Agent tenant policy retrieved", data={"policy": _serialize_policy(record)})

