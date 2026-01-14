"""
Agent model registry admin API (BFF).

Provides CRUD endpoints for global model metadata/capabilities.
"""

from __future__ import annotations

import hmac
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from shared.models.requests import ApiResponse
from shared.security.auth_utils import extract_presented_token, get_expected_token
from shared.services.agent_model_registry import AgentModelRecord, AgentModelRegistry

logger = logging.getLogger(__name__)

_ADMIN_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")

router = APIRouter(prefix="/admin/agent-models", tags=["Agent Model Admin"])


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


class AgentModelUpsertRequest(BaseModel):
    model_id: str = Field(..., min_length=1, max_length=200)
    provider: str = Field(..., min_length=1, max_length=50, description="openai_compat|anthropic|google|custom")
    display_name: Optional[str] = Field(default=None, max_length=200)
    status: str = Field(default="ACTIVE", max_length=50)
    supports_json_mode: bool = Field(default=True)
    supports_native_tool_calling: bool = Field(default=False)
    max_context_tokens: Optional[int] = Field(default=None, ge=0)
    max_output_tokens: Optional[int] = Field(default=None, ge=0)
    prompt_per_1k: Optional[float] = Field(default=None, ge=0)
    completion_per_1k: Optional[float] = Field(default=None, ge=0)
    metadata: dict = Field(default_factory=dict)


async def get_agent_model_registry() -> AgentModelRegistry:
    from bff.main import get_agent_model_registry as _get_agent_model_registry

    return await _get_agent_model_registry()


def _serialize_model(record: AgentModelRecord) -> dict:
    return {
        "model_id": record.model_id,
        "provider": record.provider,
        "display_name": record.display_name,
        "status": record.status,
        "supports_json_mode": record.supports_json_mode,
        "supports_native_tool_calling": record.supports_native_tool_calling,
        "max_context_tokens": record.max_context_tokens,
        "max_output_tokens": record.max_output_tokens,
        "prompt_per_1k": record.prompt_per_1k,
        "completion_per_1k": record.completion_per_1k,
        "metadata": record.metadata,
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


@router.post("", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def upsert_model(
    body: AgentModelUpsertRequest,
    registry: AgentModelRegistry = Depends(get_agent_model_registry),
) -> ApiResponse:
    try:
        record = await registry.upsert_model(
            model_id=body.model_id.strip(),
            provider=body.provider.strip().lower(),
            display_name=body.display_name,
            status=body.status.strip().upper(),
            supports_json_mode=body.supports_json_mode,
            supports_native_tool_calling=body.supports_native_tool_calling,
            max_context_tokens=body.max_context_tokens,
            max_output_tokens=body.max_output_tokens,
            prompt_per_1k=body.prompt_per_1k,
            completion_per_1k=body.completion_per_1k,
            metadata=body.metadata,
        )
        return ApiResponse.created(message="Agent model upserted", data={"model": _serialize_model(record)})
    except Exception as exc:
        logger.error("Failed to upsert agent model: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def list_models(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    provider_filter: Optional[str] = Query(default=None, alias="provider"),
    registry: AgentModelRegistry = Depends(get_agent_model_registry),
) -> ApiResponse:
    try:
        records = await registry.list_models(
            status=status_filter.strip().upper() if status_filter else None,
            provider=provider_filter.strip().lower() if provider_filter else None,
        )
        return ApiResponse.success(
            message="Agent models retrieved",
            data={"models": [_serialize_model(record) for record in records]},
        )
    except Exception as exc:
        logger.error("Failed to list agent models: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/{model_id}", response_model=ApiResponse, dependencies=[Depends(require_admin)])
async def get_model(
    model_id: str,
    registry: AgentModelRegistry = Depends(get_agent_model_registry),
) -> ApiResponse:
    try:
        record = await registry.get_model(model_id=model_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Model not found")
        return ApiResponse.success(message="Agent model retrieved", data={"model": _serialize_model(record)})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get agent model: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

