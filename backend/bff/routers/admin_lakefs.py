"""
Admin lakeFS credential endpoints (BFF).

These endpoints store encrypted credentials in Postgres via PipelineRegistry.
Composed by `bff.routers.admin`.
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from typing import Any, Dict, Literal

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from bff.routers.registry_deps import get_pipeline_registry
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Admin Operations"])


class LakeFSCredentialsUpsertRequest(BaseModel):
    """Upsert request for lakeFS credentials stored in Postgres (encrypted)."""

    principal_type: Literal["user", "service"] = Field(..., description="Principal type (user|service)")
    principal_id: str = Field(..., description="User ID or service name")
    access_key_id: str = Field(..., description="lakeFS access key id")
    secret_access_key: str = Field(..., description="lakeFS secret access key")


@router.get("/lakefs/credentials")
@trace_endpoint("bff.admin.list_lakefs_credentials")
async def list_lakefs_credentials(
    registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> Dict[str, Any]:
    items = await registry.list_lakefs_credentials()
    return {"status": "success", "data": {"credentials": items, "count": len(items)}}


@router.post("/lakefs/credentials")
@trace_endpoint("bff.admin.upsert_lakefs_credentials")
async def upsert_lakefs_credentials(
    payload: LakeFSCredentialsUpsertRequest,
    request: Request,
    registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> Dict[str, Any]:
    actor = getattr(request.state, "admin_actor", None)
    await registry.upsert_lakefs_credentials(
        principal_type=payload.principal_type,
        principal_id=payload.principal_id,
        access_key_id=payload.access_key_id,
        secret_access_key=payload.secret_access_key,
        created_by=str(actor) if actor else None,
    )
    return {"status": "success", "data": {"principal_type": payload.principal_type, "principal_id": payload.principal_id}}

