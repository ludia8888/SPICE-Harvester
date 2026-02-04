"""
Pipeline catalog endpoints (BFF).

List/create pipelines. Composed by `bff.routers.pipeline` via router composition
(Composite pattern).
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Request

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from bff.services import pipeline_catalog_service
from shared.dependencies.providers import AuditLogStoreDep
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.event_store import event_store

router = APIRouter(tags=["Pipeline Builder"])


@router.get("", response_model=ApiResponse)
@trace_endpoint("list_pipelines")
async def list_pipelines(
    db_name: str,
    branch: Optional[str] = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_catalog_service.list_pipelines(
        db_name=db_name,
        branch=branch,
        pipeline_registry=pipeline_registry,
        request=request,
    )


@router.post("", response_model=ApiResponse)
@trace_endpoint("create_pipeline")
async def create_pipeline(
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_catalog_service.create_pipeline(
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        event_store=event_store,
        request=request,
    )
