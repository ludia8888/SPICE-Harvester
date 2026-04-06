"""
Pipeline detail endpoints (BFF).

Fetch pipeline details/readiness and update pipeline metadata/definition.
Composed by `bff.routers.pipeline` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, Request

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from bff.services import pipeline_detail_service
from shared.dependencies.providers import AuditLogStoreDep
from shared.models.responses import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.event_store import event_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])


@router.get("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("get_pipeline")
async def get_pipeline(
    pipeline_id: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    branch: Optional[str] = Query(default=None),
    preview_node_id: Optional[str] = Query(default=None, alias="preview_node_id"),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_detail_service.get_pipeline(
        pipeline_id=pipeline_id,
        pipeline_registry=pipeline_registry,
        branch=branch,
        preview_node_id=preview_node_id,
        request=request,
    )


@router.get("/{pipeline_id}/readiness", response_model=ApiResponse)
@trace_endpoint("get_pipeline_readiness")
async def get_pipeline_readiness(
    pipeline_id: str,
    branch: Optional[str] = Query(default=None),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_detail_service.get_pipeline_readiness(
        pipeline_id=pipeline_id,
        branch=branch,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        request=request,
    )


@router.put("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("update_pipeline")
async def update_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_detail_service.update_pipeline(
        pipeline_id=pipeline_id,
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        request=request,
        event_store=event_store,
    )

