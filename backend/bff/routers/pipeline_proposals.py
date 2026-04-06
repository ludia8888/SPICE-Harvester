"""
Pipeline proposal endpoints (BFF).

Composed by `bff.routers.pipeline` via router composition (Composite pattern).
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, Request

from bff.routers.pipeline_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry
from bff.services import pipeline_proposal_service
from shared.dependencies.providers import AuditLogStoreDep
from shared.models.responses import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Pipeline Builder"])

_normalize_mapping_spec_ids = pipeline_proposal_service.normalize_mapping_spec_ids


@router.get("/proposals", response_model=ApiResponse)
@trace_endpoint("list_pipeline_proposals")
async def list_pipeline_proposals(
    db_name: str,
    branch: Optional[str] = None,
    status_filter: Optional[str] = Query(default=None, alias="status"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_proposal_service.list_pipeline_proposals(
        db_name=db_name,
        branch=branch,
        status_filter=status_filter,
        pipeline_registry=pipeline_registry,
        request=request,
    )


@router.post("/{pipeline_id}/proposals", response_model=ApiResponse)
@trace_endpoint("submit_pipeline_proposal")
async def submit_pipeline_proposal(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_proposal_service.submit_pipeline_proposal(
        pipeline_id=pipeline_id,
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        request=request,
    )


@router.post("/{pipeline_id}/proposals/approve", response_model=ApiResponse)
@trace_endpoint("approve_pipeline_proposal")
async def approve_pipeline_proposal(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_proposal_service.approve_pipeline_proposal(
        pipeline_id=pipeline_id,
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        request=request,
    )


@router.post("/{pipeline_id}/proposals/reject", response_model=ApiResponse)
@trace_endpoint("reject_pipeline_proposal")
async def reject_pipeline_proposal(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_proposal_service.reject_pipeline_proposal(
        pipeline_id=pipeline_id,
        payload=payload,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        request=request,
    )
