"""
Pipeline execution endpoints (BFF).

Thin router that delegates domain logic to `bff.services.pipeline_execution_service`.
Composed by `bff.routers.pipeline` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, Request

from bff.dependencies import get_oms_client
from bff.routers.pipeline_deps import (
    get_dataset_registry,
    get_objectify_registry,
    get_pipeline_job_queue,
    get_pipeline_registry,
)
from bff.routers.pipeline_ops import _acquire_pipeline_publish_lock, _release_pipeline_publish_lock
from bff.services import pipeline_execution_service
from bff.services.oms_client import OMSClient
from shared.dependencies.providers import AuditLogStoreDep, LineageStoreDep
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.pipeline.pipeline_control_plane_events import emit_pipeline_control_plane_event
from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.event_store import event_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])


@router.post("/{pipeline_id}/preview", response_model=ApiResponse)
@trace_endpoint("preview_pipeline")
async def preview_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_job_queue: PipelineJobQueue = Depends(get_pipeline_job_queue),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_execution_service.preview_pipeline(
        pipeline_id=pipeline_id,
        payload=payload,
        request=request,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        pipeline_job_queue=pipeline_job_queue,
        dataset_registry=dataset_registry,
        event_store=event_store,
    )


@router.post("/{pipeline_id}/build", response_model=ApiResponse)
@trace_endpoint("build_pipeline")
async def build_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_job_queue: PipelineJobQueue = Depends(get_pipeline_job_queue),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    oms_client: OMSClient = Depends(get_oms_client),
    request: Request = None,
) -> ApiResponse:
    return await pipeline_execution_service.build_pipeline(
        pipeline_id=pipeline_id,
        payload=payload,
        request=request,
        audit_store=audit_store,
        pipeline_registry=pipeline_registry,
        pipeline_job_queue=pipeline_job_queue,
        dataset_registry=dataset_registry,
        oms_client=oms_client,
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
    )


@router.post("/{pipeline_id}/deploy", response_model=ApiResponse)
@trace_endpoint("deploy_pipeline")
async def deploy_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    oms_client: OMSClient = Depends(get_oms_client),
    *,
    lineage_store: LineageStoreDep,
    audit_store: AuditLogStoreDep,
) -> ApiResponse:
    return await pipeline_execution_service.deploy_pipeline(
        pipeline_id=pipeline_id,
        payload=payload,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        oms_client=oms_client,
        lineage_store=lineage_store,
        audit_store=audit_store,
        emit_pipeline_control_plane_event=emit_pipeline_control_plane_event,
        _acquire_pipeline_publish_lock=_acquire_pipeline_publish_lock,
        _release_pipeline_publish_lock=_release_pipeline_publish_lock,
    )


__all__ = [
    "build_pipeline",
    "deploy_pipeline",
    "preview_pipeline",
    "router",
]

