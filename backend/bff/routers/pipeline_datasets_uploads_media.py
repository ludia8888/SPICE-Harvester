"""Pipeline dataset media upload endpoints (BFF).

Composed by `bff.routers.pipeline_datasets_uploads` via router composition (Composite pattern).
"""

from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile

import bff.routers.pipeline_datasets_ops as ops
from bff.routers.pipeline_datasets_deps import get_objectify_job_queue
from bff.routers.pipeline_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry
from bff.services import pipeline_dataset_media_upload_service
from shared.dependencies.providers import LineageStoreDep
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Pipeline Builder"])


@router.post("/datasets/media-upload", response_model=ApiResponse)
@trace_endpoint("upload_media_dataset")
async def upload_media_dataset(
    db_name: str = Query(..., description="Database name"),
    branch: Optional[str] = Query(default=None),
    files: list[UploadFile] = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    return await pipeline_dataset_media_upload_service.upload_media_dataset(
        db_name=db_name,
        branch=branch,
        files=files,
        dataset_name=dataset_name,
        description=description,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        lineage_store=lineage_store,
        flush_dataset_ingest_outbox=ops.flush_dataset_ingest_outbox,
        build_dataset_event_payload=ops.build_dataset_event_payload,
    )
