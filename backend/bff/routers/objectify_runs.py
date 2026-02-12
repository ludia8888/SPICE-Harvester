"""Objectify run endpoints (BFF).

Composed by `bff.routers.objectify` via router composition (Composite pattern).

Business logic lives in `bff.services.objectify_run_service` (Facade).
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, Request

from bff.routers.objectify_deps import (
    get_dataset_registry,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
)
from bff.dependencies import get_oms_client
from bff.schemas.objectify_requests import TriggerObjectifyRequest
from bff.services import objectify_run_service
from bff.services.oms_client import OMSClient
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Objectify"])


@router.post("/datasets/{dataset_id}/run", response_model=Dict[str, Any])
async def run_objectify(
    dataset_id: str,
    body: TriggerObjectifyRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    return await objectify_run_service.run_objectify(
        dataset_id=dataset_id,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        job_queue=job_queue,
        pipeline_registry=pipeline_registry,
        oms_client=oms_client,
    )
