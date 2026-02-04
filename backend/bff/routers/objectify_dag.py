"""Objectify DAG orchestration endpoints (BFF).

Composed by `bff.routers.objectify` via router composition (Composite pattern).

Business logic lives in `bff.services.objectify_dag_service` (Facade).
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, Request

from bff.dependencies import OMSClientDep
from bff.routers.objectify_deps import get_dataset_registry, get_objectify_job_queue, get_objectify_registry
from bff.schemas.objectify_requests import RunObjectifyDAGRequest
from bff.services import objectify_dag_service
from bff.services.oms_client import OMSClient
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

router = APIRouter(tags=["Objectify"])


@router.post("/databases/{db_name}/run-dag", response_model=Dict[str, Any])
async def run_objectify_dag(
    db_name: str,
    body: RunObjectifyDAGRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    oms_client: OMSClient = OMSClientDep,
) -> Dict[str, Any]:
    return await objectify_dag_service.run_objectify_dag(
        db_name=db_name,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        job_queue=job_queue,
        oms_client=oms_client,
    )

