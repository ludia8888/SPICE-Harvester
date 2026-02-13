from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends

from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry
from shared.models.requests import ApiResponse
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ops", tags=["Ops"])


@router.get("/status", response_model=Dict[str, Any])
@trace_endpoint("bff.ops.ops_status")
async def ops_status(
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    ingest_metrics = await dataset_registry.get_ingest_outbox_metrics()
    objectify_metrics = await objectify_registry.get_objectify_metrics()

    return ApiResponse.success(
        message="Ops status",
        data={
            "ingest_outbox": ingest_metrics,
            "objectify": objectify_metrics,
        },
    ).to_dict()
