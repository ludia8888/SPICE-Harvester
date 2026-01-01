from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends

from shared.models.requests import ApiResponse
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ops", tags=["Ops"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    from bff.main import get_objectify_registry as _get_objectify_registry

    return await _get_objectify_registry()


@router.get("/status", response_model=Dict[str, Any])
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
