"""
Objectify mapping spec endpoints (BFF).

Composed by `bff.routers.objectify` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.dependencies import OMSClientDep
from bff.routers.objectify_deps import get_dataset_registry, get_objectify_registry
from bff.schemas.objectify_requests import CreateMappingSpecRequest
from bff.services.objectify_mapping_spec_service import create_mapping_spec as create_mapping_spec_service
from shared.models.requests import ApiResponse
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Objectify"])


@router.post("/mapping-specs", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
async def create_mapping_spec(
    body: CreateMappingSpecRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    oms_client=OMSClientDep,
):
    return await create_mapping_spec_service(
        body=body,
        request=request,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        oms_client=oms_client,
    )


@router.get("/mapping-specs", response_model=Dict[str, Any])
async def list_mapping_specs(
    dataset_id: Optional[str] = Query(default=None),
    include_inactive: bool = Query(default=False),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    try:
        records = await objectify_registry.list_mapping_specs(dataset_id=dataset_id, include_inactive=include_inactive)
        return ApiResponse.success(
            message="Mapping specs retrieved",
            data={"mapping_specs": [record.__dict__ for record in records]},
        ).to_dict()
    except Exception as exc:
        logger.error("Failed to list mapping specs: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

