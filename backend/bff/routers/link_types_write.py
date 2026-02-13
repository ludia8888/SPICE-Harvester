"""Link type write endpoints (BFF).

Composed by `bff.routers.link_types` via router composition (Composite pattern).

This module defines HTTP routes only. Business logic lives in
`bff.services.link_types_write_service` (Facade).
"""

from typing import Optional
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, Query, Request, status

from bff.dependencies import OMSClientDep
from bff.routers.link_types_deps import get_dataset_registry, get_objectify_registry
from bff.routers.objectify_job_ops import enqueue_objectify_job_for_mapping_spec
from bff.routers.role_deps import enforce_required_database_role
from bff.schemas.link_types_requests import LinkTypeRequest, LinkTypeUpdateRequest
from bff.services import link_types_write_service
from bff.services.oms_client import OMSClient
from bff.services.objectify_mapping_spec_service import create_mapping_spec as create_mapping_spec_service
from shared.models.requests import ApiResponse
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

router = APIRouter(tags=["Ontology Link Types"])


@router.post("/link-types", status_code=status.HTTP_201_CREATED, response_model=ApiResponse)
@trace_endpoint("bff.link_types.create_link_type")
async def create_link_type(
    db_name: str,
    body: LinkTypeRequest,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    expected_head_commit: Optional[str] = Query(
        default=None,
        description="Optimistic concurrency guard (defaults to branch head)",
    ),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    return await link_types_write_service.create_link_type(
        db_name=db_name,
        body=body,
        request=request,
        branch=branch,
        expected_head_commit=expected_head_commit,
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        enforce_role=enforce_required_database_role,
        create_mapping_spec=create_mapping_spec_service,
        enqueue_job=enqueue_objectify_job_for_mapping_spec,
    )


@router.put("/link-types/{link_type_id}", response_model=ApiResponse)
@trace_endpoint("bff.link_types.update_link_type")
async def update_link_type(
    db_name: str,
    link_type_id: str,
    body: LinkTypeUpdateRequest,
    request: Request,
    branch: str = Query(..., description="Target branch"),
    expected_head_commit: Optional[str] = Query(
        default=None,
        description="Optimistic concurrency guard (defaults to branch head)",
    ),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    return await link_types_write_service.update_link_type(
        db_name=db_name,
        link_type_id=link_type_id,
        body=body,
        request=request,
        branch=branch,
        expected_head_commit=expected_head_commit,
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        enforce_role=enforce_required_database_role,
        create_mapping_spec=create_mapping_spec_service,
        enqueue_job=enqueue_objectify_job_for_mapping_spec,
    )


@router.post("/link-types/{link_type_id}/reindex", response_model=ApiResponse)
@trace_endpoint("bff.link_types.reindex_link_type")
async def reindex_link_type(
    db_name: str,
    link_type_id: str,
    request: Request,
    dataset_version_id: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    return await link_types_write_service.reindex_link_type(
        db_name=db_name,
        link_type_id=link_type_id,
        request=request,
        dataset_version_id=dataset_version_id,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        enforce_role=enforce_required_database_role,
        enqueue_job=enqueue_objectify_job_for_mapping_spec,
    )
