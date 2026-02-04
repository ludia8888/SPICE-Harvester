"""Object type contract endpoints (BFF).

The router stays thin by delegating business logic to a service module
(Facade / Service Layer). Test patch points for role enforcement are preserved.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.dependencies import OMSClientDep
from bff.routers.object_types_deps import get_dataset_registry, get_objectify_registry
from bff.schemas.object_types_requests import ObjectTypeContractRequest, ObjectTypeContractUpdate
from bff.services import object_type_contract_service
from bff.services.oms_client import OMSClient
from shared.models.requests import ApiResponse
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Object Types"])


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


@router.post("/object-types", status_code=status.HTTP_201_CREATED, response_model=ApiResponse)
async def create_object_type_contract(
    db_name: str,
    body: ObjectTypeContractRequest,
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
    db_name = validate_db_name(db_name)
    await _require_domain_role(request, db_name=db_name)
    return await object_type_contract_service.create_object_type_contract(
        db_name=db_name,
        body=body,
        request=request,
        branch=branch,
        expected_head_commit=expected_head_commit,
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )


@router.get("/object-types/{class_id}", response_model=ApiResponse)
async def get_object_type_contract(
    db_name: str,
    class_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ApiResponse:
    db_name = validate_db_name(db_name)
    await _require_domain_role(request, db_name=db_name)
    return await object_type_contract_service.get_object_type_contract(
        db_name=db_name,
        class_id=class_id,
        request=request,
        branch=branch,
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )


@router.put("/object-types/{class_id}", response_model=ApiResponse)
async def update_object_type_contract(
    db_name: str,
    class_id: str,
    body: ObjectTypeContractUpdate,
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
    db_name = validate_db_name(db_name)
    return await object_type_contract_service.update_object_type_contract(
        db_name=db_name,
        class_id=class_id,
        body=body,
        request=request,
        branch=branch,
        expected_head_commit=expected_head_commit,
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )
