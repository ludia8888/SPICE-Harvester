"""Ontology extension endpoints (BFF).

Expose Foundry-style ontology resource CRUD through FE-safe BFF routes.
"""

from typing import Optional

from fastapi import APIRouter, Query, Request, status

from bff.dependencies import OMSClientDep
from bff.routers.foundry_ontology_v2_common import _default_expected_head_commit
from bff.services.database_role_guard import enforce_database_role_or_http_error
from bff.schemas.ontology_extensions_requests import OntologyDeploymentRecordRequest, OntologyResourceRequest
from bff.services import ontology_extensions_service
from bff.services.oms_client import OMSClient
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role

router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Extensions"])


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    await enforce_database_role_or_http_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=DOMAIN_MODEL_ROLES,
        enforce_fn=enforce_database_role,
    )


@router.get("/resources")
@trace_endpoint("bff.ontology_extensions.list_resources")
async def list_resources(
    db_name: str,
    request: Request,
    resource_type: Optional[str] = Query(None, description="Resource type filter"),
    branch: str = Query("main", description="Target branch"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.list_resources(
        oms_client=oms_client,
        db_name=db_name,
        resource_type=resource_type,
        branch=branch,
        limit=limit,
        offset=offset,
    )


@router.get("/resources/{resource_type}")
@trace_endpoint("bff.ontology_extensions.list_resources_by_type")
async def list_resources_by_type(
    db_name: str,
    resource_type: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.list_resources(
        oms_client=oms_client,
        db_name=db_name,
        resource_type=resource_type,
        branch=branch,
        limit=limit,
        offset=offset,
    )


@router.post("/resources/{resource_type}", status_code=status.HTTP_201_CREATED)
@trace_endpoint("bff.ontology_extensions.create_resource")
async def create_resource(
    db_name: str,
    resource_type: str,
    payload: OntologyResourceRequest,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    expected_head_commit: Optional[str] = Query(None, description="Optimistic concurrency guard token"),
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.create_resource(
        oms_client=oms_client,
        db_name=db_name,
        resource_type=resource_type,
        payload=payload,
        branch=branch,
        expected_head_commit=expected_head_commit or _default_expected_head_commit(branch),
    )


@router.get("/resources/{resource_type}/{resource_id}")
@trace_endpoint("bff.ontology_extensions.get_resource")
async def get_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.get_resource(
        oms_client=oms_client,
        db_name=db_name,
        resource_type=resource_type,
        resource_id=resource_id,
        branch=branch,
    )


@router.put("/resources/{resource_type}/{resource_id}")
@trace_endpoint("bff.ontology_extensions.update_resource")
async def update_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    payload: OntologyResourceRequest,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    expected_head_commit: Optional[str] = Query(None, description="Optimistic concurrency guard token"),
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.update_resource(
        oms_client=oms_client,
        db_name=db_name,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=payload,
        branch=branch,
        expected_head_commit=expected_head_commit or _default_expected_head_commit(branch),
    )


@router.delete("/resources/{resource_type}/{resource_id}")
@trace_endpoint("bff.ontology_extensions.delete_resource")
async def delete_resource(
    db_name: str,
    resource_type: str,
    resource_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    expected_head_commit: Optional[str] = Query(None, description="Optimistic concurrency guard token"),
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.delete_resource(
        oms_client=oms_client,
        db_name=db_name,
        resource_type=resource_type,
        resource_id=resource_id,
        branch=branch,
        expected_head_commit=expected_head_commit or _default_expected_head_commit(branch),
    )


@router.post("/records/deployments")
@trace_endpoint("bff.ontology_extensions.record_deployment")
async def record_deployment(
    db_name: str,
    payload: OntologyDeploymentRecordRequest,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_extensions_service.record_deployment(
        oms_client=oms_client,
        db_name=db_name,
        target_branch=payload.target_branch,
        ontology_commit_id=payload.ontology_commit_id,
        snapshot_rid=payload.snapshot_rid,
        deployed_by=payload.deployed_by or "system",
        metadata=payload.metadata,
    )
