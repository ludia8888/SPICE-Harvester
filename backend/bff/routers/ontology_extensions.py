"""Ontology extension endpoints (BFF).

This module provides fixed subpaths under `/ontology/*` that must be registered
before legacy ontology routes (see `bff.main` include order).

Routers are kept thin: OMS orchestration + error handling lives in
`bff.services.ontology_extensions_service` (Facade), and request schemas live in
`bff.schemas.ontology_extensions_requests`.
"""

from typing import Optional

from fastapi import APIRouter, Query, status

from bff.dependencies import OMSClientDep
from bff.schemas.ontology_extensions_requests import (
    OntologyApproveRequest,
    OntologyDeployRequest,
    OntologyProposalRequest,
    OntologyResourceRequest,
)
from bff.services import ontology_extensions_service
from bff.services.oms_client import OMSClient
from shared.models.requests import BranchCreateRequest

router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Extensions"])


def _resource_routes(resource_type: str):
    async def list_route(
        db_name: str,
        branch: str = Query("main", description="Target branch"),
        limit: int = Query(200, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        oms_client: OMSClient = OMSClientDep,
    ):
        return await ontology_extensions_service.list_resources(
            oms_client=oms_client,
            db_name=db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )

    async def create_route(
        db_name: str,
        payload: OntologyResourceRequest,
        branch: str = Query(..., description="Target branch"),
        expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
        oms_client: OMSClient = OMSClientDep,
    ):
        return await ontology_extensions_service.create_resource(
            oms_client=oms_client,
            db_name=db_name,
            resource_type=resource_type,
            payload=payload,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

    async def get_route(
        db_name: str,
        resource_id: str,
        branch: str = Query("main", description="Target branch"),
        oms_client: OMSClient = OMSClientDep,
    ):
        return await ontology_extensions_service.get_resource(
            oms_client=oms_client,
            db_name=db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )

    async def update_route(
        db_name: str,
        resource_id: str,
        payload: OntologyResourceRequest,
        branch: str = Query(..., description="Target branch"),
        expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
        oms_client: OMSClient = OMSClientDep,
    ):
        return await ontology_extensions_service.update_resource(
            oms_client=oms_client,
            db_name=db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            payload=payload,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

    async def delete_route(
        db_name: str,
        resource_id: str,
        branch: str = Query(..., description="Target branch"),
        expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
        oms_client: OMSClient = OMSClientDep,
    ):
        return await ontology_extensions_service.delete_resource(
            oms_client=oms_client,
            db_name=db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
            expected_head_commit=expected_head_commit,
        )

    return list_route, create_route, get_route, update_route, delete_route


for _resource_type in (
    "shared-properties",
    "value-types",
    "interfaces",
    "groups",
    "functions",
    "action-types",
):
    _list, _create, _get, _update, _delete = _resource_routes(_resource_type)
    router.add_api_route(f"/{_resource_type}", _list, methods=["GET"])
    router.add_api_route(f"/{_resource_type}", _create, methods=["POST"], status_code=status.HTTP_201_CREATED)
    router.add_api_route(f"/{_resource_type}/{{resource_id}}", _get, methods=["GET"])
    router.add_api_route(f"/{_resource_type}/{{resource_id}}", _update, methods=["PUT"])
    router.add_api_route(f"/{_resource_type}/{{resource_id}}", _delete, methods=["DELETE"])


@router.get("/branches")
async def list_ontology_branches(
    db_name: str,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.list_ontology_branches(oms_client=oms_client, db_name=db_name)


@router.post("/branches", status_code=status.HTTP_201_CREATED)
async def create_ontology_branch(
    db_name: str,
    request: BranchCreateRequest,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.create_ontology_branch(oms_client=oms_client, db_name=db_name, request=request)


@router.get("/proposals")
async def list_ontology_proposals(
    db_name: str,
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = Query(100, ge=1, le=1000),
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.list_ontology_proposals(
        oms_client=oms_client,
        db_name=db_name,
        status_filter=status_filter,
        limit=limit,
    )


@router.post("/proposals", status_code=status.HTTP_201_CREATED)
async def create_ontology_proposal(
    db_name: str,
    request: OntologyProposalRequest,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.create_ontology_proposal(
        oms_client=oms_client,
        db_name=db_name,
        request=request,
    )


@router.post("/proposals/{proposal_id}/approve")
async def approve_ontology_proposal(
    db_name: str,
    proposal_id: str,
    request: OntologyApproveRequest,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.approve_ontology_proposal(
        oms_client=oms_client,
        db_name=db_name,
        proposal_id=proposal_id,
        request=request,
    )


@router.post("/deploy")
async def deploy_ontology(
    db_name: str,
    request: OntologyDeployRequest,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.deploy_ontology(
        oms_client=oms_client,
        db_name=db_name,
        request=request,
    )


@router.get("/health")
async def ontology_health(
    db_name: str,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_extensions_service.ontology_health(oms_client=oms_client, db_name=db_name, branch=branch)
