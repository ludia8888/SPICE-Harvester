"""
BFF router for ontology extensions (resources, governance, health).
"""

import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from shared.models.requests import BranchCreateRequest
from shared.security.input_sanitizer import sanitize_input, validate_db_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}/ontology", tags=["Ontology Extensions"])


def _extract_httpx_detail(exc: httpx.HTTPStatusError) -> Any:
    detail: Any = exc.response.text
    try:
        detail_json = exc.response.json()
        if isinstance(detail_json, dict):
            detail = detail_json.get("detail") or detail_json
    except Exception:
        pass
    return detail


class OntologyResourceRequest(BaseModel):
    id: Optional[str] = Field(None, description="Resource id (optional)")
    label: Any = Field(..., description="Resource label")
    description: Optional[Any] = Field(None, description="Resource description")
    spec: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class OntologyProposalRequest(BaseModel):
    source_branch: str = Field(..., description="Source branch name")
    target_branch: str = Field("main", description="Target branch name")
    title: str = Field(..., description="Proposal title")
    description: Optional[str] = Field(None, description="Proposal description")
    author: str = Field("system", description="Proposal author")


class OntologyApproveRequest(BaseModel):
    merge_message: Optional[str] = Field(None, description="Merge message override")
    author: str = Field("system", description="Approval author")


class OntologyDeployRequest(BaseModel):
    proposal_id: str = Field(..., description="Proposal (pull request) id")
    ontology_commit_id: str = Field(..., description="Approved ontology commit id")
    merge_message: Optional[str] = Field(None, description="Merge message override")
    author: str = Field("system", description="Deploy author")
    definition_hash: Optional[str] = Field(None, description="Ontology definition hash")


async def _list_resources(
    oms_client: OMSClient,
    db_name: str,
    *,
    resource_type: Optional[str],
    branch: str,
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    return await oms_client.list_ontology_resources(
        db_name,
        resource_type=resource_type,
        branch=branch,
        limit=limit,
        offset=offset,
    )


async def _create_resource(
    oms_client: OMSClient,
    db_name: str,
    *,
    resource_type: str,
    payload: OntologyResourceRequest,
    branch: str,
    expected_head_commit: Optional[str],
) -> Dict[str, Any]:
    sanitized = sanitize_input(payload.model_dump(exclude_unset=True))
    return await oms_client.create_ontology_resource(
        db_name,
        resource_type=resource_type,
        payload=sanitized,
        branch=branch,
        expected_head_commit=expected_head_commit,
    )


async def _update_resource(
    oms_client: OMSClient,
    db_name: str,
    *,
    resource_type: str,
    resource_id: str,
    payload: OntologyResourceRequest,
    branch: str,
    expected_head_commit: Optional[str],
) -> Dict[str, Any]:
    sanitized = sanitize_input(payload.model_dump(exclude_unset=True))
    return await oms_client.update_ontology_resource(
        db_name,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=sanitized,
        branch=branch,
        expected_head_commit=expected_head_commit,
    )


def _resource_routes(resource_type: str):
    async def list_route(
        db_name: str,
        branch: str = Query("main", description="Target branch"),
        limit: int = Query(200, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        oms_client: OMSClient = OMSClientDep,
    ):
        try:
            db_name = validate_db_name(db_name)
            return await _list_resources(
                oms_client,
                db_name,
                resource_type=resource_type,
                branch=branch,
                limit=limit,
                offset=offset,
            )
        except httpx.HTTPStatusError as e:
            detail = _extract_httpx_detail(e)
            raise HTTPException(status_code=e.response.status_code, detail=detail) from e
        except Exception as e:
            logger.error("Failed to list ontology resources: %s", e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    async def create_route(
        db_name: str,
        payload: OntologyResourceRequest,
        branch: str = Query(..., description="Target branch"),
        expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
        oms_client: OMSClient = OMSClientDep,
    ):
        try:
            db_name = validate_db_name(db_name)
            return await _create_resource(
                oms_client,
                db_name,
                resource_type=resource_type,
                payload=payload,
                branch=branch,
                expected_head_commit=expected_head_commit,
            )
        except httpx.HTTPStatusError as e:
            detail = _extract_httpx_detail(e)
            raise HTTPException(status_code=e.response.status_code, detail=detail) from e
        except Exception as e:
            logger.error("Failed to create ontology resource: %s", e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    async def get_route(
        db_name: str,
        resource_id: str,
        branch: str = Query("main", description="Target branch"),
        oms_client: OMSClient = OMSClientDep,
    ):
        try:
            db_name = validate_db_name(db_name)
            return await oms_client.get_ontology_resource(
                db_name,
                resource_type=resource_type,
                resource_id=resource_id,
                branch=branch,
            )
        except httpx.HTTPStatusError as e:
            detail = _extract_httpx_detail(e)
            raise HTTPException(status_code=e.response.status_code, detail=detail) from e
        except Exception as e:
            logger.error("Failed to get ontology resource: %s", e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    async def update_route(
        db_name: str,
        resource_id: str,
        payload: OntologyResourceRequest,
        branch: str = Query(..., description="Target branch"),
        expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
        oms_client: OMSClient = OMSClientDep,
    ):
        try:
            db_name = validate_db_name(db_name)
            return await _update_resource(
                oms_client,
                db_name,
                resource_type=resource_type,
                resource_id=resource_id,
                payload=payload,
                branch=branch,
                expected_head_commit=expected_head_commit,
            )
        except httpx.HTTPStatusError as e:
            detail = _extract_httpx_detail(e)
            raise HTTPException(status_code=e.response.status_code, detail=detail) from e
        except Exception as e:
            logger.error("Failed to update ontology resource: %s", e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    async def delete_route(
        db_name: str,
        resource_id: str,
        branch: str = Query(..., description="Target branch"),
        expected_head_commit: str = Query(..., description="Optimistic concurrency guard"),
        oms_client: OMSClient = OMSClientDep,
    ):
        try:
            db_name = validate_db_name(db_name)
            return await oms_client.delete_ontology_resource(
                db_name,
                resource_type=resource_type,
                resource_id=resource_id,
                branch=branch,
                expected_head_commit=expected_head_commit,
            )
        except httpx.HTTPStatusError as e:
            detail = _extract_httpx_detail(e)
            raise HTTPException(status_code=e.response.status_code, detail=detail) from e
        except Exception as e:
            logger.error("Failed to delete ontology resource: %s", e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return list_route, create_route, get_route, update_route, delete_route


shared_list, shared_create, shared_get, shared_update, shared_delete = _resource_routes("shared-properties")
value_list, value_create, value_get, value_update, value_delete = _resource_routes("value-types")
iface_list, iface_create, iface_get, iface_update, iface_delete = _resource_routes("interfaces")
group_list, group_create, group_get, group_update, group_delete = _resource_routes("groups")
func_list, func_create, func_get, func_update, func_delete = _resource_routes("functions")
action_list, action_create, action_get, action_update, action_delete = _resource_routes("action-types")


router.add_api_route("/shared-properties", shared_list, methods=["GET"])
router.add_api_route("/shared-properties", shared_create, methods=["POST"], status_code=status.HTTP_201_CREATED)
router.add_api_route("/shared-properties/{resource_id}", shared_get, methods=["GET"])
router.add_api_route("/shared-properties/{resource_id}", shared_update, methods=["PUT"])
router.add_api_route("/shared-properties/{resource_id}", shared_delete, methods=["DELETE"])

router.add_api_route("/value-types", value_list, methods=["GET"])
router.add_api_route("/value-types", value_create, methods=["POST"], status_code=status.HTTP_201_CREATED)
router.add_api_route("/value-types/{resource_id}", value_get, methods=["GET"])
router.add_api_route("/value-types/{resource_id}", value_update, methods=["PUT"])
router.add_api_route("/value-types/{resource_id}", value_delete, methods=["DELETE"])

router.add_api_route("/interfaces", iface_list, methods=["GET"])
router.add_api_route("/interfaces", iface_create, methods=["POST"], status_code=status.HTTP_201_CREATED)
router.add_api_route("/interfaces/{resource_id}", iface_get, methods=["GET"])
router.add_api_route("/interfaces/{resource_id}", iface_update, methods=["PUT"])
router.add_api_route("/interfaces/{resource_id}", iface_delete, methods=["DELETE"])

router.add_api_route("/groups", group_list, methods=["GET"])
router.add_api_route("/groups", group_create, methods=["POST"], status_code=status.HTTP_201_CREATED)
router.add_api_route("/groups/{resource_id}", group_get, methods=["GET"])
router.add_api_route("/groups/{resource_id}", group_update, methods=["PUT"])
router.add_api_route("/groups/{resource_id}", group_delete, methods=["DELETE"])

router.add_api_route("/functions", func_list, methods=["GET"])
router.add_api_route("/functions", func_create, methods=["POST"], status_code=status.HTTP_201_CREATED)
router.add_api_route("/functions/{resource_id}", func_get, methods=["GET"])
router.add_api_route("/functions/{resource_id}", func_update, methods=["PUT"])
router.add_api_route("/functions/{resource_id}", func_delete, methods=["DELETE"])

router.add_api_route("/action-types", action_list, methods=["GET"])
router.add_api_route("/action-types", action_create, methods=["POST"], status_code=status.HTTP_201_CREATED)
router.add_api_route("/action-types/{resource_id}", action_get, methods=["GET"])
router.add_api_route("/action-types/{resource_id}", action_update, methods=["PUT"])
router.add_api_route("/action-types/{resource_id}", action_delete, methods=["DELETE"])


@router.get("/branches")
async def list_ontology_branches(
    db_name: str,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        return await oms_client.list_ontology_branches(db_name)
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to list ontology branches: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/branches", status_code=status.HTTP_201_CREATED)
async def create_ontology_branch(
    db_name: str,
    request: BranchCreateRequest,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        payload = sanitize_input(request.model_dump(mode="json"))
        return await oms_client.create_ontology_branch(db_name, payload)
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to create ontology branch: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/proposals")
async def list_ontology_proposals(
    db_name: str,
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = Query(100, ge=1, le=1000),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        return await oms_client.list_ontology_proposals(
            db_name, status_filter=status_filter, limit=limit
        )
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to list ontology proposals: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/proposals", status_code=status.HTTP_201_CREATED)
async def create_ontology_proposal(
    db_name: str,
    request: OntologyProposalRequest,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        payload = sanitize_input(request.model_dump(mode="json"))
        return await oms_client.create_ontology_proposal(db_name, payload)
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to create ontology proposal: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/proposals/{proposal_id}/approve")
async def approve_ontology_proposal(
    db_name: str,
    proposal_id: str,
    request: OntologyApproveRequest,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        payload = sanitize_input(request.model_dump(mode="json"))
        return await oms_client.approve_ontology_proposal(db_name, proposal_id, payload)
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to approve ontology proposal: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/deploy")
async def deploy_ontology(
    db_name: str,
    request: OntologyDeployRequest,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        payload = sanitize_input(request.model_dump(mode="json"))
        return await oms_client.deploy_ontology(db_name, payload)
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to deploy ontology: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/health")
async def ontology_health(
    db_name: str,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = validate_db_name(db_name)
        return await oms_client.get_ontology_health(db_name, branch=branch)
    except httpx.HTTPStatusError as e:
        detail = _extract_httpx_detail(e)
        raise HTTPException(status_code=e.response.status_code, detail=detail) from e
    except Exception as e:
        logger.error("Failed to fetch ontology health: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
