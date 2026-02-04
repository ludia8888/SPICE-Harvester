"""Link type read endpoints (BFF).

Composed by `bff.routers.link_types` via router composition (Composite pattern).
"""

import logging

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.dependencies import OMSClientDep
from bff.routers.role_deps import require_database_role
from bff.routers.link_types_deps import get_dataset_registry
from bff.services.oms_client import OMSClient
from shared.models.requests import ApiResponse
from shared.security.database_access import DATA_ENGINEER_ROLES, DOMAIN_MODEL_ROLES
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Ontology Link Types"])

LINK_EDIT_ROLES = DOMAIN_MODEL_ROLES | DATA_ENGINEER_ROLES

require_link_edit_role = require_database_role(LINK_EDIT_ROLES)


@router.get("/link-types", response_model=ApiResponse)
async def list_link_types(
    db_name: str,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        resources = await oms_client.list_ontology_resources(db_name, resource_type="link_type", branch=branch)
        items = resources.get("resources") if isinstance(resources, dict) else None
        if not isinstance(items, list):
            items = []

        enriched = []
        for entry in items:
            if not isinstance(entry, dict):
                continue
            link_id = str(entry.get("id") or "").strip()
            relationship_spec = None
            if link_id:
                record = await dataset_registry.get_relationship_spec(link_type_id=link_id)
                if record:
                    relationship_spec = record.__dict__
            enriched.append({"link_type": entry, "relationship_spec": relationship_spec})

        return ApiResponse.success(
            message="Link types retrieved",
            data={"link_types": enriched, "total": len(enriched)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list link types: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.get("/link-types/{link_type_id}", response_model=ApiResponse)
async def get_link_type(
    db_name: str,
    link_type_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    _: None = Depends(require_link_edit_role),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")

        try:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type_id,
                branch=branch,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link type not found") from exc
            raise
        resource = payload.get("data") if isinstance(payload, dict) else payload

        relationship_spec = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        return ApiResponse.success(
            message="Link type retrieved",
            data={
                "link_type": resource,
                "relationship_spec": relationship_spec.__dict__ if relationship_spec else None,
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get link type: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
