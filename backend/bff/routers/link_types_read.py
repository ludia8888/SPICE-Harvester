"""Link type read endpoints (BFF).

Composed by `bff.routers.link_types` via router composition (Composite pattern).
"""

import logging
from shared.observability.tracing import trace_endpoint

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from shared.errors.error_types import ErrorCode, classified_http_exception

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


def _unwrap_data(payload):
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _extract_resources(payload):
    data = _unwrap_data(payload)
    resources = data.get("resources") if isinstance(data, dict) else None
    if not isinstance(resources, list):
        return []
    return [entry for entry in resources if isinstance(entry, dict)]


def _normalize_object_ref(raw):
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    for prefix in ("object_type:", "object:", "class:"):
        if value.startswith(prefix):
            value = value[len(prefix) :].strip()
            break
    if "@" in value:
        value = value.split("@", 1)[0].strip()
    return value or None


def _localized_text(value):
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        for key in ("en", "ko"):
            key_value = value.get(key)
            if isinstance(key_value, str) and key_value.strip():
                return key_value.strip()
        for key_value in value.values():
            if isinstance(key_value, str) and key_value.strip():
                return key_value.strip()
    return None


def _map_cardinality(raw):
    value = str(raw or "").strip().lower()
    if value in {"1:1", "n:1", "one"}:
        return "ONE"
    if value in {"1:n", "n:1+", "n:m", "n:n", "many"}:
        return "MANY"
    if value == "m:1":
        return "ONE"
    return None


def _to_foundry_outgoing_link_type(resource, *, source_object_type):
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    from_ref = _normalize_object_ref(spec.get("from") if isinstance(spec, dict) else None)
    if from_ref is None:
        from_ref = _normalize_object_ref(resource.get("from"))
    if from_ref != source_object_type:
        return None

    to_ref = _normalize_object_ref(spec.get("to") if isinstance(spec, dict) else None)
    if to_ref is None:
        to_ref = _normalize_object_ref(resource.get("to"))

    relationship_spec = spec.get("relationship_spec") if isinstance(spec.get("relationship_spec"), dict) else {}
    if not to_ref and isinstance(relationship_spec, dict):
        to_ref = _normalize_object_ref(relationship_spec.get("target_object_type"))

    link_type_api_name = str(resource.get("id") or "").strip()
    if not link_type_api_name:
        return None

    out = {"apiName": link_type_api_name}
    if to_ref:
        out["objectTypeApiName"] = to_ref

    display_name = _localized_text(resource.get("label"))
    if display_name:
        out["displayName"] = display_name

    status_value = str(spec.get("status") or resource.get("status") or "ACTIVE").strip().upper()
    out["status"] = status_value or "ACTIVE"

    cardinality = _map_cardinality(spec.get("cardinality") if isinstance(spec, dict) else None)
    if cardinality is None:
        cardinality = _map_cardinality(resource.get("cardinality"))
    if cardinality:
        out["cardinality"] = cardinality

    foreign_key_property = None
    if isinstance(relationship_spec, dict):
        foreign_key_property = relationship_spec.get("fk_column") or relationship_spec.get("source_key_column")
    if isinstance(foreign_key_property, str) and foreign_key_property.strip():
        out["foreignKeyPropertyApiName"] = foreign_key_property.strip()

    link_type_rid = str(resource.get("rid") or "").strip()
    if link_type_rid:
        out["linkTypeRid"] = link_type_rid
    return out


@router.get("/link-types", response_model=ApiResponse)
@trace_endpoint("bff.link_types.list_link_types")
async def list_link_types(
    db_name: str,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        payload = await oms_client.list_ontology_resources(db_name, resource_type="link_type", branch=branch)
        items = _extract_resources(payload)

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
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


@router.get("/link-types/{link_type_id}", response_model=ApiResponse)
@trace_endpoint("bff.link_types.get_link_type")
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
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "link_type_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        try:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type_id,
                branch=branch,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Link type not found", code=ErrorCode.RESOURCE_NOT_FOUND) from exc
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
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)
