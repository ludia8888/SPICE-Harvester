"""Object type contract endpoints (BFF).

The router stays thin by delegating business logic to a service module
(Facade / Service Layer). Test patch points for role enforcement are preserved.
"""

import base64
from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

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
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc


def _unwrap_data(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _extract_resources(payload: Any) -> List[Dict[str, Any]]:
    data = _unwrap_data(payload)
    resources = data.get("resources") if isinstance(data, dict) else None
    if not isinstance(resources, list):
        return []
    return [entry for entry in resources if isinstance(entry, dict)]


def _extract_resource(payload: Any) -> Dict[str, Any]:
    data = _unwrap_data(payload)
    if isinstance(data, dict):
        return data
    return {}


def _decode_page_token(page_token: Optional[str]) -> int:
    if page_token is None:
        return 0
    token = str(page_token).strip()
    if not token:
        return 0
    try:
        padding = "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(f"{token}{padding}".encode("ascii")).decode("utf-8")
        offset = int(decoded)
    except Exception as exc:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "pageToken must be base64-encoded non-negative integer offset",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        ) from exc
    if offset < 0:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "pageToken offset must be >= 0",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return offset


def _encode_page_token(offset: int) -> str:
    raw = str(max(0, int(offset))).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _localized_text(value: Any) -> Optional[str]:
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


def _extract_ontology_properties(payload: Any) -> List[Dict[str, Any]]:
    data = _unwrap_data(payload)
    properties = data.get("properties") if isinstance(data, dict) else None
    if not isinstance(properties, list):
        return []
    return [entry for entry in properties if isinstance(entry, dict)]


def _normalize_foundry_data_type(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        nested_type = value.get("type")
        if isinstance(nested_type, str) and nested_type.strip():
            value = nested_type
        else:
            value = value.get("@class")

    raw = str(value or "").strip()
    if not raw:
        return None

    lowered = raw.lower()
    if lowered.startswith("xsd:"):
        lowered = lowered.split(":", 1)[1]

    mapping = {
        "string": "string",
        "normalizedstring": "string",
        "token": "string",
        "int": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "decimal": "decimal",
        "boolean": "boolean",
        "datetime": "timestamp",
        "date": "date",
        "time": "time",
        "json": "json",
    }
    return mapping.get(lowered, lowered)


def _to_foundry_data_type(value: Any) -> Optional[Dict[str, Any]]:
    type_name = _normalize_foundry_data_type(value)
    if not type_name:
        return None
    return {"type": type_name}


def _to_foundry_object_type(resource: Dict[str, Any], *, ontology_payload: Any) -> Dict[str, Any]:
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    pk_spec = spec.get("pk_spec") if isinstance(spec.get("pk_spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}

    primary_key = None
    raw_primary_keys = pk_spec.get("primary_key")
    if isinstance(raw_primary_keys, list):
        for value in raw_primary_keys:
            candidate = str(value or "").strip()
            if candidate:
                primary_key = candidate
                break

    title_property = None
    raw_title_keys = pk_spec.get("title_key")
    if isinstance(raw_title_keys, list):
        for value in raw_title_keys:
            candidate = str(value or "").strip()
            if candidate:
                title_property = candidate
                break

    properties: Dict[str, Dict[str, Any]] = {}
    for prop in _extract_ontology_properties(ontology_payload):
        api_name = str(prop.get("name") or prop.get("id") or "").strip()
        if not api_name:
            continue

        item: Dict[str, Any] = {}
        display_name = _localized_text(prop.get("label"))
        if display_name:
            item["displayName"] = display_name
        description = _localized_text(prop.get("description"))
        if description:
            item["description"] = description

        data_type = _to_foundry_data_type(prop.get("type") or prop.get("data_type"))
        if data_type:
            item["dataType"] = data_type

        required = prop.get("required")
        if isinstance(required, bool):
            item["required"] = required
        prop_status = str(prop.get("status") or "ACTIVE").strip().upper()
        if prop_status:
            item["status"] = prop_status
        prop_rid = str(prop.get("rid") or "").strip()
        if prop_rid:
            item["rid"] = prop_rid
        properties[api_name] = item

    out: Dict[str, Any] = {
        "apiName": str(resource.get("id") or "").strip(),
        "status": str(spec.get("status") or "ACTIVE").strip().upper() or "ACTIVE",
    }
    display_name = _localized_text(resource.get("label")) or out["apiName"]
    if display_name:
        out["displayName"] = display_name
    plural_display_name = (
        _localized_text(resource.get("plural_label"))
        or _localized_text(spec.get("plural_display_name"))
        or _localized_text(metadata.get("pluralDisplayName"))
    )
    if not plural_display_name and display_name:
        plural_display_name = display_name
    if plural_display_name:
        out["pluralDisplayName"] = plural_display_name
    description = _localized_text(resource.get("description"))
    if description:
        out["description"] = description
    if primary_key:
        out["primaryKey"] = primary_key
    if title_property:
        out["titleProperty"] = title_property
    if properties:
        out["properties"] = properties
    visibility = str(spec.get("visibility") or resource.get("visibility") or "").strip().upper()
    out["visibility"] = visibility or "NORMAL"
    icon = resource.get("icon")
    if not isinstance(icon, dict):
        icon = spec.get("icon") if isinstance(spec.get("icon"), dict) else metadata.get("icon")
    if isinstance(icon, dict):
        out["icon"] = icon
    rid = str(resource.get("rid") or "").strip()
    if rid:
        out["rid"] = rid
    return out


@router.get("/object-types", response_model=ApiResponse)
@trace_endpoint("bff.object_types.list_object_type_contracts")
async def list_object_type_contracts(
    db_name: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: Optional[str] = Query(default=None, alias="pageToken"),
    oms_client: OMSClient = OMSClientDep,
) -> ApiResponse:
    db_name = validate_db_name(db_name)
    await _require_domain_role(request, db_name=db_name)

    offset = _decode_page_token(page_token)
    resources_payload = await oms_client.list_ontology_resources(
        db_name,
        resource_type="object_type",
        branch=branch,
        limit=page_size,
        offset=offset,
    )
    resources = _extract_resources(resources_payload)

    object_types: List[Dict[str, Any]] = []
    for resource in resources:
        class_id = str(resource.get("id") or "").strip()
        if not class_id:
            continue
        ontology_payload: Any = None
        try:
            ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
        except Exception as exc:  # pragma: no cover - enrichment best-effort
            logger.warning(
                "Failed to enrich object type metadata (%s/%s): %s",
                db_name,
                class_id,
                exc,
            )
        object_types.append(_to_foundry_object_type(resource, ontology_payload=ontology_payload))

    next_page_token = _encode_page_token(offset + len(resources)) if len(resources) == page_size else None
    return ApiResponse.success(
        message="Object types retrieved",
        data={
            "data": object_types,
            "nextPageToken": next_page_token,
        },
    )


@router.post("/object-types", status_code=status.HTTP_201_CREATED, response_model=ApiResponse)
@trace_endpoint("bff.object_types.create_object_type_contract")
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
@trace_endpoint("bff.object_types.get_object_type_contract")
async def get_object_type_contract(
    db_name: str,
    class_id: str,
    request: Request,
    branch: str = Query("main", description="Target branch"),
    oms_client: OMSClient = OMSClientDep,
) -> ApiResponse:
    db_name = validate_db_name(db_name)
    await _require_domain_role(request, db_name=db_name)
    class_id = str(class_id or "").strip()
    if not class_id:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "class_id is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    try:
        resource_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=class_id,
            branch=branch,
        )
        resource = _extract_resource(resource_payload)
        if not resource:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                "Object type not found",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )
        ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
        object_type = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
        if not object_type.get("apiName"):
            object_type["apiName"] = class_id
        return ApiResponse.success(message="Object type retrieved", data=object_type)
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == status.HTTP_404_NOT_FOUND:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                "Object type not found",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            ) from exc
        try:
            detail: Any = exc.response.json()
        except Exception:
            detail = exc.response.text if exc.response is not None else str(exc)
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        raise classified_http_exception(status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get object type: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


@router.put("/object-types/{class_id}", response_model=ApiResponse)
@trace_endpoint("bff.object_types.update_object_type_contract")
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
