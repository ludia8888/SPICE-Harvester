"""Object type contract endpoints (BFF).

The router stays thin by delegating business logic to a service module
(Facade / Service Layer). Test patch points for role enforcement are preserved.
"""

from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.dependencies import OMSClientDep
from bff.routers.object_types_deps import get_dataset_registry, get_objectify_registry
from bff.schemas.object_types_requests import ObjectTypeContractRequest, ObjectTypeContractUpdate
from bff.services.database_role_guard import enforce_database_role_or_http_error
from bff.services import object_type_contract_service
from bff.services.oms_client import OMSClient
from shared.models.requests import ApiResponse
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.utils.key_spec import derive_key_spec_from_properties, normalize_key_spec
from shared.utils.language import first_localized_text
from shared.utils.ontology_fields import list_ontology_properties
from shared.utils.payload_utils import extract_payload_object, extract_payload_rows, unwrap_data_payload

logger = logging.getLogger(__name__)

# Public v1 object-type contract routes are code-deleted from BFF runtime.
# This module is retained for internal helper logic and direct service-level tests.
router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Object Types"])


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    await enforce_database_role_or_http_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=DOMAIN_MODEL_ROLES,
        enforce_fn=enforce_database_role,
    )


def _unwrap_data(payload: Any) -> Dict[str, Any]:
    return unwrap_data_payload(payload)


def _extract_resources(payload: Any) -> List[Dict[str, Any]]:
    return extract_payload_rows(payload, key="resources")


def _extract_resource(payload: Any) -> Dict[str, Any]:
    return extract_payload_object(payload)


def _localized_text(value: Any) -> Optional[str]:
    return first_localized_text(value)


def _extract_ontology_properties(payload: Any) -> List[Dict[str, Any]]:
    return list_ontology_properties(payload)


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
    normalized_pk_spec = normalize_key_spec(pk_spec)
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    class_api_name = str(resource.get("id") or "").strip()

    primary_key = None
    raw_primary_keys = normalized_pk_spec.get("primary_key") or []
    if isinstance(raw_primary_keys, list):
        for value in raw_primary_keys:
            candidate = str(value or "").strip()
            if candidate:
                primary_key = candidate
                break

    title_property = None
    raw_title_keys = normalized_pk_spec.get("title_key") or []
    if isinstance(raw_title_keys, list):
        for value in raw_title_keys:
            candidate = str(value or "").strip()
            if candidate:
                title_property = candidate
                break

    normalized_properties: List[Dict[str, Any]] = []
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
        normalized_properties.append({**prop, "name": api_name})

        prop_status = str(prop.get("status") or "ACTIVE").strip().upper()
        if prop_status:
            item["status"] = prop_status
        prop_rid = str(prop.get("rid") or "").strip()
        if prop_rid:
            item["rid"] = prop_rid
        properties[api_name] = item

    derived_pk_spec = derive_key_spec_from_properties(normalized_properties)
    inferred_primary_keys = derived_pk_spec.get("primary_key") or []
    inferred_title_keys = derived_pk_spec.get("title_key") or []

    out: Dict[str, Any] = {
        "apiName": class_api_name,
        "status": str(spec.get("status") or "ACTIVE").strip().upper() or "ACTIVE",
    }

    if not primary_key:
        if inferred_primary_keys:
            primary_key = inferred_primary_keys[0]
        else:
            expected_pk = f"{class_api_name.lower()}_id" if class_api_name else ""
            if expected_pk and expected_pk in properties:
                primary_key = expected_pk
            elif "id" in properties:
                primary_key = "id"

    if not title_property:
        if inferred_title_keys:
            title_property = inferred_title_keys[0]
        elif "name" in properties:
            title_property = "name"
        elif primary_key:
            title_property = primary_key

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
