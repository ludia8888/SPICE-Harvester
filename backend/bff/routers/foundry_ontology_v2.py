"""Foundry Ontologies v2 read-compat router.

Exposes a Foundry-style `/api/v2/ontologies/...` read surface on top of
existing OMS/BFF ontology resources.
"""

import asyncio
import logging
from typing import Any, Dict

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, Response, StreamingResponse

from bff.dependencies import OMSClientDep, get_redis_service
from bff.services.database_role_guard import enforce_database_role_or_permission_error
from bff.routers import foundry_ontology_v2_metadata as _metadata
from bff.routers import foundry_ontology_v2_object_rows as _object_rows
from bff.routers import foundry_ontology_v2_queries as _query_routes
from bff.routers import foundry_ontology_v2_read_routes as _read_routes
from bff.routers.foundry_ontology_v2_actions import (
    get_action_type_by_rid_route,
    get_action_type_route,
    list_action_types_route,
    _normalize_apply_action_response_payload,
    _resolve_apply_action_mode,
)
from bff.routers.foundry_ontology_v2_common import (
    _decode_page_token,
    _default_expected_head_commit,
    _encode_page_token,
    _extract_api_response_data,
)
from bff.routers import foundry_ontology_v2_media as _media
from bff.routers.foundry_ontology_v2_object_sets import (
    _collect_object_set_object_types,
    _is_search_around_object_set,
    _load_temporary_object_set,
    _resolve_object_set_definition,
    _resolve_object_set_definition_with_store,
    _resolve_object_set_object_type,
    _store_temporary_object_set,
    _temporary_object_set_store,
)
from bff.routers.foundry_ontology_v2_object_rows import (
    _build_backing_sources_from_v2_payload,
    _build_object_set_search_payload,
    _build_pk_spec_from_v2_payload,
    _build_primary_key_where,
    _collect_load_links_rows,
    _coerce_primary_key_values,
    _dedupe_rows_by_identity,
    _extract_object_set_where,
    _extract_linked_primary_keys,
    _extract_linked_primary_keys_page,
    _linked_primary_key_exists,
    _load_rows_for_multi_object_types,
    _load_rows_for_search_around_object_set,
    _load_rows_for_single_object_type,
    _normalize_link_type_values,
    _normalize_primary_key_text,
    _normalize_select_values,
    _pagination_scope,
    _parse_order_by,
    _project_row_with_required_fields,
    _resolve_object_primary_key_field,
    _resolve_source_object_type_from_row,
    _resolve_source_primary_key_from_row,
    _sort_rows_by_order_by,
    _strip_typed_ref,
    _to_int_or_none,
)
from bff.routers.foundry_ontology_v2_serialization import (
    _apply_query_execute_options,
    _coerce_optional_bool,
    _dict_or_none,
    _extract_ontology_resource,
    _extract_ontology_resource_rows,
    _extract_project_policy_inheritance,
    _extract_query_execution_plan,
    _first_non_none,
    _list_or_none,
    _localized_text,
    _materialize_query_execution_value,
    _resolve_query_execution_object_type,
    _resolve_query_placeholder_key,
    _to_foundry_action_type,
    _to_foundry_action_type_map,
    _to_foundry_named_metadata,
    _to_foundry_named_metadata_map,
    _to_foundry_ontology,
    _to_foundry_query_type,
    _to_foundry_query_type_map_key,
    _to_foundry_query_type_metadata_map,
)
from bff.routers.foundry_ontology_v2_errors import (
    ApiFeaturePreviewUsageOnlyError,
    ObjectSetNotFoundError,
    OntologyNotFoundError,
    PermissionDeniedError,
    _ONTOLOGY_HANDLED_EXCEPTIONS,
    _error_parameters,
    _foundry_error,
    _internal_error_response,
    _not_found_error,
    _passthrough_upstream_error_payload,
    _permission_denied,
    _preflight_error_response,
    _service_http_error_response,
    _upstream_status_error_response,
    _upstream_transport_error_response,
)
from bff.routers.object_types_deps import get_dataset_registry, get_objectify_registry
from bff.routers.link_types_read import (
    _extract_resources as _extract_link_resources,
    _normalize_object_ref,
    _to_foundry_incoming_link_type,
    _to_foundry_outgoing_link_type,
)
from bff.routers.object_types import (
    _extract_resource as _extract_object_resource,
    _extract_resources as _extract_object_resources,
    _to_foundry_object_type,
)
from bff.routers.foundry_ontology_v2_models import (
    ApplyActionRequestOptionsV2,
    ApplyActionRequestV2,
    BatchApplyActionRequestItemV2,
    BatchApplyActionRequestOptionsV2,
    BatchApplyActionRequestV2,
    ExecuteQueryRequestV2,
    ObjectTypeContractCreateRequestV2,
    ObjectTypeContractUpdateRequestV2,
)
from bff.schemas.object_types_requests import ObjectTypeContractRequest, ObjectTypeContractUpdate
from bff.services.oms_client import OMSClient
from bff.services import object_type_contract_service
from shared.config.settings import get_settings
from shared.foundry.auth import require_scopes
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, READ_ROLES, enforce_database_role, resolve_database_actor
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_db_name,
)
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.storage.redis_service import RedisService
from shared.utils.payload_utils import extract_payload_object

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/ontologies", tags=["Foundry Ontologies v2"])

_SEARCH_ROUTING_AUDIT_STORE = _object_rows._SEARCH_ROUTING_AUDIT_STORE
_SEARCH_ROUTING_AUDIT_DISABLED_UNTIL = _object_rows._SEARCH_ROUTING_AUDIT_DISABLED_UNTIL
_SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS = _object_rows._SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS
AuditLogStore = _object_rows.AuditLogStore
time = _object_rows.time


async def _audit_search_around_compute_routing(**kwargs: Any) -> None:
    global _SEARCH_ROUTING_AUDIT_STORE, _SEARCH_ROUTING_AUDIT_DISABLED_UNTIL, _SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS
    _object_rows._SEARCH_ROUTING_AUDIT_STORE = _SEARCH_ROUTING_AUDIT_STORE
    _object_rows._SEARCH_ROUTING_AUDIT_DISABLED_UNTIL = _SEARCH_ROUTING_AUDIT_DISABLED_UNTIL
    _object_rows._SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS = _SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS
    _object_rows.AuditLogStore = AuditLogStore
    _object_rows.time = time
    try:
        await _object_rows._audit_search_around_compute_routing(**kwargs)
    finally:
        _SEARCH_ROUTING_AUDIT_STORE = _object_rows._SEARCH_ROUTING_AUDIT_STORE
        _SEARCH_ROUTING_AUDIT_DISABLED_UNTIL = _object_rows._SEARCH_ROUTING_AUDIT_DISABLED_UNTIL
        _SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS = _object_rows._SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS


async def _resolve_object_set_definition(object_set: Any) -> dict[str, Any]:
    if isinstance(object_set, dict):
        return dict(object_set)
    if isinstance(object_set, str):
        raise ValueError("Temporary objectSet RID resolution requires redis_service")
    raise ValueError("objectSet is required")


async def _resolve_object_set_definition_with_store(
    object_set: Any,
    *,
    redis_service: RedisService,
) -> dict[str, Any]:
    if isinstance(object_set, dict):
        return dict(object_set)
    if isinstance(object_set, str):
        return await _load_temporary_object_set(object_set, redis_service=redis_service)
    raise ValueError("objectSet is required")


_object_rows._SEARCH_ROUTING_AUDIT_CALLBACK = lambda **kwargs: _audit_search_around_compute_routing(**kwargs)

_FORWARDED_ACTOR_HEADER_KEYS = (
    "X-User-ID",
    "X-User-Type",
    "X-User",
    "X-Actor",
    "X-User-Roles",
)

_ONTOLOGY_READ = require_scopes(["api:ontologies-read"])
_ONTOLOGY_WRITE = require_scopes(["api:ontologies-write"])
_QUERY_TYPE_PRIMARY_BRANCH = "main"
_QUERY_TYPE_FALLBACK_BRANCH = "main"

_rid_component = _metadata._rid_component
_default_object_type_rid = _metadata._default_object_type_rid
_default_property_rid = _metadata._default_property_rid
_default_link_type_rid = _metadata._default_link_type_rid
_default_property_contract = _metadata._default_property_contract
_strictify_foundry_object_type = _metadata._strictify_foundry_object_type
_strictify_outgoing_link_type = _metadata._strictify_outgoing_link_type
_strictify_object_type_full_metadata = _metadata._strictify_object_type_full_metadata
_log_strict_compat_summary = _metadata._log_strict_compat_summary
_full_metadata_branch_contract = _metadata._full_metadata_branch_contract
_extract_object_type_relationships = _metadata._extract_object_type_relationships
_derive_outgoing_link_types_from_relationships = _metadata._derive_outgoing_link_types_from_relationships
_extract_databases = _metadata._extract_databases
_find_resource_by_rid = _metadata._find_resource_by_rid
_group_outgoing_link_types_by_source = _metadata._group_outgoing_link_types_by_source
_group_incoming_link_types_by_target = _metadata._group_incoming_link_types_by_target
_strip_prefix = _metadata._strip_prefix
_normalize_interface_ref = _metadata._normalize_interface_ref
_normalize_shared_property_ref = _metadata._normalize_shared_property_ref
_coerce_string_list = _metadata._coerce_string_list
_ordered_unique = _metadata._ordered_unique
_extract_interface_implementations = _metadata._extract_interface_implementations
_extract_interface_names = _metadata._extract_interface_names
_extract_ontology_properties_payload = _metadata._extract_ontology_properties_payload
_extract_shared_property_type_mapping = _metadata._extract_shared_property_type_mapping
_to_foundry_object_type_full_metadata = _metadata._to_foundry_object_type_full_metadata
_list_all_resources_for_type = _metadata._list_all_resources_for_type
_looks_like_action_type_resource = _metadata._looks_like_action_type_resource
_list_action_type_resources_with_fallback = _metadata._list_action_type_resources_with_fallback
_find_action_type_resource_by_id = _metadata._find_action_type_resource_by_id
_find_action_type_resource_by_rid = _metadata._find_action_type_resource_by_rid
_list_resources_best_effort = _metadata._list_resources_best_effort
_list_action_type_resources_best_effort = _metadata._list_action_type_resources_best_effort
_get_ontology_payload_best_effort = _metadata._get_ontology_payload_best_effort


def _query_type_branch_candidates() -> tuple[str, ...]:
    branches: list[str] = []
    for candidate in (_QUERY_TYPE_PRIMARY_BRANCH, _QUERY_TYPE_FALLBACK_BRANCH):
        normalized = str(candidate or "").strip()
        if normalized and normalized not in branches:
            branches.append(normalized)
    return tuple(branches)


def _object_set_runtime_value_error_response(
    *,
    exc: ValueError,
    ontology: str,
    parameters: dict[str, Any],
) -> JSONResponse:
    message = str(exc or "")
    if message.startswith("LinkType not found:"):
        return _not_found_error(
            "LinkTypeNotFound",
            ontology=ontology,
            link_type=message.split(":", 1)[1].strip(),
            parameters=parameters,
        )
    if message.startswith("Object type not found:"):
        return _not_found_error(
            "ObjectTypeNotFound",
            ontology=ontology,
            object_type=message.split(":", 1)[1].strip(),
            parameters=parameters,
        )
    return _foundry_error(
        status.HTTP_400_BAD_REQUEST,
        error_code="INVALID_ARGUMENT",
        error_name="InvalidArgument",
        parameters={**parameters, "message": message or "Invalid argument"},
    )

def _is_foundry_v2_strict_compat_enabled(*, db_name: str | None) -> bool:
    _ = db_name
    return True
def _require_preview_true_for_strict_compat(
    *,
    preview: bool,
    strict_compat: bool,
    endpoint: str,
) -> None:
    if strict_compat and not preview:
        raise ApiFeaturePreviewUsageOnlyError(f"preview=true is required for {endpoint} in strict compat mode")


def _linked_object_parameters(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    link_type: str,
    linked_primary_key: str,
) -> Dict[str, Any]:
    return _error_parameters(
        ontology=ontology,
        object_type=object_type,
        primary_key=primary_key,
        link_type=link_type,
        linked_primary_key=linked_primary_key,
    )


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    request_method = str(request.scope.get("method") or "GET").upper()
    allow_if_registry_unavailable = request_method in {"GET", "HEAD", "OPTIONS"}
    await enforce_database_role_or_permission_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=DOMAIN_MODEL_ROLES,
        denied_error_factory=PermissionDeniedError,
        allow_if_registry_unavailable=allow_if_registry_unavailable,
        enforce_fn=enforce_database_role,
    )


async def _require_domain_write_role(request: Request, *, db_name: str) -> None:
    await enforce_database_role_or_permission_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=DOMAIN_MODEL_ROLES,
        denied_error_factory=PermissionDeniedError,
        allow_if_registry_unavailable=False,
        enforce_fn=enforce_database_role,
    )


async def _require_read_role(request: Request, *, db_name: str) -> None:
    await enforce_database_role_or_permission_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=READ_ROLES,
        denied_error_factory=PermissionDeniedError,
        allow_if_registry_unavailable=True,
        enforce_fn=enforce_database_role,
    )


def _extract_actor_forward_headers(request: Request) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key in _FORWARDED_ACTOR_HEADER_KEYS:
        value = str(request.headers.get(key) or "").strip()
        if value:
            headers[key] = value
    return headers


def _validate_ontology_db_name(ontology: str) -> str:
    return validate_db_name(str(ontology or "").strip())


def _validate_branch(branch: str) -> str:
    return validate_branch_name(str(branch or "").strip())


async def _resolve_ontology_db_name(*, ontology: str, oms_client: OMSClient) -> str:
    raw = str(ontology or "").strip()
    if not raw:
        raise ValueError("ontology is required")

    normalized_api_name: str | None = None
    try:
        normalized_api_name = _validate_ontology_db_name(raw)
    except (ValueError, SecurityViolationError):
        # Fall through to Foundry-style identifier resolution (apiName/rid).
        if any(ch.isspace() for ch in raw):
            raise ValueError("ontology is invalid")

    payload = await oms_client.list_databases()
    rows = _extract_databases(payload)
    lowered = raw.lower()
    normalized_lowered = normalized_api_name.lower() if normalized_api_name else None
    for row in rows:
        candidate_name = str(row.get("name") or row.get("db_name") or row.get("apiName") or "").strip()
        candidate_rid = str(row.get("rid") or row.get("ontologyRid") or "").strip()
        if not candidate_name:
            continue
        candidate_lowered = candidate_name.lower()
        if raw == candidate_rid or lowered == candidate_lowered:
            return _validate_ontology_db_name(candidate_name)
        if normalized_lowered is not None and normalized_lowered == candidate_lowered:
            return _validate_ontology_db_name(candidate_name)

    raise OntologyNotFoundError(f"ontology not found: {raw}")

@router.get("", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_ontologies")
async def list_ontologies_v2(
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.list_ontologies_route(
        request=request,
        oms_client=oms_client,
        extract_databases=_extract_databases,
        to_foundry_ontology=_to_foundry_ontology,
        foundry_error=_foundry_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.get("/{ontologyRid}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_ontology")
async def get_ontology_v2(
    ontologyRid: str,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.get_ontology_route(
        ontology=ontologyRid,
        request=request,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        to_foundry_ontology=_to_foundry_ontology,
        not_found_error=_not_found_error,
        foundry_error=_foundry_error,
    )


@router.get("/{ontologyRid}/fullMetadata", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_full_metadata")
async def get_full_metadata_v2(
    ontologyRid: str,
    request: Request,
    preview: bool = Query(False, description="Must be true for preview endpoints"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _metadata.get_full_metadata_route(
        ontology=ontologyRid,
        request=request,
        preview=preview,
        branch=branch,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        list_resources_best_effort=_list_resources_best_effort,
        list_action_type_resources_best_effort=_list_action_type_resources_best_effort,
        get_ontology_payload_best_effort=_get_ontology_payload_best_effort,
        group_outgoing_link_types_by_source=_group_outgoing_link_types_by_source,
        group_incoming_link_types_by_target=_group_incoming_link_types_by_target,
        to_foundry_object_type_full_metadata=_to_foundry_object_type_full_metadata,
        strictify_object_type_full_metadata=_strictify_object_type_full_metadata,
        to_foundry_ontology=_to_foundry_ontology,
        full_metadata_branch_contract=_full_metadata_branch_contract,
        to_foundry_action_type_map=_to_foundry_action_type_map,
        to_foundry_query_type_metadata_map=_to_foundry_query_type_metadata_map,
        to_foundry_named_metadata_map=_to_foundry_named_metadata_map,
        log_strict_compat_summary=_log_strict_compat_summary,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/actionTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_action_types")
async def list_action_types_v2(
    ontologyRid: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await list_action_types_route(
        ontology=ontologyRid,
        request=request,
        page_size=page_size,
        page_token=page_token,
        branch=branch,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        pagination_scope=_pagination_scope,
        decode_page_token=_decode_page_token,
        encode_page_token=_encode_page_token,
        extract_ontology_resource_rows=_extract_ontology_resource_rows,
        list_action_type_resources_with_fallback=_list_action_type_resources_with_fallback,
        to_foundry_action_type=_to_foundry_action_type,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/actionTypes/{actionTypeApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_action_type")
async def get_action_type_v2(
    ontologyRid: str,
    actionTypeApiName: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await get_action_type_route(
        ontology=ontologyRid,
        action_type_api_name=actionTypeApiName,
        request=request,
        branch=branch,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        extract_ontology_resource=_extract_ontology_resource,
        find_action_type_resource_by_id=_find_action_type_resource_by_id,
        to_foundry_action_type=_to_foundry_action_type,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/actionTypes/byRid/{actionTypeRid}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_action_type_by_rid")
async def get_action_type_by_rid_v2(
    ontologyRid: str,
    actionTypeRid: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await get_action_type_by_rid_route(
        ontology=ontologyRid,
        action_type_rid=actionTypeRid,
        request=request,
        branch=branch,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        find_resource_by_rid=_find_resource_by_rid,
        find_action_type_resource_by_rid=_find_action_type_resource_by_rid,
        to_foundry_action_type=_to_foundry_action_type,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.post("/{ontologyRid}/actions/{actionApiName}/apply", dependencies=[_ONTOLOGY_WRITE])
@trace_endpoint("bff.foundry_v2_ontology.apply_action")
async def apply_action_v2(
    ontologyRid: str,
    actionApiName: str,
    body: ApplyActionRequestV2,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    oms_client: OMSClient = OMSClientDep,
):
    ontology = ontologyRid
    action = actionApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        action_type = str(action or "").strip()
        if not action_type:
            raise ValueError("action is required")
        await _require_domain_role(request, db_name=db_name)
        principal_type, principal_id = resolve_database_actor(request.headers)
        metadata = {"user_id": principal_id, "user_type": principal_type}
        mode = _resolve_apply_action_mode(
            explicit_mode=(body.options.mode if body.options else None),
        )
        parameters = dict(body.parameters or {})
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"action": str(action)},
        )

    try:
        oms_payload: Dict[str, Any] = {
            "options": {"mode": mode},
            "parameters": parameters,
            "metadata": metadata,
        }
        response = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/actions/{action_type}/apply",
            params={
                "branch": branch,
                "sdkPackageRid": sdk_package_rid,
                "sdkVersion": sdk_version,
                "transactionId": transaction_id,
            },
            json=oms_payload,
        )
        normalized_response = _normalize_apply_action_response_payload(
            response=response,
            request_parameters=parameters,
        )
        return normalized_response
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"actionType": action_type},
            not_found_response=_not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"action": action_type},
            ),
            passthrough_payload=True,
            normalize_non_foundry_payload=True,
            action_surface=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionType": action_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to apply action (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"action": action_type},
        )


@router.post("/{ontologyRid}/actions/{actionApiName}/applyBatch", dependencies=[_ONTOLOGY_WRITE])
@trace_endpoint("bff.foundry_v2_ontology.apply_action_batch")
async def apply_action_batch_v2(
    ontologyRid: str,
    actionApiName: str,
    body: BatchApplyActionRequestV2,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    ontology = ontologyRid
    action = actionApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        action_type = str(action or "").strip()
        if not action_type:
            raise ValueError("action is required")
        await _require_domain_role(request, db_name=db_name)
        principal_type, principal_id = resolve_database_actor(request.headers)
        metadata = {"user_id": principal_id, "user_type": principal_type}
        requests = list(body.requests or [])
        if not requests:
            raise ValueError("requests must not be empty")
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"action": str(action)},
        )

    try:
        parameters_list = [dict(item.parameters or {}) for item in requests]
        oms_payload: Dict[str, Any] = {
            "requests": [{"parameters": params} for params in parameters_list],
            "metadata": metadata,
        }
        if body.options and body.options.return_edits:
            oms_payload["options"] = {"returnEdits": body.options.return_edits}
        response = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/actions/{action_type}/applyBatch",
            params={
                "branch": branch,
                "sdkPackageRid": sdk_package_rid,
                "sdkVersion": sdk_version,
            },
            json=oms_payload,
        )
        if isinstance(response, dict):
            return response
        return {}
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"actionType": action_type},
            not_found_response=_not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"action": action_type},
            ),
            passthrough_payload=True,
            normalize_non_foundry_payload=True,
            action_surface=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionType": action_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to apply action batch (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"action": action_type},
        )


@router.get("/{ontologyRid}/queryTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_query_types")
async def list_query_types_v2(
    ontologyRid: str,
    request: Request,
    page_size: int = Query(100, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.list_query_types_route(
        ontology=ontologyRid,
        request=request,
        page_size=page_size,
        page_token=page_token,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        pagination_scope=_pagination_scope,
        decode_page_token=_decode_page_token,
        encode_page_token=_encode_page_token,
        query_type_branch_candidates=_query_type_branch_candidates,
        extract_ontology_resource_rows=_extract_ontology_resource_rows,
        to_foundry_query_type=_to_foundry_query_type,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/queryTypes/{queryApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_query_type")
async def get_query_type_v2(
    ontologyRid: str,
    queryApiName: str,
    request: Request,
    version: str | None = Query(default=None),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    _ = sdk_package_rid, sdk_version
    return await _metadata.get_query_type_route(
        ontology=ontologyRid,
        query_api_name=queryApiName,
        request=request,
        version=version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        query_type_branch_candidates=_query_type_branch_candidates,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_query_type=_to_foundry_query_type,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.post("/{ontologyRid}/queries/{queryApiName}/execute", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.execute_query")
async def execute_query_v2(
    ontologyRid: str,
    queryApiName: str,
    body: ExecuteQueryRequestV2,
    request: Request,
    version: str | None = Query(default=None),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.execute_query_route(
        ontology=ontologyRid,
        query_api_name=queryApiName,
        body=body,
        request=request,
        version=version,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        transaction_id=transaction_id,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        require_domain_role=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        not_found_error=_not_found_error,
        foundry_error=_foundry_error,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        query_type_branch_candidates=_query_type_branch_candidates,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_query_type=_to_foundry_query_type,
        extract_query_execution_plan=_extract_query_execution_plan,
        apply_query_execute_options=_apply_query_execute_options,
        materialize_query_execution_value=_materialize_query_execution_value,
    )


@router.get("/{ontologyRid}/interfaceTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_interface_types")
async def list_interface_types_v2(
    ontologyRid: str,
    request: Request,
    preview: bool = Query(False),
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.list_named_ontology_resources_route(
        ontology=ontologyRid,
        request=request,
        preview=preview,
        page_size=page_size,
        page_token=page_token,
        branch=branch,
        oms_client=oms_client,
        resource_type="interface",
        collection_scope="v2/interfaceTypes",
        endpoint="ontologies/{ontologyRid}/interfaceTypes",
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_access=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        pagination_scope=_pagination_scope,
        decode_page_token=_decode_page_token,
        encode_page_token=_encode_page_token,
        extract_ontology_resource_rows=_extract_ontology_resource_rows,
        to_foundry_named_metadata=_to_foundry_named_metadata,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/interfaceTypes/{interfaceTypeApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_interface_type")
async def get_interface_type_v2(
    ontologyRid: str,
    interfaceTypeApiName: str,
    request: Request,
    preview: bool = Query(False),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    _ = sdk_package_rid, sdk_version
    return await _read_routes.get_named_ontology_resource_route(
        ontology=ontologyRid,
        resource_api_name=interfaceTypeApiName,
        request=request,
        preview=preview,
        branch=branch,
        oms_client=oms_client,
        resource_type="interface",
        resource_parameter="interfaceType",
        not_found_name="InterfaceTypeNotFound",
        endpoint="ontologies/{ontologyRid}/interfaceTypes/{interfaceTypeApiName}",
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_access=_require_domain_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_named_metadata=_to_foundry_named_metadata,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/sharedPropertyTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_shared_property_types")
async def list_shared_property_types_v2(
    ontologyRid: str,
    request: Request,
    preview: bool = Query(False),
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.list_named_ontology_resources_route(
        ontology=ontologyRid,
        request=request,
        preview=preview,
        page_size=page_size,
        page_token=page_token,
        branch=branch,
        oms_client=oms_client,
        resource_type="shared_property",
        collection_scope="v2/sharedPropertyTypes",
        endpoint="ontologies/{ontologyRid}/sharedPropertyTypes",
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_access=_require_read_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        pagination_scope=_pagination_scope,
        decode_page_token=_decode_page_token,
        encode_page_token=_encode_page_token,
        extract_ontology_resource_rows=_extract_ontology_resource_rows,
        to_foundry_named_metadata=_to_foundry_named_metadata,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/sharedPropertyTypes/{sharedPropertyTypeApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_shared_property_type")
async def get_shared_property_type_v2(
    ontologyRid: str,
    sharedPropertyTypeApiName: str,
    request: Request,
    preview: bool = Query(False),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.get_named_ontology_resource_route(
        ontology=ontologyRid,
        resource_api_name=sharedPropertyTypeApiName,
        request=request,
        preview=preview,
        branch=branch,
        oms_client=oms_client,
        resource_type="shared_property",
        resource_parameter="sharedPropertyType",
        not_found_name="SharedPropertyTypeNotFound",
        endpoint="ontologies/{ontologyRid}/sharedPropertyTypes/{sharedPropertyTypeApiName}",
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_access=_require_read_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_named_metadata=_to_foundry_named_metadata,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/valueTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_value_types")
async def list_value_types_v2(
    ontologyRid: str,
    request: Request,
    preview: bool = Query(False),
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.list_value_types_route(
        ontology=ontologyRid,
        request=request,
        preview=preview,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        list_all_resources_for_type=_list_all_resources_for_type,
        to_foundry_named_metadata=_to_foundry_named_metadata,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/valueTypes/{valueTypeApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_value_type")
async def get_value_type_v2(
    ontologyRid: str,
    valueTypeApiName: str,
    request: Request,
    preview: bool = Query(False),
    oms_client: OMSClient = OMSClientDep,
):
    return await _read_routes.get_named_ontology_resource_route(
        ontology=ontologyRid,
        resource_api_name=valueTypeApiName,
        request=request,
        preview=preview,
        branch="main",
        oms_client=oms_client,
        resource_type="value_type",
        resource_parameter="valueType",
        not_found_name="ValueTypeNotFound",
        endpoint="ontologies/{ontologyRid}/valueTypes/{valueTypeApiName}",
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=None,
        is_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        require_access=_require_read_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_named_metadata=_to_foundry_named_metadata,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.get("/{ontologyRid}/objectTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_object_types")
async def list_object_types_v2(
    ontologyRid: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        await _require_read_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/objectTypes", db_name, branch, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(exc, ontology=str(ontology))

    try:
        resources_payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="object_type",
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        resources = _extract_object_resources(resources_payload)

        object_types: list[Dict[str, Any]] = []
        strict_fix_count = 0
        for resource in resources:
            class_id = str(resource.get("id") or "").strip()
            if not class_id:
                continue
            ontology_payload: Any = None
            try:
                ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
            except httpx.HTTPError as exc:
                logger.warning(
                    "Failed to enrich object type metadata (%s/%s): %s",
                    db_name,
                    class_id,
                    exc,
                )
            mapped = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
            if strict_compat:
                mapped, fixes = _strictify_foundry_object_type(
                    mapped,
                    db_name=db_name,
                    object_type_hint=class_id,
                )
                strict_fix_count += fixes
            object_types.append(mapped)

        _log_strict_compat_summary(
            route="list_object_types_v2",
            db_name=db_name,
            branch=branch,
            fixes=strict_fix_count,
        )

        next_page_token = _encode_page_token(offset + len(resources), scope=page_scope) if len(resources) == page_size else None
        return {"data": object_types, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error("OntologyNotFound", ontology=db_name)
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to list object types (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name},
        )


@router.post("/{ontologyRid}/objectTypes", dependencies=[_ONTOLOGY_WRITE])
@trace_endpoint("bff.foundry_v2_ontology.create_object_type")
async def create_object_type_v2(
    ontologyRid: str,
    body: ObjectTypeContractCreateRequestV2,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    expected_head_commit: str | None = Query(default=None, alias="expectedHeadCommit"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        object_type = str(body.apiName or "").strip()
        if not object_type:
            raise ValueError("apiName is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(getattr(body, "apiName", "") or "")},
        )

    try:
        payload = body.model_dump(exclude_unset=True)
        resolved_expected_head_commit = (
            str(expected_head_commit or "").strip() or _default_expected_head_commit(branch)
        )
        request_payload = ObjectTypeContractRequest(
            class_id=object_type,
            backing_dataset_id=payload.get("backingDatasetId"),
            backing_datasource_id=payload.get("backingDatasourceId"),
            backing_datasource_version_id=payload.get("backingDatasourceVersionId"),
            backing_sources=_build_backing_sources_from_v2_payload(
                backing_source=payload.get("backingSource"),
                backing_sources=payload.get("backingSources"),
            ),
            dataset_version_id=payload.get("datasetVersionId"),
            schema_hash=payload.get("schemaHash"),
            pk_spec=_build_pk_spec_from_v2_payload(
                pk_spec=payload.get("pkSpec"),
                primary_key=payload.get("primaryKey"),
                title_property=payload.get("titleProperty"),
            ),
            mapping_spec_id=payload.get("mappingSpecId"),
            mapping_spec_version=payload.get("mappingSpecVersion"),
            status=payload.get("status") or "ACTIVE",
            auto_generate_mapping=bool(payload.get("autoGenerateMapping", False)),
            metadata=payload.get("metadata") or {},
        )
        service_response = await object_type_contract_service.create_object_type_contract(
            db_name=db_name,
            body=request_payload,
            request=request,
            branch=branch,
            expected_head_commit=resolved_expected_head_commit,
            oms_client=oms_client,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
        )
        service_data = _extract_api_response_data(service_response)
        resource = service_data.get("object_type") if isinstance(service_data.get("object_type"), dict) else {}
        if not resource:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="object_type",
                resource_id=object_type,
                branch=branch,
            )
            resource = _extract_object_resource(payload)
        ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        out = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
        if not out.get("apiName"):
            out["apiName"] = object_type
        return out
    except HTTPException as exc:
        return _service_http_error_response(exc, ontology=db_name, object_type=object_type)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to create object type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
        )


@router.patch("/{ontologyRid}/objectTypes/{objectTypeApiName}", dependencies=[_ONTOLOGY_WRITE])
@trace_endpoint("bff.foundry_v2_ontology.update_object_type")
async def update_object_type_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    body: ObjectTypeContractUpdateRequestV2,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    expected_head_commit: str | None = Query(default=None, alias="expectedHeadCommit"),
    oms_client: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
):
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType)},
        )

    try:
        payload = body.model_dump(exclude_unset=True)
        resolved_expected_head_commit = (
            str(expected_head_commit or "").strip() or _default_expected_head_commit(branch)
        )
        update_payload: Dict[str, Any] = {}

        if any(
            key in payload
            for key in ("primaryKey", "titleProperty", "pkSpec")
        ):
            update_payload["pk_spec"] = _build_pk_spec_from_v2_payload(
                pk_spec=payload.get("pkSpec"),
                primary_key=payload.get("primaryKey"),
                title_property=payload.get("titleProperty"),
            )
        if any(
            key in payload
            for key in ("backingSource", "backingSources")
        ):
            update_payload["backing_sources"] = _build_backing_sources_from_v2_payload(
                backing_source=payload.get("backingSource"),
                backing_sources=payload.get("backingSources"),
            )
        if "backingDatasetId" in payload:
            update_payload["backing_dataset_id"] = payload.get("backingDatasetId")
        if "backingDatasourceId" in payload:
            update_payload["backing_datasource_id"] = payload.get("backingDatasourceId")
        if "backingDatasourceVersionId" in payload:
            update_payload["backing_datasource_version_id"] = payload.get("backingDatasourceVersionId")
        if "datasetVersionId" in payload:
            update_payload["dataset_version_id"] = payload.get("datasetVersionId")
        if "schemaHash" in payload:
            update_payload["schema_hash"] = payload.get("schemaHash")
        if "mappingSpecId" in payload:
            update_payload["mapping_spec_id"] = payload.get("mappingSpecId")
        if "mappingSpecVersion" in payload:
            update_payload["mapping_spec_version"] = payload.get("mappingSpecVersion")
        if "status" in payload:
            update_payload["status"] = payload.get("status")
        if "metadata" in payload:
            update_payload["metadata"] = payload.get("metadata")
        if "migration" in payload:
            update_payload["migration"] = payload.get("migration")

        service_response = await object_type_contract_service.update_object_type_contract(
            db_name=db_name,
            class_id=object_type,
            body=ObjectTypeContractUpdate(**update_payload),
            request=request,
            branch=branch,
            expected_head_commit=resolved_expected_head_commit,
            oms_client=oms_client,
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
        )
        service_data = _extract_api_response_data(service_response)
        resource = service_data.get("object_type") if isinstance(service_data.get("object_type"), dict) else {}
        if not resource:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="object_type",
                resource_id=object_type,
                branch=branch,
            )
            resource = _extract_object_resource(payload)
        ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        out = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
        if not out.get("apiName"):
            out["apiName"] = object_type
        return out
    except HTTPException as exc:
        return _service_http_error_response(exc, ontology=db_name, object_type=object_type)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to update object type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
        )


@router.get("/{ontologyRid}/objectTypes/{objectTypeApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_object_type")
async def get_object_type_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=object_type,
            branch=branch,
        )
        resource = _extract_object_resource(payload)
        if not resource:
            return _not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type)
        ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        out = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
        if not out.get("apiName"):
            out["apiName"] = object_type
        if strict_compat:
            out, strict_fix_count = _strictify_foundry_object_type(
                out,
                db_name=db_name,
                object_type_hint=object_type,
            )
            _log_strict_compat_summary(
                route="get_object_type_v2",
                db_name=db_name,
                branch=branch,
                fixes=strict_fix_count,
            )
        return out
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type)
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to get object type (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


@router.get("/{ontologyRid}/objectTypes/{objectTypeApiName}/fullMetadata", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_object_type_full_metadata")
async def get_object_type_full_metadata_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/objectTypes/{objectTypeApiName}/fullMetadata",
        )
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType), "preview": preview},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=object_type,
            branch=branch,
        )
        resource = _extract_object_resource(payload)
        if not resource:
            return _not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type)

        ontology_payload: Any = None
        try:
            ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        except httpx.HTTPError as exc:
            logger.warning(
                "Failed to load object type full metadata ontology payload (%s/%s): %s",
                db_name,
                object_type,
                exc,
            )
            ontology_payload = None

        link_resources = await _list_all_resources_for_type(
            db_name=db_name,
            branch=branch,
            resource_type="link_type",
            oms_client=oms_client,
        )
        outgoing = _group_outgoing_link_types_by_source(link_resources).get(object_type) or []
        incoming = _group_incoming_link_types_by_target(link_resources).get(object_type) or []
        # Foundry ObjectTypeFullMetadata.linkTypes includes both directions.
        # Deduplicate by apiName to avoid showing the same link type twice when
        # from == to (self-referencing link types).
        seen_api_names: set[str] = set()
        merged_link_types: list[dict[str, Any]] = []
        for lt in outgoing + incoming:
            api_name = lt.get("apiName", "")
            key = (api_name, lt.get("objectTypeApiName", ""))
            if key not in seen_api_names:
                seen_api_names.add(key)
                merged_link_types.append(lt)
        out = _to_foundry_object_type_full_metadata(
            resource,
            ontology_payload=ontology_payload,
            link_types=merged_link_types,
        )
        if strict_compat:
            out, strict_fix_count, strict_dropped_count = _strictify_object_type_full_metadata(
                out,
                db_name=db_name,
                object_type_hint=object_type,
            )
            _log_strict_compat_summary(
                route="get_object_type_full_metadata_v2",
                db_name=db_name,
                branch=branch,
                fixes=strict_fix_count,
                dropped=strict_dropped_count,
            )
        return out
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectType": object_type},
            not_found_response=_not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectType": object_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get object type full metadata (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
        )


@router.get("/{ontologyRid}/objectTypes/{objectTypeApiName}/outgoingLinkTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_outgoing_link_types")
async def list_outgoing_link_types_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        source_object_type = str(objectType or "").strip()
        if not source_object_type:
            raise ValueError("objectType is required")
        await _require_read_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/outgoingLinkTypes", db_name, branch, source_object_type, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType)},
        )

    try:
        scan_limit = min(max(page_size, 500), 1000)
        scan_offset = 0
        filtered_index = 0
        data: list[dict] = []
        has_more = False
        strict_fix_count = 0
        strict_dropped_count = 0

        while not has_more:
            payload = await oms_client.list_ontology_resources(
                db_name,
                resource_type="link_type",
                branch=branch,
                limit=scan_limit,
                offset=scan_offset,
            )
            resources = _extract_link_resources(payload)
            if not resources:
                break
            for resource in resources:
                mapped = _to_foundry_outgoing_link_type(resource, source_object_type=source_object_type)
                if mapped is None:
                    continue
                if strict_compat:
                    mapped, fixes, is_resolved = _strictify_outgoing_link_type(
                        mapped,
                        db_name=db_name,
                        source_object_type=source_object_type,
                    )
                    strict_fix_count += fixes
                    if not is_resolved:
                        strict_dropped_count += 1
                        continue
                if filtered_index < offset:
                    filtered_index += 1
                    continue
                if len(data) < page_size:
                    data.append(mapped)
                    filtered_index += 1
                    continue
                has_more = True
                break
            scan_offset += len(resources)
            if len(resources) < scan_limit:
                break

        if not data:
            try:
                object_payload = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="object_type",
                    resource_id=source_object_type,
                    branch=branch,
                )
                object_resource = _extract_ontology_resource(object_payload)
                if isinstance(object_resource, dict):
                    derived_all = _derive_outgoing_link_types_from_relationships(
                        _extract_object_type_relationships(object_resource)
                    )
                    strict_all: list[dict[str, Any]] = []
                    for mapped in derived_all:
                        if not isinstance(mapped, dict):
                            continue
                        if strict_compat:
                            mapped, fixes, is_resolved = _strictify_outgoing_link_type(
                                dict(mapped),
                                db_name=db_name,
                                source_object_type=source_object_type,
                            )
                            strict_fix_count += fixes
                            if not is_resolved:
                                strict_dropped_count += 1
                                continue
                        strict_all.append(mapped)
                    start = max(0, int(offset))
                    data = strict_all[start : start + int(page_size)]
                    has_more = (start + len(data)) < len(strict_all)
            except httpx.HTTPStatusError:
                pass

        if strict_compat:
            _log_strict_compat_summary(
                route="list_outgoing_link_types_v2",
                db_name=db_name,
                branch=branch,
                fixes=strict_fix_count,
                dropped=strict_dropped_count,
            )

        next_offset = offset + len(data)
        next_page_token = _encode_page_token(next_offset, scope=page_scope) if has_more else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "OntologyNotFound",
                ontology=db_name,
                parameters={"objectType": source_object_type},
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": source_object_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to list outgoing link types (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": source_object_type},
        )


@router.get(
    "/{ontologyRid}/objectTypes/{objectTypeApiName}/outgoingLinkTypes/{linkTypeApiName}",
    dependencies=[_ONTOLOGY_READ],
)
@trace_endpoint("bff.foundry_v2_ontology.get_outgoing_link_type")
async def get_outgoing_link_type_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    linkTypeApiName: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    objectType = objectTypeApiName
    linkType = linkTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        source_object_type = str(objectType or "").strip()
        link_type = str(linkType or "").strip()
        if not source_object_type:
            raise ValueError("objectType is required")
        if not link_type:
            raise ValueError("linkType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType), "linkType": str(linkType)},
        )

    try:
        mapped: dict[str, Any] | None = None
        try:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type,
                branch=branch,
            )
            resource = _extract_ontology_resource(payload)
            if isinstance(resource, dict):
                mapped = _to_foundry_outgoing_link_type(resource, source_object_type=source_object_type)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
            if status_code != status.HTTP_404_NOT_FOUND:
                raise

        if mapped is None:
            try:
                object_payload = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="object_type",
                    resource_id=source_object_type,
                    branch=branch,
                )
                object_resource = _extract_ontology_resource(object_payload)
                if isinstance(object_resource, dict):
                    for rel in _extract_object_type_relationships(object_resource):
                        predicate = str(rel.get("predicate") or rel.get("apiName") or rel.get("id") or "").strip()
                        if predicate != link_type:
                            continue
                        target = str(
                            rel.get("target")
                            or rel.get("objectTypeApiName")
                            or rel.get("target_object_type")
                            or ""
                        ).strip()
                        if not target:
                            continue
                        mapped = {"apiName": predicate, "objectTypeApiName": target}
                        break
            except httpx.HTTPStatusError:
                mapped = None

        if mapped is None:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=source_object_type,
                link_type=link_type,
            )
        if strict_compat:
            mapped, strict_fix_count, is_resolved = _strictify_outgoing_link_type(
                mapped,
                db_name=db_name,
                source_object_type=source_object_type,
            )
            _log_strict_compat_summary(
                route="get_outgoing_link_type_v2",
                db_name=db_name,
                branch=branch,
                fixes=strict_fix_count,
                dropped=0 if is_resolved else 1,
            )
            if not is_resolved:
                return _not_found_error(
                    "LinkTypeNotFound",
                    ontology=db_name,
                    object_type=source_object_type,
                    link_type=link_type,
                )
        return mapped
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=source_object_type,
                link_type=link_type,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": source_object_type, "linkType": link_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to get outgoing link type (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": source_object_type, "linkType": link_type},
        )


# ---------------------------------------------------------------------------
# Incoming Link Types — Foundry v2 ``LinkTypeSideV2`` (incoming perspective)
# ---------------------------------------------------------------------------


@router.get("/{ontologyRid}/objectTypes/{objectTypeApiName}/incomingLinkTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_incoming_link_types")
async def list_incoming_link_types_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        target_object_type = str(objectType or "").strip()
        if not target_object_type:
            raise ValueError("objectType is required")
        await _require_read_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/incomingLinkTypes", db_name, branch, target_object_type, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType)},
        )

    try:
        scan_limit = min(max(page_size, 500), 1000)
        scan_offset = 0
        filtered_index = 0
        data: list[dict] = []
        has_more = False
        strict_fix_count = 0
        strict_dropped_count = 0

        while not has_more:
            payload = await oms_client.list_ontology_resources(
                db_name,
                resource_type="link_type",
                branch=branch,
                limit=scan_limit,
                offset=scan_offset,
            )
            resources = _extract_link_resources(payload)
            if not resources:
                break
            for resource in resources:
                mapped = _to_foundry_incoming_link_type(resource, target_object_type=target_object_type)
                if mapped is None:
                    continue
                if strict_compat:
                    mapped, fixes, is_resolved = _strictify_outgoing_link_type(
                        mapped,
                        db_name=db_name,
                        source_object_type=target_object_type,
                    )
                    strict_fix_count += fixes
                    if not is_resolved:
                        strict_dropped_count += 1
                        continue
                if filtered_index < offset:
                    filtered_index += 1
                    continue
                if len(data) < page_size:
                    data.append(mapped)
                    filtered_index += 1
                    continue
                has_more = True
                break
            scan_offset += len(resources)
            if len(resources) < scan_limit:
                break

        if strict_compat:
            _log_strict_compat_summary(
                route="list_incoming_link_types_v2",
                db_name=db_name,
                branch=branch,
                fixes=strict_fix_count,
                dropped=strict_dropped_count,
            )

        next_offset = offset + len(data)
        next_page_token = _encode_page_token(next_offset, scope=page_scope) if has_more else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "OntologyNotFound",
                ontology=db_name,
                parameters={"objectType": target_object_type},
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": target_object_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to list incoming link types (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": target_object_type},
        )


@router.get(
    "/{ontologyRid}/objectTypes/{objectTypeApiName}/incomingLinkTypes/{linkTypeApiName}",
    dependencies=[_ONTOLOGY_READ],
)
@trace_endpoint("bff.foundry_v2_ontology.get_incoming_link_type")
async def get_incoming_link_type_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    linkTypeApiName: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    objectType = objectTypeApiName
    linkType = linkTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        target_object_type = str(objectType or "").strip()
        link_type = str(linkType or "").strip()
        if not target_object_type:
            raise ValueError("objectType is required")
        if not link_type:
            raise ValueError("linkType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType), "linkType": str(linkType)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type,
            branch=branch,
        )
        resource = _extract_ontology_resource(payload)
        if not isinstance(resource, dict):
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=target_object_type,
                link_type=link_type,
            )
        mapped = _to_foundry_incoming_link_type(resource, target_object_type=target_object_type)
        if mapped is None:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=target_object_type,
                link_type=link_type,
            )
        if strict_compat:
            mapped, strict_fix_count, is_resolved = _strictify_outgoing_link_type(
                mapped,
                db_name=db_name,
                source_object_type=target_object_type,
            )
            _log_strict_compat_summary(
                route="get_incoming_link_type_v2",
                db_name=db_name,
                branch=branch,
                fixes=strict_fix_count,
                dropped=0 if is_resolved else 1,
            )
            if not is_resolved:
                return _not_found_error(
                    "LinkTypeNotFound",
                    ontology=db_name,
                    object_type=target_object_type,
                    link_type=link_type,
                )
        return mapped
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=target_object_type,
                link_type=link_type,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": target_object_type, "linkType": link_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to get incoming link type (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": target_object_type, "linkType": link_type},
        )


@router.post("/{ontologyRid}/objects/{objectTypeApiName}/search", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.search_objects")
async def search_objects_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.search_objects_route(
        ontology=ontologyRid,
        object_type_api_name=objectTypeApiName,
        payload=payload,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        foundry_error=_foundry_error,
        passthrough_upstream_error_payload=_passthrough_upstream_error_payload,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.post("/{ontologyRid}/objects/{objectTypeApiName}/count", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.count_objects")
async def count_objects_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.count_objects_route(
        ontology=ontologyRid,
        object_type_api_name=objectTypeApiName,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        foundry_error=_foundry_error,
        passthrough_upstream_error_payload=_passthrough_upstream_error_payload,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.post("/{ontologyRid}/objects/{objectTypeApiName}/aggregate", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.aggregate_objects")
async def aggregate_objects_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.aggregate_objects_route(
        ontology=ontologyRid,
        object_type_api_name=objectTypeApiName,
        payload=payload,
        request=request,
        branch=branch,
        transaction_id=transaction_id,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
    )


@router.post("/{ontologyRid}/objectSets/loadObjects", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_objects")
async def load_object_set_objects_v2(
    ontologyRid: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    return await _query_routes.load_object_set_objects_route(
        ontology=ontologyRid,
        payload=payload,
        request=request,
        branch=branch,
        transaction_id=transaction_id,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        redis_service=redis_service,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        resolve_object_set_definition_with_store=_resolve_object_set_definition_with_store,
        is_search_around_object_set=_is_search_around_object_set,
        resolve_object_set_object_type=_resolve_object_set_object_type,
        build_object_set_search_payload=_build_object_set_search_payload,
        load_rows_for_search_around_object_set=_load_rows_for_search_around_object_set,
        load_rows_for_single_object_type=_load_rows_for_single_object_type,
        preflight_error_response=_preflight_error_response,
        object_set_runtime_value_error_response=_object_set_runtime_value_error_response,
        passthrough_upstream_error_payload=_passthrough_upstream_error_payload,
        foundry_error=_foundry_error,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        resolve_database_actor=resolve_database_actor,
    )


@router.post("/{ontologyRid}/objectSets/loadLinks", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_links")
async def load_object_set_links_v2(
    ontologyRid: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    return await _query_routes.load_object_set_links_route(
        ontology=ontologyRid,
        payload=payload,
        request=request,
        branch=branch,
        preview=preview,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        redis_service=redis_service,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_foundry_v2_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        resolve_object_set_definition_with_store=_resolve_object_set_definition_with_store,
        normalize_link_type_values=_normalize_link_type_values,
        collect_object_set_object_types=_collect_object_set_object_types,
        require_read_role=_require_read_role,
        build_object_set_search_payload=_build_object_set_search_payload,
        pagination_scope=_pagination_scope,
        load_rows_for_single_object_type=_load_rows_for_single_object_type,
        load_rows_for_multi_object_types=_load_rows_for_multi_object_types,
        get_ontology_resource=oms_client.get_ontology_resource,
        extract_ontology_resource=_extract_ontology_resource,
        resolve_source_object_type_from_row=_resolve_source_object_type_from_row,
        resolve_object_primary_key_field=_resolve_object_primary_key_field,
        to_foundry_outgoing_link_type=_to_foundry_outgoing_link_type,
        collect_load_links_rows=_collect_load_links_rows,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.post("/{ontologyRid}/objectSets/loadObjectsMultipleObjectTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_objects_multiple_object_types")
async def load_object_set_multiple_object_types_v2(
    ontologyRid: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    return await _query_routes.load_object_set_multiple_object_types_route(
        ontology=ontologyRid,
        payload=payload,
        request=request,
        branch=branch,
        preview=preview,
        transaction_id=transaction_id,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        redis_service=redis_service,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_foundry_v2_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        resolve_object_set_definition_with_store=_resolve_object_set_definition_with_store,
        is_search_around_object_set=_is_search_around_object_set,
        collect_object_set_object_types=_collect_object_set_object_types,
        require_read_role=_require_read_role,
        resolve_database_actor=resolve_database_actor,
        build_object_set_search_payload=_build_object_set_search_payload,
        pagination_scope=_pagination_scope,
        normalize_select_values=_normalize_select_values,
        load_rows_for_search_around_object_set=_load_rows_for_search_around_object_set,
        load_rows_for_single_object_type=_load_rows_for_single_object_type,
        load_rows_for_multi_object_types=_load_rows_for_multi_object_types,
        preflight_error_response=_preflight_error_response,
        object_set_runtime_value_error_response=_object_set_runtime_value_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.post("/{ontologyRid}/objectSets/loadObjectsOrInterfaces", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_objects_or_interfaces")
async def load_object_set_objects_or_interfaces_v2(
    ontologyRid: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    return await _query_routes.load_object_set_objects_or_interfaces_route(
        ontology=ontologyRid,
        payload=payload,
        request=request,
        branch=branch,
        preview=preview,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        redis_service=redis_service,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        is_foundry_v2_strict_compat_enabled=_is_foundry_v2_strict_compat_enabled,
        require_preview_true_for_strict_compat=_require_preview_true_for_strict_compat,
        resolve_object_set_definition_with_store=_resolve_object_set_definition_with_store,
        is_search_around_object_set=_is_search_around_object_set,
        collect_object_set_object_types=_collect_object_set_object_types,
        require_read_role=_require_read_role,
        resolve_database_actor=resolve_database_actor,
        build_object_set_search_payload=_build_object_set_search_payload,
        pagination_scope=_pagination_scope,
        normalize_select_values=_normalize_select_values,
        load_rows_for_search_around_object_set=_load_rows_for_search_around_object_set,
        load_rows_for_single_object_type=_load_rows_for_single_object_type,
        load_rows_for_multi_object_types=_load_rows_for_multi_object_types,
        preflight_error_response=_preflight_error_response,
        object_set_runtime_value_error_response=_object_set_runtime_value_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.post("/{ontologyRid}/objectSets/aggregate", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.aggregate")
async def aggregate_object_set_v2(
    ontologyRid: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    """Delegate objectSet aggregate to OMS ES-native aggregate engine."""
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        object_set = await _resolve_object_set_definition_with_store(
            payload.get("objectSet"),
            redis_service=redis_service,
        )
        object_types = _collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "aggregate"},
        )

    try:
        # Build the aggregate payload with the objectSet's where clause merged in
        aggregate_payload = dict(payload)
        aggregate_payload.pop("objectSet", None)

        # Extract where from objectSet filter if present
        object_set_filter = _extract_object_set_where(object_set)
        if object_set_filter is not None:
            existing_where = aggregate_payload.get("where")
            if existing_where is not None:
                # Combine objectSet filter with payload where via AND
                aggregate_payload["where"] = {
                    "type": "and",
                    "value": [object_set_filter, existing_where],
                }
            else:
                aggregate_payload["where"] = object_set_filter

        # Delegate to the first objectType (most objectSets are single-type)
        object_type = object_types[0]
        return await oms_client.aggregate_objects_v2(
            db_name,
            object_type,
            aggregate_payload,
            branch=branch,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "aggregate"},
            not_found_response=_not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "aggregate"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "aggregate"},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to aggregate objectSet (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "aggregate"},
        )


@router.post("/{ontologyRid}/objectSets/createTemporary", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.create_temporary")
async def create_temporary_object_set_v2(
    ontologyRid: str,
    payload: Dict[str, Any],
    request: Request,
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version
        await _require_read_role(request, db_name=db_name)
        object_set = await _resolve_object_set_definition_with_store(
            payload.get("objectSet"),
            redis_service=redis_service,
        )
        object_set_rid = await _store_temporary_object_set(object_set, redis_service=redis_service)
        return {"objectSetRid": object_set_rid}
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "createTemporary"},
        )


@router.get("/{ontologyRid}/objectSets/{objectSetRid}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.object_sets.get")
async def get_object_set_v2(
    ontologyRid: str,
    objectSetRid: str,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
    redis_service: RedisService = Depends(get_redis_service),
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_read_role(request, db_name=db_name)
        return await _load_temporary_object_set(objectSetRid, redis_service=redis_service)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSetRid": str(objectSetRid)},
        )


@router.get("/{ontologyRid}/objects/{objectTypeApiName}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_objects")
async def list_objects_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    request: Request,
    page_size: int = Query(1000, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    select: list[str] | None = Query(default=None),
    order_by: str | None = Query(default=None, alias="orderBy"),
    exclude_rid: bool | None = Query(default=None, alias="excludeRid"),
    snapshot: bool | None = Query(default=None),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_read_role(request, db_name=db_name)

        payload: Dict[str, Any] = {"pageSize": page_size}
        if page_token:
            payload["pageToken"] = page_token
        if select:
            payload["select"] = [str(value).strip() for value in select if str(value).strip()]
        parsed_order_by = _parse_order_by(order_by)
        if parsed_order_by is not None:
            payload["orderBy"] = parsed_order_by
        if exclude_rid is not None:
            payload["excludeRid"] = bool(exclude_rid)
        if snapshot is not None:
            payload["snapshot"] = bool(snapshot)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType)},
        )

    try:
        result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
            params={"branch": branch},
            json=payload,
        )
        if not isinstance(result, dict):
            return _foundry_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="INTERNAL",
                error_name="Internal",
                parameters={"ontology": db_name, "objectType": object_type},
            )
        return result
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = _passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type)
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to list objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


@router.get("/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_object")
async def get_object_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    request: Request,
    select: list[str] | None = Query(default=None),
    exclude_rid: bool | None = Query(default=None, alias="excludeRid"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.get_object_route(
        ontology=ontologyRid,
        object_type_api_name=objectTypeApiName,
        primary_key=primaryKey,
        request=request,
        select=select,
        exclude_rid=exclude_rid,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        not_found_error=_not_found_error,
        foundry_error=_foundry_error,
        passthrough_upstream_error_payload=_passthrough_upstream_error_payload,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        extract_object_resource=_extract_object_resource,
        to_foundry_object_type=_to_foundry_object_type,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/links/{linkTypeApiName}",
    dependencies=[_ONTOLOGY_READ],
)
@trace_endpoint("bff.foundry_v2_ontology.list_linked_objects")
async def list_linked_objects_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    linkTypeApiName: str,
    request: Request,
    page_size: int = Query(1000, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    select: list[str] | None = Query(default=None),
    order_by: str | None = Query(default=None, alias="orderBy"),
    exclude_rid: bool | None = Query(default=None, alias="excludeRid"),
    snapshot: bool | None = Query(default=None),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.list_linked_objects_route(
        ontology=ontologyRid,
        object_type_api_name=objectTypeApiName,
        primary_key=primaryKey,
        link_type_api_name=linkTypeApiName,
        request=request,
        page_size=page_size,
        page_token=page_token,
        select=select,
        order_by=order_by,
        exclude_rid=exclude_rid,
        snapshot=snapshot,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        foundry_error=_foundry_error,
        not_found_error=_not_found_error,
        passthrough_upstream_error_payload=_passthrough_upstream_error_payload,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        parse_order_by=_parse_order_by,
        pagination_scope=_pagination_scope,
        decode_page_token=_decode_page_token,
        encode_page_token=_encode_page_token,
        resolve_object_primary_key_field=_resolve_object_primary_key_field,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_outgoing_link_type=_to_foundry_outgoing_link_type,
        extract_object_type_relationships=_extract_object_type_relationships,
        extract_linked_primary_keys_page=_extract_linked_primary_keys_page,
        build_primary_key_where=_build_primary_key_where,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/links/{linkTypeApiName}/{linkedObjectPrimaryKey}",
    dependencies=[_ONTOLOGY_READ],
)
@trace_endpoint("bff.foundry_v2_ontology.get_linked_object")
async def get_linked_object_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    linkTypeApiName: str,
    linkedObjectPrimaryKey: str,
    request: Request,
    select: list[str] | None = Query(default=None),
    exclude_rid: bool | None = Query(default=None, alias="excludeRid"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    return await _query_routes.get_linked_object_route(
        ontology=ontologyRid,
        object_type_api_name=objectTypeApiName,
        primary_key=primaryKey,
        link_type_api_name=linkTypeApiName,
        linked_object_primary_key=linkedObjectPrimaryKey,
        request=request,
        select=select,
        exclude_rid=exclude_rid,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_read_role=_require_read_role,
        preflight_error_response=_preflight_error_response,
        foundry_error=_foundry_error,
        not_found_error=_not_found_error,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
        linked_object_parameters=_linked_object_parameters,
        resolve_object_primary_key_field=_resolve_object_primary_key_field,
        extract_ontology_resource=_extract_ontology_resource,
        to_foundry_outgoing_link_type=_to_foundry_outgoing_link_type,
        linked_primary_key_exists=_linked_primary_key_exists,
        project_row_with_required_fields=_project_row_with_required_fields,
    )


# =====================================================================
# Time Series Property endpoints (Foundry v2 — proxy to OMS)
# =====================================================================


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/timeseries/{property}/firstPoint",
    dependencies=[_ONTOLOGY_READ],
)
async def get_timeseries_first_point_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Get the first (earliest) point of a time series property."""
    return await _media.get_timeseries_first_point_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/timeseries/{property}/lastPoint",
    dependencies=[_ONTOLOGY_READ],
)
async def get_timeseries_last_point_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Get the last (most recent) point of a time series property."""
    return await _media.get_timeseries_last_point_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.post(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/timeseries/{property}/streamPoints",
    response_model=None,
    dependencies=[_ONTOLOGY_READ],
)
async def stream_timeseries_points_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> StreamingResponse | JSONResponse:
    """Stream all points of a time series property with optional range filter."""
    return await _media.stream_timeseries_points_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


# =====================================================================
# Attachment Property endpoints (Foundry v2 — proxy to OMS)
# =====================================================================


@router.post(
    "/attachments/upload",
    dependencies=[_ONTOLOGY_WRITE],
    responses={
        400: {"description": "InvalidArgument"},
        401: {"description": "Unauthorized"},
        403: {"description": "PermissionDenied"},
        500: {"description": "Internal error"},
    },
)
async def upload_attachment_v2(
    request: Request,
    filename: str = Query(..., description="The name of the file being uploaded"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Upload an attachment payload (Foundry v2 shape)."""
    return await _media.upload_attachment_response(
        request=request,
        filename=filename,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/attachments/{property}",
    dependencies=[_ONTOLOGY_READ],
)
async def list_attachment_property_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """List attachment metadata for a property (single or multiple)."""
    return await _media.list_attachment_property_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/attachments/{property}/{attachmentRid}",
    dependencies=[_ONTOLOGY_READ],
)
async def get_attachment_by_rid_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    attachmentRid: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Get metadata for a specific attachment by its RID."""
    return await _media.get_attachment_by_rid_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        attachment_rid=attachmentRid,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/attachments/{property}/content",
    response_model=None,
    dependencies=[_ONTOLOGY_READ],
)
async def get_attachment_content_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> Response | JSONResponse:
    """Get the content of a single-valued attachment property."""
    return await _media.get_attachment_content_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )


@router.get(
    "/{ontologyRid}/objects/{objectTypeApiName}/{primaryKey}/attachments/{property}/{attachmentRid}/content",
    response_model=None,
    dependencies=[_ONTOLOGY_READ],
)
async def get_attachment_content_by_rid_v2(
    ontologyRid: str,
    objectTypeApiName: str,
    primaryKey: str,
    property: str,
    attachmentRid: str,
    request: Request,
    branch: str = Query("main"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
) -> Response | JSONResponse:
    """Get the content of an attachment by its RID."""
    return await _media.get_attachment_content_by_rid_response(
        ontology=ontologyRid,
        object_type=objectTypeApiName,
        primary_key=primaryKey,
        property_name=property,
        attachment_rid=attachmentRid,
        request=request,
        branch=branch,
        sdk_package_rid=sdk_package_rid,
        sdk_version=sdk_version,
        oms_client=oms_client,
        resolve_ontology_db_name=_resolve_ontology_db_name,
        validate_branch=_validate_branch,
        require_domain_role=_require_domain_role,
        extract_actor_forward_headers=_extract_actor_forward_headers,
        preflight_error_response=_preflight_error_response,
        upstream_status_error_response=_upstream_status_error_response,
        upstream_transport_error_response=_upstream_transport_error_response,
        internal_error_response=_internal_error_response,
        preflight_exceptions=(OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError),
        handled_exceptions=_ONTOLOGY_HANDLED_EXCEPTIONS,
    )
