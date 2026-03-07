"""Foundry Ontologies v2 read-compat router.

Exposes a Foundry-style `/api/v2/ontologies/...` read surface on top of
existing OMS/BFF ontology resources.
"""

import asyncio
import logging
import time
from typing import Any, Dict
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from bff.dependencies import OMSClientDep
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
from bff.schemas.object_types_requests import ObjectTypeContractRequest, ObjectTypeContractUpdate
from bff.services.oms_client import OMSClient
from bff.services import object_type_contract_service
from shared.config.settings import get_settings
from shared.foundry.compute_routing import choose_search_around_backend
from shared.foundry.spark_on_demand_dispatcher import get_spark_on_demand_dispatcher
from shared.foundry.auth import require_scopes
from shared.foundry.errors import foundry_error
from shared.foundry.rids import build_rid
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, READ_ROLES, enforce_database_role, resolve_database_actor
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_db_name,
)
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.core.audit_log_store import AuditLogStore
from shared.utils.action_permission_profile import ActionPermissionProfileError, resolve_action_permission_profile
from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token
from shared.utils.object_type_backing import list_backing_sources

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/ontologies", tags=["Foundry Ontologies v2"])

_DEFAULT_OBJECT_TYPE_ICON: Dict[str, str] = {
    "type": "blueprint",
    "name": "table",
    "color": "#4C6A9A",
}
_FOUNDRY_PAGE_TOKEN_TTL_SECONDS = 60 * 60 * 24
_TEMP_OBJECT_SET_TTL_SECONDS = 60 * 60
_TEMP_OBJECT_SET_STORE: dict[str, tuple[float, dict[str, Any]]] = {}
_TEMP_OBJECT_SET_LOCK = asyncio.Lock()
_SEARCH_ROUTING_AUDIT_STORE: AuditLogStore | None = None
_SEARCH_ROUTING_AUDIT_DISABLED = False
_FORWARDED_ACTOR_HEADER_KEYS = (
    "X-User-ID",
    "X-User-Type",
    "X-User",
    "X-Actor",
    "X-User-Roles",
)
_ACTION_TYPE_RESOURCE_TYPE_ALIASES = {
    "action_type",
    "action_types",
    "action-type",
    "action-types",
    "action",
    "actions",
}
_ACTION_TYPE_SPEC_HINT_FIELDS = {
    "input_schema",
    "writeback_target",
    "implementation",
    "validation_rules",
    "target_object_type",
}

_ONTOLOGY_READ = require_scopes(["api:ontologies-read"])
_ONTOLOGY_WRITE = require_scopes(["api:ontologies-write"])
_QUERY_TYPE_PRIMARY_BRANCH = "main"
_QUERY_TYPE_FALLBACK_BRANCH = "main"


def _query_type_branch_candidates() -> tuple[str, ...]:
    branches: list[str] = []
    for candidate in (_QUERY_TYPE_PRIMARY_BRANCH, _QUERY_TYPE_FALLBACK_BRANCH):
        normalized = str(candidate or "").strip()
        if normalized and normalized not in branches:
            branches.append(normalized)
    return tuple(branches)


class OntologyNotFoundError(Exception):
    pass


class PermissionDeniedError(Exception):
    pass


class ApiFeaturePreviewUsageOnlyError(ValueError):
    pass


class ObjectSetNotFoundError(ValueError):
    pass


_ONTOLOGY_HANDLED_EXCEPTIONS = (
    OntologyNotFoundError,
    PermissionDeniedError,
    ApiFeaturePreviewUsageOnlyError,
    ObjectSetNotFoundError,
    SecurityViolationError,
    ActionPermissionProfileError,
    httpx.HTTPError,
    HTTPException,
    RuntimeError,
    LookupError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    OSError,
    AssertionError,
)


class ApplyActionRequestOptionsV2(BaseModel):
    mode: str | None = None
    return_edits: str | None = Field(default=None, alias="returnEdits")


class ApplyActionRequestV2(BaseModel):
    options: ApplyActionRequestOptionsV2 | None = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class BatchApplyActionRequestItemV2(BaseModel):
    parameters: Dict[str, Any] = Field(default_factory=dict)


class BatchApplyActionRequestOptionsV2(BaseModel):
    return_edits: str | None = Field(default=None, alias="returnEdits")


class BatchApplyActionRequestV2(BaseModel):
    options: BatchApplyActionRequestOptionsV2 | None = None
    requests: list[BatchApplyActionRequestItemV2] = Field(default_factory=list, min_length=1, max_length=20)


class ExecuteQueryRequestV2(BaseModel):
    parameters: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] | None = None


class ObjectTypeContractCreateRequestV2(BaseModel):
    apiName: str = Field(..., min_length=1)
    status: str | None = None
    primaryKey: str | None = None
    titleProperty: str | None = None
    pkSpec: Dict[str, Any] | None = None
    backingSource: Dict[str, Any] | None = None
    backingSources: list[Dict[str, Any]] | None = None
    backingDatasetId: str | None = None
    backingDatasourceId: str | None = None
    backingDatasourceVersionId: str | None = None
    datasetVersionId: str | None = None
    schemaHash: str | None = None
    mappingSpecId: str | None = None
    mappingSpecVersion: int | None = None
    autoGenerateMapping: bool | None = None
    metadata: Dict[str, Any] | None = None


class ObjectTypeContractUpdateRequestV2(BaseModel):
    status: str | None = None
    primaryKey: str | None = None
    titleProperty: str | None = None
    pkSpec: Dict[str, Any] | None = None
    backingSource: Dict[str, Any] | None = None
    backingSources: list[Dict[str, Any]] | None = None
    backingDatasetId: str | None = None
    backingDatasourceId: str | None = None
    backingDatasourceVersionId: str | None = None
    datasetVersionId: str | None = None
    schemaHash: str | None = None
    mappingSpecId: str | None = None
    mappingSpecVersion: int | None = None
    metadata: Dict[str, Any] | None = None
    migration: Dict[str, Any] | None = None


def _build_pk_spec_from_v2_payload(
    *,
    pk_spec: Dict[str, Any] | None,
    primary_key: str | None,
    title_property: str | None,
) -> Dict[str, Any]:
    if isinstance(pk_spec, dict) and pk_spec:
        return pk_spec
    normalized_primary = str(primary_key or "").strip()
    normalized_title = str(title_property or "").strip()
    if not normalized_primary and not normalized_title:
        return {}
    return {
        "primary_key": [normalized_primary] if normalized_primary else [],
        "title_key": [normalized_title] if normalized_title else [],
    }


def _build_backing_sources_from_v2_payload(
    *,
    backing_source: Dict[str, Any] | None,
    backing_sources: list[Dict[str, Any]] | None,
) -> list[Dict[str, Any]]:
    return list_backing_sources(
        {
            "backing_source": backing_source if isinstance(backing_source, dict) else None,
            "backing_sources": backing_sources if isinstance(backing_sources, list) else None,
        }
    )


def _extract_api_response_data(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    data = getattr(payload, "data", None)
    if isinstance(data, dict):
        return data
    return {}


def _service_http_error_response(
    exc: HTTPException,
    *,
    ontology: str,
    object_type: str | None = None,
) -> JSONResponse:
    status_code = int(getattr(exc, "status_code", status.HTTP_500_INTERNAL_SERVER_ERROR))
    detail = getattr(exc, "detail", None)
    message = str(exc)
    if isinstance(detail, dict):
        message = str(detail.get("message") or detail.get("detail") or detail.get("error") or message)
    elif detail:
        message = str(detail)

    if status_code == status.HTTP_404_NOT_FOUND:
        if object_type:
            return _not_found_error("ObjectTypeNotFound", ontology=ontology, object_type=object_type)
        return _not_found_error("OntologyNotFound", ontology=ontology)
    if status_code == status.HTTP_403_FORBIDDEN:
        return _permission_denied(ontology=ontology, message=message)

    error_code = "INVALID_ARGUMENT" if 400 <= status_code < 500 else "INTERNAL"
    error_name = "InvalidArgument" if 400 <= status_code < 500 else "Internal"
    parameters = _error_parameters(
        ontology=ontology,
        object_type=object_type,
        parameters={"message": message},
    )
    return _foundry_error(status_code, error_code=error_code, error_name=error_name, parameters=parameters)


def _default_expected_head_commit(branch: str) -> str:
    normalized = str(branch or "").strip() or "main"
    if normalized.lower().startswith("branch:"):
        return normalized
    return f"branch:{normalized}"


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return foundry_error(
        int(status_code),
        error_code=error_code,
        error_name=error_name,
        parameters=parameters or {},
    )


def _passthrough_upstream_error_payload(exc: httpx.HTTPStatusError) -> JSONResponse | None:
    response = exc.response
    if response is None:
        return None
    try:
        payload = response.json()
    except ValueError:
        logger.warning("Failed to decode upstream error payload as JSON", exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    return JSONResponse(status_code=response.status_code, content=payload)


def _named_not_found(error_name: str, *, parameters: Dict[str, Any]) -> JSONResponse:
    return _foundry_error(
        status.HTTP_404_NOT_FOUND,
        error_code="NOT_FOUND",
        error_name=error_name,
        parameters=parameters,
    )


def _error_parameters(
    *,
    ontology: str,
    object_type: str | None = None,
    link_type: str | None = None,
    primary_key: str | None = None,
    linked_primary_key: str | None = None,
    parameters: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"ontology": ontology}
    if object_type is not None:
        payload["objectType"] = object_type
    if link_type is not None:
        payload["linkType"] = link_type
    if primary_key is not None:
        payload["primaryKey"] = primary_key
    if linked_primary_key is not None:
        payload["linkedObjectPrimaryKey"] = linked_primary_key
    if parameters:
        payload.update({str(key): value for key, value in parameters.items() if value is not None})
    return payload


def _not_found_error(
    error_name: str,
    *,
    ontology: str,
    object_type: str | None = None,
    link_type: str | None = None,
    primary_key: str | None = None,
    linked_primary_key: str | None = None,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return _named_not_found(
        error_name,
        parameters=_error_parameters(
            ontology=ontology,
            object_type=object_type,
            link_type=link_type,
            primary_key=primary_key,
            linked_primary_key=linked_primary_key,
            parameters=parameters,
        ),
    )


def _permission_denied(
    *,
    ontology: str,
    message: str = "Permission denied",
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    payload = {"ontology": ontology, "message": message}
    if parameters:
        payload.update(parameters)
    return _foundry_error(
        status.HTTP_403_FORBIDDEN,
        error_code="PERMISSION_DENIED",
        error_name="PermissionDenied",
        parameters=payload,
    )


def _preflight_error_response(
    exc: Exception,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    scoped = {str(key): value for key, value in (parameters or {}).items() if value is not None}
    if isinstance(exc, OntologyNotFoundError):
        return _not_found_error("OntologyNotFound", ontology=ontology, parameters=scoped or None)
    if isinstance(exc, ObjectSetNotFoundError):
        return _not_found_error("ObjectSetNotFound", ontology=ontology, parameters=scoped or None)
    if isinstance(exc, PermissionDeniedError):
        return _permission_denied(ontology=ontology, message=str(exc), parameters=scoped or None)
    if isinstance(exc, ApiFeaturePreviewUsageOnlyError):
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="ApiFeaturePreviewUsageOnly",
            parameters={"ontology": ontology, **scoped, "message": str(exc)},
        )
    if isinstance(exc, (ValueError, SecurityViolationError)):
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": ontology, **scoped, "message": str(exc)},
        )
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": ontology, **scoped},
        )
    if isinstance(exc, httpx.HTTPError):
        return _foundry_error(
            status.HTTP_502_BAD_GATEWAY,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": ontology, **scoped},
        )
    logger.error("Foundry v2 preflight failed (%s): %s", ontology, exc)
    return _foundry_error(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code="INTERNAL",
        error_name="Internal",
        parameters={"ontology": ontology, **scoped},
    )


def _decode_page_token(page_token: str | None, *, scope: str | None = None) -> int:
    return decode_offset_page_token(
        page_token,
        ttl_seconds=_FOUNDRY_PAGE_TOKEN_TTL_SECONDS,
        expected_scope=scope,
    )


def _encode_page_token(offset: int, *, scope: str | None = None) -> str:
    return encode_offset_page_token(offset, scope=scope)


def _parse_order_by(order_by: str | None) -> Dict[str, Any] | None:
    raw = str(order_by or "").strip()
    if not raw:
        return None

    fields: list[Dict[str, str]] = []
    for segment in raw.split(","):
        token = segment.strip()
        if not token:
            continue

        field_expr, sep, direction_expr = token.partition(":")
        field_expr = field_expr.strip()
        if field_expr.startswith("properties."):
            field = field_expr[len("properties.") :].strip()
        elif field_expr.startswith("p."):
            field = field_expr[len("p.") :].strip()
        else:
            raise ValueError("orderBy fields must be prefixed by properties. or p.")
        if not field:
            raise ValueError("orderBy field is required")

        direction = direction_expr.strip().lower() if sep else "asc"
        if direction not in {"asc", "desc"}:
            raise ValueError("orderBy sort direction must be asc or desc")

        fields.append({"field": field, "direction": direction})

    if not fields:
        raise ValueError("orderBy must contain at least one field")
    return {"orderType": "fields", "fields": fields}


def _temporary_object_set_rid() -> str:
    return f"ri.object-set.main.versioned-object-set.{uuid4()}"


def _prune_expired_temporary_object_sets(now_epoch: float) -> None:
    expired = [rid for rid, (expires_at, _) in _TEMP_OBJECT_SET_STORE.items() if expires_at <= now_epoch]
    for rid in expired:
        _TEMP_OBJECT_SET_STORE.pop(rid, None)


async def _store_temporary_object_set(object_set: dict[str, Any]) -> str:
    rid = _temporary_object_set_rid()
    now_epoch = time.time()
    async with _TEMP_OBJECT_SET_LOCK:
        _prune_expired_temporary_object_sets(now_epoch)
        _TEMP_OBJECT_SET_STORE[rid] = (now_epoch + _TEMP_OBJECT_SET_TTL_SECONDS, dict(object_set))
    return rid


async def _load_temporary_object_set(rid: str) -> dict[str, Any]:
    normalized_rid = str(rid or "").strip()
    if not normalized_rid:
        raise ObjectSetNotFoundError("objectSetRid is required")
    now_epoch = time.time()
    async with _TEMP_OBJECT_SET_LOCK:
        _prune_expired_temporary_object_sets(now_epoch)
        record = _TEMP_OBJECT_SET_STORE.get(normalized_rid)
        if record is None:
            raise ObjectSetNotFoundError(f"ObjectSet not found: {normalized_rid}")
        expires_at, object_set = record
        if expires_at <= now_epoch:
            _TEMP_OBJECT_SET_STORE.pop(normalized_rid, None)
            raise ObjectSetNotFoundError(f"ObjectSet not found: {normalized_rid}")
        return dict(object_set)


def _collect_object_set_object_types(object_set: Any) -> list[str]:
    stack: list[Any] = [object_set]
    visited: set[int] = set()
    out: list[str] = []
    seen: set[str] = set()

    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        marker = id(current)
        if marker in visited:
            continue
        visited.add(marker)

        for key in ("objectType", "objectTypeApiName"):
            candidate = str(current.get(key) or "").strip()
            if candidate and candidate not in seen:
                seen.add(candidate)
                out.append(candidate)

        for value in current.values():
            if isinstance(value, dict):
                stack.append(value)
            elif isinstance(value, list):
                stack.extend(item for item in value if isinstance(item, dict))
    return out


def _resolve_object_set_object_type(object_set: Any) -> str | None:
    object_types = _collect_object_set_object_types(object_set)
    return object_types[0] if object_types else None


async def _resolve_object_set_definition(object_set: Any) -> dict[str, Any]:
    if isinstance(object_set, dict):
        return dict(object_set)
    if isinstance(object_set, str):
        return await _load_temporary_object_set(object_set)
    raise ValueError("objectSet is required")


def _is_search_around_object_set(object_set: Any) -> bool:
    if not isinstance(object_set, dict):
        return False
    object_set_type = str(object_set.get("type") or "").strip().lower()
    return object_set_type == "searcharound"


def _extract_search_around_link_type(object_set: dict[str, Any]) -> str:
    link = object_set.get("link")
    if isinstance(link, str):
        link_type = link.strip()
    elif isinstance(link, dict):
        link_type = str(link.get("apiName") or link.get("linkType") or "").strip()
    else:
        link_type = ""
    if not link_type:
        raise ValueError("searchAround objectSet requires link")
    return link_type


def _dedupe_rows_by_identity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        object_type = str(
            row.get("__apiName")
            or row.get("objectTypeApiName")
            or row.get("class_id")
            or ""
        ).strip()
        primary_key = str(
            row.get("__primaryKey")
            or row.get("primaryKey")
            or row.get("instance_id")
            or row.get("id")
            or ""
        ).strip()
        if not object_type or not primary_key:
            deduped.append(row)
            continue
        marker = (object_type, primary_key)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(row)
    return deduped


def _normalize_sort_value(value: Any) -> tuple[int, str]:
    if value is None:
        return (0, "")
    if isinstance(value, bool):
        return (1, "1" if value else "0")
    if isinstance(value, (int, float)):
        return (2, f"{float(value):020.6f}")
    if isinstance(value, str):
        return (3, value.casefold())
    return (4, str(value))


def _sort_rows_by_order_by(rows: list[dict[str, Any]], order_by: Any) -> list[dict[str, Any]]:
    sorted_rows = list(rows)
    if not isinstance(order_by, dict):
        sorted_rows.sort(key=lambda row: (str(row.get("class_id") or ""), str(row.get("instance_id") or "")))
        return sorted_rows

    if str(order_by.get("orderType") or "fields").strip() != "fields":
        return sorted_rows

    raw_fields = order_by.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        return sorted_rows

    # Deterministic tie-breaker first; stable sorts below keep field order precedence.
    sorted_rows.sort(key=lambda row: str(row.get("instance_id") or ""))
    for item in reversed(raw_fields):
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        if not field:
            continue
        direction = str(item.get("direction") or "asc").strip().lower()
        reverse = direction == "desc"
        sorted_rows.sort(
            key=lambda row: _normalize_sort_value(_value_by_field(row, field)),
            reverse=reverse,
        )

    return sorted_rows


async def _resolve_search_around_target_primary_keys(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    object_set: dict[str, Any],
) -> tuple[dict[str, list[str]], str]:
    link_type = _extract_search_around_link_type(object_set)
    source_object_set = await _resolve_object_set_definition(object_set.get("objectSet"))
    source_object_types = _collect_object_set_object_types(source_object_set)
    if not source_object_types:
        raise ValueError("searchAround.objectSet.objectType is required")

    try:
        link_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type,
            branch=branch,
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            raise ValueError(f"LinkType not found: {link_type}") from exc
        raise
    link_resource = link_payload.get("data") if isinstance(link_payload, dict) else link_payload
    if not isinstance(link_resource, dict):
        raise ValueError(f"LinkType not found: {link_type}")

    link_sides_by_source_type: dict[str, dict[str, Any]] = {}
    for source_object_type in source_object_types:
        link_side = _to_foundry_outgoing_link_type(link_resource, source_object_type=source_object_type)
        if not isinstance(link_side, dict):
            continue
        target_object_type = str(link_side.get("objectTypeApiName") or "").strip()
        if not target_object_type:
            continue
        link_sides_by_source_type[source_object_type] = link_side

    if not link_sides_by_source_type:
        raise ValueError(f"LinkType not found: {link_type}")

    source_search_payload = _build_object_set_search_payload(
        object_set=source_object_set,
        payload={"pageSize": 1000},
        require_select=False,
    )

    source_rows: list[dict[str, Any]] = []
    for source_object_type in source_object_types:
        source_rows.extend(
            await _load_all_rows_for_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=source_object_type,
                search_payload=source_search_payload,
            )
        )

    target_primary_keys_by_type: dict[str, list[str]] = {}
    seen_primary_keys_by_type: dict[str, set[str]] = {}
    default_source_type = source_object_types[0] if len(source_object_types) == 1 else None

    for source_row in source_rows:
        source_object_type = _resolve_source_object_type_from_row(
            source_row,
            fallback_object_type=default_source_type,
        )
        if not source_object_type:
            continue
        link_side = link_sides_by_source_type.get(source_object_type)
        if not isinstance(link_side, dict):
            continue

        target_object_type = str(link_side.get("objectTypeApiName") or "").strip()
        if not target_object_type:
            continue

        foreign_key_property = str(link_side.get("foreignKeyPropertyApiName") or "").strip() or None
        linked_primary_keys = _extract_linked_primary_keys(
            source_row,
            link_type=link_type,
            foreign_key_property=foreign_key_property,
        )
        if not linked_primary_keys:
            continue

        target_primary_keys = target_primary_keys_by_type.setdefault(target_object_type, [])
        seen_primary_keys = seen_primary_keys_by_type.setdefault(target_object_type, set())
        for linked_primary_key in linked_primary_keys:
            if linked_primary_key in seen_primary_keys:
                continue
            seen_primary_keys.add(linked_primary_key)
            target_primary_keys.append(linked_primary_key)

    return target_primary_keys_by_type, link_type


def _estimate_search_around_candidate_count(target_primary_keys_by_type: dict[str, list[str]]) -> int:
    total = 0
    for values in target_primary_keys_by_type.values():
        if isinstance(values, list):
            total += len(values)
    return max(0, total)


async def _execute_search_around_index_pruning(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    payload: dict[str, Any],
    endpoint_scope: str,
    link_type: str,
    target_object_types: list[str],
    target_primary_keys_by_type: dict[str, list[str]],
) -> tuple[list[dict[str, Any]], str, str | None, list[str]]:
    if not target_object_types:
        return [], "0", None, []

    page_size = _to_int_or_none(payload.get("pageSize"))
    if page_size is None:
        page_size = 1000
    if page_size < 1 or page_size > 1000:
        raise ValueError("pageSize must be between 1 and 1000")

    search_payload: dict[str, Any] = {"pageSize": 1000}
    select_values = _normalize_select_values(payload)
    if select_values:
        search_payload["select"] = select_values

    order_by = _normalize_object_set_order_by(payload.get("orderBy"))
    if order_by is not None:
        search_payload["orderBy"] = order_by

    if "excludeRid" in payload:
        search_payload["excludeRid"] = bool(payload.get("excludeRid"))
    if "snapshot" in payload:
        search_payload["snapshot"] = bool(payload.get("snapshot"))

    all_rows: list[dict[str, Any]] = []
    for target_object_type in target_object_types:
        primary_key_values = target_primary_keys_by_type.get(target_object_type) or []
        if not primary_key_values:
            continue
        target_primary_key_field = await _resolve_object_primary_key_field(
            db_name=db_name,
            object_type=target_object_type,
            branch=branch,
            oms_client=oms_client,
        )
        target_search_payload = dict(search_payload)
        target_search_payload["where"] = _build_primary_key_where(target_primary_key_field, primary_key_values)
        all_rows.extend(
            await _load_all_rows_for_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=target_object_type,
                search_payload=target_search_payload,
            )
        )

    deduped_rows = _dedupe_rows_by_identity(all_rows)
    sorted_rows = _sort_rows_by_order_by(deduped_rows, order_by)

    exclude_rid = bool(payload.get("excludeRid")) if "excludeRid" in payload else False
    projected_rows = [
        _project_row_with_required_fields(
            row,
            select_fields=select_values if select_values else None,
            exclude_rid=exclude_rid,
        )
        for row in sorted_rows
    ]

    page_scope = _pagination_scope(
        endpoint_scope,
        db_name,
        branch,
        link_type,
        ",".join(sorted(target_object_types)),
        str(payload.get("objectSet") or ""),
        str(page_size),
        ",".join(select_values),
    )
    offset = _decode_page_token(payload.get("pageToken"), scope=page_scope)
    page_rows = projected_rows[offset : offset + page_size]
    next_offset = offset + len(page_rows)
    next_page_token = _encode_page_token(next_offset, scope=page_scope) if next_offset < len(projected_rows) else None
    return page_rows, str(len(projected_rows)), next_page_token, target_object_types


async def _audit_search_around_compute_routing(
    *,
    db_name: str,
    branch: str,
    endpoint_scope: str,
    actor: str | None,
    link_type: str,
    target_object_types: list[str],
    decision_metadata: dict[str, Any],
) -> None:
    logger.info(
        "Search Around compute route (ontology=%s branch=%s endpoint=%s actor=%s decision=%s)",
        db_name,
        branch,
        endpoint_scope,
        actor or "system",
        decision_metadata,
    )

    global _SEARCH_ROUTING_AUDIT_STORE, _SEARCH_ROUTING_AUDIT_DISABLED
    if _SEARCH_ROUTING_AUDIT_DISABLED:
        return

    try:
        if _SEARCH_ROUTING_AUDIT_STORE is None:
            _SEARCH_ROUTING_AUDIT_STORE = AuditLogStore()
        await _SEARCH_ROUTING_AUDIT_STORE.log(
            partition_key=f"ontology_compute:{db_name}",
            actor=actor or "system",
            action="SEARCH_AROUND_COMPUTE_ROUTED",
            status="success",
            resource_type="ontology",
            resource_id=db_name,
            metadata={
                "branch": branch,
                "endpoint_scope": endpoint_scope,
                "link_type": link_type,
                "target_object_types": list(target_object_types),
                **decision_metadata,
            },
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        _SEARCH_ROUTING_AUDIT_DISABLED = True
        logger.warning(
            "Search Around compute routing audit disabled after persistence error (ontology=%s): %s",
            db_name,
            exc,
            exc_info=True,
        )


async def _load_rows_for_search_around_object_set(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    object_set: dict[str, Any],
    payload: dict[str, Any],
    endpoint_scope: str,
    actor: str | None = None,
) -> tuple[list[dict[str, Any]], str, str | None, list[str]]:
    target_primary_keys_by_type, link_type = await _resolve_search_around_target_primary_keys(
        oms_client=oms_client,
        db_name=db_name,
        branch=branch,
        object_set=object_set,
    )

    target_object_types = [object_type for object_type, values in target_primary_keys_by_type.items() if values]
    settings = get_settings()
    estimated_count = _estimate_search_around_candidate_count(target_primary_keys_by_type)
    routing_decision = choose_search_around_backend(
        estimated_count=estimated_count,
        threshold=int(settings.ontology.search_around_spark_threshold),
    )
    execution_backend = "spark_on_demand" if routing_decision.spark_routed else "index_pruning"
    decision_metadata = routing_decision.as_metadata(execution_backend=execution_backend)

    if routing_decision.spark_routed:
        dispatcher = get_spark_on_demand_dispatcher()
        dispatch_result = await dispatcher.dispatch(
            route="search_around",
            payload={
                "ontology": db_name,
                "branch": branch,
                "endpoint_scope": endpoint_scope,
                "estimated_count": estimated_count,
                "target_object_types": list(target_object_types),
            },
            execute=lambda _ctx: _execute_search_around_index_pruning(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                payload=payload,
                endpoint_scope=endpoint_scope,
                link_type=link_type,
                target_object_types=target_object_types,
                target_primary_keys_by_type=target_primary_keys_by_type,
            ),
            timeout_seconds=60.0,
        )
        decision_metadata.update(
            {
                "spark_job_id": dispatch_result.job_id,
                "queue_wait_ms": dispatch_result.queue_wait_ms,
                "execution_ms": dispatch_result.execution_ms,
            }
        )
        rows, total_count, next_page_token, resolved_object_types = dispatch_result.result
    else:
        rows, total_count, next_page_token, resolved_object_types = await _execute_search_around_index_pruning(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            payload=payload,
            endpoint_scope=endpoint_scope,
            link_type=link_type,
            target_object_types=target_object_types,
            target_primary_keys_by_type=target_primary_keys_by_type,
        )

    await _audit_search_around_compute_routing(
        db_name=db_name,
        branch=branch,
        endpoint_scope=endpoint_scope,
        actor=actor,
        link_type=link_type,
        target_object_types=target_object_types,
        decision_metadata=decision_metadata,
    )
    return rows, total_count, next_page_token, resolved_object_types


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


def _extract_object_set_where(object_set: Any) -> Dict[str, Any] | None:
    if not isinstance(object_set, dict):
        return None

    direct_where = object_set.get("where")
    if isinstance(direct_where, dict):
        return direct_where

    direct_filter = object_set.get("filter")
    if isinstance(direct_filter, dict):
        return direct_filter

    query = object_set.get("query")
    if isinstance(query, dict):
        query_where = query.get("where")
        if isinstance(query_where, dict):
            return query_where
    return None


def _normalize_object_set_order_by(order_by: Any) -> Dict[str, Any] | None:
    if order_by is None:
        return None
    if isinstance(order_by, dict):
        return order_by
    if isinstance(order_by, str):
        return _parse_order_by(order_by)
    raise ValueError("orderBy must be a string or object")


def _normalize_select_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    select = payload.get("select")
    if isinstance(select, list):
        values.extend(str(value).strip() for value in select if str(value).strip())
    select_v2 = payload.get("selectV2")
    if isinstance(select_v2, list):
        for value in select_v2:
            if isinstance(value, str):
                text = value.strip()
            elif isinstance(value, dict):
                text = str(value.get("property") or value.get("apiName") or "").strip()
            else:
                text = ""
            if text:
                values.append(text)
    return list(dict.fromkeys(values))


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    return parsed


def _build_object_set_search_payload(
    *,
    object_set: dict[str, Any],
    payload: dict[str, Any],
    default_page_size: int = 1000,
    require_select: bool = False,
) -> dict[str, Any]:
    page_size = _to_int_or_none(payload.get("pageSize"))
    if page_size is None:
        page_size = default_page_size
    if page_size < 1 or page_size > 1000:
        raise ValueError("pageSize must be between 1 and 1000")

    search_payload: dict[str, Any] = {"pageSize": page_size}

    page_token = str(payload.get("pageToken") or "").strip()
    if page_token:
        search_payload["pageToken"] = page_token

    where_clause = _extract_object_set_where(object_set)
    if where_clause is not None:
        search_payload["where"] = where_clause

    select_values = _normalize_select_values(payload)
    if select_values:
        search_payload["select"] = select_values
    elif require_select:
        raise ValueError("select or selectV2 is required")

    order_by = _normalize_object_set_order_by(payload.get("orderBy"))
    if order_by is not None:
        search_payload["orderBy"] = order_by

    if "excludeRid" in payload:
        search_payload["excludeRid"] = bool(payload.get("excludeRid"))
    if "snapshot" in payload:
        search_payload["snapshot"] = bool(payload.get("snapshot"))

    return search_payload


def _get_result_rows(result: Any) -> list[dict[str, Any]]:
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def _get_total_count(result: Any) -> str:
    if isinstance(result, dict):
        total = result.get("totalCount")
        if total is not None:
            return str(total)
    return "0"


def _get_next_page_token(result: Any) -> str | None:
    if not isinstance(result, dict):
        return None
    next_page_token = result.get("nextPageToken")
    if next_page_token is None:
        return None
    token = str(next_page_token).strip()
    return token or None


async def _search_object_type_rows(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    object_type: str,
    search_payload: dict[str, Any],
) -> dict[str, Any]:
    return await oms_client.post(
        f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
        params={"branch": branch},
        json=search_payload,
    )


def _derive_page_size(search_payload: dict[str, Any]) -> int:
    page_size = _to_int_or_none(search_payload.get("pageSize"))
    if page_size is None:
        return 1000
    return page_size


def _project_row_with_required_fields(
    row: dict[str, Any],
    *,
    select_fields: list[str] | None,
    exclude_rid: bool,
) -> dict[str, Any]:
    if select_fields:
        projected = {key: row.get(key) for key in select_fields if key in row}
    else:
        projected = dict(row)

    for required_key in ("__apiName", "__primaryKey", "instance_id", "class_id"):
        if required_key in row:
            projected.setdefault(required_key, row.get(required_key))

    if exclude_rid:
        projected.pop("__rid", None)
    elif "__rid" in row:
        projected.setdefault("__rid", row.get("__rid"))

    return projected


_MAX_ROWS_LOAD_ALL = 50_000


async def _load_all_rows_for_object_type(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    object_type: str,
    search_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = dict(search_payload)
    payload["pageSize"] = max(1, min(1000, _derive_page_size(payload)))
    payload.pop("pageToken", None)

    rows: list[dict[str, Any]] = []
    seen_page_tokens: set[str] = set()
    next_page_token: str | None = None

    while True:
        if next_page_token:
            payload["pageToken"] = next_page_token
        else:
            payload.pop("pageToken", None)

        result = await _search_object_type_rows(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            object_type=object_type,
            search_payload=payload,
        )
        rows.extend(_get_result_rows(result))

        if len(rows) > _MAX_ROWS_LOAD_ALL:
            logger.warning(
                "_load_all_rows_for_object_type exceeded %d row limit for %s/%s, truncating",
                _MAX_ROWS_LOAD_ALL, db_name, object_type,
            )
            break

        candidate = _get_next_page_token(result)
        if candidate is None:
            break
        if candidate in seen_page_tokens:
            raise ValueError("object search pagination token loop detected")
        seen_page_tokens.add(candidate)
        next_page_token = candidate

    return rows


async def _load_rows_for_single_object_type(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    object_type: str,
    search_payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], str, str | None]:
    result = await _search_object_type_rows(
        oms_client=oms_client,
        db_name=db_name,
        branch=branch,
        object_type=object_type,
        search_payload=search_payload,
    )
    rows = _get_result_rows(result)
    total_count = _get_total_count(result)
    next_page_token = _get_next_page_token(result)
    return rows, total_count, next_page_token


async def _load_rows_for_multi_object_types(
    *,
    oms_client: OMSClient,
    db_name: str,
    branch: str,
    object_types: list[str],
    search_payload: dict[str, Any],
    page_token: str | None,
    pagination_scope: str,
) -> tuple[list[dict[str, Any]], str, str | None]:
    page_size = _derive_page_size(search_payload)
    offset = _decode_page_token(page_token, scope=pagination_scope)

    combined: list[dict[str, Any]] = []
    stripped_payload = dict(search_payload)
    stripped_payload.pop("pageToken", None)
    stripped_payload["pageSize"] = max(page_size, 1000)
    for object_type in object_types:
        result = await _search_object_type_rows(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            object_type=object_type,
            search_payload=stripped_payload,
        )
        rows = _get_result_rows(result)
        for row in rows:
            combined.append(dict(row))

    total = len(combined)
    page_rows = combined[offset : offset + page_size]
    next_offset = offset + len(page_rows)
    next_page_token = _encode_page_token(next_offset, scope=pagination_scope) if next_offset < total else None
    return page_rows, str(total), next_page_token


def _normalize_link_type_values(payload: dict[str, Any]) -> list[str]:
    links = payload.get("links")
    if not isinstance(links, list) or not links:
        raise ValueError("links is required")

    normalized: list[str] = []
    for raw in links:
        if isinstance(raw, str):
            link_type = raw.strip()
        elif isinstance(raw, dict):
            link_type = str(raw.get("apiName") or raw.get("linkType") or "").strip()
        else:
            link_type = ""
        if not link_type:
            raise ValueError("links must be an array of link type API names")
        normalized.append(link_type)

    deduped = list(dict.fromkeys(normalized))
    if not deduped:
        raise ValueError("links is required")
    return deduped


def _resolve_source_object_type_from_row(
    row: dict[str, Any],
    *,
    fallback_object_type: str | None = None,
) -> str | None:
    for key in ("__apiName", "objectTypeApiName", "__objectType", "objectType", "class_id", "classId"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    if fallback_object_type:
        return fallback_object_type
    return None


def _resolve_source_primary_key_from_row(
    row: dict[str, Any],
    *,
    primary_key_field: str | None = None,
) -> str | None:
    for key in ("__primaryKey", "primaryKey", "instance_id", "id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    if primary_key_field:
        value = _value_by_field(row, primary_key_field)
        text = str(value or "").strip()
        if text:
            return text
    return None


def _build_object_locator(*, object_type: str, primary_key: str) -> dict[str, str]:
    return {"__apiName": object_type, "__primaryKey": primary_key}


def _build_linked_object_locator(
    *,
    link_type: str,
    target_object_type: str,
    target_primary_key: str,
) -> dict[str, Any]:
    return {
        "targetObject": _build_object_locator(object_type=target_object_type, primary_key=target_primary_key),
        "linkType": link_type,
    }


def _collect_load_links_rows(
    *,
    rows: list[dict[str, Any]],
    requested_links: list[str],
    link_sides_by_source_type: dict[str, dict[str, dict[str, Any]]],
    source_primary_key_fields: dict[str, str],
    default_object_type: str | None = None,
) -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = []
    for row in rows:
        source_object_type = _resolve_source_object_type_from_row(
            row,
            fallback_object_type=default_object_type,
        )
        if not source_object_type:
            continue
        source_primary_key = _resolve_source_primary_key_from_row(
            row,
            primary_key_field=source_primary_key_fields.get(source_object_type),
        )
        if not source_primary_key:
            continue
        source_link_sides = link_sides_by_source_type.get(source_object_type, {})
        linked_objects: list[dict[str, Any]] = []
        for link_type in requested_links:
            link_side = source_link_sides.get(link_type)
            if not isinstance(link_side, dict):
                continue
            target_object_type = str(link_side.get("objectTypeApiName") or "").strip()
            if not target_object_type:
                continue
            foreign_key_property = str(link_side.get("foreignKeyPropertyApiName") or "").strip() or None
            linked_primary_keys = _extract_linked_primary_keys(
                row,
                link_type=link_type,
                foreign_key_property=foreign_key_property,
            )
            for linked_primary_key in linked_primary_keys:
                linked_objects.append(
                    _build_linked_object_locator(
                        link_type=link_type,
                        target_object_type=target_object_type,
                        target_primary_key=linked_primary_key,
                    )
                )
        if linked_objects:
            data.append(
                {
                    "sourceObject": _build_object_locator(
                        object_type=source_object_type,
                        primary_key=source_primary_key,
                    ),
                    "linkedObjects": linked_objects,
                }
            )
    return data


def _value_by_field(row: dict[str, Any], field: str | None) -> Any:
    if not field:
        return None
    text = str(field).strip()
    if not text:
        return None
    if "." in text:
        current: Any = row
        for part in text.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current
    if text.startswith("properties.") and isinstance(row.get("properties"), dict):
        return row.get("properties", {}).get(text[len("properties.") :])
    return row.get(text)


# Legacy Python aggregate functions removed.
# Aggregation is now delegated to OMS ES-native engine via oms_client.aggregate_objects_v2().


def _pagination_scope(*parts: Any) -> str:
    normalized = [str(part).strip() for part in parts if str(part).strip()]
    return "|".join(normalized)


def _is_foundry_v2_strict_compat_enabled(*, db_name: str | None) -> bool:
    _ = db_name
    return True


def _rid_component(value: Any, *, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    normalized = "".join(ch if (ch.isalnum() or ch in {"_", "-", "."}) else "_" for ch in text)
    normalized = normalized.strip("._-")
    return normalized or fallback


def _default_object_type_rid(*, db_name: str, object_type: str) -> str:
    object_type_id = f"{_rid_component(db_name, fallback='db')}.{_rid_component(object_type, fallback='objectType')}"
    return build_rid("object-type", object_type_id)


def _default_property_rid(*, db_name: str, object_type: str, property_name: str) -> str:
    prop_id = (
        f"{_rid_component(db_name, fallback='db')}.{_rid_component(object_type, fallback='objectType')}.{_rid_component(property_name, fallback='property')}"
    )
    return build_rid("property", prop_id)


def _default_link_type_rid(*, db_name: str, source_object_type: str, link_type: str) -> str:
    link_id = (
        f"{_rid_component(db_name, fallback='db')}.{_rid_component(source_object_type, fallback='objectType')}.{_rid_component(link_type, fallback='linkType')}"
    )
    return build_rid("link-type", link_id)


def _default_property_contract(*, db_name: str, object_type: str, property_name: str) -> Dict[str, Any]:
    return {
        "dataType": {"type": "string"},
        "rid": _default_property_rid(db_name=db_name, object_type=object_type, property_name=property_name),
    }


def _strictify_foundry_object_type(
    object_type: Dict[str, Any],
    *,
    db_name: str,
    object_type_hint: str | None = None,
) -> tuple[Dict[str, Any], int]:
    out = dict(object_type or {})
    fixes = 0

    api_name = str(out.get("apiName") or object_type_hint or "").strip()
    if not api_name:
        api_name = _rid_component(object_type_hint, fallback="ObjectType")
        out["apiName"] = api_name
        fixes += 1

    display_name = str(out.get("displayName") or "").strip()
    if not display_name:
        out["displayName"] = api_name
        display_name = api_name
        fixes += 1

    plural_display_name = str(out.get("pluralDisplayName") or "").strip()
    if not plural_display_name:
        out["pluralDisplayName"] = display_name
        fixes += 1

    icon = out.get("icon")
    if not isinstance(icon, dict):
        out["icon"] = dict(_DEFAULT_OBJECT_TYPE_ICON)
        fixes += 1
    else:
        normalized_icon = dict(icon)
        changed = False
        if str(normalized_icon.get("type") or "").strip().lower() != "blueprint":
            normalized_icon["type"] = "blueprint"
            changed = True
        if not str(normalized_icon.get("name") or "").strip():
            normalized_icon["name"] = _DEFAULT_OBJECT_TYPE_ICON["name"]
            changed = True
        if not str(normalized_icon.get("color") or "").strip():
            normalized_icon["color"] = _DEFAULT_OBJECT_TYPE_ICON["color"]
            changed = True
        if changed:
            out["icon"] = normalized_icon
            fixes += 1

    rid = str(out.get("rid") or "").strip()
    if not rid:
        out["rid"] = _default_object_type_rid(db_name=db_name, object_type=api_name)
        fixes += 1

    properties = out.get("properties")
    if not isinstance(properties, dict):
        properties = {}
        out["properties"] = properties
        fixes += 1

    normalized_properties: Dict[str, Dict[str, Any]] = {}
    for prop_name, raw_prop in properties.items():
        prop_api_name = str(prop_name or "").strip()
        if not prop_api_name:
            continue
        prop = dict(raw_prop) if isinstance(raw_prop, dict) else {}
        if not isinstance(prop.get("dataType"), dict) or not str((prop.get("dataType") or {}).get("type") or "").strip():
            prop["dataType"] = {"type": "string"}
            fixes += 1
        prop_rid = str(prop.get("rid") or "").strip()
        if not prop_rid:
            prop["rid"] = _default_property_rid(
                db_name=db_name,
                object_type=api_name,
                property_name=prop_api_name,
            )
            fixes += 1
        normalized_properties[prop_api_name] = prop
    out["properties"] = normalized_properties

    primary_key = str(out.get("primaryKey") or "").strip()
    if not primary_key:
        if "id" in normalized_properties:
            primary_key = "id"
        elif normalized_properties:
            primary_key = next(iter(normalized_properties))
        else:
            primary_key = "id"
        out["primaryKey"] = primary_key
        fixes += 1

    if primary_key not in normalized_properties:
        normalized_properties[primary_key] = _default_property_contract(
            db_name=db_name,
            object_type=api_name,
            property_name=primary_key,
        )
        fixes += 1

    title_property = str(out.get("titleProperty") or "").strip()
    if not title_property:
        if "name" in normalized_properties:
            title_property = "name"
        else:
            title_property = primary_key
        out["titleProperty"] = title_property
        fixes += 1

    if title_property not in normalized_properties:
        normalized_properties[title_property] = _default_property_contract(
            db_name=db_name,
            object_type=api_name,
            property_name=title_property,
        )
        fixes += 1

    return out, fixes


def _strictify_outgoing_link_type(
    link_type_payload: Dict[str, Any],
    *,
    db_name: str,
    source_object_type: str,
) -> tuple[Dict[str, Any], int, bool]:
    out = dict(link_type_payload or {})
    fixes = 0

    api_name = str(out.get("apiName") or "").strip()
    if not api_name:
        return out, fixes, False

    display_name = str(out.get("displayName") or "").strip()
    if not display_name:
        out["displayName"] = api_name
        fixes += 1

    status_value = str(out.get("status") or "").strip().upper()
    if not status_value:
        out["status"] = "ACTIVE"
        fixes += 1

    cardinality = str(out.get("cardinality") or "").strip().upper()
    if cardinality not in {"ONE", "MANY"}:
        out["cardinality"] = "MANY"
        fixes += 1

    link_type_rid = str(out.get("linkTypeRid") or "").strip()
    if not link_type_rid:
        out["linkTypeRid"] = _default_link_type_rid(
            db_name=db_name,
            source_object_type=source_object_type,
            link_type=api_name,
        )
        fixes += 1

    object_type_api_name = str(out.get("objectTypeApiName") or "").strip()
    is_resolved = bool(object_type_api_name)
    return out, fixes, is_resolved


def _strictify_object_type_full_metadata(
    payload: Dict[str, Any],
    *,
    db_name: str,
    object_type_hint: str,
) -> tuple[Dict[str, Any], int, int]:
    out = dict(payload or {})
    fixes = 0
    dropped = 0

    object_type = out.get("objectType")
    if isinstance(object_type, dict):
        strict_object_type, object_type_fixes = _strictify_foundry_object_type(
            object_type,
            db_name=db_name,
            object_type_hint=object_type_hint,
        )
        out["objectType"] = strict_object_type
        fixes += object_type_fixes
        source_object_type = str(strict_object_type.get("apiName") or object_type_hint).strip() or object_type_hint
    else:
        source_object_type = object_type_hint

    strict_links: list[Dict[str, Any]] = []
    for link_type in out.get("linkTypes") or []:
        if not isinstance(link_type, dict):
            continue
        strict_link, link_fixes, is_resolved = _strictify_outgoing_link_type(
            link_type,
            db_name=db_name,
            source_object_type=source_object_type,
        )
        fixes += link_fixes
        if not is_resolved:
            dropped += 1
            continue
        strict_links.append(strict_link)
    out["linkTypes"] = strict_links
    return out, fixes, dropped


def _log_strict_compat_summary(
    *,
    route: str,
    db_name: str,
    branch: str | None,
    fixes: int,
    dropped: int = 0,
) -> None:
    if fixes <= 0 and dropped <= 0:
        return
    logger.info(
        "Foundry v2 strict compat normalization: route=%s db=%s branch=%s fixes=%d dropped=%d",
        route,
        db_name,
        branch or "",
        fixes,
        dropped,
    )


def _full_metadata_branch_contract(*, branch: str) -> Dict[str, str]:
    return {"rid": branch}


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


def _strip_typed_ref(value: str) -> str:
    """Convert typed refs like ``Customer/cust-1`` → ``cust-1``.

    Our instance payloads store relationship refs as ``<TargetClass>/<instance_id>``.
    Foundry v2 linked-object APIs operate on primary key values only, so we
    normalize here before building search filters.
    """
    text = str(value or "").strip()
    if not text:
        return ""
    # Avoid stripping URLs or other multi-segment strings.
    if text.count("/") == 1 and not text.startswith("http://") and not text.startswith("https://"):
        _, right = text.split("/", 1)
        right = right.strip()
        if right:
            return right
    return text


def _iter_primary_key_values(value: Any):
    if value is None:
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_primary_key_values(item)
        return
    if isinstance(value, dict):
        for key in ("__primaryKey", "primaryKey", "id", "instance_id"):
            candidate = value.get(key)
            if candidate is None:
                continue
            text = _strip_typed_ref(str(candidate))
            if text:
                yield text
        return
    text = _strip_typed_ref(str(value))
    if text:
        yield text


def _coerce_primary_key_values(value: Any) -> list[str]:
    return list(_iter_primary_key_values(value))


def _extract_object_type_relationships(resource: dict[str, Any]) -> list[dict[str, Any]]:
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    rels = spec.get("relationships") if isinstance(spec.get("relationships"), list) else resource.get("relationships")
    if not isinstance(rels, list):
        return []
    return [entry for entry in rels if isinstance(entry, dict)]


def _derive_outgoing_link_types_from_relationships(
    relationships: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    derived: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for rel in relationships:
        predicate = str(rel.get("predicate") or rel.get("apiName") or rel.get("id") or "").strip()
        target = str(rel.get("target") or rel.get("objectTypeApiName") or rel.get("target_object_type") or "").strip()
        if not predicate or not target:
            continue
        key = (predicate, target)
        if key in seen:
            continue
        seen.add(key)
        derived.append({"apiName": predicate, "objectTypeApiName": target})
    derived.sort(key=lambda item: str(item.get("apiName") or ""))
    return derived


def _extract_linked_primary_keys(
    source_row: dict[str, Any],
    *,
    link_type: str,
    foreign_key_property: str | None,
) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in _iter_linked_primary_keys(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
    ):
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _iter_linked_primary_keys(
    source_row: dict[str, Any],
    *,
    link_type: str,
    foreign_key_property: str | None,
):
    if foreign_key_property:
        yield from _iter_primary_key_values(source_row.get(foreign_key_property))

    for key in (link_type, f"{link_type}Ids", f"{link_type}_ids"):
        yield from _iter_primary_key_values(source_row.get(key))

    links = source_row.get("links")
    if isinstance(links, dict):
        yield from _iter_primary_key_values(links.get(link_type))


def _extract_linked_primary_keys_page(
    source_row: dict[str, Any],
    *,
    link_type: str,
    foreign_key_property: str | None,
    offset: int,
    page_size: int,
) -> tuple[list[str], bool]:
    """Extract a deduplicated page without materializing every linked PK."""
    start = max(0, int(offset))
    limit = max(1, int(page_size))
    seen: set[str] = set()
    page: list[str] = []
    deduped_index = 0
    has_more = False

    for value in _iter_linked_primary_keys(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
    ):
        if value in seen:
            continue
        seen.add(value)

        if deduped_index < start:
            deduped_index += 1
            continue

        if len(page) < limit:
            page.append(value)
            deduped_index += 1
            continue

        has_more = True
        break

    return page, has_more


def _linked_primary_key_exists(
    source_row: dict[str, Any],
    *,
    link_type: str,
    foreign_key_property: str | None,
    linked_primary_key: str,
) -> bool:
    target = str(linked_primary_key or "").strip()
    if not target:
        return False
    seen: set[str] = set()
    for value in _iter_linked_primary_keys(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
    ):
        if value in seen:
            continue
        seen.add(value)
        if value == target:
            return True
    return False


def _build_primary_key_where(primary_key_field: str, primary_key_values: list[str]) -> dict[str, Any]:
    if not primary_key_values:
        return {"type": "not", "value": {"type": "isNull", "field": "instance_id"}}
    if len(primary_key_values) == 1:
        return {"type": "eq", "field": primary_key_field, "value": primary_key_values[0]}
    return {
        "type": "or",
        "value": [
            {"type": "eq", "field": primary_key_field, "value": value}
            for value in primary_key_values
        ],
    }


async def _resolve_object_primary_key_field(
    *,
    db_name: str,
    object_type: str,
    branch: str,
    oms_client: OMSClient,
) -> str:
    payload = await oms_client.get_ontology_resource(
        db_name,
        resource_type="object_type",
        resource_id=object_type,
        branch=branch,
    )
    resource = _extract_object_resource(payload)
    if not resource:
        raise ValueError(f"Object type not found: {object_type}")

    ontology_payload: Any = None
    try:
        ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
    except httpx.HTTPError as exc:
        logger.warning(
            "Failed to load object type metadata for primary key resolution (%s/%s): %s",
            db_name,
            object_type,
            exc,
        )
        ontology_payload = None

    object_type_contract = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
    primary_key_field = str(object_type_contract.get("primaryKey") or "").strip()
    if not primary_key_field:
        raise ValueError(f"primaryKey not configured for object type: {object_type}")
    return primary_key_field


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)
    except ValueError as exc:
        if str(exc).strip().lower() == "permission denied":
            raise PermissionDeniedError("Permission denied") from exc
        raise


async def _require_read_role(request: Request, *, db_name: str) -> None:
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=READ_ROLES)
    except ValueError as exc:
        if str(exc).strip().lower() == "permission denied":
            raise PermissionDeniedError("Permission denied") from exc
        raise


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


def _extract_databases(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    rows = data.get("databases") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
        elif isinstance(row, str):
            out.append({"name": row})
    return out


def _extract_ontology_resource_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    container = data if isinstance(data, dict) else payload
    rows = container.get("resources") if isinstance(container, dict) else None
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _extract_ontology_resource(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    row = data if isinstance(data, dict) else payload
    if not isinstance(row, dict):
        return None
    return row


def _localized_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        for key in ("en", "ko"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        for item in value.values():
            if isinstance(item, str) and item.strip():
                return item.strip()
    return None


def _to_foundry_named_metadata(resource: dict[str, Any]) -> dict[str, Any] | None:
    api_name = str(resource.get("id") or "").strip()
    if not api_name:
        return None

    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}

    out: dict[str, Any] = {"apiName": api_name}

    display_name = (
        _localized_text(resource.get("label"))
        or _localized_text(spec.get("display_name"))
        or _localized_text(spec.get("displayName"))
        or _localized_text(metadata.get("displayName"))
    )
    if display_name:
        out["displayName"] = display_name

    description = (
        _localized_text(resource.get("description"))
        or _localized_text(spec.get("description"))
        or _localized_text(metadata.get("description"))
    )
    if description:
        out["description"] = description

    status_value = str(spec.get("status") or resource.get("status") or metadata.get("status") or "ACTIVE").strip()
    if status_value:
        out["status"] = status_value.upper()

    rid = str(resource.get("rid") or "").strip()
    if rid:
        out["rid"] = rid

    return out


def _to_foundry_named_metadata_map(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for resource in resources:
        api_name = str(resource.get("id") or "").strip()
        if not api_name:
            continue
        mapped = _to_foundry_named_metadata(resource)
        if isinstance(mapped, dict) and mapped:
            out[api_name] = mapped
    return out


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, dict) else None


def _list_or_none(value: Any) -> list[Any] | None:
    return list(value) if isinstance(value, list) else None


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _coerce_optional_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return default


def _extract_project_policy_inheritance(
    *,
    permission_policy: dict[str, Any] | None,
    spec: dict[str, Any],
) -> dict[str, Any] | None:
    policy = permission_policy if isinstance(permission_policy, dict) else {}
    inherit = _coerce_optional_bool(
        _first_non_none(
            policy.get("inherit_project_policy"),
            policy.get("inheritProjectPolicy"),
            spec.get("inherit_project_policy"),
            spec.get("inheritProjectPolicy"),
        ),
        default=False,
    )
    if not inherit:
        return None

    scope = str(
        _first_non_none(
            policy.get("project_policy_scope"),
            policy.get("projectPolicyScope"),
            spec.get("project_policy_scope"),
            spec.get("projectPolicyScope"),
            "action_access",
        )
        or "action_access"
    ).strip() or "action_access"
    subject_type = str(
        _first_non_none(
            policy.get("project_policy_subject_type"),
            policy.get("projectPolicySubjectType"),
            spec.get("project_policy_subject_type"),
            spec.get("projectPolicySubjectType"),
            "project",
        )
        or "project"
    ).strip() or "project"
    subject_id = str(
        _first_non_none(
            policy.get("project_policy_subject_id"),
            policy.get("projectPolicySubjectId"),
            spec.get("project_policy_subject_id"),
            spec.get("projectPolicySubjectId"),
            "",
        )
        or ""
    ).strip()
    require_policy = _coerce_optional_bool(
        _first_non_none(
            policy.get("require_project_policy"),
            policy.get("requireProjectPolicy"),
            spec.get("require_project_policy"),
            spec.get("requireProjectPolicy"),
        ),
        default=True,
    )
    out: dict[str, Any] = {
        "enabled": True,
        "scope": scope,
        "subjectType": subject_type,
        "requirePolicy": require_policy,
    }
    if subject_id:
        out["subjectId"] = subject_id
    return out


def _to_foundry_action_type(resource: dict[str, Any]) -> dict[str, Any] | None:
    mapped = _to_foundry_named_metadata(resource)
    if mapped is None:
        return None
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    parameters = (
        _dict_or_none(resource.get("parameters"))
        or _dict_or_none(spec.get("parameters"))
        or _dict_or_none(metadata.get("parameters"))
    )
    if parameters:
        mapped["parameters"] = parameters
    operations = (
        _list_or_none(resource.get("operations"))
        or _list_or_none(spec.get("operations"))
        or _list_or_none(metadata.get("operations"))
    )
    if operations:
        mapped["operations"] = operations
    tool_description = (
        str(resource.get("toolDescription") or "").strip()
        or str(spec.get("toolDescription") or "").strip()
        or str(metadata.get("toolDescription") or "").strip()
    )
    if tool_description:
        mapped["toolDescription"] = tool_description

    permission_policy = (
        _dict_or_none(resource.get("permissionPolicy"))
        or _dict_or_none(resource.get("permission_policy"))
        or _dict_or_none(spec.get("permission_policy"))
        or _dict_or_none(spec.get("permissionPolicy"))
        or _dict_or_none(metadata.get("permission_policy"))
        or _dict_or_none(metadata.get("permissionPolicy"))
    )
    if permission_policy:
        mapped["permissionPolicy"] = permission_policy

    writeback_target = (
        _dict_or_none(resource.get("writebackTarget"))
        or _dict_or_none(resource.get("writeback_target"))
        or _dict_or_none(spec.get("writeback_target"))
        or _dict_or_none(spec.get("writebackTarget"))
        or _dict_or_none(metadata.get("writeback_target"))
        or _dict_or_none(metadata.get("writebackTarget"))
    )
    if writeback_target:
        mapped["writebackTarget"] = writeback_target

    conflict_policy = str(
        _first_non_none(
            resource.get("conflictPolicy"),
            resource.get("conflict_policy"),
            spec.get("conflict_policy"),
            spec.get("conflictPolicy"),
            metadata.get("conflict_policy"),
            metadata.get("conflictPolicy"),
            "",
        )
        or ""
    ).strip()
    if conflict_policy:
        mapped["conflictPolicy"] = conflict_policy

    target_object_type = str(
        _first_non_none(
            resource.get("targetObjectType"),
            resource.get("target_object_type"),
            spec.get("target_object_type"),
            spec.get("targetObjectType"),
            metadata.get("target_object_type"),
            metadata.get("targetObjectType"),
            "",
        )
        or ""
    ).strip()
    if target_object_type:
        mapped["targetObjectType"] = target_object_type

    validation_rules = (
        _list_or_none(resource.get("validationRules"))
        or _list_or_none(resource.get("validation_rules"))
        or _list_or_none(spec.get("validation_rules"))
        or _list_or_none(spec.get("validationRules"))
        or _list_or_none(metadata.get("validation_rules"))
        or _list_or_none(metadata.get("validationRules"))
    )
    if validation_rules:
        mapped["validationRules"] = validation_rules

    project_policy_inheritance = _extract_project_policy_inheritance(
        permission_policy=permission_policy,
        spec=spec,
    )
    if project_policy_inheritance:
        mapped["projectPolicyInheritance"] = project_policy_inheritance

    permission_model = "ontology_roles"
    edits_beyond_actions = False
    try:
        profile = resolve_action_permission_profile(spec)
        permission_model = profile.permission_model
        edits_beyond_actions = profile.edits_beyond_actions
    except ActionPermissionProfileError:
        raw_permission_model = str(
            _first_non_none(
                spec.get("permission_model"),
                spec.get("permissionModel"),
                metadata.get("permission_model"),
                metadata.get("permissionModel"),
                "",
            )
            or ""
        ).strip()
        if raw_permission_model:
            permission_model = raw_permission_model
        edits_beyond_actions = _coerce_optional_bool(
            _first_non_none(
                spec.get("edits_beyond_actions"),
                spec.get("editsBeyondActions"),
                metadata.get("edits_beyond_actions"),
                metadata.get("editsBeyondActions"),
            ),
            default=False,
        )

    mapped["permissionModel"] = permission_model
    mapped["editsBeyondActions"] = edits_beyond_actions
    dynamic_security: dict[str, Any] = {
        "permissionModel": permission_model,
        "editsBeyondActions": edits_beyond_actions,
    }
    if permission_policy:
        dynamic_security["permissionPolicy"] = permission_policy
    if project_policy_inheritance:
        dynamic_security["projectPolicyInheritance"] = project_policy_inheritance
    mapped["dynamicSecurity"] = dynamic_security
    return mapped


def _to_foundry_action_type_map(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for resource in resources:
        api_name = str(resource.get("id") or "").strip()
        if not api_name:
            continue
        mapped = _to_foundry_action_type(resource)
        if isinstance(mapped, dict) and mapped:
            out[api_name] = mapped
    return out


def _to_foundry_query_type(resource: dict[str, Any]) -> dict[str, Any] | None:
    mapped = _to_foundry_named_metadata(resource)
    if mapped is None:
        return None
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    parameters = (
        _dict_or_none(resource.get("parameters"))
        or _dict_or_none(spec.get("parameters"))
        or _dict_or_none(metadata.get("parameters"))
    )
    if parameters:
        mapped["parameters"] = parameters
    output = resource.get("output")
    if output is None:
        output = spec.get("output")
    if output is None:
        output = metadata.get("output")
    if output is not None:
        mapped["output"] = output
    version = (
        str(resource.get("version") or "").strip()
        or str(spec.get("version") or "").strip()
        or str(metadata.get("version") or "").strip()
    )
    if version:
        mapped["version"] = version
    return mapped


def _to_foundry_query_type_map_key(resource: dict[str, Any]) -> str | None:
    mapped = _to_foundry_query_type(resource)
    if not mapped:
        return None
    api_name = str(mapped.get("apiName") or "").strip()
    if not api_name:
        return None
    version = str(mapped.get("version") or "").strip()
    if not version:
        return api_name
    return f"{api_name}:{version}"


def _to_foundry_query_type_metadata_map(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for resource in resources:
        key = _to_foundry_query_type_map_key(resource)
        if not key:
            continue
        mapped = _to_foundry_query_type(resource)
        if isinstance(mapped, dict) and mapped:
            out[key] = mapped
    return out


def _resolve_query_placeholder_key(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None

    if text.startswith("${") and text.endswith("}") and len(text) > 3:
        key = text[2:-1].strip()
        return key or None

    if text.startswith("{{") and text.endswith("}}") and len(text) > 4:
        key = text[2:-2].strip()
        return key or None

    if text.startswith("$") and len(text) > 1:
        key = text[1:].strip()
        if key and all(ch.isalnum() or ch == "_" for ch in key):
            return key

    return None


def _materialize_query_execution_value(value: Any, *, parameters: Dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _materialize_query_execution_value(inner, parameters=parameters)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [_materialize_query_execution_value(item, parameters=parameters) for item in value]

    placeholder = _resolve_query_placeholder_key(value)
    if placeholder is None:
        return value
    if placeholder not in parameters:
        raise ValueError(f"Missing required query parameter: {placeholder}")
    return parameters[placeholder]


_QUERY_OBJECT_TYPE_CANONICAL_FIELDS: tuple[str, ...] = ("objectTypeApiName",)
_QUERY_OBJECT_TYPE_FALLBACK_FIELDS: tuple[str, ...] = (
    "objectType",
    "targetObjectType",
    "target_object_type",
)


def _resolve_query_execution_object_type(
    *,
    execution: dict[str, Any],
    search: dict[str, Any],
    spec: dict[str, Any],
    metadata: dict[str, Any],
) -> str | None:
    sources = (execution, search, spec, metadata)
    for field in _QUERY_OBJECT_TYPE_CANONICAL_FIELDS:
        for source in sources:
            normalized = str(source.get(field) or "").strip()
            if normalized:
                return normalized

    for field in _QUERY_OBJECT_TYPE_FALLBACK_FIELDS:
        for source in sources:
            normalized = str(source.get(field) or "").strip()
            if normalized:
                return normalized
    return None


def _extract_query_execution_plan(resource: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}

    execution: dict[str, Any] = {}
    for candidate in (
        resource.get("execution"),
        spec.get("execution"),
        metadata.get("execution"),
        resource.get("query"),
        spec.get("query"),
        metadata.get("query"),
    ):
        if isinstance(candidate, dict):
            execution = candidate
            break

    search = execution.get("search") if isinstance(execution.get("search"), dict) else {}

    object_type = _resolve_query_execution_object_type(
        execution=execution,
        search=search,
        spec=spec,
        metadata=metadata,
    )

    payload: dict[str, Any] = {}
    for source in (search, execution):
        if not isinstance(source, dict):
            continue
        where = source.get("where")
        if isinstance(where, dict):
            payload["where"] = where
        elif "where" in source and where is not None:
            payload["where"] = where

        filter_clause = source.get("filter")
        if "where" not in payload and isinstance(filter_clause, dict):
            payload["where"] = filter_clause

        for key in ("pageSize", "pageToken", "select", "selectV2", "orderBy", "excludeRid", "snapshot"):
            if key in source and source.get(key) is not None:
                payload[key] = source.get(key)

    return object_type, payload


def _apply_query_execute_options(
    *,
    base_payload: dict[str, Any],
    options: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(base_payload)
    if not isinstance(options, dict):
        return payload

    for key in ("where", "filter", "pageSize", "pageToken", "select", "selectV2", "orderBy", "excludeRid", "snapshot"):
        if key not in options:
            continue
        value = options.get(key)
        if key == "filter":
            if "where" not in payload and value is not None:
                payload["where"] = value
            continue
        if value is not None:
            payload[key] = value

    return payload


def _scoped_error_parameters(
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"ontology": ontology}
    if parameters:
        payload.update({str(key): value for key, value in parameters.items() if value is not None})
    return payload


def _normalize_non_foundry_upstream_error(
    exc: httpx.HTTPStatusError,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
    action_surface: bool = False,
) -> JSONResponse | None:
    response = exc.response
    if response is None:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    if "errorCode" in payload and "errorName" in payload:
        return None

    status_code = int(response.status_code)
    error_code = str(payload.get("code") or "").strip().upper()
    message = str(payload.get("message") or "").strip()
    merged_parameters = _scoped_error_parameters(ontology=ontology, parameters=parameters)
    if message:
        merged_parameters["message"] = message

    if status_code in {status.HTTP_400_BAD_REQUEST, status.HTTP_422_UNPROCESSABLE_ENTITY} or error_code == "REQUEST_VALIDATION_FAILED":
        if action_surface:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="ACTION_VALIDATION_FAILED",
                error_name="ActionValidationFailed",
                parameters=merged_parameters,
            )
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_403_FORBIDDEN:
        if action_surface:
            return _foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="EditObjectPermissionDenied",
                parameters=merged_parameters,
            )
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_404_NOT_FOUND:
        if action_surface:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="ActionTypeNotFound",
                parameters=merged_parameters,
            )
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_409_CONFLICT:
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="Conflict",
            parameters=merged_parameters,
        )
    return None


def _upstream_status_error_response(
    exc: httpx.HTTPStatusError,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
    not_found_response: JSONResponse | None = None,
    passthrough_payload: bool = False,
    normalize_non_foundry_payload: bool = False,
    action_surface: bool = False,
) -> JSONResponse:
    status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
    if normalize_non_foundry_payload:
        normalized = _normalize_non_foundry_upstream_error(
            exc,
            ontology=ontology,
            parameters=parameters,
            action_surface=action_surface,
        )
        if normalized is not None:
            return normalized
    if passthrough_payload:
        passthrough = _passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
    if status_code == status.HTTP_404_NOT_FOUND and not_found_response is not None:
        return not_found_response
    return _foundry_error(
        status_code,
        error_code="UPSTREAM_ERROR",
        error_name="UpstreamError",
        parameters=_scoped_error_parameters(ontology=ontology, parameters=parameters),
    )


def _upstream_transport_error_response(
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return _foundry_error(
        status.HTTP_502_BAD_GATEWAY,
        error_code="UPSTREAM_ERROR",
        error_name="UpstreamError",
        parameters=_scoped_error_parameters(ontology=ontology, parameters=parameters),
    )


def _internal_error_response(
    *,
    log_message: str,
    exc: Exception,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    logger.error("%s: %s", log_message, exc)
    return _foundry_error(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code="INTERNAL",
        error_name="Internal",
        parameters=_scoped_error_parameters(ontology=ontology, parameters=parameters),
    )


async def _find_resource_by_rid(
    *,
    db_name: str,
    branch: str,
    resource_type: str,
    rid: str,
    oms_client: OMSClient,
    page_size: int = 500,
) -> dict[str, Any] | None:
    normalized_rid = str(rid or "").strip()
    if not normalized_rid:
        return None
    rows = await _list_all_resources_for_type(
        db_name=db_name,
        branch=branch,
        resource_type=resource_type,
        oms_client=oms_client,
        page_size=page_size,
    )
    for row in rows:
        candidate = str(row.get("rid") or "").strip()
        if candidate and candidate == normalized_rid:
            return row
    return None


def _group_outgoing_link_types_by_source(resources: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for resource in resources:
        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        source = _normalize_object_ref(spec.get("from"))
        if not source:
            source = _normalize_object_ref(resource.get("from"))
        if not source:
            continue
        mapped = _to_foundry_outgoing_link_type(resource, source_object_type=source)
        if mapped is None:
            continue
        grouped.setdefault(source, []).append(mapped)
    return grouped


def _group_incoming_link_types_by_target(resources: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group incoming link types by their *target* object type.

    This is the inverse of ``_group_outgoing_link_types_by_source``.  For each
    link-type resource whose ``to`` (target) field is object type *T*, we
    produce a ``LinkTypeSideV2`` entry and group it under *T*.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for resource in resources:
        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        target = _normalize_object_ref(spec.get("to"))
        if not target:
            target = _normalize_object_ref(resource.get("to"))
        if not target:
            relationship_spec = spec.get("relationship_spec") if isinstance(spec.get("relationship_spec"), dict) else {}
            if isinstance(relationship_spec, dict):
                target = _normalize_object_ref(relationship_spec.get("target_object_type"))
        if not target:
            continue
        mapped = _to_foundry_incoming_link_type(resource, target_object_type=target)
        if mapped is None:
            continue
        grouped.setdefault(target, []).append(mapped)
    return grouped


_INTERFACE_REF_KEYS = (
    "implementsInterfaces",
    "implements_interfaces",
    "interfaceRefs",
    "interface_refs",
    "interfaceRef",
    "interface_ref",
    "interfaces",
    "interface",
)

_INTERFACE_IMPLEMENTATION_KEYS = (
    "implementsInterfaces2",
    "implements_interfaces2",
    "interfaceImplementations",
    "interface_implementations",
)

_SHARED_PROPERTY_MAPPING_KEYS = (
    "sharedPropertyTypeMapping",
    "shared_property_type_mapping",
    "sharedPropertyMapping",
    "shared_property_mapping",
)


def _strip_prefix(text: str, *, prefixes: tuple[str, ...]) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    lowered = normalized.lower()
    for prefix in prefixes:
        if lowered.startswith(prefix):
            return normalized[len(prefix) :].strip()
    return normalized


def _normalize_interface_ref(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    normalized = _strip_prefix(value, prefixes=("interface:", "interfaces:"))
    return normalized.strip()


def _normalize_shared_property_ref(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    normalized = _strip_prefix(
        value,
        prefixes=(
            "shared_property:",
            "shared_properties:",
            "shared-property:",
            "shared-properties:",
            "shared:",
        ),
    )
    return normalized.strip()


def _coerce_string_list(value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(value, str):
        candidate = value.strip()
        if candidate:
            values.append(candidate)
        return values
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                candidate = item.strip()
                if candidate:
                    values.append(candidate)
                continue
            if isinstance(item, dict):
                candidate = (
                    item.get("apiName")
                    or item.get("interfaceType")
                    or item.get("interface")
                    or item.get("name")
                    or item.get("id")
                )
                if isinstance(candidate, str) and candidate.strip():
                    values.append(candidate.strip())
    return values


def _ordered_unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _extract_interface_implementations(resource: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    for container in (resource, spec, metadata):
        if not isinstance(container, dict):
            continue
        for key in _INTERFACE_IMPLEMENTATION_KEYS:
            raw = container.get(key)
            if not isinstance(raw, dict):
                continue
            for raw_name, implementation in raw.items():
                interface_name = _normalize_interface_ref(raw_name)
                if not interface_name:
                    continue
                out[interface_name] = dict(implementation) if isinstance(implementation, dict) else {}
    return out


def _extract_interface_names(resource: dict[str, Any]) -> list[str]:
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    refs: list[str] = []
    for container in (resource, spec, metadata):
        if not isinstance(container, dict):
            continue
        for key in _INTERFACE_REF_KEYS:
            refs.extend(_coerce_string_list(container.get(key)))
    refs.extend(list(_extract_interface_implementations(resource).keys()))
    normalized = [_normalize_interface_ref(ref) for ref in refs]
    return _ordered_unique([ref for ref in normalized if ref])


def _extract_ontology_properties_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    properties = data.get("properties") if isinstance(data, dict) else None
    if not isinstance(properties, list):
        return []
    return [property_item for property_item in properties if isinstance(property_item, dict)]


def _extract_shared_property_type_mapping(resource: dict[str, Any], *, ontology_payload: Any) -> dict[str, str]:
    mapping: dict[str, str] = {}

    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    for container in (resource, spec, metadata):
        if not isinstance(container, dict):
            continue
        for key in _SHARED_PROPERTY_MAPPING_KEYS:
            raw_mapping = container.get(key)
            if not isinstance(raw_mapping, dict):
                continue
            for raw_shared_ref, raw_local_property in raw_mapping.items():
                shared_ref = _normalize_shared_property_ref(raw_shared_ref)
                local_property = str(raw_local_property or "").strip()
                if shared_ref and local_property:
                    mapping[shared_ref] = local_property

    for property_item in _extract_ontology_properties_payload(ontology_payload):
        local_property = str(property_item.get("name") or property_item.get("id") or "").strip()
        if not local_property:
            continue
        prop_metadata = property_item.get("metadata") if isinstance(property_item.get("metadata"), dict) else {}
        raw_shared_ref = (
            property_item.get("shared_property_ref")
            or property_item.get("sharedPropertyRef")
            or prop_metadata.get("shared_property_ref")
            or prop_metadata.get("sharedPropertyRef")
        )
        shared_ref = _normalize_shared_property_ref(raw_shared_ref)
        if shared_ref and shared_ref not in mapping:
            mapping[shared_ref] = local_property

    return mapping


def _to_foundry_object_type_full_metadata(
    resource: dict[str, Any],
    *,
    ontology_payload: Any,
    link_types: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    object_type = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
    interface_names = _extract_interface_names(resource)
    interface_implementations = _extract_interface_implementations(resource)
    for interface_name in interface_names:
        interface_implementations.setdefault(interface_name, {})
    return {
        "objectType": object_type,
        "linkTypes": [dict(link_type) for link_type in (link_types or []) if isinstance(link_type, dict)],
        "implementsInterfaces": interface_names,
        "implementsInterfaces2": interface_implementations,
        "sharedPropertyTypeMapping": _extract_shared_property_type_mapping(resource, ontology_payload=ontology_payload),
    }


async def _list_all_resources_for_type(
    *,
    db_name: str,
    branch: str,
    resource_type: str | None,
    oms_client: OMSClient,
    page_size: int = 500,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    limit = max(1, min(1000, int(page_size)))
    while True:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )
        page_rows = _extract_ontology_resource_rows(payload)
        if not page_rows:
            break
        rows.extend(page_rows)
        if len(page_rows) < limit:
            break
        offset += len(page_rows)
    return rows


def _looks_like_action_type_resource(resource: dict[str, Any]) -> bool:
    if not isinstance(resource, dict):
        return False
    resource_type_raw = str(resource.get("resource_type") or resource.get("resourceType") or "").strip().lower()
    if resource_type_raw in _ACTION_TYPE_RESOURCE_TYPE_ALIASES:
        return True
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    return any(field in spec for field in _ACTION_TYPE_SPEC_HINT_FIELDS)


async def _list_action_type_resources_with_fallback(
    *,
    db_name: str,
    branch: str,
    oms_client: OMSClient,
) -> list[dict[str, Any]]:
    rows = await _list_all_resources_for_type(
        db_name=db_name,
        branch=branch,
        resource_type="action_type",
        oms_client=oms_client,
    )
    if rows:
        return rows
    all_rows = await _list_all_resources_for_type(
        db_name=db_name,
        branch=branch,
        resource_type=None,
        oms_client=oms_client,
    )
    filtered = [row for row in all_rows if _looks_like_action_type_resource(row)]
    deduped: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in filtered:
        resource_id = str(row.get("id") or "").strip()
        if resource_id and resource_id in seen_ids:
            continue
        if resource_id:
            seen_ids.add(resource_id)
        deduped.append(row)
    return deduped


async def _find_action_type_resource_by_id(
    *,
    db_name: str,
    branch: str,
    action_type: str,
    oms_client: OMSClient,
) -> dict[str, Any] | None:
    normalized_action_type = str(action_type or "").strip()
    if not normalized_action_type:
        return None
    rows = await _list_action_type_resources_with_fallback(
        db_name=db_name,
        branch=branch,
        oms_client=oms_client,
    )
    for row in rows:
        if str(row.get("id") or "").strip() == normalized_action_type:
            return row
    return None


async def _find_action_type_resource_by_rid(
    *,
    db_name: str,
    branch: str,
    action_type_rid: str,
    oms_client: OMSClient,
) -> dict[str, Any] | None:
    normalized_rid = str(action_type_rid or "").strip()
    if not normalized_rid:
        return None
    rows = await _list_action_type_resources_with_fallback(
        db_name=db_name,
        branch=branch,
        oms_client=oms_client,
    )
    for row in rows:
        if str(row.get("rid") or "").strip() == normalized_rid:
            return row
    return None


async def _list_resources_best_effort(
    *,
    db_name: str,
    branch: str,
    resource_type: str,
    oms_client: OMSClient,
) -> list[dict[str, Any]]:
    try:
        return await _list_all_resources_for_type(
            db_name=db_name,
            branch=branch,
            resource_type=resource_type,
            oms_client=oms_client,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "Failed to list ontology resources for full metadata (%s/%s/%s): %s",
            db_name,
            branch,
            resource_type,
            exc,
        )
        return []


async def _list_action_type_resources_best_effort(
    *,
    db_name: str,
    branch: str,
    oms_client: OMSClient,
) -> list[dict[str, Any]]:
    try:
        return await _list_action_type_resources_with_fallback(
            db_name=db_name,
            branch=branch,
            oms_client=oms_client,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "Failed to list action type resources for full metadata (%s/%s): %s",
            db_name,
            branch,
            exc,
        )
        return []


async def _get_ontology_payload_best_effort(
    *,
    db_name: str,
    branch: str,
    object_type: str,
    oms_client: OMSClient,
) -> Any:
    try:
        return await oms_client.get_ontology(db_name, object_type, branch=branch)
    except httpx.HTTPError as exc:
        logger.warning(
            "Failed to enrich full metadata object type (%s/%s/%s): %s",
            db_name,
            branch,
            object_type,
            exc,
        )
        return None


def _to_foundry_ontology(row: dict[str, Any]) -> dict[str, Any]:
    api_name = str(row.get("name") or row.get("db_name") or row.get("apiName") or "").strip()
    display_name = (
        str(row.get("displayName") or "").strip()
        or str(row.get("label") or "").strip()
        or api_name
    )
    out: dict[str, Any] = {
        "apiName": api_name,
        "displayName": display_name,
    }
    description = str(row.get("description") or "").strip()
    if description:
        out["description"] = description
    rid = str(row.get("rid") or row.get("ontologyRid") or "").strip()
    if rid:
        out["rid"] = rid
    return out


@router.get("", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_ontologies")
async def list_ontologies_v2(
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        _ = request
        payload = await oms_client.list_databases()
        rows = _extract_databases(payload)
        data = [
            _to_foundry_ontology(row)
            for row in rows
            if str(row.get("name") or row.get("db_name") or row.get("apiName") or "").strip()
        ]
        return {"data": data}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to list ontologies (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={},
        )


@router.get("/{ontologyRid}", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.get_ontology")
async def get_ontology_v2(
    ontologyRid: str,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(exc, ontology=str(ontology))

    try:
        payload = await oms_client.get_database(db_name)
        row = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
        if not isinstance(row, dict):
            row = {"name": db_name}
        out = _to_foundry_ontology(row)
        if not out.get("apiName"):
            out["apiName"] = db_name
        if not out.get("displayName"):
            out["displayName"] = db_name
        return out
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
        logger.error("Failed to get ontology (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name},
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
    strict_compat = False
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="GET /api/v2/ontologies/{ontologyRid}/fullMetadata",
        )
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
        )

    try:
        database_payload = await oms_client.get_database(db_name)
        database_row = (
            database_payload.get("data")
            if isinstance(database_payload, dict) and isinstance(database_payload.get("data"), dict)
            else database_payload
        )
        if not isinstance(database_row, dict):
            database_row = {"name": db_name}

        (
            object_resources,
            link_resources,
            action_resources,
            query_resources,
            interface_resources,
            shared_property_resources,
            value_type_resources,
        ) = await asyncio.gather(
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="object_type",
                oms_client=oms_client,
            ),
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="link_type",
                oms_client=oms_client,
            ),
            _list_action_type_resources_best_effort(
                db_name=db_name,
                branch=branch,
                oms_client=oms_client,
            ),
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="function",
                oms_client=oms_client,
            ),
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="interface",
                oms_client=oms_client,
            ),
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="shared_property",
                oms_client=oms_client,
            ),
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="value_type",
                oms_client=oms_client,
            ),
        )

        link_types_by_source = _group_outgoing_link_types_by_source(link_resources)
        link_types_by_target = _group_incoming_link_types_by_target(link_resources)

        object_type_ids: list[str] = []
        for resource in object_resources:
            object_type_id = str(resource.get("id") or "").strip()
            if object_type_id:
                object_type_ids.append(object_type_id)

        ontology_payloads = await asyncio.gather(
            *[
                _get_ontology_payload_best_effort(
                    db_name=db_name,
                    branch=branch,
                    object_type=object_type_id,
                    oms_client=oms_client,
                )
                for object_type_id in object_type_ids
            ]
        )
        ontology_payload_by_object_type = dict(zip(object_type_ids, ontology_payloads))

        object_types: dict[str, dict[str, Any]] = {}
        strict_fix_count = 0
        strict_dropped_count = 0
        for resource in object_resources:
            object_type_id = str(resource.get("id") or "").strip()
            if not object_type_id:
                continue
            # Merge outgoing + incoming link types for fullMetadata.
            outgoing_lt = link_types_by_source.get(object_type_id) or []
            incoming_lt = link_types_by_target.get(object_type_id) or []
            seen_lt: set[tuple[str, str]] = set()
            merged_lt: list[dict[str, Any]] = []
            for lt in outgoing_lt + incoming_lt:
                key = (lt.get("apiName", ""), lt.get("objectTypeApiName", ""))
                if key not in seen_lt:
                    seen_lt.add(key)
                    merged_lt.append(lt)
            mapped = _to_foundry_object_type_full_metadata(
                resource,
                ontology_payload=ontology_payload_by_object_type.get(object_type_id),
                link_types=merged_lt,
            )
            if strict_compat:
                mapped, object_fixes, object_dropped = _strictify_object_type_full_metadata(
                    mapped,
                    db_name=db_name,
                    object_type_hint=object_type_id,
                )
                strict_fix_count += object_fixes
                strict_dropped_count += object_dropped
            object_types[object_type_id] = mapped

        ontology_contract = _to_foundry_ontology(database_row)
        if not ontology_contract.get("apiName"):
            ontology_contract["apiName"] = db_name
        if not ontology_contract.get("displayName"):
            ontology_contract["displayName"] = db_name

        _log_strict_compat_summary(
            route="get_full_metadata_v2",
            db_name=db_name,
            branch=branch,
            fixes=strict_fix_count,
            dropped=strict_dropped_count,
        )

        return {
            "ontology": ontology_contract,
            "branch": _full_metadata_branch_contract(branch=branch),
            "objectTypes": object_types,
            "actionTypes": _to_foundry_action_type_map(action_resources),
            "queryTypes": _to_foundry_query_type_metadata_map(query_resources),
            "interfaceTypes": _to_foundry_named_metadata_map(interface_resources),
            "sharedPropertyTypes": _to_foundry_named_metadata_map(shared_property_resources),
            "valueTypes": _to_foundry_named_metadata_map(value_type_resources),
        }
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=_not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get full metadata (v2)",
            exc=exc,
            ontology=db_name,
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
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/actionTypes", db_name, branch, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(exc, ontology=str(ontology))

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="action_type",
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        raw_resources = _extract_ontology_resource_rows(payload)
        resources = raw_resources
        total_available: int | None = None
        if not resources:
            fallback_resources = await _list_action_type_resources_with_fallback(
                db_name=db_name,
                branch=branch,
                oms_client=oms_client,
            )
            total_available = len(fallback_resources)
            resources = fallback_resources[offset : offset + page_size]
        data = [
            mapped
            for mapped in (_to_foundry_action_type(resource) for resource in resources)
            if mapped is not None
        ]
        if total_available is not None:
            consumed = offset + len(resources)
            next_page_token = _encode_page_token(consumed, scope=page_scope) if consumed < total_available else None
        else:
            next_page_token = _encode_page_token(offset + len(raw_resources), scope=page_scope) if len(raw_resources) == page_size else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=_not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to list action types (v2)",
            exc=exc,
            ontology=db_name,
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
    ontology = ontologyRid
    actionType = actionTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        action_type = str(actionType or "").strip()
        if not action_type:
            raise ValueError("actionType is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"actionType": str(actionType)},
        )

    try:
        resource: dict[str, Any] | None = None
        try:
            payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="action_type",
                resource_id=action_type,
                branch=branch,
            )
            resource = _extract_ontology_resource(payload)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
            if status_code != status.HTTP_404_NOT_FOUND:
                raise

        if not resource:
            resource = await _find_action_type_resource_by_id(
                db_name=db_name,
                branch=branch,
                action_type=action_type,
                oms_client=oms_client,
            )

        if not resource:
            return _not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionType": action_type},
            )
        mapped = _to_foundry_action_type(resource)
        if mapped is None:
            return _not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionType": action_type},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"actionType": action_type},
            not_found_response=_not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionType": action_type},
            ),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionType": action_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get action type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"actionType": action_type},
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
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        action_type_rid = str(actionTypeRid or "").strip()
        if not action_type_rid:
            raise ValueError("actionTypeRid is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"actionTypeRid": str(actionTypeRid)},
        )

    try:
        resource = await _find_resource_by_rid(
            db_name=db_name,
            branch=branch,
            resource_type="action_type",
            rid=action_type_rid,
            oms_client=oms_client,
        )
        if not resource:
            resource = await _find_action_type_resource_by_rid(
                db_name=db_name,
                branch=branch,
                action_type_rid=action_type_rid,
                oms_client=oms_client,
            )
        if not resource:
            return _not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionTypeRid": action_type_rid},
            )
        mapped = _to_foundry_action_type(resource)
        if mapped is None:
            return _not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionTypeRid": action_type_rid},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"actionTypeRid": action_type_rid},
            not_found_response=_not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionTypeRid": action_type_rid},
            ),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionTypeRid": action_type_rid},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get action type by rid (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"actionTypeRid": action_type_rid},
        )


def _resolve_apply_action_mode(*, explicit_mode: str | None) -> str:
    mode = str(explicit_mode or "").strip().upper()
    if not mode:
        return "VALIDATE_AND_EXECUTE"
    if mode not in {"VALIDATE_ONLY", "VALIDATE_AND_EXECUTE"}:
        raise ValueError("options.mode must be VALIDATE_ONLY or VALIDATE_AND_EXECUTE")
    return mode


def _default_action_parameter_results(parameters: Dict[str, Any] | None) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    if not isinstance(parameters, dict):
        return results
    for raw_name in parameters.keys():
        name = str(raw_name or "").strip()
        if not name:
            continue
        results[name] = {
            "required": True,
            "evaluatedConstraints": [],
            "result": "VALID",
        }
    return results


def _extract_action_audit_log_id(payload: Dict[str, Any], data_payload: Dict[str, Any]) -> str | None:
    candidates = (
        payload.get("auditLogId"),
        payload.get("action_log_id"),
        data_payload.get("auditLogId"),
        data_payload.get("action_log_id"),
        payload.get("command_id"),
        data_payload.get("command_id"),
    )
    for raw_value in candidates:
        text = str(raw_value or "").strip()
        if text:
            return text
    return None


def _normalize_action_writeback_status(
    raw_status: Any,
    *,
    audit_log_id: str | None,
    side_effect_delivery: Any,
) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in {"confirmed", "missing", "not_configured"}:
        return normalized
    if audit_log_id or side_effect_delivery is not None:
        return "confirmed"
    return "not_configured"


def _normalize_apply_action_response_payload(
    *,
    response: Any,
    request_parameters: Dict[str, Any] | None,
) -> Dict[str, Any]:
    fallback_parameters = _default_action_parameter_results(request_parameters)
    if not isinstance(response, dict):
        return {
            "validation": {"result": "VALID"},
            "parameters": fallback_parameters,
            "auditLogId": None,
            "action_log_id": None,
            "sideEffectDelivery": None,
            "writebackStatus": "not_configured",
        }

    normalized: Dict[str, Any] = dict(response)
    validation = normalized.get("validation")
    if not isinstance(validation, dict):
        validation = {}
    normalized_validation = dict(validation)

    validation_result = str(normalized_validation.get("result") or "").strip().upper()
    if validation_result not in {"VALID", "INVALID"}:
        validation_result = "VALID"
    normalized_validation["result"] = validation_result

    submission_criteria = normalized_validation.get("submissionCriteria")
    if not isinstance(submission_criteria, list):
        submission_criteria = []
    normalized_validation["submissionCriteria"] = submission_criteria

    nested_parameters = normalized_validation.pop("parameters", None)
    top_parameters = normalized.get("parameters")
    if not isinstance(top_parameters, dict):
        top_parameters = nested_parameters if isinstance(nested_parameters, dict) else None
    if not isinstance(top_parameters, dict):
        top_parameters = fallback_parameters
    elif not top_parameters and fallback_parameters:
        top_parameters = fallback_parameters

    normalized_validation["parameters"] = top_parameters
    normalized["validation"] = normalized_validation
    normalized["parameters"] = top_parameters
    data_payload = normalized.get("data") if isinstance(normalized.get("data"), dict) else {}

    audit_log_id = _extract_action_audit_log_id(normalized, data_payload)
    side_effect_delivery = normalized.get("sideEffectDelivery")
    if side_effect_delivery is None:
        side_effect_delivery = data_payload.get("sideEffectDelivery")
    writeback_status = _normalize_action_writeback_status(
        normalized.get("writebackStatus") or data_payload.get("writebackStatus"),
        audit_log_id=audit_log_id,
        side_effect_delivery=side_effect_delivery,
    )

    normalized["auditLogId"] = audit_log_id
    normalized["action_log_id"] = audit_log_id
    normalized["sideEffectDelivery"] = side_effect_delivery
    normalized["writebackStatus"] = writeback_status

    if isinstance(data_payload, dict):
        data_payload.setdefault("auditLogId", audit_log_id)
        data_payload.setdefault("action_log_id", audit_log_id)
        data_payload.setdefault("sideEffectDelivery", side_effect_delivery)
        data_payload.setdefault("writebackStatus", writeback_status)
        normalized["data"] = data_payload
    return normalized


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
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/queryTypes", db_name, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(exc, ontology=str(ontology))

    try:
        resources: list[dict[str, Any]] = []
        for branch_name in _query_type_branch_candidates():
            payload = await oms_client.list_ontology_resources(
                db_name,
                resource_type="function",
                branch=branch_name,
                limit=page_size,
                offset=offset,
            )
            branch_resources = _extract_ontology_resource_rows(payload)
            if branch_resources:
                resources = branch_resources
                break
            if not resources:
                resources = branch_resources
        data = [
            mapped
            for mapped in (_to_foundry_query_type(resource) for resource in resources)
            if mapped is not None
        ]
        next_page_token = _encode_page_token(offset + len(resources), scope=page_scope) if len(resources) == page_size else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=_not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to list query types (v2)",
            exc=exc,
            ontology=db_name,
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
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version, version
        query_api_name = str(queryApiName or "").strip()
        if not query_api_name:
            raise ValueError("queryApiName is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"queryApiName": str(queryApiName)},
        )

    try:
        resource = None
        for branch_name in _query_type_branch_candidates():
            try:
                payload = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="function",
                    resource_id=query_api_name,
                    branch=branch_name,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == status.HTTP_404_NOT_FOUND:
                    continue
                raise
            candidate = _extract_ontology_resource(payload)
            if candidate:
                resource = candidate
                break
        if not resource:
            return _not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": query_api_name},
            )
        mapped = _to_foundry_query_type(resource)
        if mapped is None:
            return _not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": query_api_name},
            )
        requested_version = str(version or "").strip()
        if requested_version:
            resolved_version = str(mapped.get("version") or "").strip()
            if resolved_version and resolved_version != requested_version:
                return _not_found_error(
                    "QueryTypeNotFound",
                    ontology=db_name,
                    parameters={"queryApiName": query_api_name},
                )
        return mapped
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
            not_found_response=_not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": query_api_name},
            ),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get query type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
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
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version, transaction_id
        query_api_name = str(queryApiName or "").strip()
        if not query_api_name:
            raise ValueError("queryApiName is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"queryApiName": str(queryApiName)},
        )

    try:
        resource = None
        query_resource_branch = _query_type_branch_candidates()[0]
        for branch_name in _query_type_branch_candidates():
            try:
                payload = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="function",
                    resource_id=query_api_name,
                    branch=branch_name,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == status.HTTP_404_NOT_FOUND:
                    continue
                raise
            candidate = _extract_ontology_resource(payload)
            if candidate:
                resource = candidate
                query_resource_branch = branch_name
                break
        if not resource:
            return _not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": query_api_name},
            )

        query_type = _to_foundry_query_type(resource)
        if query_type is None:
            return _not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": query_api_name},
            )

        requested_version = str(version or "").strip()
        if requested_version:
            resolved_version = str(query_type.get("version") or "").strip()
            if resolved_version and resolved_version != requested_version:
                return _not_found_error(
                    "QueryTypeNotFound",
                    ontology=db_name,
                    parameters={"queryApiName": query_api_name},
                )

        object_type, execution_payload = _extract_query_execution_plan(resource)
        if not object_type:
            raise ValueError("query execution spec requires objectType")

        execution_payload = _apply_query_execute_options(
            base_payload=execution_payload,
            options=body.options,
        )
        parameters_payload = dict(body.parameters or {})
        resolved_object_type = _materialize_query_execution_value(
            object_type,
            parameters=parameters_payload,
        )
        normalized_object_type = str(resolved_object_type or "").strip()
        if not normalized_object_type:
            raise ValueError("query execution spec resolved an empty objectType")
        resolved_payload = _materialize_query_execution_value(
            execution_payload,
            parameters=parameters_payload,
        )
        if not isinstance(resolved_payload, dict):
            raise ValueError("query execution spec must resolve to an object payload")
        if "pageSize" not in resolved_payload:
            resolved_payload["pageSize"] = 100

        result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{normalized_object_type}/search",
            params={"branch": query_resource_branch},
            json=resolved_payload,
        )
        if not isinstance(result, dict):
            raise ValueError("query execution result must be a JSON object")
        return {"value": result}
    except ValueError as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": db_name, "queryApiName": query_api_name, "message": str(exc)},
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
            not_found_response=_not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": query_api_name},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to execute query (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
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
    strict_compat = False
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/interfaceTypes",
        )
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/interfaceTypes", db_name, branch, page_size, "1" if preview else "0")
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview},
        )

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="interface",
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        resources = _extract_ontology_resource_rows(payload)
        data = [
            mapped
            for mapped in (_to_foundry_named_metadata(resource) for resource in resources)
            if mapped is not None
        ]
        next_page_token = _encode_page_token(offset + len(resources), scope=page_scope) if len(resources) == page_size else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=_not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to list interface types (v2)",
            exc=exc,
            ontology=db_name,
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
    strict_compat = False
    ontology = ontologyRid
    interfaceType = interfaceTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/interfaceTypes/{interfaceTypeApiName}",
        )
        _ = sdk_package_rid, sdk_version
        interface_type = str(interfaceType or "").strip()
        if not interface_type:
            raise ValueError("interfaceType is required")
        await _require_domain_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview, "interfaceType": str(interfaceType)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="interface",
            resource_id=interface_type,
            branch=branch,
        )
        resource = _extract_ontology_resource(payload)
        if not resource:
            return _not_found_error(
                "InterfaceTypeNotFound",
                ontology=db_name,
                parameters={"interfaceType": interface_type},
            )
        mapped = _to_foundry_named_metadata(resource)
        if mapped is None:
            return _not_found_error(
                "InterfaceTypeNotFound",
                ontology=db_name,
                parameters={"interfaceType": interface_type},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"interfaceType": interface_type},
            not_found_response=_not_found_error(
                "InterfaceTypeNotFound",
                ontology=db_name,
                parameters={"interfaceType": interface_type},
            ),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"interfaceType": interface_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get interface type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"interfaceType": interface_type},
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
    strict_compat = False
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/sharedPropertyTypes",
        )
        await _require_read_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/sharedPropertyTypes", db_name, branch, page_size, "1" if preview else "0")
        offset = _decode_page_token(page_token, scope=page_scope)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview},
        )

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="shared_property",
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        resources = _extract_ontology_resource_rows(payload)
        data = [
            mapped
            for mapped in (_to_foundry_named_metadata(resource) for resource in resources)
            if mapped is not None
        ]
        next_page_token = _encode_page_token(offset + len(resources), scope=page_scope) if len(resources) == page_size else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=_not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to list shared property types (v2)",
            exc=exc,
            ontology=db_name,
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
    strict_compat = False
    ontology = ontologyRid
    sharedPropertyType = sharedPropertyTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/sharedPropertyTypes/{sharedPropertyTypeApiName}",
        )
        shared_property_type = str(sharedPropertyType or "").strip()
        if not shared_property_type:
            raise ValueError("sharedPropertyType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview, "sharedPropertyType": str(sharedPropertyType)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="shared_property",
            resource_id=shared_property_type,
            branch=branch,
        )
        resource = _extract_ontology_resource(payload)
        if not resource:
            return _not_found_error(
                "SharedPropertyTypeNotFound",
                ontology=db_name,
                parameters={"sharedPropertyType": shared_property_type},
            )
        mapped = _to_foundry_named_metadata(resource)
        if mapped is None:
            return _not_found_error(
                "SharedPropertyTypeNotFound",
                ontology=db_name,
                parameters={"sharedPropertyType": shared_property_type},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"sharedPropertyType": shared_property_type},
            not_found_response=_not_found_error(
                "SharedPropertyTypeNotFound",
                ontology=db_name,
                parameters={"sharedPropertyType": shared_property_type},
            ),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"sharedPropertyType": shared_property_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get shared property type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"sharedPropertyType": shared_property_type},
        )


@router.get("/{ontologyRid}/valueTypes", dependencies=[_ONTOLOGY_READ])
@trace_endpoint("bff.foundry_v2_ontology.list_value_types")
async def list_value_types_v2(
    ontologyRid: str,
    request: Request,
    preview: bool = Query(False),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/valueTypes",
        )
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview},
        )

    try:
        resources = await _list_all_resources_for_type(
            db_name=db_name,
            branch="main",
            resource_type="value_type",
            oms_client=oms_client,
        )
        data = [
            mapped
            for mapped in (_to_foundry_named_metadata(resource) for resource in resources)
            if mapped is not None
        ]
        return {"data": data}
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=_not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to list value types (v2)",
            exc=exc,
            ontology=db_name,
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
    strict_compat = False
    ontology = ontologyRid
    valueType = valueTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/valueTypes/{valueTypeApiName}",
        )
        value_type = str(valueType or "").strip()
        if not value_type:
            raise ValueError("valueType is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview, "valueType": str(valueType)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="value_type",
            resource_id=value_type,
            branch="main",
        )
        resource = _extract_ontology_resource(payload)
        if not resource:
            return _not_found_error(
                "ValueTypeNotFound",
                ontology=db_name,
                parameters={"valueType": value_type},
            )
        mapped = _to_foundry_named_metadata(resource)
        if mapped is None:
            return _not_found_error(
                "ValueTypeNotFound",
                ontology=db_name,
                parameters={"valueType": value_type},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"valueType": value_type},
            not_found_response=_not_found_error(
                "ValueTypeNotFound",
                ontology=db_name,
                parameters={"valueType": value_type},
            ),
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"valueType": value_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get value type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"valueType": value_type},
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
                object_resource = object_payload.get("data") if isinstance(object_payload, dict) else object_payload
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
            resource = payload.get("data") if isinstance(payload, dict) else payload
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
                object_resource = object_payload.get("data") if isinstance(object_payload, dict) else object_payload
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
        resource = payload.get("data") if isinstance(payload, dict) else payload
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
        logger.error("Failed to search objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
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
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType)},
        )

    try:
        result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{object_type}/count",
            params={
                "branch": branch,
                "sdkPackageRid": sdk_package_rid,
                "sdkVersion": sdk_version,
            },
        )
        if not isinstance(result, dict):
            return {"count": None}
        return {"count": result.get("count")}
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
        logger.error("Failed to count objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
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
    """Delegate aggregation to OMS ES-native aggregate engine."""
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        object_type = str(objectType or "").strip()
        _ = transaction_id, sdk_package_rid, sdk_version
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
        return await oms_client.aggregate_objects_v2(
            db_name,
            object_type,
            payload,
            branch=branch,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectType": object_type},
            not_found_response=_not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                object_type=object_type,
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectType": object_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to aggregate objects (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
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
):
    search_around = False
    object_type: str | None = None
    search_payload: dict[str, Any] = {}
    actor: str | None = None
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        search_around = _is_search_around_object_set(object_set)
        if not search_around:
            object_type = _resolve_object_set_object_type(object_set)
            if not object_type:
                raise ValueError("objectSet.objectType is required")
        await _require_read_role(request, db_name=db_name)
        _, actor = resolve_database_actor(request.headers)
        if not search_around:
            search_payload = _build_object_set_search_payload(
                object_set=object_set,
                payload=payload,
                require_select=True,
            )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjects"},
        )

    try:
        if search_around:
            rows, total_count, next_page_token, _ = await _load_rows_for_search_around_object_set(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_set=object_set,
                payload=payload,
                endpoint_scope="v2/objectSets/loadObjects",
                actor=actor,
            )
        else:
            rows, total_count, next_page_token = await _load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_type or "",
                search_payload=search_payload,
            )
        response: dict[str, Any] = {
            "data": rows,
            "nextPageToken": next_page_token,
            "totalCount": total_count,
        }
        if payload.get("includeComputeUsage"):
            response["computeUsage"] = 0
        return response
    except ValueError as exc:
        return _object_set_runtime_value_error_response(
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjects"},
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = _passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadObjects"},
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectSet": "loadObjects"},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to load object set objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectSet": "loadObjects"},
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
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontologyRid}/objectSets/loadLinks",
        )
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        requested_links = _normalize_link_type_values(payload)
        object_types = _collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await _require_read_role(request, db_name=db_name)
        search_payload = _build_object_set_search_payload(
            object_set=object_set,
            payload={
                "pageToken": payload.get("pageToken"),
                "pageSize": 1000,
            },
            require_select=False,
        )
        request_page_token = search_payload.get("pageToken")
        if request_page_token is not None:
            request_page_token = str(request_page_token)
        pagination_scope = _pagination_scope(
            "v2/objectSets/loadLinks",
            db_name,
            branch,
            ",".join(sorted(object_types)),
            ",".join(requested_links),
            str(payload.get("objectSet") or ""),
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadLinks"},
        )

    try:
        if len(object_types) == 1:
            rows, _, next_page_token = await _load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_types[0],
                search_payload=search_payload,
            )
        else:
            rows, _, next_page_token = await _load_rows_for_multi_object_types(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_types=object_types,
                search_payload=search_payload,
                page_token=request_page_token,
                pagination_scope=pagination_scope,
            )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "loadLinks"},
            not_found_response=_not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadLinks"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "loadLinks"},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to load object set source rows (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadLinks"},
        )

    link_resources: dict[str, dict[str, Any]] = {}
    for link_type in requested_links:
        try:
            link_payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type,
                branch=branch,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
            if status_code == status.HTTP_404_NOT_FOUND:
                return _not_found_error(
                    "LinkTypeNotFound",
                    ontology=db_name,
                    link_type=link_type,
                    parameters={"objectSet": "loadLinks"},
                )
            return _upstream_status_error_response(
                exc,
                ontology=db_name,
                parameters={"objectSet": "loadLinks", "linkType": link_type},
                passthrough_payload=True,
            )
        except httpx.HTTPError:
            return _upstream_transport_error_response(
                ontology=db_name,
                parameters={"objectSet": "loadLinks", "linkType": link_type},
            )

        link_resource = link_payload.get("data") if isinstance(link_payload, dict) else link_payload
        if not isinstance(link_resource, dict):
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                link_type=link_type,
                parameters={"objectSet": "loadLinks"},
            )
        link_resources[link_type] = link_resource

    default_object_type = object_types[0] if len(object_types) == 1 else None
    source_object_types = list(
        dict.fromkeys(
            object_types
            + [
                object_type
                for row in rows
                if (
                    object_type := _resolve_source_object_type_from_row(
                        row,
                        fallback_object_type=default_object_type,
                    )
                )
            ]
        )
    )

    link_sides_by_source_type: dict[str, dict[str, dict[str, Any]]] = {}
    for source_object_type in source_object_types:
        source_link_sides: dict[str, dict[str, Any]] = {}
        for link_type, link_resource in link_resources.items():
            link_side = _to_foundry_outgoing_link_type(link_resource, source_object_type=source_object_type)
            if isinstance(link_side, dict):
                source_link_sides[link_type] = link_side
        link_sides_by_source_type[source_object_type] = source_link_sides

    for link_type in requested_links:
        if not any(link_type in source_link_sides for source_link_sides in link_sides_by_source_type.values()):
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                link_type=link_type,
                parameters={"objectSet": "loadLinks"},
            )

    source_primary_key_fields: dict[str, str] = {}
    for source_object_type in source_object_types:
        try:
            source_primary_key_fields[source_object_type] = await _resolve_object_primary_key_field(
                db_name=db_name,
                object_type=source_object_type,
                branch=branch,
                oms_client=oms_client,
            )
        except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
            logger.warning(
                "Failed to resolve source object primary key for link expansion (ontology=%s sourceObjectType=%s branch=%s): %s",
                db_name,
                source_object_type,
                branch,
                exc,
                exc_info=True,
            )
            continue

    data = _collect_load_links_rows(
        rows=rows,
        requested_links=requested_links,
        link_sides_by_source_type=link_sides_by_source_type,
        source_primary_key_fields=source_primary_key_fields,
        default_object_type=default_object_type,
    )
    response: dict[str, Any] = {
        "data": data,
        "nextPageToken": next_page_token,
    }
    if payload.get("includeComputeUsage"):
        response["computeUsage"] = 0
    return response


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
):
    search_around = False
    object_types: list[str] = []
    search_payload: dict[str, Any] = {}
    request_page_token: str | None = None
    pagination_scope: str | None = None
    actor: str | None = None
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsMultipleObjectTypes",
        )
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        search_around = _is_search_around_object_set(object_set)
        if not search_around:
            object_types = _collect_object_set_object_types(object_set)
            if not object_types:
                raise ValueError("objectSet.objectType is required")
        await _require_read_role(request, db_name=db_name)
        _, actor = resolve_database_actor(request.headers)
        if not search_around:
            search_payload = _build_object_set_search_payload(
                object_set=object_set,
                payload=payload,
                require_select=True,
            )
            request_page_token = search_payload.get("pageToken")
            if request_page_token is not None:
                request_page_token = str(request_page_token)
            pagination_scope = _pagination_scope(
                "v2/objectSets/loadObjectsMultipleObjectTypes",
                db_name,
                branch,
                ",".join(sorted(object_types)),
                str(payload.get("pageSize") or ""),
                ",".join(_normalize_select_values(payload)),
                str(payload.get("snapshot") if "snapshot" in payload else ""),
            )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )

    try:
        if search_around:
            rows, total_count, next_page_token, resolved_object_types = await _load_rows_for_search_around_object_set(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_set=object_set,
                payload=payload,
                endpoint_scope="v2/objectSets/loadObjectsMultipleObjectTypes",
                actor=actor,
            )
            object_types = resolved_object_types
        elif len(object_types) == 1:
            rows, total_count, next_page_token = await _load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_types[0],
                search_payload=search_payload,
            )
        else:
            rows, total_count, next_page_token = await _load_rows_for_multi_object_types(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_types=object_types,
                search_payload=search_payload,
                page_token=request_page_token,
                pagination_scope=pagination_scope or "",
            )
        response: dict[str, Any] = {
            "data": rows,
            "nextPageToken": next_page_token,
            "totalCount": total_count,
            "interfaceToObjectTypeMappings": {},
            "interfaceToObjectTypeMappingsV2": {},
        }
        if payload.get("includeComputeUsage"):
            response["computeUsage"] = 0
        return response
    except ValueError as exc:
        return _object_set_runtime_value_error_response(
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
            not_found_response=_not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to load object set multiple object types (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
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
):
    search_around = False
    object_types: list[str] = []
    search_payload: dict[str, Any] = {}
    request_page_token: str | None = None
    pagination_scope: str | None = None
    actor: str | None = None
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsOrInterfaces",
        )
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        search_around = _is_search_around_object_set(object_set)
        if not search_around:
            object_types = _collect_object_set_object_types(object_set)
            if not object_types:
                raise ValueError("objectSet.objectType is required")
        await _require_read_role(request, db_name=db_name)
        _, actor = resolve_database_actor(request.headers)
        if not search_around:
            search_payload = _build_object_set_search_payload(
                object_set=object_set,
                payload=payload,
                require_select=True,
            )
            request_page_token = search_payload.get("pageToken")
            if request_page_token is not None:
                request_page_token = str(request_page_token)
            pagination_scope = _pagination_scope(
                "v2/objectSets/loadObjectsOrInterfaces",
                db_name,
                branch,
                ",".join(sorted(object_types)),
                str(payload.get("pageSize") or ""),
                ",".join(_normalize_select_values(payload)),
                str(payload.get("snapshot") if "snapshot" in payload else ""),
            )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )

    try:
        if search_around:
            rows, total_count, next_page_token, _ = await _load_rows_for_search_around_object_set(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_set=object_set,
                payload=payload,
                endpoint_scope="v2/objectSets/loadObjectsOrInterfaces",
                actor=actor,
            )
        elif len(object_types) == 1:
            rows, total_count, next_page_token = await _load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_types[0],
                search_payload=search_payload,
            )
        else:
            rows, total_count, next_page_token = await _load_rows_for_multi_object_types(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_types=object_types,
                search_payload=search_payload,
                page_token=request_page_token,
                pagination_scope=pagination_scope or "",
            )
        return {
            "data": rows,
            "nextPageToken": next_page_token,
            "totalCount": total_count,
        }
    except ValueError as exc:
        return _object_set_runtime_value_error_response(
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
            not_found_response=_not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadObjectsOrInterfaces"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to load object set objects or interfaces (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
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
):
    """Delegate objectSet aggregate to OMS ES-native aggregate engine."""
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
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
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version
        await _require_read_role(request, db_name=db_name)
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        object_set_rid = await _store_temporary_object_set(object_set)
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
):
    ontology = ontologyRid
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_read_role(request, db_name=db_name)
        return await _load_temporary_object_set(objectSetRid)
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        primary_key_value = str(primaryKey or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType), "primaryKey": str(primaryKey)},
        )

    try:
        object_type_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=object_type,
            branch=branch,
        )
        object_type_resource = _extract_object_resource(object_type_payload)
        if not object_type_resource:
            return _not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                parameters={"primaryKey": primary_key_value},
            )

        ontology_payload: Any = None
        try:
            ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        except httpx.HTTPError as exc:
            logger.warning(
                "Failed to load ontology metadata for object lookup (%s/%s): %s",
                db_name,
                object_type,
                exc,
            )
            ontology_payload = None

        object_type_contract = _to_foundry_object_type(object_type_resource, ontology_payload=ontology_payload)
        primary_key_field = str(object_type_contract.get("primaryKey") or "").strip()
        if not primary_key_field:
            return _foundry_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="INTERNAL",
                error_name="Internal",
                parameters={"ontology": db_name, "objectType": object_type, "message": "primaryKey not configured"},
            )

        payload: Dict[str, Any] = {
            "where": {"type": "eq", "field": primary_key_field, "value": primary_key_value},
            "pageSize": 1,
        }
        if select:
            payload["select"] = [str(value).strip() for value in select if str(value).strip()]
        if exclude_rid is not None:
            payload["excludeRid"] = bool(exclude_rid)

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
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
            )
        data = result.get("data")
        if not isinstance(data, list) or not data:
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        row = data[0]
        if not isinstance(row, dict):
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        return row
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = _passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to get object (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    linkType = linkTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        primary_key_value = str(primaryKey or "").strip()
        link_type = str(linkType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        if not link_type:
            raise ValueError("linkType is required")
        normalized_select = [str(value).strip() for value in (select or []) if str(value).strip()]
        parsed_order_by = _parse_order_by(order_by)
        page_scope = _pagination_scope(
            "v2/linkedObjects",
            db_name,
            branch,
            object_type,
            primary_key_value,
            link_type,
            page_size,
            ",".join(normalized_select),
            str(order_by or "").strip(),
            "" if exclude_rid is None else ("1" if exclude_rid else "0"),
            "" if snapshot is None else ("1" if snapshot else "0"),
        )
        offset = _decode_page_token(page_token, scope=page_scope)
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(objectType), "primaryKey": str(primaryKey), "linkType": str(linkType)},
        )

    try:
        source_primary_key_field = await _resolve_object_primary_key_field(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except ValueError:
        return _not_found_error(
            "ObjectTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            parameters={"primaryKey": primary_key_value},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to resolve source primary key field (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )

    try:
        source_payload = {
            "where": {"type": "eq", "field": source_primary_key_field, "value": primary_key_value},
            "pageSize": 1,
        }
        source_result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
            params={"branch": branch},
            json=source_payload,
        )
        source_rows = source_result.get("data") if isinstance(source_result, dict) else None
        if not isinstance(source_rows, list) or not source_rows or not isinstance(source_rows[0], dict):
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        source_row = source_rows[0]
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to resolve source object (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )

    try:
        link_type_side: dict[str, Any] | None = None
        try:
            link_payload = await oms_client.get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type,
                branch=branch,
            )
            link_resource = link_payload.get("data") if isinstance(link_payload, dict) else link_payload
            if isinstance(link_resource, dict):
                link_type_side = _to_foundry_outgoing_link_type(link_resource, source_object_type=object_type)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
            if status_code != status.HTTP_404_NOT_FOUND:
                raise

        if not link_type_side:
            try:
                object_payload = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="object_type",
                    resource_id=object_type,
                    branch=branch,
                )
                object_resource = object_payload.get("data") if isinstance(object_payload, dict) else object_payload
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
                        link_type_side = {"apiName": predicate, "objectTypeApiName": target}
                        break
            except httpx.HTTPStatusError:
                link_type_side = None

        linked_object_type = str((link_type_side or {}).get("objectTypeApiName") or "").strip()
        if not linked_object_type:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        linked_primary_key_field = await _resolve_object_primary_key_field(
            db_name=db_name,
            object_type=linked_object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )
    except ValueError:
        return _not_found_error(
            "LinkTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            link_type=link_type,
            parameters={"primaryKey": primary_key_value},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to resolve link context (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )

    foreign_key_property = str(link_type_side.get("foreignKeyPropertyApiName") or "").strip() or None
    paged_primary_keys, has_more = _extract_linked_primary_keys_page(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
        offset=offset,
        page_size=page_size,
    )
    if not paged_primary_keys:
        return {"data": [], "nextPageToken": None}

    search_payload: Dict[str, Any] = {
        "where": _build_primary_key_where(linked_primary_key_field, paged_primary_keys),
        "pageSize": max(1, len(paged_primary_keys)),
    }
    if normalized_select:
        search_payload["select"] = normalized_select
    if parsed_order_by is not None:
        search_payload["orderBy"] = parsed_order_by
    if exclude_rid is not None:
        search_payload["excludeRid"] = bool(exclude_rid)
    if snapshot is not None:
        search_payload["snapshot"] = bool(snapshot)

    try:
        linked_result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{linked_object_type}/search",
            params={"branch": branch},
            json=search_payload,
        )
        data = linked_result.get("data") if isinstance(linked_result, dict) else []
        if not isinstance(data, list):
            data = []
        next_offset = offset + len(paged_primary_keys)
        next_page_token = _encode_page_token(next_offset, scope=page_scope) if has_more else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = _passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to list linked objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    linkType = linkTypeApiName
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        primary_key_value = str(primaryKey or "").strip()
        link_type = str(linkType or "").strip()
        linked_primary_key_value = str(linkedObjectPrimaryKey or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        if not link_type:
            raise ValueError("linkType is required")
        if not linked_primary_key_value:
            raise ValueError("linkedObjectPrimaryKey is required")
        await _require_read_role(request, db_name=db_name)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={
                "objectType": str(objectType),
                "primaryKey": str(primaryKey),
                "linkType": str(linkType),
                "linkedObjectPrimaryKey": str(linkedObjectPrimaryKey),
            },
        )

    error_parameters = _linked_object_parameters(
        ontology=db_name,
        object_type=object_type,
        primary_key=primary_key_value,
        link_type=link_type,
        linked_primary_key=linked_primary_key_value,
    )

    normalized_select = [str(value).strip() for value in (select or []) if str(value).strip()]

    try:
        source_primary_key_field = await _resolve_object_primary_key_field(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except ValueError:
        return _not_found_error(
            "ObjectTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            primary_key=primary_key_value,
            link_type=link_type,
            linked_primary_key=linked_primary_key_value,
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to resolve source primary key field (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters=error_parameters,
        )

    try:
        source_payload = {
            "where": {"type": "eq", "field": source_primary_key_field, "value": primary_key_value},
            "pageSize": 1,
        }
        source_result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
            params={"branch": branch},
            json=source_payload,
        )
        source_rows = source_result.get("data") if isinstance(source_result, dict) else None
        if not isinstance(source_rows, list) or not source_rows or not isinstance(source_rows[0], dict):
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        source_row = source_rows[0]
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters=error_parameters,
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to resolve source object (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters=error_parameters,
        )

    try:
        link_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="link_type",
            resource_id=link_type,
            branch=branch,
        )
        link_resource = link_payload.get("data") if isinstance(link_payload, dict) else link_payload
        if not isinstance(link_resource, dict):
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        link_type_side = _to_foundry_outgoing_link_type(link_resource, source_object_type=object_type)
        if not link_type_side:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        linked_object_type = str(link_type_side.get("objectTypeApiName") or "").strip()
        if not linked_object_type:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        linked_primary_key_field = await _resolve_object_primary_key_field(
            db_name=db_name,
            object_type=linked_object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters=error_parameters,
        )
    except ValueError:
        return _not_found_error(
            "LinkTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            primary_key=primary_key_value,
            link_type=link_type,
            linked_primary_key=linked_primary_key_value,
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to resolve link context (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters=error_parameters,
        )

    foreign_key_property = str(link_type_side.get("foreignKeyPropertyApiName") or "").strip() or None
    if not _linked_primary_key_exists(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
        linked_primary_key=linked_primary_key_value,
    ):
        return _not_found_error(
            "LinkedObjectNotFound",
            ontology=db_name,
            object_type=object_type,
            primary_key=primary_key_value,
            link_type=link_type,
            linked_primary_key=linked_primary_key_value,
        )

    payload: Dict[str, Any] = {
        "where": {"type": "eq", "field": linked_primary_key_field, "value": linked_primary_key_value},
        "pageSize": 1,
    }
    if normalized_select:
        payload["select"] = normalized_select
    if exclude_rid is not None:
        payload["excludeRid"] = bool(exclude_rid)

    try:
        linked_result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{linked_object_type}/search",
            params={"branch": branch},
            json=payload,
        )
        rows = linked_result.get("data") if isinstance(linked_result, dict) else None
        if not isinstance(rows, list) or not rows:
            return _not_found_error(
                "LinkedObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        row = rows[0]
        if not isinstance(row, dict):
            return _not_found_error(
                "LinkedObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        row = _project_row_with_required_fields(
            row,
            select_fields=normalized_select,
            exclude_rid=bool(exclude_rid),
        )
        return row
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _not_found_error(
                "LinkedObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters=error_parameters,
        )
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        logger.error("Failed to get linked object (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        result = await oms_client.get_timeseries_first_point(
            db_name,
            objectType,
            primaryKey,
            property,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get timeseries firstPoint (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        result = await oms_client.get_timeseries_last_point(
            db_name,
            objectType,
            primaryKey,
            property,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get timeseries lastPoint (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        body = await request.json() if await request.body() else {}
        response = await oms_client.stream_timeseries_points(
            db_name,
            objectType,
            primaryKey,
            property,
            body,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return StreamingResponse(
            content=iter([response.content]),
            media_type=response.headers.get("content-type", "application/x-ndjson"),
        )
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to stream timeseries points (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
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
    error_parameters: Dict[str, Any] = {"filename": filename}
    try:
        body = await request.body()
        _ = sdk_package_rid, sdk_version
        forward_headers = _extract_actor_forward_headers(request)
        result = await oms_client.upload_attachment(
            filename=filename,
            data=body,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology="attachments", parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology="attachments", parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to upload attachment (v2)", exc=exc,
            ontology="attachments", parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        result = await oms_client.list_property_attachments(
            db_name,
            objectType,
            primaryKey,
            property,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to list attachment property (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
        "attachmentRid": attachmentRid,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        result = await oms_client.get_attachment_by_rid(
            db_name,
            objectType,
            primaryKey,
            property,
            attachmentRid,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get attachment by RID (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        response = await oms_client.get_attachment_content(
            db_name,
            objectType,
            primaryKey,
            property,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "application/octet-stream"),
        )
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get attachment content (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
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
    ontology = ontologyRid
    objectType = objectTypeApiName
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
        "attachmentRid": attachmentRid,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        forward_headers = _extract_actor_forward_headers(request)
        response = await oms_client.get_attachment_content_by_rid(
            db_name,
            objectType,
            primaryKey,
            property,
            attachmentRid,
            branch=branch,
            headers=forward_headers if forward_headers else None,
        )
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "application/octet-stream"),
        )
    except (OntologyNotFoundError, PermissionDeniedError, ValueError, SecurityViolationError) as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters=error_parameters,
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except _ONTOLOGY_HANDLED_EXCEPTIONS as exc:
        return _internal_error_response(
            log_message="Failed to get attachment content by RID (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )
