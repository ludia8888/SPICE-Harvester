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
from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from bff.dependencies import OMSClientDep
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
from bff.services.oms_client import OMSClient
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role, resolve_database_actor
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_db_name,
)
from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token

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


class OntologyNotFoundError(Exception):
    pass


class PermissionDeniedError(Exception):
    pass


class ApiFeaturePreviewUsageOnlyError(ValueError):
    pass


class ObjectSetNotFoundError(ValueError):
    pass


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
    requests: list[BatchApplyActionRequestItemV2] = Field(default_factory=list, min_length=1, max_length=500)


class ExecuteQueryRequestV2(BaseModel):
    parameters: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] | None = None


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "errorCode": error_code,
            "errorName": error_name,
            "errorInstanceId": str(uuid4()),
            "parameters": parameters or {},
        },
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
    return f"ri.spice.main.object-type.{_rid_component(db_name, fallback='db')}.{_rid_component(object_type, fallback='objectType')}"


def _default_property_rid(*, db_name: str, object_type: str, property_name: str) -> str:
    return (
        f"ri.spice.main.property."
        f"{_rid_component(db_name, fallback='db')}.{_rid_component(object_type, fallback='objectType')}.{_rid_component(property_name, fallback='property')}"
    )


def _default_link_type_rid(*, db_name: str, source_object_type: str, link_type: str) -> str:
    return (
        f"ri.spice.main.link-type."
        f"{_rid_component(db_name, fallback='db')}.{_rid_component(source_object_type, fallback='objectType')}.{_rid_component(link_type, fallback='linkType')}"
    )


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
            text = str(candidate).strip()
            if text:
                yield text
        return
    text = str(value).strip()
    if text:
        yield text


def _coerce_primary_key_values(value: Any) -> list[str]:
    return list(_iter_primary_key_values(value))


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
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_403_FORBIDDEN:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_404_NOT_FOUND:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
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
) -> JSONResponse:
    status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
    if normalize_non_foundry_payload:
        normalized = _normalize_non_foundry_upstream_error(
            exc,
            ontology=ontology,
            parameters=parameters,
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
    resource_type: str,
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


@router.get("")
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
    except Exception as exc:
        logger.error("Failed to list ontologies (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={},
        )


@router.get("/{ontology}")
@trace_endpoint("bff.foundry_v2_ontology.get_ontology")
async def get_ontology_v2(
    ontology: str,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to get ontology (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name},
        )


@router.get("/{ontology}/fullMetadata")
@trace_endpoint("bff.foundry_v2_ontology.get_full_metadata")
async def get_full_metadata_v2(
    ontology: str,
    request: Request,
    preview: bool = Query(False, description="Must be true for preview endpoints"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="GET /api/v2/ontologies/{ontology}/fullMetadata",
        )
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
            _list_resources_best_effort(
                db_name=db_name,
                branch=branch,
                resource_type="action_type",
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get full metadata (v2)",
            exc=exc,
            ontology=db_name,
        )


@router.get("/{ontology}/actionTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_action_types")
async def list_action_types_v2(
    ontology: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/actionTypes", db_name, branch, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
        return _preflight_error_response(exc, ontology=str(ontology))

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="action_type",
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        resources = _extract_ontology_resource_rows(payload)
        data = [
            mapped
            for mapped in (_to_foundry_action_type(resource) for resource in resources)
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to list action types (v2)",
            exc=exc,
            ontology=db_name,
        )


@router.get("/{ontology}/actionTypes/{actionType}")
@trace_endpoint("bff.foundry_v2_ontology.get_action_type")
async def get_action_type_v2(
    ontology: str,
    actionType: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        action_type = str(actionType or "").strip()
        if not action_type:
            raise ValueError("actionType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"actionType": str(actionType)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="action_type",
            resource_id=action_type,
            branch=branch,
        )
        resource = _extract_ontology_resource(payload)
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get action type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"actionType": action_type},
        )


@router.get("/{ontology}/actionTypes/byRid/{actionTypeRid}")
@trace_endpoint("bff.foundry_v2_ontology.get_action_type_by_rid")
async def get_action_type_by_rid_v2(
    ontology: str,
    actionTypeRid: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        action_type_rid = str(actionTypeRid or "").strip()
        if not action_type_rid:
            raise ValueError("actionTypeRid is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
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


def _foundry_valid_action_validation_payload() -> Dict[str, Any]:
    return {
        "validation": {
            "result": "VALID",
        },
        "parameters": {},
    }


@router.post("/{ontology}/actions/{action}/apply")
@trace_endpoint("bff.foundry_v2_ontology.apply_action")
async def apply_action_v2(
    ontology: str,
    action: str,
    body: ApplyActionRequestV2,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    oms_client: OMSClient = OMSClientDep,
):
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
    except Exception as exc:
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
        if mode == "VALIDATE_ONLY":
            if isinstance(response, dict) and isinstance(response.get("validation"), dict):
                return response
            return _foundry_valid_action_validation_payload()
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
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionType": action_type},
        )
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to apply action (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"action": action_type},
        )


@router.post("/{ontology}/actions/{action}/applyBatch")
@trace_endpoint("bff.foundry_v2_ontology.apply_action_batch")
async def apply_action_batch_v2(
    ontology: str,
    action: str,
    body: BatchApplyActionRequestV2,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
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
    except Exception as exc:
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
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionType": action_type},
        )
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to apply action batch (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"action": action_type},
        )


@router.get("/{ontology}/queryTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_query_types")
async def list_query_types_v2(
    ontology: str,
    request: Request,
    page_size: int = Query(100, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/queryTypes", db_name, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
        return _preflight_error_response(exc, ontology=str(ontology))

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="function",
            branch="main",
            limit=page_size,
            offset=offset,
        )
        resources = _extract_ontology_resource_rows(payload)
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to list query types (v2)",
            exc=exc,
            ontology=db_name,
        )


@router.get("/{ontology}/queryTypes/{queryApiName}")
@trace_endpoint("bff.foundry_v2_ontology.get_query_type")
async def get_query_type_v2(
    ontology: str,
    queryApiName: str,
    request: Request,
    version: str | None = Query(default=None),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version, version
        query_api_name = str(queryApiName or "").strip()
        if not query_api_name:
            raise ValueError("queryApiName is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"queryApiName": str(queryApiName)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="function",
            resource_id=query_api_name,
            branch="main",
        )
        resource = _extract_ontology_resource(payload)
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get query type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
        )


@router.post("/{ontology}/queries/{queryApiName}/execute")
@trace_endpoint("bff.foundry_v2_ontology.execute_query")
async def execute_query_v2(
    ontology: str,
    queryApiName: str,
    body: ExecuteQueryRequestV2,
    request: Request,
    version: str | None = Query(default=None),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version, transaction_id
        query_api_name = str(queryApiName or "").strip()
        if not query_api_name:
            raise ValueError("queryApiName is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"queryApiName": str(queryApiName)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="function",
            resource_id=query_api_name,
            branch="main",
        )
        resource = _extract_ontology_resource(payload)
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
            params={"branch": "main"},
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to execute query (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"queryApiName": query_api_name},
        )


@router.get("/{ontology}/interfaceTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_interface_types")
async def list_interface_types_v2(
    ontology: str,
    request: Request,
    preview: bool = Query(False),
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/interfaceTypes",
        )
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/interfaceTypes", db_name, branch, page_size, "1" if preview else "0")
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to list interface types (v2)",
            exc=exc,
            ontology=db_name,
        )


@router.get("/{ontology}/interfaceTypes/{interfaceType}")
@trace_endpoint("bff.foundry_v2_ontology.get_interface_type")
async def get_interface_type_v2(
    ontology: str,
    interfaceType: str,
    request: Request,
    preview: bool = Query(False),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/interfaceTypes/{interfaceType}",
        )
        _ = sdk_package_rid, sdk_version
        interface_type = str(interfaceType or "").strip()
        if not interface_type:
            raise ValueError("interfaceType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get interface type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"interfaceType": interface_type},
        )


@router.get("/{ontology}/sharedPropertyTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_shared_property_types")
async def list_shared_property_types_v2(
    ontology: str,
    request: Request,
    preview: bool = Query(False),
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/sharedPropertyTypes",
        )
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/sharedPropertyTypes", db_name, branch, page_size, "1" if preview else "0")
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to list shared property types (v2)",
            exc=exc,
            ontology=db_name,
        )


@router.get("/{ontology}/sharedPropertyTypes/{sharedPropertyType}")
@trace_endpoint("bff.foundry_v2_ontology.get_shared_property_type")
async def get_shared_property_type_v2(
    ontology: str,
    sharedPropertyType: str,
    request: Request,
    preview: bool = Query(False),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}",
        )
        shared_property_type = str(sharedPropertyType or "").strip()
        if not shared_property_type:
            raise ValueError("sharedPropertyType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get shared property type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"sharedPropertyType": shared_property_type},
        )


@router.get("/{ontology}/valueTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_value_types")
async def list_value_types_v2(
    ontology: str,
    request: Request,
    preview: bool = Query(False),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/valueTypes",
        )
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to list value types (v2)",
            exc=exc,
            ontology=db_name,
        )


@router.get("/{ontology}/valueTypes/{valueType}")
@trace_endpoint("bff.foundry_v2_ontology.get_value_type")
async def get_value_type_v2(
    ontology: str,
    valueType: str,
    request: Request,
    preview: bool = Query(False),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/valueTypes/{valueType}",
        )
        value_type = str(valueType or "").strip()
        if not value_type:
            raise ValueError("valueType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get value type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"valueType": value_type},
        )


@router.get("/{ontology}/objectTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_object_types")
async def list_object_types_v2(
    ontology: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/objectTypes", db_name, branch, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to list object types (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name},
        )


@router.get("/{ontology}/objectTypes/{objectType}")
@trace_endpoint("bff.foundry_v2_ontology.get_object_type")
async def get_object_type_v2(
    ontology: str,
    objectType: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to get object type (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


@router.get("/{ontology}/objectTypes/{objectType}/fullMetadata")
@trace_endpoint("bff.foundry_v2_ontology.get_object_type_full_metadata")
async def get_object_type_full_metadata_v2(
    ontology: str,
    objectType: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontology}/objectTypes/{objectType}/fullMetadata",
        )
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get object type full metadata (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
        )


@router.get("/{ontology}/objectTypes/{objectType}/outgoingLinkTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_outgoing_link_types")
async def list_outgoing_link_types_v2(
    ontology: str,
    objectType: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        source_object_type = str(objectType or "").strip()
        if not source_object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/outgoingLinkTypes", db_name, branch, source_object_type, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to list outgoing link types (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": source_object_type},
        )


@router.get("/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}")
@trace_endpoint("bff.foundry_v2_ontology.get_outgoing_link_type")
async def get_outgoing_link_type_v2(
    ontology: str,
    objectType: str,
    linkType: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
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
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
                object_type=source_object_type,
                link_type=link_type,
            )
        mapped = _to_foundry_outgoing_link_type(resource, source_object_type=source_object_type)
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
    except Exception as exc:
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


@router.get("/{ontology}/objectTypes/{objectType}/incomingLinkTypes")
@trace_endpoint("bff.foundry_v2_ontology.list_incoming_link_types")
async def list_incoming_link_types_v2(
    ontology: str,
    objectType: str,
    request: Request,
    page_size: int = Query(500, alias="pageSize", ge=1, le=1000),
    page_token: str | None = Query(default=None, alias="pageToken"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        target_object_type = str(objectType or "").strip()
        if not target_object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
        page_scope = _pagination_scope("v2/incomingLinkTypes", db_name, branch, target_object_type, page_size)
        offset = _decode_page_token(page_token, scope=page_scope)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to list incoming link types (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": target_object_type},
        )


@router.get("/{ontology}/objectTypes/{objectType}/incomingLinkTypes/{linkType}")
@trace_endpoint("bff.foundry_v2_ontology.get_incoming_link_type")
async def get_incoming_link_type_v2(
    ontology: str,
    objectType: str,
    linkType: str,
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
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
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to get incoming link type (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": target_object_type, "linkType": link_type},
        )


@router.post("/{ontology}/objects/{objectType}/search")
@trace_endpoint("bff.foundry_v2_ontology.search_objects")
async def search_objects_v2(
    ontology: str,
    objectType: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to search objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


@router.post("/{ontology}/objects/{objectType}/aggregate")
@trace_endpoint("bff.foundry_v2_ontology.aggregate_objects")
async def aggregate_objects_v2(
    ontology: str,
    objectType: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    """Delegate aggregation to OMS ES-native aggregate engine."""
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        object_type = str(objectType or "").strip()
        _ = transaction_id, sdk_package_rid, sdk_version
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to aggregate objects (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
        )


@router.post("/{ontology}/objectSets/loadObjects")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_objects")
async def load_object_set_objects_v2(
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        object_type = _resolve_object_set_object_type(object_set)
        if not object_type:
            raise ValueError("objectSet.objectType is required")
        await _require_domain_role(request, db_name=db_name)
        search_payload = _build_object_set_search_payload(
            object_set=object_set,
            payload=payload,
            require_select=True,
        )
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjects"},
        )

    try:
        rows, total_count, next_page_token = await _load_rows_for_single_object_type(
            oms_client=oms_client,
            db_name=db_name,
            branch=branch,
            object_type=object_type,
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
    except Exception as exc:
        logger.error("Failed to load object set objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectSet": "loadObjects"},
        )


@router.post("/{ontology}/objectSets/loadLinks")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_links")
async def load_object_set_links_v2(
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontology}/objectSets/loadLinks",
        )
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        requested_links = _normalize_link_type_values(payload)
        object_types = _collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await _require_domain_role(request, db_name=db_name)
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
    except Exception as exc:
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
    except Exception as exc:
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
        except Exception:
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


@router.post("/{ontology}/objectSets/loadObjectsMultipleObjectTypes")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_objects_multiple_object_types")
async def load_object_set_multiple_object_types_v2(
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes",
        )
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        object_types = _collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await _require_domain_role(request, db_name=db_name)
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
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )

    try:
        if len(object_types) == 1:
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
                pagination_scope=pagination_scope,
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to load object set multiple object types (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )


@router.post("/{ontology}/objectSets/loadObjectsOrInterfaces")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.load_objects_or_interfaces")
async def load_object_set_objects_or_interfaces_v2(
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    preview: bool = Query(False),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
        _require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces",
        )
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        object_types = _collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await _require_domain_role(request, db_name=db_name)
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
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )

    try:
        if len(object_types) == 1:
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
                pagination_scope=pagination_scope,
            )
        return {
            "data": rows,
            "nextPageToken": next_page_token,
            "totalCount": total_count,
        }
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to load object set objects or interfaces (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )


@router.post("/{ontology}/objectSets/aggregate")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.aggregate")
async def aggregate_object_set_v2(
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    transaction_id: str | None = Query(default=None, alias="transactionId"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    """Delegate objectSet aggregate to OMS ES-native aggregate engine."""
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        object_types = _collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to aggregate objectSet (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "aggregate"},
        )


@router.post("/{ontology}/objectSets/createTemporary")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.create_temporary")
async def create_temporary_object_set_v2(
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version
        await _require_domain_role(request, db_name=db_name)
        object_set = await _resolve_object_set_definition(payload.get("objectSet"))
        object_set_rid = await _store_temporary_object_set(object_set)
        return {"objectSetRid": object_set_rid}
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "createTemporary"},
        )


@router.get("/{ontology}/objectSets/{objectSetRid}")
@trace_endpoint("bff.foundry_v2_ontology.object_sets.get")
async def get_object_set_v2(
    ontology: str,
    objectSetRid: str,
    request: Request,
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_domain_role(request, db_name=db_name)
        return await _load_temporary_object_set(objectSetRid)
    except Exception as exc:
        return _preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSetRid": str(objectSetRid)},
        )


@router.get("/{ontology}/objects/{objectType}")
@trace_endpoint("bff.foundry_v2_ontology.list_objects")
async def list_objects_v2(
    ontology: str,
    objectType: str,
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
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)

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
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to list objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}")
@trace_endpoint("bff.foundry_v2_ontology.get_object")
async def get_object_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    request: Request,
    select: list[str] | None = Query(default=None),
    exclude_rid: bool | None = Query(default=None, alias="excludeRid"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
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
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to get object (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}")
@trace_endpoint("bff.foundry_v2_ontology.list_linked_objects")
async def list_linked_objects_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    linkType: str,
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
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to resolve source object (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
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
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        link_type_side = _to_foundry_outgoing_link_type(link_resource, source_object_type=object_type)
        if not link_type_side:
            return _not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        linked_object_type = str(link_type_side.get("objectTypeApiName") or "").strip()
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
    except Exception as exc:
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
    except Exception as exc:
        logger.error("Failed to list linked objects (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}")
@trace_endpoint("bff.foundry_v2_ontology.get_linked_object")
async def get_linked_object_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    linkType: str,
    linkedObjectPrimaryKey: str,
    request: Request,
    select: list[str] | None = Query(default=None),
    exclude_rid: bool | None = Query(default=None, alias="excludeRid"),
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    sdk_package_rid: str | None = Query(default=None, alias="sdkPackageRid"),
    sdk_version: str | None = Query(default=None, alias="sdkVersion"),
    oms_client: OMSClient = OMSClientDep,
):
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
        await _require_domain_role(request, db_name=db_name)
    except Exception as exc:
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
    except Exception as exc:
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
    except Exception as exc:
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
    except Exception as exc:
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
    except Exception as exc:
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


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/firstPoint")
async def get_timeseries_first_point_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Get the first (earliest) point of a time series property."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        result = await oms_client.get_timeseries_first_point(
            db_name, objectType, primaryKey, property, branch=branch,
        )
        return JSONResponse(content=result)
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get timeseries firstPoint (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/lastPoint")
async def get_timeseries_last_point_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Get the last (most recent) point of a time series property."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        result = await oms_client.get_timeseries_last_point(
            db_name, objectType, primaryKey, property, branch=branch,
        )
        return JSONResponse(content=result)
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get timeseries lastPoint (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )


@router.post("/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/streamPoints", response_model=None)
async def stream_timeseries_points_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    request: Request,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> StreamingResponse | JSONResponse:
    """Stream all points of a time series property with optional range filter."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        body = await request.json() if await request.body() else {}
        response = await oms_client.stream_timeseries_points(
            db_name, objectType, primaryKey, property, body, branch=branch,
        )
        return StreamingResponse(
            content=iter([response.content]),
            media_type=response.headers.get("content-type", "application/x-ndjson"),
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to stream timeseries points (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )


# =====================================================================
# Attachment Property endpoints (Foundry v2 — proxy to OMS)
# =====================================================================


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}")
async def list_attachment_property_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """List attachment metadata for a property (single or multiple)."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        result = await oms_client.list_property_attachments(
            db_name, objectType, primaryKey, property, branch=branch,
        )
        return JSONResponse(content=result)
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to list attachment property (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}")
async def get_attachment_by_rid_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    attachmentRid: str,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> JSONResponse:
    """Get metadata for a specific attachment by its RID."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
        "attachmentRid": attachmentRid,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        result = await oms_client.get_attachment_by_rid(
            db_name, objectType, primaryKey, property, attachmentRid, branch=branch,
        )
        return JSONResponse(content=result)
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get attachment by RID (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}/content", response_model=None)
async def get_attachment_content_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> Response | JSONResponse:
    """Get the content of a single-valued attachment property."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        response = await oms_client.get_attachment_content(
            db_name, objectType, primaryKey, property, branch=branch,
        )
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "application/octet-stream"),
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get attachment content (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )


@router.get("/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}/content", response_model=None)
async def get_attachment_content_by_rid_v2(
    ontology: str,
    objectType: str,
    primaryKey: str,
    property: str,
    attachmentRid: str,
    branch: str = Query("main"),
    oms_client: OMSClient = OMSClientDep,
) -> Response | JSONResponse:
    """Get the content of an attachment by its RID."""
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": objectType,
        "primaryKey": primaryKey,
        "property": property,
        "attachmentRid": attachmentRid,
    }
    try:
        db_name = await _resolve_ontology_db_name(ontology, oms_client=oms_client, branch=branch)
        response = await oms_client.get_attachment_content_by_rid(
            db_name, objectType, primaryKey, property, attachmentRid, branch=branch,
        )
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "application/octet-stream"),
        )
    except httpx.HTTPStatusError as exc:
        return _upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True,
        )
    except httpx.HTTPError:
        return _upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except Exception as exc:
        return _internal_error_response(
            log_message="Failed to get attachment content by RID (v2)", exc=exc,
            ontology=ontology, parameters=error_parameters,
        )
