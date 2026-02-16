"""Foundry Ontologies v2 read-compat router.

Exposes a Foundry-style `/api/v2/ontologies/...` read surface on top of
existing OMS/BFF ontology resources.
"""

import asyncio
import logging
from typing import Any, Dict
from uuid import uuid4

import httpx
from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse

from bff.dependencies import OMSClientDep
from bff.routers.link_types_read import (
    _extract_resources as _extract_link_resources,
    _normalize_object_ref,
    _to_foundry_outgoing_link_type,
)
from bff.routers.object_types import (
    _extract_resource as _extract_object_resource,
    _extract_resources as _extract_object_resources,
    _to_foundry_object_type,
)
from bff.services.oms_client import OMSClient
from shared.config.settings import get_settings
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
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


class OntologyNotFoundError(Exception):
    pass


class PermissionDeniedError(Exception):
    pass


class ApiFeaturePreviewUsageOnlyError(ValueError):
    pass


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
    return decode_offset_page_token(page_token, ttl_seconds=60, expected_scope=scope)


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


def _pagination_scope(*parts: Any) -> str:
    normalized = [str(part).strip() for part in parts if str(part).strip()]
    return "|".join(normalized)


def _strict_compat_allowlist() -> set[str]:
    raw = str(get_settings().features.foundry_v2_strict_compat_db_allowlist or "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def _is_foundry_v2_strict_compat_enabled(*, db_name: str | None) -> bool:
    features = get_settings().features
    if bool(features.enable_foundry_v2_strict_compat):
        return True
    normalized_db = str(db_name or "").strip().lower()
    if not normalized_db:
        return False
    return normalized_db in _strict_compat_allowlist()


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


def _full_metadata_branch_contract(*, branch: str, strict_compat: bool) -> Dict[str, str]:
    return {"rid": branch} if strict_compat else {"name": branch}


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


def _scoped_error_parameters(
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"ontology": ontology}
    if parameters:
        payload.update({str(key): value for key, value in parameters.items() if value is not None})
    return payload


def _upstream_status_error_response(
    exc: httpx.HTTPStatusError,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
    not_found_response: JSONResponse | None = None,
    passthrough_payload: bool = False,
) -> JSONResponse:
    status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
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
    branch: str = Query("main", description="Ontology branch name or branch RID"),
    oms_client: OMSClient = OMSClientDep,
):
    strict_compat = False
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = _validate_branch(branch)
        strict_compat = _is_foundry_v2_strict_compat_enabled(db_name=db_name)
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
            mapped = _to_foundry_object_type_full_metadata(
                resource,
                ontology_payload=ontology_payload_by_object_type.get(object_type_id),
                link_types=link_types_by_source.get(object_type_id) or [],
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
            "branch": _full_metadata_branch_contract(branch=branch, strict_compat=strict_compat),
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
        link_types = _group_outgoing_link_types_by_source(link_resources).get(object_type) or []
        out = _to_foundry_object_type_full_metadata(
            resource,
            ontology_payload=ontology_payload,
            link_types=link_types,
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
            f"/api/v1/objects/{db_name}/{object_type}/search",
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
            f"/api/v1/objects/{db_name}/{object_type}/search",
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
            f"/api/v1/objects/{db_name}/{object_type}/search",
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
            f"/api/v1/objects/{db_name}/{object_type}/search",
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
            f"/api/v1/objects/{db_name}/{linked_object_type}/search",
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
            f"/api/v1/objects/{db_name}/{object_type}/search",
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
            f"/api/v1/objects/{db_name}/{linked_object_type}/search",
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
        if normalized_select:
            row = {k: row.get(k) for k in normalized_select if k in row}
        if exclude_rid:
            row.pop("__rid", None)
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
