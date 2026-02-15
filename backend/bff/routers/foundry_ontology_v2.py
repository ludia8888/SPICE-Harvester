"""Foundry Ontologies v2 read-compat router.

Exposes a Foundry-style `/api/v2/ontologies/...` read surface on top of
existing OMS/BFF ontology resources.
"""

import base64
import logging
from typing import Any, Dict
from uuid import uuid4

import httpx
from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse

from bff.dependencies import OMSClientDep
from bff.routers.link_types_read import (
    _extract_resources as _extract_link_resources,
    _to_foundry_outgoing_link_type,
)
from bff.routers.object_types import (
    _extract_resource as _extract_object_resource,
    _extract_resources as _extract_object_resources,
    _to_foundry_object_type,
)
from bff.services.oms_client import OMSClient
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
from shared.security.input_sanitizer import SecurityViolationError, validate_db_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/ontologies", tags=["Foundry Ontologies v2"])


class OntologyNotFoundError(Exception):
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


def _decode_page_token(page_token: str | None) -> int:
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
        raise ValueError("pageToken must be base64-encoded non-negative integer offset") from exc
    if offset < 0:
        raise ValueError("pageToken offset must be >= 0")
    return offset


def _encode_page_token(offset: int) -> str:
    raw = str(max(0, int(offset))).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


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


def _coerce_primary_key_values(value: Any) -> list[str]:
    collected: list[str] = []

    if value is None:
        return collected

    if isinstance(value, list):
        for item in value:
            collected.extend(_coerce_primary_key_values(item))
        return collected

    if isinstance(value, dict):
        for key in ("__primaryKey", "primaryKey", "id", "instance_id"):
            candidate = value.get(key)
            if candidate is None:
                continue
            text = str(candidate).strip()
            if text:
                collected.append(text)
        return collected

    text = str(value).strip()
    if text:
        collected.append(text)
    return collected


def _extract_linked_primary_keys(
    source_row: dict[str, Any],
    *,
    link_type: str,
    foreign_key_property: str | None,
) -> list[str]:
    values: list[str] = []

    if foreign_key_property:
        values.extend(_coerce_primary_key_values(source_row.get(foreign_key_property)))

    for key in (link_type, f"{link_type}Ids", f"{link_type}_ids"):
        values.extend(_coerce_primary_key_values(source_row.get(key)))

    links = source_row.get("links")
    if isinstance(links, dict):
        values.extend(_coerce_primary_key_values(links.get(link_type)))

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


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
    except Exception:
        ontology_payload = None

    object_type_contract = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
    primary_key_field = str(object_type_contract.get("primaryKey") or "").strip()
    if not primary_key_field:
        raise ValueError(f"primaryKey not configured for object type: {object_type}")
    return primary_key_field


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=DOMAIN_MODEL_ROLES)


def _validate_ontology_db_name(ontology: str) -> str:
    return validate_db_name(str(ontology or "").strip())


async def _resolve_ontology_db_name(*, ontology: str, oms_client: OMSClient) -> str:
    raw = str(ontology or "").strip()
    if not raw:
        raise ValueError("ontology is required")

    try:
        return _validate_ontology_db_name(raw)
    except (ValueError, SecurityViolationError):
        # Fall through to Foundry-style identifier resolution (apiName/rid).
        if any(ch.isspace() for ch in raw):
            raise ValueError("ontology is invalid")

    payload = await oms_client.list_databases()
    rows = _extract_databases(payload)
    lowered = raw.lower()
    for row in rows:
        candidate_name = str(row.get("name") or row.get("db_name") or row.get("apiName") or "").strip()
        candidate_rid = str(row.get("rid") or row.get("ontologyRid") or "").strip()
        if not candidate_name:
            continue
        if raw == candidate_rid or lowered == candidate_name.lower():
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
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )

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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name},
            )
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
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await _require_domain_role(request, db_name=db_name)
        offset = _decode_page_token(page_token)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )

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
        for resource in resources:
            class_id = str(resource.get("id") or "").strip()
            if not class_id:
                continue
            ontology_payload: Any = None
            try:
                ontology_payload = await oms_client.get_ontology(db_name, class_id, branch=branch)
            except Exception as exc:
                logger.warning(
                    "Failed to enrich object type metadata (%s/%s): %s",
                    db_name,
                    class_id,
                    exc,
                )
            object_types.append(_to_foundry_object_type(resource, ontology_payload=ontology_payload))

        next_page_token = _encode_page_token(offset + len(resources)) if len(resources) == page_size else None
        return {"data": object_types, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name},
            )
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
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "message": str(exc)},
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type},
            )
        ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        out = _to_foundry_object_type(resource, ontology_payload=ontology_payload)
        if not out.get("apiName"):
            out["apiName"] = object_type
        return out
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type},
            )
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
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        source_object_type = str(objectType or "").strip()
        if not source_object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
        offset = _decode_page_token(page_token)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "message": str(exc)},
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )

    try:
        scan_limit = min(max(page_size, 500), 1000)
        scan_offset = 0
        matched: list[dict] = []

        while True:
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
                if mapped is not None:
                    matched.append(mapped)
            scan_offset += len(resources)
            if len(resources) < scan_limit:
                break

        data = matched[offset : offset + page_size]
        next_offset = offset + len(data)
        next_page_token = _encode_page_token(next_offset) if next_offset < len(matched) else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": source_object_type},
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
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        source_object_type = str(objectType or "").strip()
        link_type = str(linkType or "").strip()
        if not source_object_type:
            raise ValueError("objectType is required")
        if not link_type:
            raise ValueError("linkType is required")
        await _require_domain_role(request, db_name=db_name)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "linkType": str(linkType)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={
                "ontology": str(ontology),
                "objectType": str(objectType),
                "linkType": str(linkType),
                "message": str(exc),
            },
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": source_object_type, "linkType": link_type},
            )
        mapped = _to_foundry_outgoing_link_type(resource, source_object_type=source_object_type)
        if mapped is None:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": source_object_type, "linkType": link_type},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": source_object_type, "linkType": link_type},
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
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        object_type = str(objectType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await _require_domain_role(request, db_name=db_name)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "message": str(exc)},
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
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
        if exc.response is not None:
            try:
                upstream_payload = exc.response.json()
                if isinstance(upstream_payload, dict):
                    return JSONResponse(status_code=status_code, content=upstream_payload)
            except Exception:
                pass
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type},
            )
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
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
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
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "message": str(exc)},
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
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
        if exc.response is not None:
            try:
                upstream_payload = exc.response.json()
                if isinstance(upstream_payload, dict):
                    return JSONResponse(status_code=status_code, content=upstream_payload)
            except Exception:
                pass
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type},
            )
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
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        object_type = str(objectType or "").strip()
        primary_key_value = str(primaryKey or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        await _require_domain_role(request, db_name=db_name)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "primaryKey": str(primaryKey)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={
                "ontology": str(ontology),
                "objectType": str(objectType),
                "primaryKey": str(primaryKey),
                "message": str(exc),
            },
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
            )

        ontology_payload: Any = None
        try:
            ontology_payload = await oms_client.get_ontology(db_name, object_type, branch=branch)
        except Exception:
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
            )
        row = data[0]
        if not isinstance(row, dict):
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
            )
        return row
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if exc.response is not None:
            try:
                upstream_payload = exc.response.json()
                if isinstance(upstream_payload, dict):
                    return JSONResponse(status_code=status_code, content=upstream_payload)
            except Exception:
                pass
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
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
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        object_type = str(objectType or "").strip()
        primary_key_value = str(primaryKey or "").strip()
        link_type = str(linkType or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        if not link_type:
            raise ValueError("linkType is required")
        offset = _decode_page_token(page_token)
        await _require_domain_role(request, db_name=db_name)
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": str(ontology), "objectType": str(objectType), "primaryKey": str(primaryKey)},
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={
                "ontology": str(ontology),
                "objectType": str(objectType),
                "primaryKey": str(primaryKey),
                "linkType": str(linkType),
                "message": str(exc),
            },
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )

    try:
        source_primary_key_field = await _resolve_object_primary_key_field(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except ValueError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
            )
        source_row = source_rows[0]
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
            )
        link_type_side = _to_foundry_outgoing_link_type(link_resource, source_object_type=object_type)
        if not link_type_side:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
            )
        linked_object_type = str(link_type_side.get("objectTypeApiName") or "").strip()
        if not linked_object_type:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
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
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
            )
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )
    except ValueError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )
    except Exception as exc:
        logger.error("Failed to resolve link context (v2): %s", exc)
        return _foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )

    linked_primary_keys = _extract_linked_primary_keys(
        source_row,
        link_type=link_type,
        foreign_key_property=str(link_type_side.get("foreignKeyPropertyApiName") or "").strip() or None,
    )
    if not linked_primary_keys:
        return {"data": [], "nextPageToken": None}

    paged_primary_keys = linked_primary_keys[offset : offset + page_size]
    if not paged_primary_keys:
        return {"data": [], "nextPageToken": None}

    search_payload: Dict[str, Any] = {
        "where": _build_primary_key_where(linked_primary_key_field, paged_primary_keys),
        "pageSize": max(1, len(paged_primary_keys)),
    }
    if select:
        search_payload["select"] = [str(value).strip() for value in select if str(value).strip()]
    parsed_order_by = _parse_order_by(order_by)
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
        next_page_token = _encode_page_token(next_offset) if next_offset < len(linked_primary_keys) else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if exc.response is not None:
            try:
                upstream_payload = exc.response.json()
                if isinstance(upstream_payload, dict):
                    return JSONResponse(status_code=status_code, content=upstream_payload)
            except Exception:
                pass
        if status_code == status.HTTP_404_NOT_FOUND:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="NotFound",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
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
    oms_client: OMSClient = OMSClientDep,
):
    try:
        db_name = await _resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
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
    except OntologyNotFoundError:
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters={
                "ontology": str(ontology),
                "objectType": str(objectType),
                "primaryKey": str(primaryKey),
                "linkType": str(linkType),
                "linkedObjectPrimaryKey": str(linkedObjectPrimaryKey),
            },
        )
    except (ValueError, SecurityViolationError) as exc:
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={
                "ontology": str(ontology),
                "objectType": str(objectType),
                "primaryKey": str(primaryKey),
                "linkType": str(linkType),
                "linkedObjectPrimaryKey": str(linkedObjectPrimaryKey),
                "message": str(exc),
            },
        )
    except Exception as exc:
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters={"ontology": str(ontology), "message": str(exc)},
        )

    cursor: str | None = None
    while True:
        list_response = await list_linked_objects_v2(
            ontology=ontology,
            objectType=objectType,
            primaryKey=primaryKey,
            linkType=linkType,
            request=request,
            page_size=1000,
            page_token=cursor,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch=branch,
            oms_client=oms_client,
        )
        if isinstance(list_response, JSONResponse):
            return list_response
        rows = list_response.get("data") if isinstance(list_response, dict) else None
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in ("__primaryKey", "primaryKey", "id", "instance_id"):
                value = row.get(key)
                if value is None:
                    continue
                if str(value).strip() == linked_primary_key_value:
                    if select:
                        row = {k: row.get(k) for k in select if k in row}
                    if exclude_rid:
                        row.pop("__rid", None)
                    return row

        cursor = list_response.get("nextPageToken") if isinstance(list_response, dict) else None
        if not cursor:
            break

    return _foundry_error(
        status.HTTP_404_NOT_FOUND,
        error_code="NOT_FOUND",
        error_name="NotFound",
        parameters={
            "ontology": db_name,
            "objectType": object_type,
            "primaryKey": primary_key_value,
            "linkType": link_type,
            "linkedObjectPrimaryKey": linked_primary_key_value,
        },
    )
