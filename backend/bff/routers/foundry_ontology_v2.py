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
        data = [_to_foundry_ontology(row) for row in rows if str(row.get("name") or row.get("db_name") or "").strip()]
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
    branch: str = Query("main", include_in_schema=False),
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
    branch: str = Query("main", include_in_schema=False),
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
    branch: str = Query("main", include_in_schema=False),
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
    branch: str = Query("main", include_in_schema=False),
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
    branch: str = Query("main", include_in_schema=False),
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
