from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


async def get_timeseries_first_point_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        result = await oms_client.get_timeseries_first_point(
            db_name,
            object_type,
            primary_key,
            property_name,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get timeseries firstPoint (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )


async def get_timeseries_last_point_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        result = await oms_client.get_timeseries_last_point(
            db_name,
            object_type,
            primary_key,
            property_name,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get timeseries lastPoint (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )


async def stream_timeseries_points_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> StreamingResponse | JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        body = await request.json() if await request.body() else {}
        response = await oms_client.stream_timeseries_points(
            db_name,
            object_type,
            primary_key,
            property_name,
            body,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return StreamingResponse(
            content=iter([response.content]),
            media_type=response.headers.get("content-type", "application/x-ndjson"),
        )
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to stream timeseries points (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )


async def upload_attachment_response(
    *,
    request: Request,
    filename: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    handled_exceptions: tuple[type[BaseException], ...],
) -> JSONResponse:
    error_parameters: Dict[str, Any] = {"filename": filename}
    try:
        body = await request.body()
        _ = sdk_package_rid, sdk_version
        forward_headers = extract_actor_forward_headers(request)
        result = await oms_client.upload_attachment(
            filename=filename,
            data=body,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology="attachments", parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology="attachments", parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to upload attachment (v2)",
            exc=exc,
            ontology="attachments",
            parameters=error_parameters,
        )


async def list_attachment_property_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        result = await oms_client.list_property_attachments(
            db_name,
            object_type,
            primary_key,
            property_name,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to list attachment property (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )


async def get_attachment_by_rid_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    attachment_rid: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
        "attachmentRid": attachment_rid,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        result = await oms_client.get_attachment_by_rid(
            db_name,
            object_type,
            primary_key,
            property_name,
            attachment_rid,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return JSONResponse(content=result)
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get attachment by RID (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )


async def get_attachment_content_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> Response | JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        response = await oms_client.get_attachment_content(
            db_name,
            object_type,
            primary_key,
            property_name,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "application/octet-stream"),
        )
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get attachment content (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )


async def get_attachment_content_by_rid_response(
    *,
    ontology: str,
    object_type: str,
    primary_key: str,
    property_name: str,
    attachment_rid: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Callable[..., Awaitable[str]],
    validate_branch: Callable[[str], str],
    require_domain_role: Callable[..., Awaitable[None]],
    extract_actor_forward_headers: Callable[[Request], Dict[str, str]],
    preflight_error_response: Callable[..., JSONResponse],
    upstream_status_error_response: Callable[..., JSONResponse],
    upstream_transport_error_response: Callable[..., JSONResponse],
    internal_error_response: Callable[..., JSONResponse],
    preflight_exceptions: tuple[type[BaseException], ...],
    handled_exceptions: tuple[type[BaseException], ...],
) -> Response | JSONResponse:
    error_parameters: Dict[str, Any] = {
        "ontology": ontology,
        "objectType": object_type,
        "primaryKey": primary_key,
        "property": property_name,
        "attachmentRid": attachment_rid,
    }
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        normalized_branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        await require_domain_role(request, db_name=db_name)
        forward_headers = extract_actor_forward_headers(request)
        response = await oms_client.get_attachment_content_by_rid(
            db_name,
            object_type,
            primary_key,
            property_name,
            attachment_rid,
            branch=normalized_branch,
            headers=forward_headers if forward_headers else None,
        )
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "application/octet-stream"),
        )
    except preflight_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology), parameters=error_parameters)
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc, ontology=ontology, parameters=error_parameters, passthrough_payload=True
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=ontology, parameters=error_parameters)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get attachment content by RID (v2)",
            exc=exc,
            ontology=ontology,
            parameters=error_parameters,
        )
