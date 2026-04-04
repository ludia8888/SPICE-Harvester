from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import Request, status

from shared.utils.payload_utils import extract_payload_object

logger = logging.getLogger("bff.routers.foundry_ontology_v2")


async def list_ontologies_route(
    *,
    request: Request,
    oms_client: Any,
    extract_databases: Any,
    to_foundry_ontology: Any,
    foundry_error: Any,
    handled_exceptions: Any,
) -> dict[str, Any]:
    try:
        _ = request
        payload = await oms_client.list_databases()
        rows = extract_databases(payload)
        data = [
            to_foundry_ontology(row)
            for row in rows
            if str(row.get("name") or row.get("db_name") or row.get("apiName") or "").strip()
        ]
        return {"data": data}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={},
        )
    except handled_exceptions as exc:
        logger.error("Failed to list ontologies (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={},
        )


async def get_ontology_route(
    *,
    ontology: str,
    request: Request,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    to_foundry_ontology: Any,
    not_found_error: Any,
    foundry_error: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await require_domain_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology))

    try:
        payload = await oms_client.get_database(db_name)
        row = extract_payload_object(payload)
        if not isinstance(row, dict):
            row = {"name": db_name}
        out = to_foundry_ontology(row)
        if not out.get("apiName"):
            out["apiName"] = db_name
        if not out.get("displayName"):
            out["displayName"] = db_name
        return out
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error("OntologyNotFound", ontology=db_name)
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name},
        )
    except handled_exceptions as exc:
        logger.error("Failed to get ontology (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name},
        )


async def list_query_types_route(
    *,
    ontology: str,
    request: Request,
    page_size: int,
    page_token: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    pagination_scope: Any,
    decode_page_token: Any,
    encode_page_token: Any,
    query_type_branch_candidates: Any,
    extract_ontology_resource_rows: Any,
    to_foundry_query_type: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        await require_domain_role(request, db_name=db_name)
        page_scope = pagination_scope("v2/queryTypes", db_name, page_size)
        offset = decode_page_token(page_token, scope=page_scope)
    except handled_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology))

    try:
        resources: list[dict[str, Any]] = []
        for branch_name in query_type_branch_candidates():
            payload = await oms_client.list_ontology_resources(
                db_name,
                resource_type="function",
                branch=branch_name,
                limit=page_size,
                offset=offset,
            )
            branch_resources = extract_ontology_resource_rows(payload)
            if branch_resources:
                resources = branch_resources
                break
            if not resources:
                resources = branch_resources
        data = [
            mapped
            for mapped in (to_foundry_query_type(resource) for resource in resources)
            if mapped is not None
        ]
        next_page_token = encode_page_token(offset + len(resources), scope=page_scope) if len(resources) == page_size else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=db_name)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to list query types (v2)",
            exc=exc,
            ontology=db_name,
        )


async def list_named_ontology_resources_route(
    *,
    ontology: str,
    request: Request,
    preview: bool,
    page_size: int,
    page_token: str | None,
    branch: str,
    oms_client: Any,
    resource_type: str,
    collection_scope: str,
    endpoint: str,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    is_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    require_access: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    pagination_scope: Any,
    decode_page_token: Any,
    encode_page_token: Any,
    extract_ontology_resource_rows: Any,
    to_foundry_named_metadata: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    strict_compat = False
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        strict_compat = is_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint=endpoint,
        )
        await require_access(request, db_name=db_name)
        page_scope = pagination_scope(collection_scope, db_name, branch, page_size, "1" if preview else "0")
        offset = decode_page_token(page_token, scope=page_scope)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview},
        )

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        resources = extract_ontology_resource_rows(payload)
        data = [
            mapped
            for mapped in (to_foundry_named_metadata(resource) for resource in resources)
            if mapped is not None
        ]
        next_page_token = encode_page_token(offset + len(resources), scope=page_scope) if len(resources) == page_size else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=db_name)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message=f"Failed to list {resource_type} types (v2)",
            exc=exc,
            ontology=db_name,
        )


async def get_named_ontology_resource_route(
    *,
    ontology: str,
    resource_api_name: str,
    request: Request,
    preview: bool,
    branch: str,
    oms_client: Any,
    resource_type: str,
    resource_parameter: str,
    not_found_name: str,
    endpoint: str,
    resolve_ontology_db_name: Any,
    validate_branch: Any | None,
    is_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    require_access: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    extract_ontology_resource: Any,
    to_foundry_named_metadata: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    strict_compat = False
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        if validate_branch is not None:
            branch = validate_branch(branch)
        strict_compat = is_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint=endpoint,
        )
        resource_id = str(resource_api_name or "").strip()
        if not resource_id:
            raise ValueError(f"{resource_parameter} is required")
        await require_access(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview, resource_parameter: str(resource_api_name)},
        )

    try:
        payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )
        resource = extract_ontology_resource(payload)
        if not resource:
            return not_found_error(
                not_found_name,
                ontology=db_name,
                parameters={resource_parameter: resource_id},
            )
        mapped = to_foundry_named_metadata(resource)
        if mapped is None:
            return not_found_error(
                not_found_name,
                ontology=db_name,
                parameters={resource_parameter: resource_id},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={resource_parameter: resource_id},
            not_found_response=not_found_error(
                not_found_name,
                ontology=db_name,
                parameters={resource_parameter: resource_id},
            ),
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={resource_parameter: resource_id},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message=f"Failed to get {resource_type} type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={resource_parameter: resource_id},
        )


async def list_value_types_route(
    *,
    ontology: str,
    request: Request,
    preview: bool,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    is_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    list_all_resources_for_type: Any,
    to_foundry_named_metadata: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    strict_compat = False
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        strict_compat = is_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="ontologies/{ontologyRid}/valueTypes",
        )
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"preview": preview},
        )

    try:
        resources = await list_all_resources_for_type(
            db_name=db_name,
            branch="main",
            resource_type="value_type",
            oms_client=oms_client,
        )
        data = [
            mapped
            for mapped in (to_foundry_named_metadata(resource) for resource in resources)
            if mapped is not None
        ]
        return {"data": data}
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            not_found_response=not_found_error("OntologyNotFound", ontology=db_name),
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(ontology=db_name)
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to list value types (v2)",
            exc=exc,
            ontology=db_name,
        )
