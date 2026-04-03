from __future__ import annotations

import logging
from typing import Any, Dict

import httpx
from fastapi import Request, status

logger = logging.getLogger(__name__)


async def search_objects_route(
    *,
    ontology: str,
    object_type_api_name: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    foundry_error: Any,
    passthrough_upstream_error_payload: Any,
    not_found_error: Any,
    handled_exceptions: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(object_type_api_name or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(object_type_api_name)},
        )

    try:
        result = await oms_client.post(
            f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
            params={"branch": branch},
            json=payload,
        )
        if not isinstance(result, dict):
            return foundry_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="INTERNAL",
                error_name="Internal",
                parameters={"ontology": db_name, "objectType": object_type},
            )
        return result
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type)
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type},
        )
    except handled_exceptions as exc:
        logger.error("Failed to search objects (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


async def count_objects_route(
    *,
    ontology: str,
    object_type_api_name: str,
    request: Request,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    foundry_error: Any,
    passthrough_upstream_error_payload: Any,
    not_found_error: Any,
    handled_exceptions: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(object_type_api_name or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(object_type_api_name)},
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
        passthrough = passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error("ObjectTypeNotFound", ontology=db_name, object_type=object_type)
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type},
        )
    except handled_exceptions as exc:
        logger.error("Failed to count objects (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type},
        )


async def aggregate_objects_route(
    *,
    ontology: str,
    object_type_api_name: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str,
    transaction_id: str | None,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        object_type = str(object_type_api_name or "").strip()
        _ = transaction_id, sdk_package_rid, sdk_version
        if not object_type:
            raise ValueError("objectType is required")
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(object_type_api_name)},
        )

    try:
        return await oms_client.aggregate_objects_v2(
            db_name,
            object_type,
            payload,
            branch=branch,
        )
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectType": object_type},
            not_found_response=not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                object_type=object_type,
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectType": object_type},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to aggregate objects (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectType": object_type},
        )


async def execute_query_route(
    *,
    ontology: str,
    query_api_name: str,
    body: Any,
    request: Request,
    version: str | None,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    transaction_id: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    not_found_error: Any,
    foundry_error: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    handled_exceptions: Any,
    query_type_branch_candidates: Any,
    extract_ontology_resource: Any,
    to_foundry_query_type: Any,
    extract_query_execution_plan: Any,
    apply_query_execute_options: Any,
    materialize_query_execution_value: Any,
) -> Any:
    db_name = str(ontology)
    normalized_query_api_name = str(query_api_name or "").strip()
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        _ = sdk_package_rid, sdk_version, transaction_id
        if not normalized_query_api_name:
            raise ValueError("queryApiName is required")
        await require_domain_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"queryApiName": str(query_api_name)},
        )

    try:
        resource = None
        query_resource_branch = query_type_branch_candidates()[0]
        for branch_name in query_type_branch_candidates():
            try:
                payload = await oms_client.get_ontology_resource(
                    db_name,
                    resource_type="function",
                    resource_id=normalized_query_api_name,
                    branch=branch_name,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == status.HTTP_404_NOT_FOUND:
                    continue
                raise
            candidate = extract_ontology_resource(payload)
            if candidate:
                resource = candidate
                query_resource_branch = branch_name
                break
        if not resource:
            return not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": normalized_query_api_name},
            )

        query_type = to_foundry_query_type(resource)
        if query_type is None:
            return not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": normalized_query_api_name},
            )

        requested_version = str(version or "").strip()
        if requested_version:
            resolved_version = str(query_type.get("version") or "").strip()
            if resolved_version and resolved_version != requested_version:
                return not_found_error(
                    "QueryTypeNotFound",
                    ontology=db_name,
                    parameters={"queryApiName": normalized_query_api_name},
                )

        object_type, execution_payload = extract_query_execution_plan(resource)
        if not object_type:
            raise ValueError("query execution spec requires objectType")

        execution_payload = apply_query_execute_options(
            base_payload=execution_payload,
            options=body.options,
        )
        parameters_payload = dict(body.parameters or {})
        resolved_object_type = materialize_query_execution_value(
            object_type,
            parameters=parameters_payload,
        )
        normalized_object_type = str(resolved_object_type or "").strip()
        if not normalized_object_type:
            raise ValueError("query execution spec resolved an empty objectType")
        resolved_payload = materialize_query_execution_value(
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
        return foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": db_name, "queryApiName": normalized_query_api_name, "message": str(exc)},
        )
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"queryApiName": normalized_query_api_name},
            not_found_response=not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": normalized_query_api_name},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"queryApiName": normalized_query_api_name},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to execute query (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"queryApiName": normalized_query_api_name},
        )


async def get_object_route(
    *,
    ontology: str,
    object_type_api_name: str,
    primary_key: str,
    request: Request,
    select: list[str] | None,
    exclude_rid: bool | None,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    not_found_error: Any,
    foundry_error: Any,
    passthrough_upstream_error_payload: Any,
    handled_exceptions: Any,
    extract_object_resource: Any,
    to_foundry_object_type: Any,
) -> Any:
    db_name = str(ontology)
    object_type = str(object_type_api_name or "").strip()
    primary_key_value = str(primary_key or "").strip()
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(object_type_api_name), "primaryKey": str(primary_key)},
        )

    try:
        object_type_payload = await oms_client.get_ontology_resource(
            db_name,
            resource_type="object_type",
            resource_id=object_type,
            branch=branch,
        )
        object_type_resource = extract_object_resource(object_type_payload)
        if not object_type_resource:
            return not_found_error(
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

        object_type_contract = to_foundry_object_type(object_type_resource, ontology_payload=ontology_payload)
        primary_key_field = str(object_type_contract.get("primaryKey") or "").strip()
        if not primary_key_field:
            return foundry_error(
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
            return foundry_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="INTERNAL",
                error_name="Internal",
                parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
            )
        data = result.get("data")
        if not isinstance(data, list) or not data:
            return not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        row = data[0]
        if not isinstance(row, dict):
            return not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        return row
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )
    except handled_exceptions as exc:
        logger.error("Failed to get object (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )


async def load_object_set_objects_route(
    *,
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str,
    transaction_id: str | None,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    redis_service: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    resolve_object_set_definition_with_store: Any,
    is_search_around_object_set: Any,
    resolve_object_set_object_type: Any,
    build_object_set_search_payload: Any,
    load_rows_for_search_around_object_set: Any,
    load_rows_for_single_object_type: Any,
    preflight_error_response: Any,
    object_set_runtime_value_error_response: Any,
    passthrough_upstream_error_payload: Any,
    foundry_error: Any,
    not_found_error: Any,
    handled_exceptions: Any,
    resolve_database_actor: Any,
) -> Any:
    search_around = False
    object_type: str | None = None
    search_payload: dict[str, Any] = {}
    actor: str | None = None
    db_name = str(ontology)
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        object_set = await resolve_object_set_definition_with_store(
            payload.get("objectSet"),
            redis_service=redis_service,
        )
        search_around = is_search_around_object_set(object_set)
        if not search_around:
            object_type = resolve_object_set_object_type(object_set)
            if not object_type:
                raise ValueError("objectSet.objectType is required")
        await require_read_role(request, db_name=db_name)
        _, actor = resolve_database_actor(request.headers)
        if not search_around:
            search_payload = build_object_set_search_payload(
                object_set=object_set,
                payload=payload,
                require_select=True,
            )
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjects"},
        )

    try:
        if search_around:
            rows, total_count, next_page_token, _ = await load_rows_for_search_around_object_set(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_set=object_set,
                payload=payload,
                endpoint_scope="v2/objectSets/loadObjects",
                actor=actor,
                redis_service=redis_service,
            )
        else:
            rows, total_count, next_page_token = await load_rows_for_single_object_type(
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
        return object_set_runtime_value_error_response(
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjects"},
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadObjects"},
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectSet": "loadObjects"},
        )
    except handled_exceptions as exc:
        logger.error("Failed to load object set objects (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectSet": "loadObjects"},
        )


async def load_object_set_multiple_object_types_route(
    *,
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str,
    preview: bool,
    transaction_id: str | None,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    redis_service: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    is_foundry_v2_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    resolve_object_set_definition_with_store: Any,
    is_search_around_object_set: Any,
    collect_object_set_object_types: Any,
    require_read_role: Any,
    resolve_database_actor: Any,
    build_object_set_search_payload: Any,
    pagination_scope: Any,
    normalize_select_values: Any,
    load_rows_for_search_around_object_set: Any,
    load_rows_for_single_object_type: Any,
    load_rows_for_multi_object_types: Any,
    preflight_error_response: Any,
    object_set_runtime_value_error_response: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
    handled_exceptions: Any,
) -> Any:
    search_around = False
    object_types: list[str] = []
    search_payload: dict[str, Any] = {}
    request_page_token: str | None = None
    pagination_scope_value: str | None = None
    actor: str | None = None
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = transaction_id, sdk_package_rid, sdk_version
        strict_compat = is_foundry_v2_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsMultipleObjectTypes",
        )
        object_set = await resolve_object_set_definition_with_store(
            payload.get("objectSet"),
            redis_service=redis_service,
        )
        search_around = is_search_around_object_set(object_set)
        if not search_around:
            object_types = collect_object_set_object_types(object_set)
            if not object_types:
                raise ValueError("objectSet.objectType is required")
        await require_read_role(request, db_name=db_name)
        _, actor = resolve_database_actor(request.headers)
        if not search_around:
            search_payload = build_object_set_search_payload(
                object_set=object_set,
                payload=payload,
                require_select=True,
            )
            request_page_token = search_payload.get("pageToken")
            if request_page_token is not None:
                request_page_token = str(request_page_token)
            pagination_scope_value = pagination_scope(
                "v2/objectSets/loadObjectsMultipleObjectTypes",
                db_name,
                branch,
                ",".join(sorted(object_types)),
                str(payload.get("pageSize") or ""),
                ",".join(normalize_select_values(payload)),
                str(payload.get("snapshot") if "snapshot" in payload else ""),
            )
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )

    try:
        if search_around:
            rows, total_count, next_page_token, resolved_object_types = await load_rows_for_search_around_object_set(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_set=object_set,
                payload=payload,
                endpoint_scope="v2/objectSets/loadObjectsMultipleObjectTypes",
                actor=actor,
                redis_service=redis_service,
            )
            object_types = resolved_object_types
        elif len(object_types) == 1:
            rows, total_count, next_page_token = await load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_types[0],
                search_payload=search_payload,
            )
        else:
            rows, total_count, next_page_token = await load_rows_for_multi_object_types(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_types=object_types,
                search_payload=search_payload,
                page_token=request_page_token,
                pagination_scope=pagination_scope_value or "",
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
        return object_set_runtime_value_error_response(
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
            not_found_response=not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to load object set multiple object types (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsMultipleObjectTypes"},
        )


async def load_object_set_objects_or_interfaces_route(
    *,
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str,
    preview: bool,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    redis_service: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    is_foundry_v2_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    resolve_object_set_definition_with_store: Any,
    is_search_around_object_set: Any,
    collect_object_set_object_types: Any,
    require_read_role: Any,
    resolve_database_actor: Any,
    build_object_set_search_payload: Any,
    pagination_scope: Any,
    normalize_select_values: Any,
    load_rows_for_search_around_object_set: Any,
    load_rows_for_single_object_type: Any,
    load_rows_for_multi_object_types: Any,
    preflight_error_response: Any,
    object_set_runtime_value_error_response: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
    handled_exceptions: Any,
) -> Any:
    search_around = False
    object_types: list[str] = []
    search_payload: dict[str, Any] = {}
    request_page_token: str | None = None
    pagination_scope_value: str | None = None
    actor: str | None = None
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        strict_compat = is_foundry_v2_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsOrInterfaces",
        )
        object_set = await resolve_object_set_definition_with_store(
            payload.get("objectSet"),
            redis_service=redis_service,
        )
        search_around = is_search_around_object_set(object_set)
        if not search_around:
            object_types = collect_object_set_object_types(object_set)
            if not object_types:
                raise ValueError("objectSet.objectType is required")
        await require_read_role(request, db_name=db_name)
        _, actor = resolve_database_actor(request.headers)
        if not search_around:
            search_payload = build_object_set_search_payload(
                object_set=object_set,
                payload=payload,
                require_select=True,
            )
            request_page_token = search_payload.get("pageToken")
            if request_page_token is not None:
                request_page_token = str(request_page_token)
            pagination_scope_value = pagination_scope(
                "v2/objectSets/loadObjectsOrInterfaces",
                db_name,
                branch,
                ",".join(sorted(object_types)),
                str(payload.get("pageSize") or ""),
                ",".join(normalize_select_values(payload)),
                str(payload.get("snapshot") if "snapshot" in payload else ""),
            )
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )

    try:
        if search_around:
            rows, total_count, next_page_token, _ = await load_rows_for_search_around_object_set(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_set=object_set,
                payload=payload,
                endpoint_scope="v2/objectSets/loadObjectsOrInterfaces",
                actor=actor,
                redis_service=redis_service,
            )
        elif len(object_types) == 1:
            rows, total_count, next_page_token = await load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_types[0],
                search_payload=search_payload,
            )
        else:
            rows, total_count, next_page_token = await load_rows_for_multi_object_types(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_types=object_types,
                search_payload=search_payload,
                page_token=request_page_token,
                pagination_scope=pagination_scope_value or "",
            )
        return {
            "data": rows,
            "nextPageToken": next_page_token,
            "totalCount": total_count,
        }
    except ValueError as exc:
        return object_set_runtime_value_error_response(
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
            not_found_response=not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadObjectsOrInterfaces"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to load object set objects or interfaces (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadObjectsOrInterfaces"},
        )


async def load_object_set_links_route(
    *,
    ontology: str,
    payload: Dict[str, Any],
    request: Request,
    branch: str,
    preview: bool,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    redis_service: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    is_foundry_v2_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    resolve_object_set_definition_with_store: Any,
    normalize_link_type_values: Any,
    collect_object_set_object_types: Any,
    require_read_role: Any,
    build_object_set_search_payload: Any,
    pagination_scope: Any,
    load_rows_for_single_object_type: Any,
    load_rows_for_multi_object_types: Any,
    get_ontology_resource: Any,
    extract_ontology_resource: Any,
    resolve_source_object_type_from_row: Any,
    resolve_object_primary_key_field: Any,
    to_foundry_outgoing_link_type: Any,
    collect_load_links_rows: Any,
    preflight_error_response: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
    handled_exceptions: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        strict_compat = is_foundry_v2_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="POST /api/v2/ontologies/{ontologyRid}/objectSets/loadLinks",
        )
        object_set = await resolve_object_set_definition_with_store(
            payload.get("objectSet"),
            redis_service=redis_service,
        )
        requested_links = normalize_link_type_values(payload)
        object_types = collect_object_set_object_types(object_set)
        if not object_types:
            raise ValueError("objectSet.objectType is required")
        await require_read_role(request, db_name=db_name)
        search_payload = build_object_set_search_payload(
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
        pagination_scope_value = pagination_scope(
            "v2/objectSets/loadLinks",
            db_name,
            branch,
            ",".join(sorted(object_types)),
            ",".join(requested_links),
            str(payload.get("objectSet") or ""),
        )
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectSet": "loadLinks"},
        )

    try:
        if len(object_types) == 1:
            rows, _, next_page_token = await load_rows_for_single_object_type(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_type=object_types[0],
                search_payload=search_payload,
            )
        else:
            rows, _, next_page_token = await load_rows_for_multi_object_types(
                oms_client=oms_client,
                db_name=db_name,
                branch=branch,
                object_types=object_types,
                search_payload=search_payload,
                page_token=request_page_token,
                pagination_scope=pagination_scope_value,
            )
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"objectSet": "loadLinks"},
            not_found_response=not_found_error(
                "ObjectTypeNotFound",
                ontology=db_name,
                parameters={"objectSet": "loadLinks"},
            ),
            passthrough_payload=True,
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"objectSet": "loadLinks"},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to load object set source rows (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"objectSet": "loadLinks"},
        )

    link_resources: dict[str, dict[str, Any]] = {}
    for link_type in requested_links:
        try:
            link_payload = await get_ontology_resource(
                db_name,
                resource_type="link_type",
                resource_id=link_type,
                branch=branch,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
            if status_code == status.HTTP_404_NOT_FOUND:
                return not_found_error(
                    "LinkTypeNotFound",
                    ontology=db_name,
                    link_type=link_type,
                    parameters={"objectSet": "loadLinks"},
                )
            return upstream_status_error_response(
                exc,
                ontology=db_name,
                parameters={"objectSet": "loadLinks", "linkType": link_type},
                passthrough_payload=True,
            )
        except httpx.HTTPError:
            return upstream_transport_error_response(
                ontology=db_name,
                parameters={"objectSet": "loadLinks", "linkType": link_type},
            )

        link_resource = extract_ontology_resource(link_payload)
        if not isinstance(link_resource, dict):
            return not_found_error(
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
                    object_type := resolve_source_object_type_from_row(
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
            link_side = to_foundry_outgoing_link_type(link_resource, source_object_type=source_object_type)
            if isinstance(link_side, dict):
                source_link_sides[link_type] = link_side
        link_sides_by_source_type[source_object_type] = source_link_sides

    for link_type in requested_links:
        if not any(link_type in source_link_sides for source_link_sides in link_sides_by_source_type.values()):
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                link_type=link_type,
                parameters={"objectSet": "loadLinks"},
            )

    source_primary_key_fields: dict[str, str] = {}
    for source_object_type in source_object_types:
        try:
            source_primary_key_fields[source_object_type] = await resolve_object_primary_key_field(
                db_name=db_name,
                object_type=source_object_type,
                branch=branch,
                oms_client=oms_client,
            )
        except handled_exceptions as exc:
            logger.warning(
                "Failed to resolve source object primary key for link expansion (ontology=%s sourceObjectType=%s branch=%s): %s",
                db_name,
                source_object_type,
                branch,
                exc,
                exc_info=True,
            )
            continue

    data = collect_load_links_rows(
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


async def list_linked_objects_route(
    *,
    ontology: str,
    object_type_api_name: str,
    primary_key: str,
    link_type_api_name: str,
    request: Request,
    page_size: int,
    page_token: str | None,
    select: list[str] | None,
    order_by: str | None,
    exclude_rid: bool | None,
    snapshot: bool | None,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    foundry_error: Any,
    not_found_error: Any,
    passthrough_upstream_error_payload: Any,
    handled_exceptions: Any,
    parse_order_by: Any,
    pagination_scope: Any,
    decode_page_token: Any,
    encode_page_token: Any,
    resolve_object_primary_key_field: Any,
    extract_ontology_resource: Any,
    to_foundry_outgoing_link_type: Any,
    extract_object_type_relationships: Any,
    extract_linked_primary_keys_page: Any,
    build_primary_key_where: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(object_type_api_name or "").strip()
        primary_key_value = str(primary_key or "").strip()
        link_type = str(link_type_api_name or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        if not link_type:
            raise ValueError("linkType is required")
        normalized_select = [str(value).strip() for value in (select or []) if str(value).strip()]
        parsed_order_by = parse_order_by(order_by)
        page_scope = pagination_scope(
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
        offset = decode_page_token(page_token, scope=page_scope)
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"objectType": str(object_type_api_name), "primaryKey": str(primary_key), "linkType": str(link_type_api_name)},
        )

    try:
        source_primary_key_field = await resolve_object_primary_key_field(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except ValueError:
        return not_found_error(
            "ObjectTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            parameters={"primaryKey": primary_key_value},
        )
    except handled_exceptions as exc:
        logger.error("Failed to resolve source primary key field (v2): %s", exc)
        return foundry_error(
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
            return not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        source_row = source_rows[0]
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value},
        )
    except handled_exceptions as exc:
        logger.error("Failed to resolve source object (v2): %s", exc)
        return foundry_error(
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
            link_resource = extract_ontology_resource(link_payload)
            if isinstance(link_resource, dict):
                link_type_side = to_foundry_outgoing_link_type(link_resource, source_object_type=object_type)
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
                object_resource = extract_ontology_resource(object_payload)
                if isinstance(object_resource, dict):
                    for rel in extract_object_type_relationships(object_resource):
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
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        linked_primary_key_field = await resolve_object_primary_key_field(
            db_name=db_name,
            object_type=linked_object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )
    except ValueError:
        return not_found_error(
            "LinkTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            link_type=link_type,
            parameters={"primaryKey": primary_key_value},
        )
    except handled_exceptions as exc:
        logger.error("Failed to resolve link context (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )

    foreign_key_property = str(link_type_side.get("foreignKeyPropertyApiName") or "").strip() or None
    paged_primary_keys, has_more = extract_linked_primary_keys_page(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
        offset=offset,
        page_size=page_size,
    )
    if not paged_primary_keys:
        return {"data": [], "nextPageToken": None}

    search_payload: Dict[str, Any] = {
        "where": build_primary_key_where(linked_primary_key_field, paged_primary_keys),
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
        next_page_token = encode_page_token(next_offset, scope=page_scope) if has_more else None
        return {"data": data, "nextPageToken": next_page_token}
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        passthrough = passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                link_type=link_type,
                parameters={"primaryKey": primary_key_value},
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )
    except handled_exceptions as exc:
        logger.error("Failed to list linked objects (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters={"ontology": db_name, "objectType": object_type, "primaryKey": primary_key_value, "linkType": link_type},
        )


async def get_linked_object_route(
    *,
    ontology: str,
    object_type_api_name: str,
    primary_key: str,
    link_type_api_name: str,
    linked_object_primary_key: str,
    request: Request,
    select: list[str] | None,
    exclude_rid: bool | None,
    branch: str,
    sdk_package_rid: str | None,
    sdk_version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_read_role: Any,
    preflight_error_response: Any,
    foundry_error: Any,
    not_found_error: Any,
    handled_exceptions: Any,
    linked_object_parameters: Any,
    resolve_object_primary_key_field: Any,
    extract_ontology_resource: Any,
    to_foundry_outgoing_link_type: Any,
    linked_primary_key_exists: Any,
    project_row_with_required_fields: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        _ = sdk_package_rid, sdk_version
        object_type = str(object_type_api_name or "").strip()
        primary_key_value = str(primary_key or "").strip()
        link_type = str(link_type_api_name or "").strip()
        linked_primary_key_value = str(linked_object_primary_key or "").strip()
        if not object_type:
            raise ValueError("objectType is required")
        if not primary_key_value:
            raise ValueError("primaryKey is required")
        if not link_type:
            raise ValueError("linkType is required")
        if not linked_primary_key_value:
            raise ValueError("linkedObjectPrimaryKey is required")
        await require_read_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={
                "objectType": str(object_type_api_name),
                "primaryKey": str(primary_key),
                "linkType": str(link_type_api_name),
                "linkedObjectPrimaryKey": str(linked_object_primary_key),
            },
        )

    error_parameters = linked_object_parameters(
        ontology=db_name,
        object_type=object_type,
        primary_key=primary_key_value,
        link_type=link_type,
        linked_primary_key=linked_primary_key_value,
    )
    normalized_select = [str(value).strip() for value in (select or []) if str(value).strip()]

    try:
        source_primary_key_field = await resolve_object_primary_key_field(
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except ValueError:
        return not_found_error(
            "ObjectTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            primary_key=primary_key_value,
            link_type=link_type,
            linked_primary_key=linked_primary_key_value,
        )
    except handled_exceptions as exc:
        logger.error("Failed to resolve source primary key field (v2): %s", exc)
        return foundry_error(
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
            return not_found_error(
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
            return not_found_error(
                "ObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters=error_parameters,
        )
    except handled_exceptions as exc:
        logger.error("Failed to resolve source object (v2): %s", exc)
        return foundry_error(
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
        link_resource = extract_ontology_resource(link_payload)
        if not isinstance(link_resource, dict):
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        link_type_side = to_foundry_outgoing_link_type(link_resource, source_object_type=object_type)
        if not link_type_side:
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        linked_object_type = str(link_type_side.get("objectTypeApiName") or "").strip()
        if not linked_object_type:
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        linked_primary_key_field = await resolve_object_primary_key_field(
            db_name=db_name,
            object_type=linked_object_type,
            branch=branch,
            oms_client=oms_client,
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "LinkTypeNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters=error_parameters,
        )
    except ValueError:
        return not_found_error(
            "LinkTypeNotFound",
            ontology=db_name,
            object_type=object_type,
            primary_key=primary_key_value,
            link_type=link_type,
            linked_primary_key=linked_primary_key_value,
        )
    except handled_exceptions as exc:
        logger.error("Failed to resolve link context (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters=error_parameters,
        )

    foreign_key_property = str(link_type_side.get("foreignKeyPropertyApiName") or "").strip() or None
    if not linked_primary_key_exists(
        source_row,
        link_type=link_type,
        foreign_key_property=foreign_key_property,
        linked_primary_key=linked_primary_key_value,
    ):
        return not_found_error(
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
            return not_found_error(
                "LinkedObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        row = rows[0]
        if not isinstance(row, dict):
            return not_found_error(
                "LinkedObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return project_row_with_required_fields(
            row,
            select_fields=normalized_select,
            exclude_rid=bool(exclude_rid),
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        if status_code == status.HTTP_404_NOT_FOUND:
            return not_found_error(
                "LinkedObjectNotFound",
                ontology=db_name,
                object_type=object_type,
                primary_key=primary_key_value,
                link_type=link_type,
                linked_primary_key=linked_primary_key_value,
            )
        return foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters=error_parameters,
        )
    except handled_exceptions as exc:
        logger.error("Failed to get linked object (v2): %s", exc)
        return foundry_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code="INTERNAL",
            error_name="Internal",
            parameters=error_parameters,
        )
