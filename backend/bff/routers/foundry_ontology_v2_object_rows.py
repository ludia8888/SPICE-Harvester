from __future__ import annotations

import logging
import time
from typing import Any, Dict

import httpx
from fastapi import status

from bff.routers.foundry_ontology_v2_common import _decode_page_token, _encode_page_token
from bff.routers.foundry_ontology_v2_errors import _ONTOLOGY_HANDLED_EXCEPTIONS
from bff.routers.foundry_ontology_v2_object_sets import (
    _collect_object_set_object_types,
    _extract_search_around_link_type,
    _resolve_object_set_definition_with_store,
)
from bff.routers.link_types_read import _to_foundry_outgoing_link_type
from bff.routers.object_types import (
    _extract_resource as _extract_object_resource,
    _to_foundry_object_type,
)
from bff.routers.foundry_ontology_v2_serialization import _extract_ontology_resource
from bff.services.oms_client import OMSClient
from shared.config.settings import get_settings
from shared.foundry.compute_routing import choose_search_around_backend
from shared.foundry.spark_on_demand_dispatcher import get_spark_on_demand_dispatcher
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.storage.redis_service import RedisService
from shared.utils.object_type_backing import list_backing_sources

logger = logging.getLogger(__name__)

_MAX_ROWS_LOAD_ALL = 50_000
_PRIMARY_KEY_VALUE_KEYS = ("__primaryKey", "primaryKey", "instance_id", "id")
_SEARCH_ROUTING_AUDIT_STORE: AuditLogStore | None = None
_SEARCH_ROUTING_AUDIT_DISABLED_UNTIL = 0.0
_SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS = 60.0
_SEARCH_ROUTING_AUDIT_CALLBACK: Any = None


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
        primary_key = _resolve_source_primary_key_from_row(row) or ""
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
    redis_service: RedisService,
) -> tuple[dict[str, list[str]], str]:
    link_type = _extract_search_around_link_type(object_set)
    source_object_set = await _resolve_object_set_definition_with_store(
        object_set.get("objectSet"),
        redis_service=redis_service,
    )
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
    link_resource = _extract_ontology_resource(link_payload)
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

    global _SEARCH_ROUTING_AUDIT_STORE, _SEARCH_ROUTING_AUDIT_DISABLED_UNTIL
    now = time.monotonic()
    if _SEARCH_ROUTING_AUDIT_DISABLED_UNTIL > now:
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
        _SEARCH_ROUTING_AUDIT_STORE = None
        _SEARCH_ROUTING_AUDIT_DISABLED_UNTIL = time.monotonic() + _SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS
        logger.warning(
            "Search Around compute routing audit temporarily disabled after persistence error (ontology=%s, retry_in=%ss): %s",
            db_name,
            int(_SEARCH_ROUTING_AUDIT_RETRY_COOLDOWN_SECONDS),
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
    redis_service: RedisService,
) -> tuple[list[dict[str, Any]], str, str | None, list[str]]:
    target_primary_keys_by_type, link_type = await _resolve_search_around_target_primary_keys(
        oms_client=oms_client,
        db_name=db_name,
        branch=branch,
        object_set=object_set,
        redis_service=redis_service,
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

    audit_callback = _SEARCH_ROUTING_AUDIT_CALLBACK or _audit_search_around_compute_routing
    await audit_callback(
        db_name=db_name,
        branch=branch,
        endpoint_scope=endpoint_scope,
        actor=actor,
        link_type=link_type,
        target_object_types=target_object_types,
        decision_metadata=decision_metadata,
    )
    return rows, total_count, next_page_token, resolved_object_types


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
    return int(value)


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
                _MAX_ROWS_LOAD_ALL,
                db_name,
                object_type,
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
    for key in _PRIMARY_KEY_VALUE_KEYS:
        value = _normalize_primary_key_text(row.get(key))
        if value:
            return value
    if primary_key_field:
        value = _normalize_primary_key_text(_value_by_field(row, primary_key_field))
        if value:
            return value
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


def _pagination_scope(*parts: Any) -> str:
    normalized = [str(part).strip() for part in parts if str(part).strip()]
    return "|".join(normalized)


def _strip_typed_ref(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.count("/") == 1 and not text.startswith("http://") and not text.startswith("https://"):
        _, right = text.split("/", 1)
        right = right.strip()
        if right:
            return right
    return text


def _normalize_primary_key_text(value: Any) -> str:
    return _strip_typed_ref(str(value or ""))


def _iter_primary_key_values(value: Any):
    if value is None:
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_primary_key_values(item)
        return
    if isinstance(value, dict):
        for key in _PRIMARY_KEY_VALUE_KEYS:
            candidate = value.get(key)
            if candidate is None:
                continue
            text = _normalize_primary_key_text(candidate)
            if text:
                yield text
        return
    text = _normalize_primary_key_text(value)
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
