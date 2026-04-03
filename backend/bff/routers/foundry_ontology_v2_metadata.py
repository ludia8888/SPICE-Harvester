from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

import httpx
from fastapi import status

from bff.routers.foundry_ontology_v2_serialization import _extract_ontology_resource_rows
from bff.routers.link_types_read import (
    _normalize_object_ref,
    _to_foundry_incoming_link_type,
    _to_foundry_outgoing_link_type,
)
from bff.routers.object_types import _to_foundry_object_type
from bff.services.oms_client import OMSClient
from shared.foundry.rids import build_rid
from shared.utils.ontology_fields import list_ontology_properties

logger = logging.getLogger(__name__)

_DEFAULT_OBJECT_TYPE_ICON: Dict[str, str] = {
    "type": "blueprint",
    "name": "table",
    "color": "#4C6A9A",
}
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


async def get_full_metadata_route(
    *,
    ontology: str,
    request: Any,
    preview: bool,
    branch: str,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    is_strict_compat_enabled: Any,
    require_preview_true_for_strict_compat: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    list_resources_best_effort: Any,
    list_action_type_resources_best_effort: Any,
    get_ontology_payload_best_effort: Any,
    group_outgoing_link_types_by_source: Any,
    group_incoming_link_types_by_target: Any,
    to_foundry_object_type_full_metadata: Any,
    strictify_object_type_full_metadata: Any,
    to_foundry_ontology: Any,
    full_metadata_branch_contract: Any,
    to_foundry_action_type_map: Any,
    to_foundry_query_type_metadata_map: Any,
    to_foundry_named_metadata_map: Any,
    log_strict_compat_summary: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    strict_compat = False
    db_name = str(ontology)
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        strict_compat = is_strict_compat_enabled(db_name=db_name)
        require_preview_true_for_strict_compat(
            preview=preview,
            strict_compat=strict_compat,
            endpoint="GET /api/v2/ontologies/{ontologyRid}/fullMetadata",
        )
        await require_domain_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology))

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
            list_resources_best_effort(db_name=db_name, branch=branch, resource_type="object_type", oms_client=oms_client),
            list_resources_best_effort(db_name=db_name, branch=branch, resource_type="link_type", oms_client=oms_client),
            list_action_type_resources_best_effort(db_name=db_name, branch=branch, oms_client=oms_client),
            list_resources_best_effort(db_name=db_name, branch=branch, resource_type="function", oms_client=oms_client),
            list_resources_best_effort(db_name=db_name, branch=branch, resource_type="interface", oms_client=oms_client),
            list_resources_best_effort(db_name=db_name, branch=branch, resource_type="shared_property", oms_client=oms_client),
            list_resources_best_effort(db_name=db_name, branch=branch, resource_type="value_type", oms_client=oms_client),
        )

        link_types_by_source = group_outgoing_link_types_by_source(link_resources)
        link_types_by_target = group_incoming_link_types_by_target(link_resources)
        object_type_ids = [str(resource.get("id") or "").strip() for resource in object_resources if str(resource.get("id") or "").strip()]
        ontology_payloads = await asyncio.gather(
            *[
                get_ontology_payload_best_effort(
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
            outgoing_lt = link_types_by_source.get(object_type_id) or []
            incoming_lt = link_types_by_target.get(object_type_id) or []
            seen_lt: set[tuple[str, str]] = set()
            merged_lt: list[dict[str, Any]] = []
            for lt in outgoing_lt + incoming_lt:
                key = (lt.get("apiName", ""), lt.get("objectTypeApiName", ""))
                if key not in seen_lt:
                    seen_lt.add(key)
                    merged_lt.append(lt)
            mapped = to_foundry_object_type_full_metadata(
                resource,
                ontology_payload=ontology_payload_by_object_type.get(object_type_id),
                link_types=merged_lt,
            )
            if strict_compat:
                mapped, object_fixes, object_dropped = strictify_object_type_full_metadata(
                    mapped,
                    db_name=db_name,
                    object_type_hint=object_type_id,
                )
                strict_fix_count += object_fixes
                strict_dropped_count += object_dropped
            object_types[object_type_id] = mapped

        ontology_contract = to_foundry_ontology(database_row)
        if not ontology_contract.get("apiName"):
            ontology_contract["apiName"] = db_name
        if not ontology_contract.get("displayName"):
            ontology_contract["displayName"] = db_name

        log_strict_compat_summary(
            route="get_full_metadata_v2",
            db_name=db_name,
            branch=branch,
            fixes=strict_fix_count,
            dropped=strict_dropped_count,
        )

        return {
            "ontology": ontology_contract,
            "branch": full_metadata_branch_contract(branch=branch),
            "objectTypes": object_types,
            "actionTypes": to_foundry_action_type_map(action_resources),
            "queryTypes": to_foundry_query_type_metadata_map(query_resources),
            "interfaceTypes": to_foundry_named_metadata_map(interface_resources),
            "sharedPropertyTypes": to_foundry_named_metadata_map(shared_property_resources),
            "valueTypes": to_foundry_named_metadata_map(value_type_resources),
        }
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
            log_message="Failed to get full metadata (v2)",
            exc=exc,
            ontology=db_name,
        )


async def get_query_type_route(
    *,
    ontology: str,
    query_api_name: str,
    request: Any,
    version: str | None,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    query_type_branch_candidates: Any,
    extract_ontology_resource: Any,
    to_foundry_query_type: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    db_name = str(ontology)
    normalized_query_api_name = str(query_api_name or "").strip()
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
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
                break
        if not resource:
            return not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": normalized_query_api_name},
            )
        mapped = to_foundry_query_type(resource)
        if mapped is None:
            return not_found_error(
                "QueryTypeNotFound",
                ontology=db_name,
                parameters={"queryApiName": normalized_query_api_name},
            )
        requested_version = str(version or "").strip()
        if requested_version:
            resolved_version = str(mapped.get("version") or "").strip()
            if resolved_version and resolved_version != requested_version:
                return not_found_error(
                    "QueryTypeNotFound",
                    ontology=db_name,
                    parameters={"queryApiName": normalized_query_api_name},
                )
        return mapped
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
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"queryApiName": normalized_query_api_name},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get query type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"queryApiName": normalized_query_api_name},
        )


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
    return list_ontology_properties(payload)


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
