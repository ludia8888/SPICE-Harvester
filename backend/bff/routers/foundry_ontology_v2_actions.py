from __future__ import annotations

import logging
from typing import Any, Dict

import httpx
from fastapi import status

logger = logging.getLogger(__name__)


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


async def list_action_types_route(
    *,
    ontology: str,
    request: Any,
    page_size: int,
    page_token: str | None,
    branch: str,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    pagination_scope: Any,
    decode_page_token: Any,
    encode_page_token: Any,
    extract_ontology_resource_rows: Any,
    list_action_type_resources_with_fallback: Any,
    to_foundry_action_type: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        await require_domain_role(request, db_name=db_name)
        page_scope = pagination_scope("v2/actionTypes", db_name, branch, page_size)
        offset = decode_page_token(page_token, scope=page_scope)
    except handled_exceptions as exc:
        return preflight_error_response(exc, ontology=str(ontology))

    try:
        payload = await oms_client.list_ontology_resources(
            db_name,
            resource_type="action_type",
            branch=branch,
            limit=page_size,
            offset=offset,
        )
        raw_resources = extract_ontology_resource_rows(payload)
        resources = raw_resources
        total_available: int | None = None
        if not resources:
            fallback_resources = await list_action_type_resources_with_fallback(
                db_name=db_name,
                branch=branch,
                oms_client=oms_client,
            )
            total_available = len(fallback_resources)
            resources = fallback_resources[offset : offset + page_size]
        data = [
            mapped
            for mapped in (to_foundry_action_type(resource) for resource in resources)
            if mapped is not None
        ]
        if total_available is not None:
            consumed = offset + len(resources)
            next_page_token = encode_page_token(consumed, scope=page_scope) if consumed < total_available else None
        else:
            next_page_token = (
                encode_page_token(offset + len(raw_resources), scope=page_scope)
                if len(raw_resources) == page_size
                else None
            )
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
            log_message="Failed to list action types (v2)",
            exc=exc,
            ontology=db_name,
        )


async def get_action_type_route(
    *,
    ontology: str,
    action_type_api_name: str,
    request: Any,
    branch: str,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    extract_ontology_resource: Any,
    find_action_type_resource_by_id: Any,
    to_foundry_action_type: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        action_type = str(action_type_api_name or "").strip()
        if not action_type:
            raise ValueError("actionType is required")
        await require_domain_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"actionType": str(action_type_api_name)},
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
            resource = extract_ontology_resource(payload)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
            if status_code != status.HTTP_404_NOT_FOUND:
                raise

        if not resource:
            resource = await find_action_type_resource_by_id(
                db_name=db_name,
                branch=branch,
                action_type=action_type,
                oms_client=oms_client,
            )

        if not resource:
            return not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionType": action_type},
            )
        mapped = to_foundry_action_type(resource)
        if mapped is None:
            return not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionType": action_type},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"actionType": action_type},
            not_found_response=not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionType": action_type},
            ),
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionType": action_type},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get action type (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"actionType": action_type},
        )


async def get_action_type_by_rid_route(
    *,
    ontology: str,
    action_type_rid: str,
    request: Any,
    branch: str,
    oms_client: Any,
    resolve_ontology_db_name: Any,
    validate_branch: Any,
    require_domain_role: Any,
    preflight_error_response: Any,
    handled_exceptions: Any,
    find_resource_by_rid: Any,
    find_action_type_resource_by_rid: Any,
    to_foundry_action_type: Any,
    upstream_status_error_response: Any,
    upstream_transport_error_response: Any,
    internal_error_response: Any,
    not_found_error: Any,
) -> Any:
    try:
        db_name = await resolve_ontology_db_name(ontology=ontology, oms_client=oms_client)
        branch = validate_branch(branch)
        normalized_rid = str(action_type_rid or "").strip()
        if not normalized_rid:
            raise ValueError("actionTypeRid is required")
        await require_domain_role(request, db_name=db_name)
    except handled_exceptions as exc:
        return preflight_error_response(
            exc,
            ontology=str(ontology),
            parameters={"actionTypeRid": str(action_type_rid)},
        )

    try:
        resource = await find_resource_by_rid(
            db_name=db_name,
            branch=branch,
            resource_type="action_type",
            rid=normalized_rid,
            oms_client=oms_client,
        )
        if not resource:
            resource = await find_action_type_resource_by_rid(
                db_name=db_name,
                branch=branch,
                action_type_rid=normalized_rid,
                oms_client=oms_client,
            )
        if not resource:
            return not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionTypeRid": normalized_rid},
            )
        mapped = to_foundry_action_type(resource)
        if mapped is None:
            return not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionTypeRid": normalized_rid},
            )
        return mapped
    except httpx.HTTPStatusError as exc:
        return upstream_status_error_response(
            exc,
            ontology=db_name,
            parameters={"actionTypeRid": normalized_rid},
            not_found_response=not_found_error(
                "ActionTypeNotFound",
                ontology=db_name,
                parameters={"actionTypeRid": normalized_rid},
            ),
        )
    except httpx.HTTPError:
        return upstream_transport_error_response(
            ontology=db_name,
            parameters={"actionTypeRid": normalized_rid},
        )
    except handled_exceptions as exc:
        return internal_error_response(
            log_message="Failed to get action type by rid (v2)",
            exc=exc,
            ontology=db_name,
            parameters={"actionTypeRid": normalized_rid},
        )
