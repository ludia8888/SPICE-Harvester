from __future__ import annotations

from typing import Any, Dict


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
