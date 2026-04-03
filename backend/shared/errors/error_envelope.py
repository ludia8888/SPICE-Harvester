from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from shared.errors.enterprise_catalog import (
    EnterpriseClass,
    EnterpriseError,
    resolve_enterprise_error,
    resolve_objectify_error,
)
from shared.errors.error_types import ErrorCategory, ErrorCode, classify_error_family
from shared.observability.tracing import get_tracing_service
from shared.utils.canonical_json import sha256_canonical_json_prefixed


_DEFAULT_CATEGORY_CODE_BY_CLASS: Dict[EnterpriseClass, Tuple[ErrorCategory, ErrorCode]] = {
    EnterpriseClass.VALIDATION: (ErrorCategory.INPUT, ErrorCode.REQUEST_VALIDATION_FAILED),
    EnterpriseClass.SECURITY: (ErrorCategory.INPUT, ErrorCode.INPUT_SANITIZATION_FAILED),
    EnterpriseClass.AUTH: (ErrorCategory.AUTH, ErrorCode.AUTH_REQUIRED),
    EnterpriseClass.PERMISSION: (ErrorCategory.PERMISSION, ErrorCode.PERMISSION_DENIED),
    EnterpriseClass.NOT_FOUND: (ErrorCategory.RESOURCE, ErrorCode.RESOURCE_NOT_FOUND),
    EnterpriseClass.CONFLICT: (ErrorCategory.CONFLICT, ErrorCode.CONFLICT),
    EnterpriseClass.STATE: (ErrorCategory.CONFLICT, ErrorCode.CONFLICT),
    EnterpriseClass.LIMIT: (ErrorCategory.RATE_LIMIT, ErrorCode.RATE_LIMITED),
    EnterpriseClass.TIMEOUT: (ErrorCategory.UPSTREAM, ErrorCode.UPSTREAM_TIMEOUT),
    EnterpriseClass.UNAVAILABLE: (ErrorCategory.UPSTREAM, ErrorCode.UPSTREAM_UNAVAILABLE),
    EnterpriseClass.INTEGRATION: (ErrorCategory.UPSTREAM, ErrorCode.UPSTREAM_ERROR),
    EnterpriseClass.INTERNAL: (ErrorCategory.INTERNAL, ErrorCode.INTERNAL_ERROR),
}


def _normalize_origin(
    *,
    service_name: str,
    origin: Optional[Dict[str, Optional[str]]] = None,
) -> Dict[str, Optional[str]]:
    payload = dict(origin or {})
    payload.setdefault("service", service_name)
    payload.setdefault("method", None)
    payload.setdefault("path", None)
    payload.setdefault("endpoint", None)
    return payload


def _derive_category_code(enterprise: EnterpriseError) -> Tuple[ErrorCategory, ErrorCode]:
    return _DEFAULT_CATEGORY_CODE_BY_CLASS.get(
        enterprise.error_class, (ErrorCategory.INTERNAL, ErrorCode.INTERNAL_ERROR)
    )


def _runbook_uri(runbook_ref: str) -> str:
    token = str(runbook_ref or "").strip() or "unknown_error"
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in token)
    safe = safe[:200] or "unknown_error"
    return f"spice://runbook/{safe}"


def _build_diagnostics(
    *,
    service_name: str,
    code: ErrorCode,
    category: ErrorCategory,
    http_status: int,
    enterprise: EnterpriseError,
    enterprise_payload: Dict[str, object],
    origin: Dict[str, Optional[str]],
    request_id: Optional[str],
    correlation_id: Optional[str],
    trace_id: Optional[str],
    span_id: Optional[str],
) -> Dict[str, Any]:
    group_seed = {
        "schema": "error_group_fingerprint.v1",
        "service": service_name,
        "code": code.value,
        "category": category.value,
        "http_status": int(http_status),
        "enterprise_code": enterprise.code,
        "external_code": enterprise.external_code,
        "domain": enterprise.domain.value,
        "class": enterprise.error_class.value,
        "subsystem": enterprise.subsystem,
        "runbook_ref": enterprise.runbook_ref,
        "origin_method": origin.get("method"),
        "origin_endpoint": origin.get("endpoint"),
    }
    group_fingerprint = sha256_canonical_json_prefixed(group_seed)
    instance_seed = {
        "schema": "error_instance_fingerprint.v1",
        "group_fingerprint": group_fingerprint,
        "request_id": request_id,
        "correlation_id": correlation_id,
        "trace_id": trace_id,
        "span_id": span_id,
        "origin_path": origin.get("path"),
    }
    instance_fingerprint = sha256_canonical_json_prefixed(instance_seed)

    return {
        "schema": "error_diagnostics.v1",
        "group_fingerprint": group_fingerprint,
        "instance_fingerprint": instance_fingerprint,
        "runbook_ref": enterprise.runbook_ref,
        "runbook_uri": _runbook_uri(enterprise.runbook_ref),
        "lookup": {
            "enterprise_code": enterprise.code,
            "catalog_fingerprint": enterprise_payload.get("catalog_fingerprint"),
            "request_id": request_id,
            "correlation_id": correlation_id,
            "trace_id": trace_id,
            "span_id": span_id,
        },
    }


def build_error_envelope(
    *,
    service_name: str,
    message: str,
    detail: Optional[str] = None,
    code: Optional[ErrorCode] = None,
    category: Optional[ErrorCategory] = None,
    status_code: Optional[int] = None,
    errors: Optional[Any] = None,
    context: Optional[Dict[str, Any]] = None,
    external_code: Optional[str] = None,
    objectify_error: Optional[str] = None,
    enterprise: Optional[EnterpriseError] = None,
    origin: Optional[Dict[str, Optional[str]]] = None,
    request_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    prefer_status_code: bool = False,
) -> Dict[str, Any]:
    resolved_enterprise = enterprise
    if resolved_enterprise is None and objectify_error:
        resolved_enterprise = resolve_objectify_error(objectify_error)
    if resolved_enterprise is None:
        resolved_enterprise = resolve_enterprise_error(
            service_name=service_name,
            code=code,
            category=category,
            status_code=status_code or 500,
            external_code=external_code,
            prefer_status_code=prefer_status_code,
        )

    resolved_category, resolved_code = _derive_category_code(resolved_enterprise)
    if category is None:
        category = resolved_category
    if code is None:
        code = resolved_code

    tracing = get_tracing_service(service_name)
    resolved_trace_id = trace_id if trace_id is not None else tracing.get_trace_id()
    resolved_span_id = tracing.get_span_id()
    if prefer_status_code and status_code:
        http_status = status_code
    else:
        http_status = resolved_enterprise.http_status if resolved_enterprise else (status_code or 500)

    origin_payload = _normalize_origin(service_name=service_name, origin=origin)
    enterprise_payload = resolved_enterprise.to_dict() if resolved_enterprise else None
    diagnostics = _build_diagnostics(
        service_name=service_name,
        code=code,
        category=category,
        http_status=http_status,
        enterprise=resolved_enterprise,
        enterprise_payload=enterprise_payload or {},
        origin=origin_payload,
        request_id=request_id,
        correlation_id=correlation_id,
        trace_id=resolved_trace_id,
        span_id=resolved_span_id,
    )

    payload: Dict[str, Any] = {
        "status": "error",
        "message": message,
        "detail": detail or message,
        "code": code.value,
        "category": category.value,
        "classification": {
            "family": classify_error_family(
                category=category,
                retryable=resolved_enterprise.retryable if resolved_enterprise else False,
            ).value,
            "retryable": resolved_enterprise.retryable if resolved_enterprise else False,
        },
        "http_status": http_status,
        "retryable": resolved_enterprise.retryable if resolved_enterprise else False,
        "enterprise": enterprise_payload,
        "origin": origin_payload,
        "trace_id": resolved_trace_id,
        "span_id": resolved_span_id,
        "request_id": request_id,
        "correlation_id": correlation_id,
        "diagnostics": diagnostics,
    }
    if errors:
        payload["errors"] = errors
    if context:
        payload["context"] = context
    return payload
