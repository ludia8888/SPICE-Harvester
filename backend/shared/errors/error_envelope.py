from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from shared.errors.enterprise_catalog import (
    EnterpriseClass,
    EnterpriseError,
    resolve_enterprise_error,
    resolve_objectify_error,
)
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.observability.tracing import get_tracing_service


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
    if prefer_status_code and status_code:
        http_status = status_code
    else:
        http_status = resolved_enterprise.http_status if resolved_enterprise else (status_code or 500)

    payload: Dict[str, Any] = {
        "status": "error",
        "message": message,
        "detail": detail or message,
        "code": code.value,
        "category": category.value,
        "http_status": http_status,
        "retryable": resolved_enterprise.retryable if resolved_enterprise else False,
        "enterprise": resolved_enterprise.to_dict() if resolved_enterprise else None,
        "origin": _normalize_origin(service_name=service_name, origin=origin),
        "trace_id": resolved_trace_id,
        "request_id": request_id,
    }
    if errors:
        payload["errors"] = errors
    if context:
        payload["context"] = context
    return payload
