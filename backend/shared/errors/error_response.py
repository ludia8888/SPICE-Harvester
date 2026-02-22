from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional, Tuple

import httpx
from fastapi import HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.enterprise_catalog import is_external_code
from shared.errors.error_envelope import build_error_envelope
from shared.security.input_sanitizer import SecurityViolationError
from shared.observability.request_context import (
    get_correlation_id as get_context_correlation_id,
    get_request_id as get_context_request_id,
)
from shared.utils.app_logger import get_logger
import logging

logger = get_logger(__name__)

try:  # optional
    import asyncpg  # type: ignore

    HAS_ASYNCPG = True
except Exception:  # pragma: no cover - optional dependency
    logging.getLogger(__name__).warning("Exception fallback at shared/errors/error_response.py:28", exc_info=True)
    asyncpg = None
    HAS_ASYNCPG = False


_STATUS_TO_CATEGORY_CODE: Dict[int, Tuple[ErrorCategory, ErrorCode]] = {
    status.HTTP_400_BAD_REQUEST: (ErrorCategory.INPUT, ErrorCode.HTTP_ERROR),
    status.HTTP_401_UNAUTHORIZED: (ErrorCategory.AUTH, ErrorCode.AUTH_REQUIRED),
    status.HTTP_403_FORBIDDEN: (ErrorCategory.PERMISSION, ErrorCode.PERMISSION_DENIED),
    status.HTTP_404_NOT_FOUND: (ErrorCategory.RESOURCE, ErrorCode.RESOURCE_NOT_FOUND),
    status.HTTP_410_GONE: (ErrorCategory.RESOURCE, ErrorCode.RESOURCE_GONE),
    status.HTTP_409_CONFLICT: (ErrorCategory.CONFLICT, ErrorCode.CONFLICT),
    status.HTTP_413_REQUEST_ENTITY_TOO_LARGE: (ErrorCategory.INPUT, ErrorCode.PAYLOAD_TOO_LARGE),
    status.HTTP_422_UNPROCESSABLE_ENTITY: (ErrorCategory.INPUT, ErrorCode.REQUEST_VALIDATION_FAILED),
    status.HTTP_429_TOO_MANY_REQUESTS: (ErrorCategory.RATE_LIMIT, ErrorCode.RATE_LIMITED),
    status.HTTP_501_NOT_IMPLEMENTED: (ErrorCategory.INTERNAL, ErrorCode.FEATURE_NOT_IMPLEMENTED),
}


def _get_request_id(request: Request) -> Optional[str]:
    return (
        request.headers.get("x-request-id")
        or request.headers.get("X-Request-ID")
        or getattr(request.state, "request_id", None)
        or get_context_request_id()
    )


def _get_correlation_id(request: Request) -> Optional[str]:
    return (
        request.headers.get("x-correlation-id")
        or request.headers.get("X-Correlation-ID")
        or getattr(request.state, "correlation_id", None)
        or get_context_correlation_id()
    )


def _get_origin(request: Request, service_name: str) -> Dict[str, Optional[str]]:
    endpoint = request.scope.get("endpoint")
    endpoint_name = None
    if endpoint is not None:
        endpoint_name = f"{endpoint.__module__}.{getattr(endpoint, '__name__', 'unknown')}"
    return {
        "service": service_name,
        "method": request.method,
        "path": request.url.path,
        "endpoint": endpoint_name,
    }


def _normalize_message(detail: Any) -> str:
    if isinstance(detail, str):
        return detail
    if isinstance(detail, dict):
        if "message" in detail and isinstance(detail["message"], str):
            return detail["message"]
        if "detail" in detail and isinstance(detail["detail"], str):
            return detail["detail"]
        return "HTTP error"
    return "HTTP error"


def _extract_upstream_metadata(body: Any) -> Tuple[Optional[ErrorCode], Optional[ErrorCategory], Optional[str]]:
    if not isinstance(body, dict):
        return None, None, None
    code_raw = body.get("code")
    category_raw = body.get("category")
    message = body.get("message") if isinstance(body.get("message"), str) else None
    code = ErrorCode(code_raw) if isinstance(code_raw, str) and code_raw in ErrorCode._value2member_map_ else None
    category = (
        ErrorCategory(category_raw)
        if isinstance(category_raw, str) and category_raw in ErrorCategory._value2member_map_
        else None
    )
    return code, category, message


def _extract_external_code(detail: Any) -> Optional[str]:
    if not isinstance(detail, dict):
        return None
    candidates: list[Any] = [
        detail.get("error_code"),
        detail.get("external_code"),
        detail.get("code"),
        detail.get("error"),
    ]
    for raw in candidates:
        if not isinstance(raw, str):
            continue
        code_raw = raw.strip()
        if not code_raw:
            continue
        if code_raw in ErrorCode._value2member_map_:
            continue
        if is_external_code(code_raw):
            return code_raw
    return None


def _classify_upstream_url(url: Optional[str], status_code: Optional[int]) -> Tuple[Optional[ErrorCode], Optional[ErrorCategory]]:
    if not url:
        return None, None
    url_lower = url.lower()
    if "oms" in url_lower and status_code in {
        status.HTTP_502_BAD_GATEWAY,
        status.HTTP_503_SERVICE_UNAVAILABLE,
        status.HTTP_504_GATEWAY_TIMEOUT,
    }:
        return ErrorCode.OMS_UNAVAILABLE, ErrorCategory.UPSTREAM
    return None, None


def _classify_db_error(exc: Exception) -> Tuple[ErrorCode, ErrorCategory, int, str]:
    name = exc.__class__.__name__.lower()
    if "timeout" in name:
        return ErrorCode.DB_TIMEOUT, ErrorCategory.INTERNAL, status.HTTP_504_GATEWAY_TIMEOUT, "Database timeout"
    if "cannotconnect" in name or "connection" in name or "toomany" in name:
        return (
            ErrorCode.DB_UNAVAILABLE,
            ErrorCategory.INTERNAL,
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Database unavailable",
        )
    if any(key in name for key in ["unique", "foreignkey", "notnull", "check", "exclusion"]):
        return (
            ErrorCode.DB_CONSTRAINT_VIOLATION,
            ErrorCategory.CONFLICT,
            status.HTTP_409_CONFLICT,
            "Database constraint violation",
        )
    return ErrorCode.DB_ERROR, ErrorCategory.INTERNAL, status.HTTP_500_INTERNAL_SERVER_ERROR, "Database error"


def _build_payload(
    request: Request,
    *,
    service_name: str,
    code: ErrorCode,
    category: ErrorCategory,
    status_code: int,
    message: str,
    detail: Optional[str] = None,
    errors: Optional[Any] = None,
    context: Optional[Dict[str, Any]] = None,
    external_code: Optional[str] = None,
) -> Dict[str, Any]:
    return build_error_envelope(
        service_name=service_name,
        message=message,
        detail=detail,
        code=code,
        category=category,
        status_code=status_code,
        errors=errors,
        context=context,
        external_code=external_code,
        origin=_get_origin(request, service_name),
        request_id=_get_request_id(request),
        correlation_id=_get_correlation_id(request),
        prefer_status_code=True,
    )


def _sanitize_header_value(value: Any, *, max_len: int = 256) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = raw.replace("\r", " ").replace("\n", " ")
    cleaned = "".join(ch for ch in cleaned if 32 <= ord(ch) <= 126)
    if not cleaned:
        return None
    return cleaned[:max_len]


def _build_error_headers(payload: Dict[str, Any]) -> Dict[str, str]:
    enterprise = payload.get("enterprise") if isinstance(payload.get("enterprise"), dict) else {}
    diagnostics = payload.get("diagnostics") if isinstance(payload.get("diagnostics"), dict) else {}

    header_pairs = (
        ("X-Error-Code", payload.get("code")),
        ("X-Error-Category", payload.get("category")),
        ("X-Enterprise-Code", enterprise.get("code")),
        ("X-Runbook-Ref", enterprise.get("runbook_ref")),
        ("X-Error-Group", diagnostics.get("group_fingerprint")),
        ("X-Error-Instance", diagnostics.get("instance_fingerprint")),
        ("X-Request-ID", payload.get("request_id")),
        ("X-Trace-ID", payload.get("trace_id")),
    )

    headers: Dict[str, str] = {}
    for key, value in header_pairs:
        normalized = _sanitize_header_value(value)
        if normalized is not None:
            headers[key] = normalized
    return headers


def _get_error_metrics_collector(service_name: str):
    from shared.observability.metrics import get_metrics_collector

    return get_metrics_collector(service_name)


def _record_error_observability(*, service_name: str, payload: Dict[str, Any]) -> None:
    try:
        collector = _get_error_metrics_collector(service_name)
        record = getattr(collector, "record_error_envelope", None)
        if callable(record):
            record(payload, source="http")
    except Exception as exc:
        logger.warning("Failed to record error observability metrics: %s", exc, exc_info=True)


def _emit_error_log(payload: Dict[str, Any]) -> None:
    enterprise = payload.get("enterprise") if isinstance(payload.get("enterprise"), Mapping) else {}
    diagnostics = payload.get("diagnostics") if isinstance(payload.get("diagnostics"), Mapping) else {}
    origin = payload.get("origin") if isinstance(payload.get("origin"), Mapping) else {}
    event = {
        "error_code": payload.get("code"),
        "error_category": payload.get("category"),
        "http_status": payload.get("http_status"),
        "enterprise_code": enterprise.get("code"),
        "enterprise_class": enterprise.get("class"),
        "enterprise_domain": enterprise.get("domain"),
        "external_code": enterprise.get("external_code"),
        "runbook_ref": enterprise.get("runbook_ref"),
        "request_id": payload.get("request_id"),
        "correlation_id": payload.get("correlation_id"),
        "trace_id": payload.get("trace_id"),
        "error_group": diagnostics.get("group_fingerprint"),
        "error_instance": diagnostics.get("instance_fingerprint"),
        "origin_service": origin.get("service"),
        "origin_method": origin.get("method"),
        "origin_path": origin.get("path"),
        "origin_endpoint": origin.get("endpoint"),
    }

    status_raw = payload.get("http_status")
    try:
        status_code = int(status_raw)
    except (TypeError, ValueError):
        status_code = 500

    if status_code >= 500:
        logger.error("API error response emitted", extra={"error_event": event})
    elif status_code >= 400:
        logger.warning("API error response emitted", extra={"error_event": event})
    else:
        logger.info("API error response emitted", extra={"error_event": event})


def _build_response(
    request: Request,
    *,
    service_name: str,
    code: ErrorCode,
    category: ErrorCategory,
    status_code: int,
    message: str,
    detail: Optional[str] = None,
    errors: Optional[Any] = None,
    context: Optional[Dict[str, Any]] = None,
    external_code: Optional[str] = None,
) -> JSONResponse:
    payload = _build_payload(
        request,
        service_name=service_name,
        code=code,
        category=category,
        status_code=status_code,
        message=message,
        detail=detail,
        errors=errors,
        context=context,
        external_code=external_code,
    )
    _record_error_observability(service_name=service_name, payload=payload)
    _emit_error_log(payload)
    headers = _build_error_headers(payload)
    return JSONResponse(
        status_code=payload.get("http_status", status_code),
        content=payload,
        headers=headers if headers else None,
    )


def _resolve_validation_error(exc: RequestValidationError) -> Tuple[ErrorCode, str]:
    json_errors = [error for error in exc.errors() if error.get("type") == "json_invalid"]
    if json_errors:
        return ErrorCode.JSON_DECODE_ERROR, "Invalid JSON format"
    return ErrorCode.REQUEST_VALIDATION_FAILED, "Request validation failed"


def install_error_handlers(
    app,
    *,
    service_name: str,
    validation_status: int = status.HTTP_422_UNPROCESSABLE_ENTITY,
) -> None:
    @app.exception_handler(SecurityViolationError)
    async def security_violation_handler(request: Request, exc: SecurityViolationError):
        logger.warning("Security violation: %s", exc)
        message = str(exc) or "Input rejected by security policy"
        return _build_response(
            request,
            service_name=service_name,
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=status.HTTP_400_BAD_REQUEST,
            message=message,
            detail=message,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(request: Request, exc: RequestValidationError):
        code, message = _resolve_validation_error(exc)
        request_path = str(getattr(getattr(request, "url", None), "path", "") or "")
        if request_path.startswith("/api/v2/"):
            from shared.foundry.errors import foundry_error

            raw_errors = exc.errors()
            safe_errors = []
            for err in raw_errors:
                safe_err = dict(err)
                ctx = safe_err.get("ctx")
                if isinstance(ctx, dict):
                    safe_err["ctx"] = {k: str(v) if isinstance(v, Exception) else v for k, v in ctx.items()}
                safe_errors.append(safe_err)
            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": message, "errors": safe_errors},
            )
        status_code = (
            status.HTTP_400_BAD_REQUEST
            if code == ErrorCode.JSON_DECODE_ERROR
            else validation_status
        )
        # Sanitize errors to ensure JSON-serialisable (Pydantic ctx may contain raw Exceptions)
        raw_errors = exc.errors()
        safe_errors = []
        for err in raw_errors:
            safe_err = dict(err)
            ctx = safe_err.get("ctx")
            if isinstance(ctx, dict):
                safe_err["ctx"] = {k: str(v) if isinstance(v, Exception) else v for k, v in ctx.items()}
            safe_errors.append(safe_err)
        return _build_response(
            request,
            service_name=service_name,
            code=code,
            category=ErrorCategory.INPUT,
            status_code=status_code,
            message=message,
            detail=str(exc),
            errors=safe_errors,
        )

    @app.exception_handler(json.JSONDecodeError)
    async def json_decode_handler(request: Request, exc: json.JSONDecodeError):
        request_path = str(getattr(getattr(request, "url", None), "path", "") or "")
        if request_path.startswith("/api/v2/"):
            from shared.foundry.errors import foundry_error

            return foundry_error(
                400,
                error_code="INVALID_ARGUMENT",
                error_name="InvalidArgument",
                parameters={"message": "Invalid JSON format"},
            )
        return _build_response(
            request,
            service_name=service_name,
            code=ErrorCode.JSON_DECODE_ERROR,
            category=ErrorCategory.INPUT,
            status_code=status.HTTP_400_BAD_REQUEST,
            message="Invalid JSON format",
            detail=str(exc),
        )

    @app.exception_handler(httpx.RequestError)
    async def httpx_request_error_handler(request: Request, exc: httpx.RequestError):
        url = str(getattr(exc.request, "url", "")) if exc.request else ""
        code, category = _classify_upstream_url(url, None)
        code = code or ErrorCode.UPSTREAM_UNAVAILABLE
        category = category or ErrorCategory.UPSTREAM
        context = {
            "upstream_url": str(getattr(exc.request, "url", "")),
            "upstream_method": getattr(exc.request, "method", ""),
        }
        return _build_response(
            request,
            service_name=service_name,
            code=code,
            category=category,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            message="Upstream request failed",
            detail=str(exc),
            context=context,
        )

    @app.exception_handler(httpx.HTTPStatusError)
    async def httpx_status_error_handler(request: Request, exc: httpx.HTTPStatusError):
        response = exc.response
        upstream_status = response.status_code if response is not None else None
        upstream_body = None
        if response is not None:
            try:
                upstream_body = response.json()
            except ValueError:
                upstream_body = response.text
        upstream_code, upstream_category, upstream_message = _extract_upstream_metadata(upstream_body)
        external_code = _extract_external_code(upstream_body)
        context = {
            "upstream_status": upstream_status,
            "upstream_url": str(getattr(exc.request, "url", "")),
            "upstream_body": upstream_body,
        }
        status_code = upstream_status or status.HTTP_502_BAD_GATEWAY
        code, category = _classify_upstream_url(context["upstream_url"], status_code)
        code = upstream_code or code or ErrorCode.UPSTREAM_ERROR
        category = upstream_category or category or ErrorCategory.UPSTREAM
        message = upstream_message or "Upstream service returned an error"
        return _build_response(
            request,
            service_name=service_name,
            code=code,
            category=category,
            status_code=status_code,
            message=message,
            detail=str(exc),
            context=context,
            external_code=external_code,
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        code = None
        category = None
        external_code = None
        if isinstance(exc.detail, dict):
            code, category, _ = _extract_upstream_metadata(exc.detail)
            external_code = _extract_external_code(exc.detail)
        if code is None or category is None:
            fallback = _STATUS_TO_CATEGORY_CODE.get(exc.status_code)
            if fallback is None:
                if exc.status_code >= 500:
                    fallback = (ErrorCategory.INTERNAL, ErrorCode.INTERNAL_ERROR)
                elif exc.status_code >= 400:
                    fallback = (ErrorCategory.INPUT, ErrorCode.HTTP_ERROR)
                else:
                    fallback = (ErrorCategory.INTERNAL, ErrorCode.HTTP_ERROR)
            fallback_category, fallback_code = fallback
            category = category or fallback_category
            code = code or fallback_code
        message = _normalize_message(exc.detail)
        detail: Any = message
        if isinstance(exc.detail, dict):
            detail = exc.detail
        return _build_response(
            request,
            service_name=service_name,
            code=code,
            category=category,
            status_code=exc.status_code,
            message=message,
            detail=detail,
            context={"detail": exc.detail} if isinstance(exc.detail, dict) else None,
            external_code=external_code,
        )

    if HAS_ASYNCPG:
        @app.exception_handler(asyncpg.PostgresError)  # type: ignore[misc]
        async def postgres_error_handler(request: Request, exc: Exception):
            code, category, status_code, message = _classify_db_error(exc)
            return _build_response(
                request,
                service_name=service_name,
                code=code,
                category=category,
                status_code=status_code,
                message=message,
                detail=str(exc),
            )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception")
        detail = None
        settings = get_settings()
        if settings.is_development or settings.is_test:
            detail = str(exc)
        return _build_response(
            request,
            service_name=service_name,
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            message="Internal server error",
            detail=detail or "An unexpected error occurred",
        )
