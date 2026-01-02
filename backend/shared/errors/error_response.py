from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from shared.config.settings import settings
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.enterprise_catalog import is_external_code
from shared.errors.error_envelope import build_error_envelope
from shared.security.input_sanitizer import SecurityViolationError
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)

try:  # optional
    import asyncpg  # type: ignore

    HAS_ASYNCPG = True
except Exception:  # pragma: no cover - optional dependency
    asyncpg = None
    HAS_ASYNCPG = False


_STATUS_TO_CATEGORY_CODE: Dict[int, Tuple[ErrorCategory, ErrorCode]] = {
    status.HTTP_400_BAD_REQUEST: (ErrorCategory.INPUT, ErrorCode.HTTP_ERROR),
    status.HTTP_401_UNAUTHORIZED: (ErrorCategory.AUTH, ErrorCode.AUTH_REQUIRED),
    status.HTTP_403_FORBIDDEN: (ErrorCategory.PERMISSION, ErrorCode.PERMISSION_DENIED),
    status.HTTP_404_NOT_FOUND: (ErrorCategory.RESOURCE, ErrorCode.RESOURCE_NOT_FOUND),
    status.HTTP_409_CONFLICT: (ErrorCategory.CONFLICT, ErrorCode.CONFLICT),
    status.HTTP_413_REQUEST_ENTITY_TOO_LARGE: (ErrorCategory.INPUT, ErrorCode.PAYLOAD_TOO_LARGE),
    status.HTTP_422_UNPROCESSABLE_ENTITY: (ErrorCategory.INPUT, ErrorCode.REQUEST_VALIDATION_FAILED),
    status.HTTP_429_TOO_MANY_REQUESTS: (ErrorCategory.RATE_LIMIT, ErrorCode.RATE_LIMITED),
}


def _get_request_id(request: Request) -> Optional[str]:
    return request.headers.get("x-request-id") or request.headers.get("X-Request-ID")


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
    code_raw = detail.get("code") or detail.get("error_code")
    if not isinstance(code_raw, str):
        code_raw = detail.get("error")
    if not isinstance(code_raw, str):
        return None
    if code_raw in ErrorCode._value2member_map_:
        return None
    if not is_external_code(code_raw):
        return None
    return code_raw


def _classify_upstream_url(url: Optional[str], status_code: Optional[int]) -> Tuple[Optional[ErrorCode], Optional[ErrorCategory]]:
    if not url:
        return None, None
    url_lower = url.lower()
    if "terminus" in url_lower:
        if status_code == status.HTTP_409_CONFLICT:
            return ErrorCode.TERMINUS_CONFLICT, ErrorCategory.CONFLICT
        if status_code in {
            status.HTTP_502_BAD_GATEWAY,
            status.HTTP_503_SERVICE_UNAVAILABLE,
            status.HTTP_504_GATEWAY_TIMEOUT,
        }:
            return ErrorCode.TERMINUS_UNAVAILABLE, ErrorCategory.UPSTREAM
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
        prefer_status_code=True,
    )


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
    return JSONResponse(status_code=payload.get("http_status", status_code), content=payload)


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
        status_code = (
            status.HTTP_400_BAD_REQUEST
            if code == ErrorCode.JSON_DECODE_ERROR
            else validation_status
        )
        return _build_response(
            request,
            service_name=service_name,
            code=code,
            category=ErrorCategory.INPUT,
            status_code=status_code,
            message=message,
            detail=str(exc),
            errors=exc.errors(),
        )

    @app.exception_handler(json.JSONDecodeError)
    async def json_decode_handler(request: Request, exc: json.JSONDecodeError):
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
        detail = message
        if isinstance(exc.detail, dict):
            detail = json.dumps(exc.detail, ensure_ascii=False)
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
