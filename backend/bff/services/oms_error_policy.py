from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Union

import httpx
from fastapi import HTTPException, status
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.security.input_sanitizer import SecurityViolationError

HttpStatusDetail = Union[str, Callable[[httpx.HTTPStatusError], Any]]


def _code_for_status(status_code: int) -> ErrorCode:
    if status_code in {400, 422}:
        return ErrorCode.REQUEST_VALIDATION_FAILED
    if status_code == 401:
        return ErrorCode.AUTH_REQUIRED
    if status_code == 403:
        return ErrorCode.PERMISSION_DENIED
    if status_code == 404:
        return ErrorCode.RESOURCE_NOT_FOUND
    if status_code == 409:
        return ErrorCode.CONFLICT
    if status_code in {502, 503, 504}:
        return ErrorCode.UPSTREAM_UNAVAILABLE
    if status_code >= 500:
        return ErrorCode.INTERNAL_ERROR
    return ErrorCode.UPSTREAM_ERROR


def raise_oms_boundary_exception(
    *,
    exc: Exception,
    action: str,
    logger: logging.Logger,
    custom_http_status_details: Optional[Dict[int, HttpStatusDetail]] = None,
) -> None:
    if isinstance(exc, httpx.HTTPStatusError):
        response = getattr(exc, "response", None)
        status_code = int(response.status_code) if response is not None else status.HTTP_502_BAD_GATEWAY
        resolver = (custom_http_status_details or {}).get(status_code)
        if resolver is not None:
            detail = resolver(exc) if callable(resolver) else resolver
            raise classified_http_exception(status_code, str(detail), code=_code_for_status(status_code)) from exc
        raise_httpx_as_http_exception(exc)

    if isinstance(exc, (SecurityViolationError, ValueError)):
        raise classified_http_exception(400, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

    if isinstance(exc, HTTPException):
        raise exc

    logger.error("Failed to %s: %s", action, exc)
    raise classified_http_exception(500, f"{action} 실패: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc
