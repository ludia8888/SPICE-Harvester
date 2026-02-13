from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable

from fastapi import HTTPException
from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.security.input_sanitizer import SecurityViolationError

_NO_FALLBACK_RETURN = object()


def _code_for_status(status_code: int) -> ErrorCode:
    if status_code == 400 or status_code == 422:
        return ErrorCode.REQUEST_VALIDATION_FAILED
    if status_code == 401:
        return ErrorCode.AUTH_REQUIRED
    if status_code == 403:
        return ErrorCode.PERMISSION_DENIED
    if status_code == 404:
        return ErrorCode.RESOURCE_NOT_FOUND
    if status_code == 409:
        return ErrorCode.CONFLICT
    if status_code == 429:
        return ErrorCode.RATE_LIMITED
    if status_code >= 500:
        return ErrorCode.INTERNAL_ERROR
    return ErrorCode.INTERNAL_ERROR


@dataclass(frozen=True)
class MessageErrorPolicy:
    patterns: tuple[str, ...]
    status_code: int
    detail: str
    fallback_return: Any = _NO_FALLBACK_RETURN

    def matches(self, normalized_message: str) -> bool:
        return any(pattern in normalized_message for pattern in self.patterns)


def apply_message_error_policies(
    *,
    exc: Exception,
    logger: logging.Logger,
    log_message: str,
    policies: Iterable[MessageErrorPolicy],
    default_status_code: int,
    default_detail: str,
) -> Any:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, (SecurityViolationError, ValueError)):
        raise classified_http_exception(400, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

    logger.error(log_message, exc)
    normalized_message = str(exc).lower()
    for policy in policies:
        if not policy.matches(normalized_message):
            continue
        if policy.fallback_return is not _NO_FALLBACK_RETURN:
            return policy.fallback_return
        raise classified_http_exception(
            policy.status_code,
            policy.detail,
            code=_code_for_status(policy.status_code),
        ) from exc

    raise classified_http_exception(
        default_status_code,
        default_detail,
        code=_code_for_status(default_status_code),
    ) from exc
