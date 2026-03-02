from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable

from fastapi import HTTPException
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.errors.http_error_mapper import code_for_http_status as _code_for_status

from shared.security.input_sanitizer import SecurityViolationError

_NO_FALLBACK_RETURN = object()


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
