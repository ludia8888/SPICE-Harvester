from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable

from fastapi import HTTPException

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
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    logger.error(log_message, exc)
    normalized_message = str(exc).lower()
    for policy in policies:
        if not policy.matches(normalized_message):
            continue
        if policy.fallback_return is not _NO_FALLBACK_RETURN:
            return policy.fallback_return
        raise HTTPException(status_code=policy.status_code, detail=policy.detail) from exc

    raise HTTPException(status_code=default_status_code, detail=default_detail) from exc
