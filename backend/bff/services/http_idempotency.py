"""HTTP idempotency header helpers (BFF).

Centralizes Idempotency-Key parsing/validation for BFF routers/services.
"""

from __future__ import annotations

from typing import Optional

from fastapi import Request
from shared.errors.error_types import ErrorCode, classified_http_exception


def require_idempotency_key(request: Optional[Request]) -> str:
    if request is None:
        raise classified_http_exception(400, "Idempotency-Key header is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    key = (request.headers.get("Idempotency-Key") or request.headers.get("X-Idempotency-Key") or "").strip()
    if not key:
        raise classified_http_exception(400, "Idempotency-Key header is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    return key


def get_idempotency_key(request: Optional[Request]) -> Optional[str]:
    if request is None:
        return None
    key = (request.headers.get("Idempotency-Key") or request.headers.get("X-Idempotency-Key") or "").strip()
    return key or None

