"""HTTP idempotency header helpers (BFF).

Centralizes Idempotency-Key parsing/validation for BFF routers/services.
"""

from __future__ import annotations

from typing import Optional

from fastapi import HTTPException, Request, status


def require_idempotency_key(request: Optional[Request]) -> str:
    if request is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Idempotency-Key header is required")
    key = (request.headers.get("Idempotency-Key") or request.headers.get("X-Idempotency-Key") or "").strip()
    if not key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Idempotency-Key header is required")
    return key


def get_idempotency_key(request: Optional[Request]) -> Optional[str]:
    if request is None:
        return None
    key = (request.headers.get("Idempotency-Key") or request.headers.get("X-Idempotency-Key") or "").strip()
    return key or None

