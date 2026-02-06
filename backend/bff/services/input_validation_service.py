"""Shared input validation helpers for BFF service layer.

Converts sanitizer/validator exceptions into HTTP 400 consistently so service
modules can reuse one policy.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request, status

from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)
from shared.security.auth_utils import enforce_db_scope


def _to_bad_request(exc: Exception) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def validated_db_name(db_name: str) -> str:
    try:
        return validate_db_name(db_name)
    except (SecurityViolationError, ValueError) as exc:
        raise _to_bad_request(exc) from exc


def validated_branch_name(branch: str) -> str:
    try:
        return validate_branch_name(branch)
    except (SecurityViolationError, ValueError) as exc:
        raise _to_bad_request(exc) from exc


def sanitized_payload(payload: Any) -> Any:
    try:
        return sanitize_input(payload)
    except SecurityViolationError as exc:
        raise _to_bad_request(exc) from exc


def enforce_db_scope_or_403(request: Request, *, db_name: str) -> None:
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
