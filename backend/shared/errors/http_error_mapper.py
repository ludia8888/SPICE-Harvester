"""Canonical HTTP status-code → ErrorCode mapper.

Consolidates the previously-triplicated ``_code_for_status()`` helpers found in
``bff.services.oms_error_policy``, ``bff.services.database_error_policy``, and
the inline ``code_by_status`` dict in ``bff.services.database_service``.

All three call-sites now delegate here so that new status codes (e.g. 429, 502-504)
are handled consistently everywhere.
"""

from __future__ import annotations

from shared.errors.error_types import ErrorCode


def code_for_http_status(status_code: int) -> ErrorCode:
    """Map an HTTP status code to the most appropriate ``ErrorCode``."""
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
    if status_code == 429:
        return ErrorCode.RATE_LIMITED
    if status_code in {502, 503, 504}:
        return ErrorCode.UPSTREAM_UNAVAILABLE
    if status_code >= 500:
        return ErrorCode.INTERNAL_ERROR
    return ErrorCode.UPSTREAM_ERROR
