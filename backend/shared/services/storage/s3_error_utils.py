from __future__ import annotations

from typing import Any


def client_error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return ""
    error = response.get("Error")
    if not isinstance(error, dict):
        return ""
    return str(error.get("Code") or "")


def is_missing_bucket_error(exc: BaseException) -> bool:
    return client_error_code(exc) in {"404", "NoSuchBucket", "NotFound"}


def is_missing_object_error(exc: BaseException) -> bool:
    return client_error_code(exc) in {"404", "NoSuchKey", "NotFound"}
