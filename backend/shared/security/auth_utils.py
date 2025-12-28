from __future__ import annotations

import os
from typing import Iterable, Mapping, Optional

from shared.utils.env_utils import parse_bool

_DB_SCOPE_HEADER_KEYS = (
    "X-DB-Name",
    "X-Project",
    "X-Project-Id",
    "X-Tenant",
    "X-Tenant-Id",
)


def get_expected_token(env_keys: Iterable[str]) -> Optional[str]:
    for key in env_keys:
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def extract_presented_token(headers: Mapping[str, str]) -> Optional[str]:
    raw = (headers.get("X-Admin-Token") or "").strip()
    if raw:
        return raw
    auth = (headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    return None


def auth_disable_allowed(allow_disable_env_keys: Iterable[str]) -> bool:
    for key in allow_disable_env_keys:
        if parse_bool(os.getenv(key, "")) is True:
            return True
    return False


def auth_required(
    require_env_key: str,
    *,
    token_env_keys: Iterable[str],
    default_required: bool = True,
    allow_pytest: bool = False,
    pytest_env_key: str = "PYTEST_CURRENT_TEST",
) -> bool:
    parsed = parse_bool(os.getenv(require_env_key, ""))
    if parsed is not None:
        return parsed
    if allow_pytest and os.getenv(pytest_env_key):
        return False
    if get_expected_token(token_env_keys):
        return True
    return default_required


def get_exempt_paths(env_key: str, *, defaults: Iterable[str]) -> set[str]:
    raw = (os.getenv(env_key) or "").strip()
    if not raw:
        return set(defaults)
    return {path.strip() for path in raw.split(",") if path.strip()} or set(defaults)


def is_exempt_path(path: str, *, exempt_paths: Iterable[str]) -> bool:
    return path in set(exempt_paths)


def get_db_scope(headers: Mapping[str, str]) -> Optional[str]:
    for key in _DB_SCOPE_HEADER_KEYS:
        value = (headers.get(key) or "").strip()
        if value:
            return value
    return None


def enforce_db_scope(
    headers: Mapping[str, str],
    *,
    db_name: str,
    require_env_key: str = "BFF_REQUIRE_DB_SCOPE",
) -> None:
    required = parse_bool(os.getenv(require_env_key, "")) is True
    scope = get_db_scope(headers)
    if not scope:
        if required:
            raise ValueError("Project scope header is required")
        return
    if scope != (db_name or "").strip():
        raise ValueError("Project scope mismatch")
