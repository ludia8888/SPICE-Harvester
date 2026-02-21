from __future__ import annotations

import os
from typing import Iterable, Mapping, Optional

from shared.config.settings import get_settings

_DB_SCOPE_HEADER_KEYS = (
    "X-DB-Name",
    "X-Project",
    "X-Project-Id",
)

BFF_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")
OMS_SERVICE_TOKEN_ENV_KEYS = (
    "OMS_CLIENT_TOKEN",
    "OMS_ADMIN_TOKEN",
    "OMS_WRITE_TOKEN",
    "ADMIN_API_KEY",
    "ADMIN_TOKEN",
)


def get_expected_token(env_keys: Iterable[str]) -> Optional[str]:
    settings = get_settings()
    auth = settings.auth
    clients = settings.clients

    values_by_key: dict[str, Optional[str]] = {
        "BFF_ADMIN_TOKEN": auth.bff_admin_token,
        "BFF_WRITE_TOKEN": auth.bff_write_token,
        "BFF_AGENT_TOKEN": auth.bff_agent_token,
        "OMS_ADMIN_TOKEN": auth.oms_admin_token,
        "OMS_WRITE_TOKEN": auth.oms_write_token,
        "ADMIN_API_KEY": auth.admin_api_key,
        "ADMIN_TOKEN": auth.admin_token,
        # Internal client tokens (used by workers/services for service-to-service calls)
        "OMS_CLIENT_TOKEN": clients.oms_client_token,
    }

    for key in env_keys:
        value = (values_by_key.get(key) or "").strip()
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
    auth = get_settings().auth
    allowed_by_key: dict[str, bool] = {
        "ALLOW_INSECURE_BFF_AUTH_DISABLE": bool(auth.allow_insecure_bff_auth_disable),
        "ALLOW_INSECURE_OMS_AUTH_DISABLE": bool(auth.allow_insecure_oms_auth_disable),
        "ALLOW_INSECURE_AUTH_DISABLE": bool(auth.allow_insecure_auth_disable),
    }
    for key in allow_disable_env_keys:
        if allowed_by_key.get(str(key), False):
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
    settings = get_settings()
    auth = settings.auth

    if require_env_key == "BFF_REQUIRE_AUTH":
        return auth.is_bff_auth_required(allow_pytest=allow_pytest, default_required=default_required)
    if require_env_key == "OMS_REQUIRE_AUTH":
        return auth.is_oms_auth_required(default_required=default_required)

    if allow_pytest and (settings.is_pytest or bool(os.environ.get(pytest_env_key))):
        return False
    if get_expected_token(token_env_keys):
        return True
    return default_required


def get_exempt_paths(env_key: str, *, defaults: Iterable[str]) -> set[str]:
    auth = get_settings().auth
    if env_key == "BFF_AUTH_EXEMPT_PATHS":
        raw = auth.bff_auth_exempt_paths
    elif env_key == "OMS_AUTH_EXEMPT_PATHS":
        raw = auth.oms_auth_exempt_paths
    else:
        raw = None

    value = str(raw or "").strip()
    if not value:
        return set(defaults)
    return {path.strip() for path in value.split(",") if path.strip()} or set(defaults)


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
    required = bool(get_settings().auth.bff_require_db_scope) if require_env_key == "BFF_REQUIRE_DB_SCOPE" else False
    scope = get_db_scope(headers)
    if not scope:
        if required:
            raise ValueError("Project scope header is required")
        return
    if scope != (db_name or "").strip():
        raise ValueError("Project scope mismatch")
