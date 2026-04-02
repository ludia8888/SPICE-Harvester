from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

from fastapi import status

from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.security.database_access import (
    DatabaseAccessRegistryUnavailableError,
    enforce_database_role,
)

DatabaseRoleEnforceFn = Callable[..., Awaitable[None]]
DeniedErrorFactory = Callable[[str], Exception]


async def enforce_database_role_or_http_error(
    *,
    headers: Mapping[str, str],
    db_name: str,
    required_roles: Iterable[Any],
    allow_if_registry_unavailable: bool = False,
    require_env_key: str = "BFF_REQUIRE_DB_ACCESS",
    enforce_fn: DatabaseRoleEnforceFn = enforce_database_role,
) -> None:
    try:
        await enforce_fn(
            headers=headers,
            db_name=db_name,
            required_roles=required_roles,
            allow_if_registry_unavailable=allow_if_registry_unavailable,
            require_env_key=require_env_key,
        )
    except DatabaseAccessRegistryUnavailableError as exc:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Database access registry unavailable",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        ) from exc
    except ValueError as exc:
        raise classified_http_exception(
            status.HTTP_403_FORBIDDEN,
            str(exc),
            code=ErrorCode.PERMISSION_DENIED,
        ) from exc


async def enforce_database_role_or_permission_error(
    *,
    headers: Mapping[str, str],
    db_name: str,
    required_roles: Iterable[Any],
    denied_error_factory: DeniedErrorFactory,
    allow_if_registry_unavailable: bool = False,
    require_env_key: str = "BFF_REQUIRE_DB_ACCESS",
    enforce_fn: DatabaseRoleEnforceFn = enforce_database_role,
) -> None:
    try:
        await enforce_fn(
            headers=headers,
            db_name=db_name,
            required_roles=required_roles,
            allow_if_registry_unavailable=allow_if_registry_unavailable,
            require_env_key=require_env_key,
        )
    except ValueError as exc:
        if str(exc).strip().lower() == "permission denied":
            raise denied_error_factory("Permission denied") from exc
        raise
