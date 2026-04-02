from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Optional

from fastapi import Request, status
from fastapi.responses import JSONResponse

from shared.security.database_access import (
    DOMAIN_MODEL_ROLES,
    DatabaseAccessRegistryUnavailableError,
    enforce_database_role,
)

_ACTOR_HEADER_KEYS = ("X-User-ID", "X-User", "X-Actor")
DomainRoleEnforceFn = Callable[..., Awaitable[None]]


def has_actor_headers(request: Request) -> bool:
    return any(str(request.headers.get(key) or "").strip() for key in _ACTOR_HEADER_KEYS)


async def require_domain_role_if_actor(
    request: Request,
    *,
    db_name: str,
    allow_if_registry_unavailable: bool = True,
    require_env_key: str = "OMS_REQUIRE_DB_ACCESS",
    enforce_fn: DomainRoleEnforceFn = enforce_database_role,
) -> None:
    if not has_actor_headers(request):
        return
    try:
        await enforce_fn(
            headers=request.headers,
            db_name=db_name,
            required_roles=DOMAIN_MODEL_ROLES,
            allow_if_registry_unavailable=allow_if_registry_unavailable,
            require_env_key=require_env_key,
        )
    except DatabaseAccessRegistryUnavailableError:
        if allow_if_registry_unavailable:
            return
        raise


async def require_domain_role_if_actor_or_response(
    request: Request,
    *,
    db_name: str,
    foundry_error: Callable[..., JSONResponse],
    allow_if_registry_unavailable: bool = True,
    require_env_key: str = "OMS_REQUIRE_DB_ACCESS",
    enforce_fn: DomainRoleEnforceFn = enforce_database_role,
) -> Optional[JSONResponse]:
    try:
        await require_domain_role_if_actor(
            request,
            db_name=db_name,
            allow_if_registry_unavailable=allow_if_registry_unavailable,
            require_env_key=require_env_key,
            enforce_fn=enforce_fn,
        )
    except Exception as exc:
        response = foundry_role_guard_error_response(foundry_error=foundry_error, exc=exc)
        if response is not None:
            return response
        raise
    return None


def foundry_role_guard_error_response(
    *,
    foundry_error: Callable[..., JSONResponse],
    exc: Exception,
) -> Optional[JSONResponse]:
    if isinstance(exc, DatabaseAccessRegistryUnavailableError):
        return foundry_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="UPSTREAM_UNAVAILABLE",
            error_name="UpstreamUnavailable",
            parameters={"message": "Database access registry unavailable"},
        )
    if isinstance(exc, ValueError):
        message = str(exc)
        if message.strip().lower() == "permission denied":
            return foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="PermissionDenied",
                parameters={"message": "Permission denied"},
            )
        return foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"message": message},
        )
    return None
