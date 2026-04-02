"""Role enforcement helpers (BFF).

Centralizes common FastAPI-friendly wrappers around
`shared.security.database_access.enforce_database_role` to avoid duplicated
try/except logic across routers.
"""


from collections.abc import Callable
from typing import Any

from fastapi import Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.services.database_role_guard import enforce_database_role_or_http_error
from shared.security.database_access import enforce_database_role


async def enforce_required_database_role(request: Request, *, db_name: str, roles: Any) -> None:  # noqa: ANN401
    await enforce_database_role_or_http_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=roles,
        enforce_fn=enforce_database_role,
    )


def require_database_role(roles: Any) -> Callable[[Request, str], Any]:  # noqa: ANN401
    async def _dependency(request: Request, db_name: str) -> None:
        await enforce_required_database_role(request, db_name=db_name, roles=roles)

    return _dependency
