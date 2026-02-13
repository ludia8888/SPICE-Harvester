"""Role enforcement helpers (BFF).

Centralizes common FastAPI-friendly wrappers around
`shared.security.database_access.enforce_database_role` to avoid duplicated
try/except logic across routers.
"""


from collections.abc import Callable
from typing import Any

from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.security.database_access import enforce_database_role


async def enforce_required_database_role(request: Request, *, db_name: str, roles: Any) -> None:  # noqa: ANN401
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=roles)
    except ValueError as exc:
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc


def require_database_role(roles: Any) -> Callable[[Request, str], Any]:  # noqa: ANN401
    async def _dependency(request: Request, db_name: str) -> None:
        await enforce_required_database_role(request, db_name=db_name, roles=roles)

    return _dependency

