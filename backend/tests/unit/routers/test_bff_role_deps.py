from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.routers import role_deps
from shared.security.database_access import DatabaseAccessRegistryUnavailableError


def _request() -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"x-user-id", b"alice")],
    }
    return Request(scope)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_required_database_role_raises_503_when_registry_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _unavailable(**kwargs):  # noqa: ANN001, ANN003
        _ = kwargs
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(role_deps, "enforce_database_role", _unavailable)

    with pytest.raises(HTTPException) as exc_info:
        await role_deps.enforce_required_database_role(_request(), db_name="demo", roles=("Owner",))

    assert exc_info.value.status_code == 503
