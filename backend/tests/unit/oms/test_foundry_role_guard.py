from __future__ import annotations

import pytest
from fastapi.responses import JSONResponse
from starlette.requests import Request

from oms.routers.foundry_role_guard import require_domain_role_if_actor_or_response
from shared.security.database_access import DatabaseAccessRegistryUnavailableError


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-user-id", b"alice")],
        }
    )


def _foundry_error(status_code: int, **payload) -> JSONResponse:  # noqa: ANN003
    return JSONResponse(status_code=status_code, content=payload)


@pytest.mark.asyncio
async def test_foundry_role_guard_allows_registry_degrade_for_reads() -> None:
    async def _unavailable(**kwargs):  # noqa: ANN003
        _ = kwargs
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    response = await require_domain_role_if_actor_or_response(
        _request(),
        db_name="demo",
        foundry_error=_foundry_error,
        enforce_fn=_unavailable,
    )

    assert response is None


@pytest.mark.asyncio
async def test_foundry_role_guard_reraises_unhandled_exceptions() -> None:
    async def _boom(**kwargs):  # noqa: ANN003
        _ = kwargs
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await require_domain_role_if_actor_or_response(
            _request(),
            db_name="demo",
            foundry_error=_foundry_error,
            enforce_fn=_boom,
        )
