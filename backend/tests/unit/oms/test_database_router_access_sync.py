from __future__ import annotations

import pytest
from fastapi import HTTPException

from oms.routers import database as database_router
from shared.security.database_access import DatabaseAccessRegistryUnavailableError


@pytest.mark.asyncio
async def test_list_databases_returns_503_when_registry_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise_registry():
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(database_router, "list_database_names", _raise_registry)

    with pytest.raises(HTTPException) as exc_info:
        await database_router.list_databases()

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_create_database_returns_202_without_touching_access_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _append_command(**kwargs):  # noqa: ANN001
        _ = kwargs
        return None

    monkeypatch.setattr(database_router, "append_event_sourcing_command", _append_command)

    response = await database_router.create_database(
        {"name": "demo", "description": "desc"},
        event_store=object(),
        command_status_service=None,
    )

    assert response.status_code == 202
