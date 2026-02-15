from __future__ import annotations

import pytest
from fastapi import HTTPException

from oms.dependencies import ensure_database_exists


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_uses_postgres_registry_when_terminus_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _configured(*, db_name: str) -> bool:
        return db_name == "demo"

    monkeypatch.setattr(
        "oms.dependencies.has_database_access_config",
        _configured,
    )
    resolved = await ensure_database_exists(db_name="demo")
    assert resolved == "demo"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_returns_name_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _configured(*, db_name: str) -> bool:
        return db_name == "demo"

    monkeypatch.setattr("oms.dependencies.has_database_access_config", _configured)
    resolved = await ensure_database_exists(db_name="demo")
    assert resolved == "demo"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_raises_404_when_missing_in_postgres_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _missing(*, db_name: str) -> bool:  # noqa: ARG001
        return False

    monkeypatch.setattr("oms.dependencies.has_database_access_config", _missing)
    with pytest.raises(HTTPException) as exc_info:
        await ensure_database_exists(db_name="demo")
    assert exc_info.value.status_code == 404


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_raises_404_when_missing() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await ensure_database_exists(db_name="demo")
    assert exc_info.value.status_code == 404
