from __future__ import annotations

import pytest
from fastapi import HTTPException

from oms.dependencies import ensure_database_exists
from shared.security.database_access import DatabaseAccessInspection, DatabaseAccessState


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_uses_postgres_registry_when_terminus_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _configured(*, db_name: str) -> DatabaseAccessInspection:
        return DatabaseAccessInspection(
            state=DatabaseAccessState.CONFIGURED if db_name == "demo" else DatabaseAccessState.UNCONFIGURED
        )

    monkeypatch.setattr(
        "oms.dependencies.inspect_database_access",
        _configured,
    )
    resolved = await ensure_database_exists(db_name="demo")
    assert resolved == "demo"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_returns_name_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _configured(*, db_name: str) -> DatabaseAccessInspection:
        return DatabaseAccessInspection(
            state=DatabaseAccessState.CONFIGURED if db_name == "demo" else DatabaseAccessState.UNCONFIGURED
        )

    monkeypatch.setattr("oms.dependencies.inspect_database_access", _configured)
    resolved = await ensure_database_exists(db_name="demo")
    assert resolved == "demo"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_raises_404_when_missing_in_postgres_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _missing(*, db_name: str) -> DatabaseAccessInspection:  # noqa: ARG001
        return DatabaseAccessInspection(state=DatabaseAccessState.UNCONFIGURED)

    monkeypatch.setattr("oms.dependencies.inspect_database_access", _missing)
    with pytest.raises(HTTPException) as exc_info:
        await ensure_database_exists(db_name="demo")
    assert exc_info.value.status_code == 404


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_raises_503_when_registry_is_unavailable_by_default() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await ensure_database_exists(db_name="demo")
    assert exc_info.value.status_code == 503


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_exists_raises_503_when_registry_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _unavailable(*, db_name: str) -> DatabaseAccessInspection:  # noqa: ARG001
        return DatabaseAccessInspection(state=DatabaseAccessState.UNAVAILABLE)

    monkeypatch.setattr("oms.dependencies.inspect_database_access", _unavailable)
    with pytest.raises(HTTPException) as exc_info:
        await ensure_database_exists(db_name="demo")
    assert exc_info.value.status_code == 503
