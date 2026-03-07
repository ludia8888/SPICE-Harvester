from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import asyncpg
import pytest

from bff.services import pipeline_udf_service
from shared.services.registries.pipeline_registry import (
    PipelineRegistry,
    PipelineUdfAlreadyExistsError,
    PipelineUdfVersionConflictError,
)


class _Tx:
    async def __aenter__(self) -> "_Tx":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _Conn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[str] = []
        self.execute_calls: list[str] = []

    def transaction(self) -> _Tx:
        return _Tx()

    async def fetchrow(self, query: str, *args):  # noqa: ANN002
        self.fetchrow_calls.append(query)
        if "SELECT latest_version" in query:
            return {"latest_version": 1}
        if "SELECT * FROM spice_pipelines.pipeline_udf_versions" in query:
            return {
                "version_id": "11111111-1111-1111-1111-111111111111",
                "udf_id": "22222222-2222-2222-2222-222222222222",
                "version": 2,
                "code": "def transform(row):\n    return row\n",
                "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        return None

    async def execute(self, query: str, *args):  # noqa: ANN002
        self.execute_calls.append(query)
        return "OK"

    async def __aenter__(self) -> "_Conn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self) -> _Conn:
        return self._conn


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_udf_maps_duplicate_name_to_409() -> None:
    class _Registry:
        async def create_udf(self, **kwargs):  # noqa: ANN003
            raise PipelineUdfAlreadyExistsError(db_name="demo", name="dup_udf")

    with pytest.raises(Exception) as exc_info:
        await pipeline_udf_service.create_udf(
            db_name="demo",
            name="dup_udf",
            code="def transform(row):\n    return row\n",
            description=None,
            pipeline_registry=_Registry(),
        )

    exc = exc_info.value
    assert getattr(exc, "status_code", None) == 409
    assert exc.detail["code"] == "CONFLICT"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_udf_version_maps_conflict_to_409() -> None:
    udf_id = "11111111-1111-1111-1111-111111111111"

    class _Registry:
        async def get_udf(self, *, udf_id: str):  # noqa: ANN001
            return SimpleNamespace(udf_id=udf_id, latest_version=1)

        async def create_udf_version(self, *, udf_id: str, code: str):  # noqa: ANN001
            raise PipelineUdfVersionConflictError(udf_id=udf_id, attempted_version=2)

    with pytest.raises(Exception) as exc_info:
        await pipeline_udf_service.create_udf_version(
            udf_id=udf_id,
            code="def transform(row):\n    return row\n",
            pipeline_registry=_Registry(),
        )

    exc = exc_info.value
    assert getattr(exc, "status_code", None) == 409
    assert exc.detail["code"] == "CONFLICT"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_registry_create_udf_raises_structured_duplicate_error() -> None:
    class _DuplicateConn(_Conn):
        async def execute(self, query: str, *args):  # noqa: ANN002
            raise asyncpg.UniqueViolationError("duplicate key value violates unique constraint")

    registry = PipelineRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(_DuplicateConn())

    with pytest.raises(PipelineUdfAlreadyExistsError):
        await registry.create_udf(
            db_name="demo",
            name="dup_udf",
            code="def transform(row):\n    return row\n",
            description=None,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_registry_create_udf_version_locks_udf_row_for_update() -> None:
    conn = _Conn()
    registry = PipelineRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(conn)

    version = await registry.create_udf_version(
        udf_id="22222222-2222-2222-2222-222222222222",
        code="def transform(row):\n    return row\n",
    )

    assert version.version == 2
    assert any("FOR UPDATE" in query for query in conn.fetchrow_calls)
