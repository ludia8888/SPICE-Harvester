from __future__ import annotations

import pytest

from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.executed: list[str] = []

    async def execute(self, sql: str) -> None:
        if self.fail:
            raise RuntimeError("boom")
        self.executed.append(sql)


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class _Registry(PostgresSchemaRegistry):
    async def _ensure_tables(self, conn):  # type: ignore[override]
        return None


@pytest.mark.asyncio
async def test_health_check_connects_when_pool_missing() -> None:
    registry = _Registry(dsn="postgres://unused")
    conn = _Conn()
    connected = {"called": False}

    async def _fake_connect() -> None:
        connected["called"] = True
        registry._pool = _Pool(conn)

    registry.connect = _fake_connect  # type: ignore[method-assign]

    assert await registry.health_check() is True
    assert connected["called"] is True
    assert conn.executed == ["SELECT 1"]


@pytest.mark.asyncio
async def test_health_check_returns_false_on_query_failure() -> None:
    registry = _Registry(dsn="postgres://unused")
    registry._pool = _Pool(_Conn(fail=True))

    assert await registry.health_check() is False
