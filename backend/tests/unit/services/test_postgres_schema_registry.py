from __future__ import annotations

import pytest

from shared.services.registries.postgres_schema_registry import MissingSchemaObjectsError, PostgresSchemaRegistry


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(
        self,
        *,
        fail: bool = False,
        lock_sequence: list[bool] | None = None,
        existing_schemas: set[str] | None = None,
        existing_relations: set[str] | None = None,
    ) -> None:
        self.fail = fail
        self.executed: list[str] = []
        self._lock_sequence = list(lock_sequence or [True])
        self._existing_schemas = set(existing_schemas or set())
        self._existing_relations = set(existing_relations or set())

    async def execute(self, sql: str, *_args) -> None:
        if self.fail:
            raise RuntimeError("boom")
        self.executed.append(sql)

    async def fetchval(self, sql: str, *_args):
        if self.fail:
            raise RuntimeError("boom")
        self.executed.append(sql)
        if sql.startswith("SELECT pg_try_advisory_lock"):
            if self._lock_sequence:
                return self._lock_sequence.pop(0)
            return False
        if "information_schema.schemata" in sql:
            return _args[0] in self._existing_schemas
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return _args[0] in self._existing_relations
        return None


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


@pytest.mark.asyncio
async def test_ensure_schema_uses_advisory_lock() -> None:
    registry = _Registry(dsn="postgres://unused", schema="spice_test")
    conn = _Conn()
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert conn.executed[0].startswith("SELECT pg_try_advisory_lock")
    assert conn.executed[1] == "CREATE SCHEMA IF NOT EXISTS spice_test"
    assert conn.executed[-1].startswith("SELECT pg_advisory_unlock")


class _FailingRegistry(PostgresSchemaRegistry):
    async def _ensure_tables(self, conn):  # type: ignore[override]
        raise RuntimeError("table init failed")


class _VerifyingRegistry(PostgresSchemaRegistry):
    async def _ensure_tables(self, conn):  # type: ignore[override]
        return None

    def _required_tables(self) -> tuple[str, ...]:
        return ("widgets",)


@pytest.mark.asyncio
async def test_ensure_schema_unlocks_on_table_error() -> None:
    registry = _FailingRegistry(dsn="postgres://unused", schema="spice_test")
    conn = _Conn()
    registry._pool = _Pool(conn)

    with pytest.raises(RuntimeError, match="table init failed"):
        await registry.ensure_schema()

    assert conn.executed[0].startswith("SELECT pg_try_advisory_lock")
    assert conn.executed[-1].startswith("SELECT pg_advisory_unlock")


@pytest.mark.asyncio
async def test_ensure_schema_retries_until_lock_available() -> None:
    registry = _Registry(dsn="postgres://unused", schema="spice_test")
    registry._schema_lock_poll_interval_seconds = 0
    conn = _Conn(lock_sequence=[False, False, True])
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    attempts = [sql for sql in conn.executed if sql.startswith("SELECT pg_try_advisory_lock")]
    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_ensure_schema_skips_bootstrap_when_required_objects_exist() -> None:
    registry = _VerifyingRegistry(
        dsn="postgres://unused",
        schema="spice_test",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_test"},
        existing_relations={"spice_test.widgets"},
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_test" not in conn.executed


@pytest.mark.asyncio
async def test_ensure_schema_raises_when_required_objects_missing_and_bootstrap_disabled() -> None:
    registry = _VerifyingRegistry(
        dsn="postgres://unused",
        schema="spice_test",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn()
    registry._pool = _Pool(conn)

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await registry.ensure_schema()


@pytest.mark.asyncio
async def test_ensure_schema_bootstraps_when_required_objects_missing_and_bootstrap_enabled() -> None:
    registry = _VerifyingRegistry(
        dsn="postgres://unused",
        schema="spice_test",
        allow_runtime_ddl_bootstrap=True,
    )
    conn = _Conn()
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_test" in conn.executed
