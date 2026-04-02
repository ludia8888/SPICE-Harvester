from __future__ import annotations

import pytest

from shared.services.events.aggregate_sequence_allocator import AggregateSequenceAllocator
from shared.services.registries.runtime_ddl import RuntimeDDLDisabledError


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self, *, existing_schemas=None, existing_relations=None) -> None:
        self._existing_schemas = set(existing_schemas or set())
        self._existing_relations = set(existing_relations or set())
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args):
        self.executed.append(sql)
        if "information_schema.schemata" in sql:
            return args[0] in self._existing_schemas
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return args[0] in self._existing_relations
        return None

    async def execute(self, sql: str, *_args):
        self.executed.append(sql)
        if "CREATE SCHEMA IF NOT EXISTS" in sql:
            self._existing_schemas.add(sql.rsplit(" ", 1)[-1])
        if "CREATE TABLE IF NOT EXISTS " in sql:
            relation = sql.split("CREATE TABLE IF NOT EXISTS ", 1)[1].split(" ", 1)[0].split("(", 1)[0]
            self._existing_relations.add(relation.strip())
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sequence_allocator_skips_runtime_ddl_when_schema_exists() -> None:
    allocator = AggregateSequenceAllocator(dsn="postgres://unused", schema="spice_event_registry")
    allocator._allow_runtime_ddl_bootstrap = False
    conn = _Conn(
        existing_schemas={"spice_event_registry"},
        existing_relations={"spice_event_registry.aggregate_versions"},
    )
    allocator._pool = _Pool(conn)

    await allocator.ensure_schema()

    assert not any("CREATE TABLE IF NOT EXISTS" in sql for sql in conn.executed)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sequence_allocator_raises_when_schema_missing_and_bootstrap_disabled() -> None:
    allocator = AggregateSequenceAllocator(dsn="postgres://unused", schema="spice_event_registry")
    allocator._allow_runtime_ddl_bootstrap = False
    allocator._pool = _Pool(_Conn())

    with pytest.raises(RuntimeDDLDisabledError, match="missing required schema objects"):
        await allocator.ensure_schema()
