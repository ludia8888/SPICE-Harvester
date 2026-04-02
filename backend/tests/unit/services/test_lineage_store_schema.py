from __future__ import annotations

import pytest

from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.postgres_schema_registry import MissingSchemaObjectsError


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
        existing_schemas: set[str] | None = None,
        existing_relations: set[str] | None = None,
    ) -> None:
        self._existing_schemas = set(existing_schemas or set())
        self._existing_relations = set(existing_relations or set())
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args):
        self.executed.append(sql)
        if sql.startswith("SELECT pg_try_advisory_lock"):
            return True
        if "information_schema.schemata" in sql:
            return args[0] in self._existing_schemas
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return args[0] in self._existing_relations
        return None

    async def execute(self, sql: str, *_args):
        self.executed.append(sql)
        if "CREATE SCHEMA IF NOT EXISTS spice_lineage" in sql:
            self._existing_schemas.add("spice_lineage")
        if "CREATE TABLE IF NOT EXISTS spice_lineage.lineage_nodes" in sql:
            self._existing_relations.add("spice_lineage.lineage_nodes")
        if "CREATE TABLE IF NOT EXISTS spice_lineage.lineage_edges" in sql:
            self._existing_relations.add("spice_lineage.lineage_edges")
        if "CREATE TABLE IF NOT EXISTS spice_lineage.lineage_backfill_queue" in sql:
            self._existing_relations.add("spice_lineage.lineage_backfill_queue")
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


def test_lineage_store_required_tables_declared() -> None:
    store = LineageStore(dsn="postgres://unused")

    assert store._required_tables() == (  # noqa: SLF001
        "lineage_nodes",
        "lineage_edges",
        "lineage_backfill_queue",
    )


@pytest.mark.asyncio
async def test_lineage_store_skips_bootstrap_when_objects_exist() -> None:
    store = LineageStore(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_lineage"},
        existing_relations={
            "spice_lineage.lineage_nodes",
            "spice_lineage.lineage_edges",
            "spice_lineage.lineage_backfill_queue",
        },
    )
    store._pool = _Pool(conn)

    await store.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_lineage" not in conn.executed


@pytest.mark.asyncio
async def test_lineage_store_raises_when_objects_missing_and_bootstrap_disabled() -> None:
    store = LineageStore(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    store._pool = _Pool(_Conn())

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await store.ensure_schema()
