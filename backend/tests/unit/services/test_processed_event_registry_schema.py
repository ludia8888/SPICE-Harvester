from __future__ import annotations

import pytest

from shared.services.registries.processed_event_registry import (
    MissingProcessedEventRegistrySchemaError,
    ProcessedEventRegistry,
)


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
        if "information_schema.schemata" in sql:
            return args[0] in self._existing_schemas
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return args[0] in self._existing_relations
        return None

    async def execute(self, sql: str, *_args):
        self.executed.append(sql)
        if sql == "CREATE SCHEMA IF NOT EXISTS spice_event_registry":
            self._existing_schemas.add("spice_event_registry")
        if "CREATE TABLE IF NOT EXISTS spice_event_registry.processed_events" in sql:
            self._existing_relations.add("spice_event_registry.processed_events")
        if "CREATE TABLE IF NOT EXISTS spice_event_registry.aggregate_versions" in sql:
            self._existing_relations.add("spice_event_registry.aggregate_versions")
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.asyncio
async def test_processed_event_registry_skips_bootstrap_when_schema_objects_exist() -> None:
    registry = ProcessedEventRegistry(dsn="postgres://unused")
    conn = _Conn(
        existing_schemas={"spice_event_registry"},
        existing_relations={
            "spice_event_registry.processed_events",
            "spice_event_registry.aggregate_versions",
        },
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_event_registry" not in conn.executed


@pytest.mark.asyncio
async def test_processed_event_registry_raises_when_bootstrap_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ProcessedEventRegistry(dsn="postgres://unused")
    registry._pool = _Pool(_Conn())
    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "false")

    with pytest.raises(MissingProcessedEventRegistrySchemaError, match="missing required schema objects"):
        await registry.ensure_schema()


@pytest.mark.asyncio
async def test_processed_event_registry_bootstraps_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ProcessedEventRegistry(dsn="postgres://unused")
    conn = _Conn()
    registry._pool = _Pool(conn)
    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "true")

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_event_registry" in conn.executed
    assert any("CREATE TABLE IF NOT EXISTS spice_event_registry.processed_events" in sql for sql in conn.executed)
