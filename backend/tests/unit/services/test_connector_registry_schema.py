from __future__ import annotations

import pytest

from shared.services.registries.connector_registry import ConnectorRegistry
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
        if sql == "CREATE SCHEMA IF NOT EXISTS spice_connectors":
            self._existing_schemas.add("spice_connectors")
        if "CREATE TABLE IF NOT EXISTS spice_connectors.connector_sources" in sql:
            self._existing_relations.add("spice_connectors.connector_sources")
        if "CREATE TABLE IF NOT EXISTS spice_connectors.connector_mappings" in sql:
            self._existing_relations.add("spice_connectors.connector_mappings")
        if "CREATE TABLE IF NOT EXISTS spice_connectors.connector_sync_state" in sql:
            self._existing_relations.add("spice_connectors.connector_sync_state")
        if "CREATE TABLE IF NOT EXISTS spice_connectors.connector_update_outbox" in sql:
            self._existing_relations.add("spice_connectors.connector_update_outbox")
        if "CREATE TABLE IF NOT EXISTS spice_connectors.connector_connection_secrets" in sql:
            self._existing_relations.add("spice_connectors.connector_connection_secrets")
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.asyncio
async def test_connector_registry_declares_required_tables() -> None:
    registry = ConnectorRegistry(dsn="postgres://unused")

    assert registry._required_tables() == (  # noqa: SLF001
        "connector_sources",
        "connector_mappings",
        "connector_sync_state",
        "connector_update_outbox",
        "connector_connection_secrets",
    )


@pytest.mark.asyncio
async def test_connector_registry_ensure_schema_skips_bootstrap_when_objects_exist() -> None:
    registry = ConnectorRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_connectors"},
        existing_relations={
            "spice_connectors.connector_sources",
            "spice_connectors.connector_mappings",
            "spice_connectors.connector_sync_state",
            "spice_connectors.connector_update_outbox",
            "spice_connectors.connector_connection_secrets",
        },
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_connectors" not in conn.executed


@pytest.mark.asyncio
async def test_connector_registry_ensure_schema_raises_when_objects_missing_and_bootstrap_disabled() -> None:
    registry = ConnectorRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn()
    registry._pool = _Pool(conn)

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await registry.ensure_schema()
