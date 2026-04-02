from __future__ import annotations

import pytest

from shared.services.registries.action_log_registry import ActionLogRegistry


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
        if sql == "CREATE SCHEMA IF NOT EXISTS spice_action_logs":
            self._existing_schemas.add("spice_action_logs")
        if "CREATE TABLE IF NOT EXISTS spice_action_logs.ontology_action_logs" in sql:
            self._existing_relations.add("spice_action_logs.ontology_action_logs")
        if "CREATE TABLE IF NOT EXISTS spice_action_logs.ontology_action_dependencies" in sql:
            self._existing_relations.add("spice_action_logs.ontology_action_dependencies")
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.asyncio
async def test_action_log_registry_declares_required_tables() -> None:
    registry = ActionLogRegistry(dsn="postgres://unused")

    assert registry._required_tables() == ("ontology_action_logs", "ontology_action_dependencies")  # noqa: SLF001


@pytest.mark.asyncio
async def test_action_log_registry_ensure_schema_skips_bootstrap_when_objects_exist() -> None:
    registry = ActionLogRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_action_logs"},
        existing_relations={
            "spice_action_logs.ontology_action_logs",
            "spice_action_logs.ontology_action_dependencies",
        },
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_action_logs" not in conn.executed
