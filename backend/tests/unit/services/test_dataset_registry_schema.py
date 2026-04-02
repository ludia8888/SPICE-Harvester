from __future__ import annotations

import pytest

from shared.services.registries.dataset_registry import DatasetRegistry
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
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


def test_dataset_registry_required_tables_declared() -> None:
    registry = DatasetRegistry(dsn="postgres://unused")

    assert registry._required_tables() == (  # noqa: SLF001
        "datasets",
        "dataset_versions",
        "dataset_ingest_requests",
        "dataset_ingest_transactions",
        "dataset_ingest_outbox",
        "backing_datasources",
        "backing_datasource_versions",
        "key_specs",
        "gate_policies",
        "gate_results",
        "access_policies",
        "instance_edits",
        "relationship_specs",
        "relationship_index_results",
        "link_edits",
        "schema_migration_plans",
    )


@pytest.mark.asyncio
async def test_dataset_registry_skips_bootstrap_when_objects_exist() -> None:
    registry = DatasetRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_datasets"},
        existing_relations={f"spice_datasets.{name}" for name in registry._required_tables()},  # noqa: SLF001
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_datasets" not in conn.executed


@pytest.mark.asyncio
async def test_dataset_registry_raises_when_objects_missing_and_bootstrap_disabled() -> None:
    registry = DatasetRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    registry._pool = _Pool(_Conn())

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await registry.ensure_schema()
