from __future__ import annotations

import pytest

from shared.services.registries.dataset_profile_registry import DatasetProfileRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
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
        if "CREATE SCHEMA IF NOT EXISTS" in sql:
            self._existing_schemas.add(sql.rsplit(" ", 1)[-1])
        if "CREATE TABLE IF NOT EXISTS " in sql:
            target = sql.split("CREATE TABLE IF NOT EXISTS ", 1)[1].split(" ", 1)[0].split("(", 1)[0]
            self._existing_relations.add(target.strip())
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.parametrize(
    ("registry_cls", "expected"),
    [
        (DatasetProfileRegistry, ("dataset_profiles",)),
        (PipelinePlanRegistry, ("pipeline_plans",)),
        (ObjectifyRegistry, ("ontology_mapping_specs", "objectify_jobs", "objectify_job_outbox", "objectify_watermarks")),
    ],
)
def test_support_registry_required_tables_declared(registry_cls, expected) -> None:
    registry = registry_cls(dsn="postgres://unused")

    assert registry._required_tables() == expected  # noqa: SLF001


@pytest.mark.asyncio
async def test_dataset_profile_registry_skips_bootstrap_when_objects_exist() -> None:
    registry = DatasetProfileRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_datasets"},
        existing_relations={"spice_datasets.dataset_profiles"},
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_datasets" not in conn.executed


@pytest.mark.asyncio
async def test_objectify_registry_raises_when_objects_missing_and_bootstrap_disabled() -> None:
    registry = ObjectifyRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    registry._pool = _Pool(_Conn())

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await registry.ensure_schema()
