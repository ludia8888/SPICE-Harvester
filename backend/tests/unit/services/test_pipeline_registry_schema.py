from __future__ import annotations

import pytest

from shared.services.registries.pipeline_registry import PipelineRegistry
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


def test_pipeline_registry_required_tables_declared() -> None:
    registry = PipelineRegistry(dsn="postgres://unused")

    assert registry._required_tables() == (  # noqa: SLF001
        "lakefs_credentials",
        "pipelines",
        "pipeline_branches",
        "pipeline_versions",
        "pipeline_runs",
        "pipeline_artifacts",
        "promotion_manifests",
        "pipeline_watermarks",
        "pipeline_dependencies",
        "pipeline_permissions",
        "pipeline_udfs",
        "pipeline_udf_versions",
    )


@pytest.mark.asyncio
async def test_pipeline_registry_skips_bootstrap_when_objects_exist() -> None:
    registry = PipelineRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_pipelines"},
        existing_relations={f"spice_pipelines.{name}" for name in registry._required_tables()},  # noqa: SLF001
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_pipelines" not in conn.executed


@pytest.mark.asyncio
async def test_pipeline_registry_raises_when_objects_missing_and_bootstrap_disabled() -> None:
    registry = PipelineRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    registry._pool = _Pool(_Conn())

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await registry.ensure_schema()


@pytest.mark.asyncio
async def test_pipeline_registry_bootstrap_executes_representative_schema_sql() -> None:
    registry = PipelineRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=True,
    )
    conn = _Conn()
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert any("CREATE TABLE IF NOT EXISTS spice_pipelines.pipelines" in sql for sql in conn.executed)
    assert any("CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_artifacts" in sql for sql in conn.executed)
    assert any("idx_pipeline_artifacts_job_id" in sql for sql in conn.executed)
