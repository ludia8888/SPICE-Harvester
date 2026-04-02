from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.services.registries.objectify_registry import ObjectifyRegistry


class _Transaction:
    async def __aenter__(self) -> "_Transaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _Connection:
    def __init__(self) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._now = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    async def __aenter__(self) -> "_Connection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetchval(self, sql: str, *args: object):
        self.fetchval_calls.append((sql, args))
        return None

    async def fetchrow(self, sql: str, *args: object):
        self.fetchrow_calls.append((sql, args))
        if "SELECT COALESCE(MAX(version), 0) AS current_version" in sql:
            return {"current_version": 1}
        if "FROM spice_objectify.ontology_mapping_specs" in sql and "WHERE mapping_spec_id = $1::uuid" in sql:
            return {
                "mapping_spec_id": args[0],
                "dataset_id": "dataset-1",
                "dataset_branch": "main",
                "artifact_output_name": "orders",
                "schema_hash": "hash-1",
                "backing_datasource_id": None,
                "backing_datasource_version_id": None,
                "target_class_id": "Order",
                "mappings": [{"source_field": "id", "target_field": "order_id"}],
                "target_field_types": {"order_id": "string"},
                "status": "ACTIVE",
                "version": 2,
                "auto_sync": True,
                "options": {"note": "test"},
                "created_at": self._now,
                "updated_at": self._now,
            }
        if "FROM spice_objectify.objectify_watermarks" in sql:
            return None
        raise AssertionError(f"Unexpected fetchrow SQL: {sql}")

    async def execute(self, sql: str, *args: object):
        self.execute_calls.append((sql, args))
        return "INSERT 0 1"


class _Pool:
    def __init__(self, conn: _Connection) -> None:
        self._conn = conn

    def acquire(self) -> _Connection:
        return self._conn


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_mapping_spec_serializes_version_allocation_with_advisory_lock() -> None:
    conn = _Connection()
    registry = ObjectifyRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(conn)

    record = await registry.create_mapping_spec(
        dataset_id="dataset-1",
        dataset_branch="main",
        artifact_output_name="orders",
        schema_hash="hash-1",
        target_class_id="Order",
        mappings=[{"source_field": "id", "target_field": "order_id"}],
        target_field_types={"order_id": "string"},
        status="ACTIVE",
        auto_sync=True,
        options={"note": "test"},
    )

    assert record.version == 2
    assert len(conn.fetchval_calls) == 1
    lock_sql, lock_args = conn.fetchval_calls[0]
    assert lock_sql.startswith("SELECT pg_advisory_xact_lock")
    assert lock_args == (
        "objectify_mapping_spec_version:dataset-1:main:Order:orders:hash-1",
    )
    assert len(conn.execute_calls) == 2
    assert "INSERT INTO spice_objectify.ontology_mapping_specs" in conn.execute_calls[0][0]
    assert "UPDATE spice_objectify.ontology_mapping_specs" in conn.execute_calls[1][0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_watermark_uses_schema_qualified_table_name() -> None:
    conn = _Connection()
    registry = ObjectifyRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(conn)

    await registry.get_watermark(mapping_spec_id="00000000-0000-0000-0000-000000000001")

    assert any(
        "FROM spice_objectify.objectify_watermarks" in sql
        for sql, _args in conn.fetchrow_calls
    )
