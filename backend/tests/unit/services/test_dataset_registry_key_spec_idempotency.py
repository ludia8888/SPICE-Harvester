from __future__ import annotations

from typing import Any, Optional

import pytest

from shared.services.registries.dataset_registry import DatasetRegistry


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeConnection:
    def __init__(
        self,
        *,
        select_rows: Optional[list[dict[str, Any] | None]] = None,
        insert_row: Optional[dict[str, Any]] = None,
    ) -> None:
        self._select_rows = list(select_rows or [])
        self._insert_row = insert_row
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append((sql, args))
        return "OK"

    async def fetchrow(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "FROM spice_datasets.key_specs" in sql:
            if self._select_rows:
                return self._select_rows.pop(0)
            return None
        if "INSERT INTO spice_datasets.key_specs" in sql:
            if self._insert_row is None:
                raise AssertionError("insert should not run")
            return self._insert_row
        raise AssertionError(f"Unexpected SQL: {sql}")


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeConnection:
        return self._conn


def _call_index(conn: _FakeConnection, needle: str) -> int:
    for index, (sql, _) in enumerate(conn.calls):
        if needle in sql:
            return index
    raise AssertionError(f"SQL containing {needle!r} was not executed")


@pytest.mark.asyncio
async def test_get_or_create_key_spec_serializes_dataset_default_scope_before_lookup() -> None:
    existing_row = {
        "key_spec_id": "key-spec-1",
        "dataset_id": "dataset-1",
        "dataset_version_id": None,
        "spec": {"primary_key": ["customer_id"]},
        "status": "ACTIVE",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    conn = _FakeConnection(select_rows=[existing_row])
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    record, created = await registry.get_or_create_key_spec(
        dataset_id="dataset-1",
        spec={"primary_key": ["customer_id"]},
    )

    assert created is False
    assert record.key_spec_id == "key-spec-1"
    assert _call_index(conn, "SELECT pg_advisory_xact_lock") < _call_index(conn, "FROM spice_datasets.key_specs")
    assert conn.calls[0][1] == ("dataset-key-spec:dataset-1:dataset-default",)
    assert not any("INSERT INTO spice_datasets.key_specs" in sql for sql, _ in conn.calls)


@pytest.mark.asyncio
async def test_get_or_create_key_spec_creates_dataset_default_scope_when_missing() -> None:
    inserted_row = {
        "key_spec_id": "key-spec-2",
        "dataset_id": "dataset-1",
        "dataset_version_id": None,
        "spec": {"primary_key": ["customer_id"]},
        "status": "ACTIVE",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    conn = _FakeConnection(select_rows=[None], insert_row=inserted_row)
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    record, created = await registry.get_or_create_key_spec(
        dataset_id="dataset-1",
        spec={"primary_key": ["customer_id"]},
    )

    assert created is True
    assert record.key_spec_id == "key-spec-2"
    assert _call_index(conn, "SELECT pg_advisory_xact_lock") < _call_index(conn, "INSERT INTO spice_datasets.key_specs")


@pytest.mark.asyncio
async def test_get_key_spec_for_dataset_falls_back_to_dataset_default_scope() -> None:
    default_row = {
        "key_spec_id": "key-spec-default",
        "dataset_id": "dataset-1",
        "dataset_version_id": None,
        "spec": {"primary_key": ["customer_id"]},
        "status": "ACTIVE",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    conn = _FakeConnection(select_rows=[None, default_row])
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    record = await registry.get_key_spec_for_dataset(
        dataset_id="dataset-1",
        dataset_version_id="version-1",
    )

    assert record is not None
    assert record.key_spec_id == "key-spec-default"
    assert sum(1 for sql, _ in conn.calls if "FROM spice_datasets.key_specs" in sql) == 2
