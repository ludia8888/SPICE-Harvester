from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from shared.services.registries import dataset_registry as dataset_registry_module
from shared.services.registries.dataset_registry import DatasetRegistry


class _UniqueViolationError(RuntimeError):
    sqlstate = "23505"


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.request_lookup_count = 0
        self.commit_lookup_count = 0
        self.raise_on_backing_insert = False

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append((sql, args))
        return "OK"

    async def fetchval(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "FROM spice_datasets.dataset_ingest_outbox" in sql:
            return None
        raise AssertionError(f"Unexpected SQL: {sql}")

    async def fetchrow(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "INSERT INTO spice_datasets.dataset_versions" in sql:
            raise _UniqueViolationError("duplicate dataset version")
        if "WHERE ingest_request_id = $1::uuid" in sql and "FROM spice_datasets.dataset_versions" in sql:
            self.request_lookup_count += 1
            if self.request_lookup_count == 1:
                return None
            return {
                "version_id": "version-from-request",
                "dataset_id": "dataset-1",
                "lakefs_commit_id": "commit-1",
                "artifact_key": "s3://bucket/data.parquet",
                "row_count": 12,
                "sample_json": {},
                "ingest_request_id": "request-1",
                "promoted_from_artifact_id": None,
                "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        if "WHERE dataset_id = $1 AND lakefs_commit_id = $2" in sql:
            self.commit_lookup_count += 1
            return {
                "version_id": "version-from-commit",
                "dataset_id": "dataset-1",
                "lakefs_commit_id": "commit-1",
                "artifact_key": "s3://bucket/data.parquet",
                "row_count": 12,
                "sample_json": {},
                "ingest_request_id": "other-request",
                "promoted_from_artifact_id": None,
                "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        if "SELECT backing_id" in sql:
            return {"backing_id": "backing-1"}
        if "INSERT INTO spice_datasets.backing_datasource_versions" in sql:
            if self.raise_on_backing_insert:
                raise RuntimeError("backing upsert failed")
            return {"backing_version_id": "backing-version-1"}
        raise AssertionError(f"Unexpected SQL: {sql}")


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeConnection:
        return self._conn


@pytest.mark.asyncio
async def test_add_version_propagates_backing_version_failure_instead_of_swallowing_it() -> None:
    conn = _FakeConnection()
    conn.raise_on_backing_insert = True
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    async def _fetchrow(sql: str, *args: Any):  # noqa: ANN401
        conn.calls.append((sql, args))
        if "INSERT INTO spice_datasets.dataset_versions" in sql:
            return {
                "version_id": "version-1",
                "dataset_id": "dataset-1",
                "lakefs_commit_id": "commit-1",
                "artifact_key": "s3://bucket/data.parquet",
                "row_count": 7,
                "sample_json": {"columns": [{"name": "id", "type": "xsd:string"}]},
                "ingest_request_id": None,
                "promoted_from_artifact_id": None,
                "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        if "SELECT backing_id" in sql:
            return {"backing_id": "backing-1"}
        if "INSERT INTO spice_datasets.backing_datasource_versions" in sql:
            raise RuntimeError("backing upsert failed")
        raise AssertionError(f"Unexpected SQL: {sql}")

    conn.fetchrow = _fetchrow  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="backing upsert failed"):
        await registry.add_version(
            dataset_id="dataset-1",
            lakefs_commit_id="commit-1",
            artifact_key="s3://bucket/data.parquet",
            row_count=7,
            sample_json={"columns": [{"name": "id", "type": "xsd:string"}]},
        )


@pytest.mark.asyncio
async def test_publish_ingest_request_recovers_duplicate_commit_by_request_id_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dataset_registry_module.asyncpg, "UniqueViolationError", _UniqueViolationError)
    conn = _FakeConnection()
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    version = await registry.publish_ingest_request(
        ingest_request_id="request-1",
        dataset_id="dataset-1",
        lakefs_commit_id="commit-1",
        artifact_key="s3://bucket/data.parquet",
        row_count=12,
        sample_json={},
        schema_json=None,
    )

    assert version.version_id == "version-from-request"
    assert conn.request_lookup_count == 2
    assert conn.commit_lookup_count == 0


@pytest.mark.asyncio
async def test_publish_ingest_request_rejects_duplicate_commit_owned_by_other_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dataset_registry_module.asyncpg, "UniqueViolationError", _UniqueViolationError)
    conn = _FakeConnection()
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    original_fetchrow = conn.fetchrow

    async def _fetchrow(sql: str, *args: Any):  # noqa: ANN401
        if "WHERE ingest_request_id = $1::uuid" in sql and "FROM spice_datasets.dataset_versions" in sql:
            conn.calls.append((sql, args))
            conn.request_lookup_count += 1
            return None
        return await original_fetchrow(sql, *args)

    conn.fetchrow = _fetchrow  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="different ingest_request_id"):
        await registry.publish_ingest_request(
            ingest_request_id="request-1",
            dataset_id="dataset-1",
            lakefs_commit_id="commit-1",
            artifact_key="s3://bucket/data.parquet",
            row_count=12,
            sample_json={},
            schema_json=None,
        )

    assert conn.commit_lookup_count == 1
