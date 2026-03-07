from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.models.objectify_job import ObjectifyJob
from shared.services.registries import objectify_registry as objectify_registry_module
from shared.services.registries.objectify_registry import ObjectifyRegistry


class _UniqueViolationError(RuntimeError):
    pass


class _Transaction:
    async def __aenter__(self) -> "_Transaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _Connection:
    def __init__(self, *, conflict_on_insert: bool) -> None:
        self.conflict_on_insert = conflict_on_insert
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._now = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    async def __aenter__(self) -> "_Connection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetchrow(self, sql: str, *args: object):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO spice_objectify.objectify_jobs" in sql:
            if self.conflict_on_insert:
                raise _UniqueViolationError("duplicate key value violates unique constraint")
            return _job_row(job_id=str(args[0]), dedupe_key=str(args[3]), status=str(args[10]))
        if "FROM spice_objectify.objectify_jobs" in sql and "WHERE dedupe_key = $1" in sql:
            return _job_row(job_id="existing-job", dedupe_key=str(args[0]), status="ENQUEUE_REQUESTED")
        raise AssertionError(f"Unexpected fetchrow SQL: {sql}")

    async def execute(self, sql: str, *args: object):
        self.execute_calls.append((sql, args))
        return "INSERT 0 1"


class _Pool:
    def __init__(self, conn: _Connection) -> None:
        self._conn = conn

    def acquire(self) -> _Connection:
        return self._conn


def _job_row(*, job_id: str, dedupe_key: str, status: str) -> dict[str, object]:
    return {
        "job_id": job_id,
        "mapping_spec_id": "map-1",
        "mapping_spec_version": 1,
        "dedupe_key": dedupe_key,
        "dataset_id": "dataset-1",
        "dataset_version_id": "version-1",
        "artifact_id": None,
        "artifact_output_name": "orders",
        "dataset_branch": "main",
        "target_class_id": "Order",
        "status": status,
        "command_id": None,
        "error": None,
        "report": {},
        "created_at": datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        "completed_at": None,
    }


@pytest.mark.asyncio
async def test_create_objectify_job_duplicate_conflict_does_not_enqueue_outbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(objectify_registry_module, "UniqueViolationError", _UniqueViolationError)
    conn = _Connection(conflict_on_insert=True)
    registry = ObjectifyRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(conn)

    record = await registry.create_objectify_job(
        job_id="new-job",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        dataset_id="dataset-1",
        dataset_version_id="version-1",
        artifact_id=None,
        artifact_output_name="orders",
        dataset_branch="main",
        target_class_id="Order",
        status="ENQUEUE_REQUESTED",
        outbox_payload={"job_id": "new-job"},
        dedupe_key="dedupe-1",
    )

    assert record.job_id == "existing-job"
    assert not any("INSERT INTO spice_objectify.objectify_job_outbox" in sql for sql, _ in conn.execute_calls)


@pytest.mark.asyncio
async def test_create_objectify_job_new_record_enqueues_outbox_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(objectify_registry_module, "UniqueViolationError", _UniqueViolationError)
    conn = _Connection(conflict_on_insert=False)
    registry = ObjectifyRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(conn)

    record = await registry.create_objectify_job(
        job_id="new-job",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        dataset_id="dataset-1",
        dataset_version_id="version-1",
        artifact_id=None,
        artifact_output_name="orders",
        dataset_branch="main",
        target_class_id="Order",
        status="ENQUEUE_REQUESTED",
        outbox_payload={"job_id": "new-job"},
        dedupe_key="dedupe-1",
    )

    assert record.job_id == "new-job"
    outbox_inserts = [sql for sql, _ in conn.execute_calls if "INSERT INTO spice_objectify.objectify_job_outbox" in sql]
    assert len(outbox_inserts) == 1


@pytest.mark.asyncio
async def test_get_or_enqueue_objectify_job_reports_duplicate_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(objectify_registry_module, "UniqueViolationError", _UniqueViolationError)
    conn = _Connection(conflict_on_insert=True)
    registry = ObjectifyRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(conn)

    result = await registry.get_or_enqueue_objectify_job(
        job=ObjectifyJob(
            job_id="new-job",
            db_name="demo_db",
            dataset_id="dataset-1",
            dataset_version_id="version-1",
            artifact_output_name="orders",
            dedupe_key="dedupe-1",
            dataset_branch="main",
            artifact_key="s3://bucket/orders.csv",
            mapping_spec_id="map-1",
            mapping_spec_version=1,
            target_class_id="Order",
        )
    )

    assert result.created is False
    assert result.record.job_id == "existing-job"
