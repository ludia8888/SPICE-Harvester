from datetime import datetime, timezone
from uuid import uuid4

import pytest

from oms.routers.command_status import get_command_status
from shared.models.commands import CommandStatus
from shared.services.registries.processed_event_registry import ProcessedEventRegistry


class DummyRegistry:
    def __init__(self, record):
        self._record = record

    async def get_event_record(self, *, event_id: str):
        return self._record


class DummyEventStore:
    def __init__(self, key=None):
        self._key = key

    async def get_event_object_key(self, *, event_id: str):
        return self._key


class _RowsConn:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, sql: str, *args):  # noqa: ANN001
        _ = sql, args
        return self._rows


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_command_status_falls_back_to_registry():
    command_id = str(uuid4())
    registry = DummyRegistry(
        {
            "status": "failed",
            "last_error": "boom",
            "handler": "instance_worker",
            "attempt_count": 2,
            "started_at": "2024-01-01T00:00:00Z",
            "processed_at": "2024-01-01T00:01:00Z",
            "heartbeat_at": "2024-01-01T00:01:00Z",
        }
    )
    event_store = DummyEventStore(key=None)

    result = await get_command_status(
        command_id=command_id,
        command_status_service=None,
        processed_event_registry=registry,
        event_store=event_store,
    )

    assert result.status == CommandStatus.FAILED
    assert result.error == "boom"
    assert result.result["source"] == "processed_event_registry"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_command_status_falls_back_to_event_store_when_registry_has_no_record():
    command_id = str(uuid4())
    registry = DummyRegistry(None)
    event_store = DummyEventStore(key="events/commands/abc.json")

    result = await get_command_status(
        command_id=command_id,
        command_status_service=None,
        processed_event_registry=registry,
        event_store=event_store,
    )

    assert result.status == CommandStatus.PENDING
    assert result.result["source"] == "event_store"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_processed_event_registry_aggregates_per_handler_status() -> None:
    now = datetime.now(timezone.utc)
    registry = ProcessedEventRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _Pool(
        _RowsConn(
            [
                {
                    "handler": "projection_a",
                    "status": "done",
                    "last_error": None,
                    "attempt_count": 1,
                    "started_at": now,
                    "processed_at": now,
                    "heartbeat_at": now,
                    "aggregate_id": "agg-1",
                    "sequence_number": 10,
                },
                {
                    "handler": "projection_b",
                    "status": "processing",
                    "last_error": None,
                    "attempt_count": 2,
                    "started_at": now,
                    "processed_at": None,
                    "heartbeat_at": now,
                    "aggregate_id": "agg-1",
                    "sequence_number": 10,
                },
            ]
        )
    )

    record = await registry.get_event_record(event_id="evt-1")

    assert record is not None
    assert record["status"] == "processing"
    assert record["handler"] is None
    assert record["attempt_count"] == 2
    assert len(record["handler_states"]) == 2
