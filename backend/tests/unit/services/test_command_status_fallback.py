from uuid import uuid4

import pytest

from oms.routers.command_status import get_command_status
from shared.models.commands import CommandStatus


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
async def test_command_status_falls_back_to_event_store_when_no_registry():
    command_id = str(uuid4())
    event_store = DummyEventStore(key="events/commands/abc.json")

    result = await get_command_status(
        command_id=command_id,
        command_status_service=None,
        processed_event_registry=None,
        event_store=event_store,
    )

    assert result.status == CommandStatus.PENDING
    assert result.result["source"] == "event_store"
