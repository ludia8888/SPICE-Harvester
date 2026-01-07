from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from shared.models.commands import CommandStatus
from shared.models.sync_wrapper import SyncOptions
from shared.services.sync_wrapper_service import SyncWrapperService


class _FakeCommandStatusService:
    def __init__(self, sequence: List[Dict[str, Any]]) -> None:
        self._sequence = sequence
        self._index = 0

    async def get_command_details(self, command_id: str) -> Dict[str, Any]:
        if self._index >= len(self._sequence):
            return self._sequence[-1]
        result = self._sequence[self._index]
        self._index += 1
        return result


@dataclass
class _FakeCommandResult:
    command_id: str


@pytest.mark.asyncio
async def test_wait_for_command_success() -> None:
    sequence = [
        {"status": CommandStatus.PENDING, "data": {}},
        {"status": CommandStatus.COMPLETED, "result": {"ok": True}, "data": {}},
    ]
    service = SyncWrapperService(_FakeCommandStatusService(sequence))

    result = await service.wait_for_command("cmd-1", SyncOptions(timeout=1, poll_interval=0.1))

    assert result.success is True
    assert result.data == {"ok": True}
    assert result.final_status == CommandStatus.COMPLETED


@pytest.mark.asyncio
async def test_wait_for_command_timeout() -> None:
    sequence = [{"status": CommandStatus.PENDING, "data": {}}]
    service = SyncWrapperService(_FakeCommandStatusService(sequence))

    result = await service.wait_for_command("cmd-2", SyncOptions(timeout=1, poll_interval=0.1))

    assert result.success is False
    assert result.final_status == "TIMEOUT"


@pytest.mark.asyncio
async def test_wait_for_command_failure() -> None:
    sequence = [{"status": CommandStatus.FAILED, "error": "boom", "data": {}}]
    service = SyncWrapperService(_FakeCommandStatusService(sequence))

    result = await service.wait_for_command("cmd-3", SyncOptions(timeout=1, poll_interval=0.1))

    assert result.success is False
    assert "boom" in (result.error or "")


@pytest.mark.asyncio
async def test_execute_sync_calls_wait() -> None:
    sequence = [{"status": CommandStatus.COMPLETED, "result": {"ok": True}, "data": {}}]
    service = SyncWrapperService(_FakeCommandStatusService(sequence))

    async def do_async(**kwargs):
        return _FakeCommandResult(command_id="cmd-4")

    result = await service.execute_sync(do_async, {"arg": "value"}, SyncOptions(timeout=1, poll_interval=0.1))
    assert result.success is True
    assert result.data == {"ok": True}
