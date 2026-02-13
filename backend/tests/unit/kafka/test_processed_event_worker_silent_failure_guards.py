from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any

import pytest

from shared.services.kafka.processed_event_worker import (
    ClaimDecision,
    ClaimResult,
    ProcessedEventKafkaWorker,
    RegistryKey,
)


class _Msg:
    def __init__(self, topic: str = "demo.topic", partition: int = 0, offset: int = 1) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _PartitionWorker(ProcessedEventKafkaWorker[dict[str, Any], None]):
    def __init__(self) -> None:
        self.consumer = object()
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub-handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10
        self.resumed: list[tuple[str, int]] = []
        self._init_partition_state(reset=True)

    def _parse_payload(self, payload: Any) -> dict[str, Any]:  # type: ignore[override]
        return dict(payload or {})

    def _registry_key(self, payload: dict[str, Any]) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(event_id=str(payload.get("event_id") or "evt"))

    async def _process_payload(self, payload: dict[str, Any]) -> None:  # type: ignore[override]
        # Let heartbeat task start so cancellation path exercises join-error logging.
        await asyncio.sleep(0.01)

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: dict[str, Any],
        raw_payload: str | None,
        error: str,
        attempt_count: int,
    ) -> None:
        return None

    def _buffer_messages(self) -> bool:
        return True

    def _uses_commit_state(self) -> bool:
        return True

    async def _resume_partition(self, *, topic: str, partition: int) -> None:
        self.resumed.append((topic, partition))

    async def handle_message(self, msg: Any) -> None:  # type: ignore[override]
        raise RuntimeError("handle-failed")


@pytest.mark.asyncio
async def test_handle_partition_message_propagates_error_when_partition_revoked() -> None:
    worker = _PartitionWorker()
    msg = _Msg()
    key = (msg.topic(), msg.partition())
    worker._revoked_partitions.add(key)

    with pytest.raises(RuntimeError, match="handle-failed"):
        await worker._handle_partition_message(msg)


@pytest.mark.asyncio
async def test_handle_partition_message_keeps_primary_error_when_cleanup_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = _PartitionWorker()
    msg = _Msg()
    key = (msg.topic(), msg.partition())

    async def _failing_resume(*, topic: str, partition: int) -> None:
        raise RuntimeError(f"resume-failed:{topic}:{partition}")

    worker._resume_partition = _failing_resume  # type: ignore[assignment]
    worker._pending_by_partition[key] = deque([_Msg(offset=2)])

    with caplog.at_level(logging.WARNING, logger="shared.services.kafka.processed_event_worker"):
        with pytest.raises(RuntimeError, match="handle-failed"):
            await worker._handle_partition_message(msg)

    assert any("partition cleanup failed after primary error" in rec.message.lower() for rec in caplog.records)


@dataclass
class _ProcessedStub:
    async def claim(self, **_kwargs: Any) -> ClaimResult:
        return ClaimResult(decision=ClaimDecision.CLAIMED, attempt_count=1)

    async def mark_done(self, **_kwargs: Any) -> None:
        return None

    async def mark_failed(self, **_kwargs: Any) -> None:
        return None


class _HeartbeatJoinWorker(ProcessedEventKafkaWorker[dict[str, Any], None]):
    def __init__(self) -> None:
        self.consumer = object()
        self.consumer_ops = None
        self.processed = _ProcessedStub()
        self.handler = "stub-handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10

    def _parse_payload(self, payload: Any) -> dict[str, Any]:  # type: ignore[override]
        return dict(payload or {})

    def _registry_key(self, payload: dict[str, Any]) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(event_id=str(payload.get("event_id") or "evt"))

    async def _process_payload(self, payload: dict[str, Any]) -> None:  # type: ignore[override]
        return None

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: dict[str, Any],
        raw_payload: str | None,
        error: str,
        attempt_count: int,
    ) -> None:
        return None

    async def _commit(self, msg: Any) -> None:  # type: ignore[override]
        return None

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:  # type: ignore[override]
        await asyncio.sleep(0)
        raise RuntimeError("heartbeat-join-failed")


@pytest.mark.asyncio
async def test_handle_claimed_logs_warning_on_heartbeat_join_failure(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _HeartbeatJoinWorker()
    msg = _Msg()

    class _FailingHeartbeatTask:
        def cancel(self) -> None:
            return None

        def __await__(self):
            async def _raise() -> None:
                raise RuntimeError("heartbeat-join-failed")

            return _raise().__await__()

    def _fake_create_task(coro: Any, *args: Any, **kwargs: Any) -> _FailingHeartbeatTask:
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        return _FailingHeartbeatTask()

    monkeypatch.setattr("shared.services.kafka.processed_event_worker.asyncio.create_task", _fake_create_task)

    with caplog.at_level(logging.WARNING, logger="shared.services.kafka.processed_event_worker"):
        await worker._handle_claimed(
            msg=msg,
            payload={"event_id": "evt-1"},
            registry_key=RegistryKey(event_id="evt-1"),
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            raw_text='{"event_id": "evt-1"}',
            start=0.0,
        )

    assert any("heartbeat task join failed after cancellation" in rec.message.lower() for rec in caplog.records)
