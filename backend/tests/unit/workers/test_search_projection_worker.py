from __future__ import annotations

import json
from contextlib import nullcontext
from typing import Any

import pytest

from search_projection_worker.main import SearchProjectionWorker
from shared.models.event_envelope import EventEnvelope


class _FakeProducerOps:
    def __init__(self) -> None:
        self.produce_calls: list[dict[str, Any]] = []
        self.flush_calls: list[float] = []
        self.close_calls: list[float] = []

    async def produce(self, **kwargs: Any) -> None:
        self.produce_calls.append(dict(kwargs))

    async def flush(self, timeout_s: float) -> int:
        self.flush_calls.append(float(timeout_s))
        return 0

    async def close(self, *, timeout_s: float = 5.0) -> None:
        self.close_calls.append(float(timeout_s))


class _FakeTracing:
    def span(self, *_args: Any, **_kwargs: Any):
        return nullcontext()


class _FakeMetrics:
    def record_event(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _FakeProcessed:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _FakeES:
    def __init__(self) -> None:
        self.disconnected = False

    async def disconnect(self) -> None:
        self.disconnected = True


class _DummyMsg:
    def topic(self) -> str:
        return "instance_events"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 11


@pytest.mark.asyncio
async def test_send_to_dlq_builds_failed_envelope_and_publishes() -> None:
    worker = SearchProjectionWorker()
    worker.tracing = _FakeTracing()
    worker.metrics = _FakeMetrics()
    worker.dlq_producer_ops = _FakeProducerOps()

    payload = EventEnvelope(
        event_id="evt-1",
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="inst-1",
        data={"name": "A"},
        metadata={"kind": "domain"},
    )

    await worker._send_to_dlq(
        msg=_DummyMsg(),
        payload=payload,
        raw_payload=None,
        error="index failed",
        attempt_count=4,
    )

    assert len(worker.dlq_producer_ops.produce_calls) == 1
    call = worker.dlq_producer_ops.produce_calls[0]
    assert call["topic"] == worker.dlq_topic
    assert call["key"] == b"inst-1"
    decoded = json.loads(call["value"].decode("utf-8"))
    assert decoded["event_type"] == "SEARCH_PROJECTION_FAILED"
    assert decoded["metadata"]["kind"] == "search_projection_dlq"
    assert decoded["metadata"]["dlq_attempt_count"] == 4
    assert decoded["metadata"]["dlq_error"] == "index failed"


@pytest.mark.asyncio
async def test_close_flushes_producer_ops_and_closes_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = SearchProjectionWorker()
    producer_ops = _FakeProducerOps()
    worker.dlq_producer_ops = producer_ops
    worker.processed = _FakeProcessed()
    worker.es = _FakeES()

    async def _noop_close_consumer_runtime() -> None:
        return None

    monkeypatch.setattr(worker, "_close_consumer_runtime", _noop_close_consumer_runtime)

    await worker.close()

    assert producer_ops.close_calls == [5.0]
    assert worker.dlq_producer_ops is None
    assert worker.processed is None
    assert worker.es is None
