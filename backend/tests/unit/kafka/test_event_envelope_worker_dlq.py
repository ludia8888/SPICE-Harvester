from __future__ import annotations

import json
from contextlib import nullcontext
from typing import Any, Optional

import pytest

from shared.models.event_envelope import EventEnvelope
from shared.services.kafka.dlq_publisher import EnvelopeDlqSpec
from shared.services.kafka.processed_event_worker import EventEnvelopeKafkaWorker


class _FakeProducerOps:
    def __init__(self) -> None:
        self.produce_calls: list[dict[str, Any]] = []
        self.flush_calls: list[float] = []

    async def produce(self, **kwargs: Any) -> None:
        self.produce_calls.append(dict(kwargs))

    async def flush(self, timeout_s: float) -> int:
        self.flush_calls.append(float(timeout_s))
        return 0


class _FakeTracing:
    def span(self, *_args: Any, **_kwargs: Any):
        return nullcontext()


class _FakeMetrics:
    def record_event(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _Msg:
    def topic(self) -> str:
        return "instance_events"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 99


class _StubEnvelopeWorker(EventEnvelopeKafkaWorker[None]):
    def __init__(self) -> None:
        self.consumer = None
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub_handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10
        self.service_name = "stub-envelope-worker"
        self.tracing = _FakeTracing()
        self.metrics = _FakeMetrics()
        self.dlq_producer_ops: Optional[Any] = None
        self._dlq_spec = EnvelopeDlqSpec(
            dlq_topic="projection_failures_dlq",
            service_name="stub-envelope-worker",
            kind="projection_dlq",
            failed_event_type="PROJECTION_FAILED",
            span_name="projection.dlq_produce",
        )

    async def _process_payload(self, payload: EventEnvelope) -> None:  # type: ignore[override]
        return None

    def _envelope_dlq_key_fallback(self, payload: EventEnvelope) -> Optional[str]:
        _ = payload
        return "fallback-key"


@pytest.mark.asyncio
async def test_publish_envelope_failure_to_dlq_uses_default_key_and_shape() -> None:
    worker = _StubEnvelopeWorker()
    producer_ops = _FakeProducerOps()
    envelope = EventEnvelope(
        event_id="evt-1",
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="",
        metadata={"kind": "domain"},
        data={"name": "A"},
    )
    spec = EnvelopeDlqSpec(
        dlq_topic="projection_failures_dlq",
        service_name="stub-envelope-worker",
        kind="projection_dlq",
        failed_event_type="PROJECTION_FAILED",
        span_name="projection.dlq_produce",
    )

    await worker._publish_envelope_failure_to_dlq(
        msg=_Msg(),
        payload=envelope,
        error="projection failed",
        attempt_count=2,
        producer_ops=producer_ops,
        spec=spec,
    )

    assert len(producer_ops.produce_calls) == 1
    call = producer_ops.produce_calls[0]
    assert call["topic"] == "projection_failures_dlq"
    assert call["key"] == b"evt-1"
    assert producer_ops.flush_calls == [10.0]
    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["event_type"] == "PROJECTION_FAILED"
    assert payload["metadata"]["kind"] == "projection_dlq"
    assert payload["metadata"]["dlq_attempt_count"] == 2
    assert payload["metadata"]["dlq_error"] == "projection failed"


@pytest.mark.asyncio
async def test_publish_envelope_failure_to_dlq_noops_without_producer() -> None:
    worker = _StubEnvelopeWorker()
    envelope = EventEnvelope(
        event_id="evt-2",
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="inst-2",
        metadata={"kind": "domain"},
        data={},
    )
    spec = EnvelopeDlqSpec(
        dlq_topic="projection_failures_dlq",
        service_name="stub-envelope-worker",
        kind="projection_dlq",
        failed_event_type="PROJECTION_FAILED",
    )

    await worker._publish_envelope_failure_to_dlq(
        msg=_Msg(),
        payload=envelope,
        error="projection failed",
        attempt_count=1,
        producer_ops=None,
        spec=spec,
    )


@pytest.mark.asyncio
async def test_send_envelope_failure_to_dlq_builds_key_with_fallback() -> None:
    worker = _StubEnvelopeWorker()
    producer_ops = _FakeProducerOps()
    envelope = EventEnvelope(
        event_id="",
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="",
        metadata={"kind": "domain"},
        data={},
    )
    spec = EnvelopeDlqSpec(
        dlq_topic="projection_failures_dlq",
        service_name="stub-envelope-worker",
        kind="projection_dlq",
        failed_event_type="PROJECTION_FAILED",
    )

    await worker._send_envelope_failure_to_dlq(
        msg=_Msg(),
        payload=envelope,
        error="projection failed",
        attempt_count=3,
        producer_ops=producer_ops,
        spec=spec,
        key_fallback="fallback-key",
    )

    assert len(producer_ops.produce_calls) == 1
    call = producer_ops.produce_calls[0]
    assert call["key"] == b"fallback-key"


@pytest.mark.asyncio
async def test_event_envelope_worker_default_send_to_dlq_uses_envelope_spec() -> None:
    worker = _StubEnvelopeWorker()
    producer_ops = _FakeProducerOps()
    worker.dlq_producer_ops = producer_ops
    envelope = EventEnvelope(
        event_id="",
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="",
        metadata={"kind": "domain"},
        data={"name": "Example"},
    )

    await worker._send_to_dlq(
        msg=_Msg(),
        payload=envelope,
        raw_payload=envelope.model_dump_json(),
        error="projection failed",
        attempt_count=5,
    )

    assert len(producer_ops.produce_calls) == 1
    call = producer_ops.produce_calls[0]
    assert call["topic"] == "projection_failures_dlq"
    assert call["key"] == b"fallback-key"
    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["metadata"]["kind"] == "projection_dlq"
    assert payload["metadata"]["dlq_attempt_count"] == 5
