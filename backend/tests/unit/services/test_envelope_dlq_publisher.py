from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any

import pytest

from shared.models.event_envelope import EventEnvelope
from shared.services.kafka.dlq_publisher import (
    EnvelopeDlqSpec,
    build_envelope_dlq_event,
    publish_envelope_dlq,
)


class _CaptureProducerOps:
    def __init__(self) -> None:
        self.produce_calls: list[dict[str, Any]] = []
        self.flush_calls: list[float] = []

    async def produce(self, **kwargs: Any) -> None:
        self.produce_calls.append(dict(kwargs))

    async def flush(self, timeout_s: float) -> int:
        self.flush_calls.append(float(timeout_s))
        return 0


class _CaptureMetrics:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def record_event(self, name: str, *, action: str) -> None:
        self.calls.append((name, action))


class _NoopTracing:
    @contextmanager
    def span(self, *_args: Any, **_kwargs: Any):
        yield None


@pytest.mark.asyncio
async def test_build_envelope_dlq_event_applies_standard_metadata() -> None:
    source = EventEnvelope(
        event_id="evt-1",
        event_type="CONNECTOR_UPDATE_DETECTED",
        aggregate_type="connector_source",
        aggregate_id="sheets:abc",
        data={"cursor": "v1"},
        metadata={"kind": "connector_update", "source_type": "google_sheets"},
    )
    spec = EnvelopeDlqSpec(
        dlq_topic="connector-updates-dlq",
        service_name="connector-sync-worker",
        kind="connector_update_dlq",
        failed_event_type="CONNECTOR_UPDATE_FAILED",
    )

    dlq_env = build_envelope_dlq_event(
        envelope=source,
        spec=spec,
        error="boom",
        attempt_count=3,
        extra_metadata={"retry_policy": "exponential"},
    )

    assert dlq_env is not source
    assert source.event_type == "CONNECTOR_UPDATE_DETECTED"
    assert dlq_env.event_type == "CONNECTOR_UPDATE_FAILED"
    assert dlq_env.metadata["kind"] == "connector_update_dlq"
    assert dlq_env.metadata["dlq_error"] == "boom"
    assert dlq_env.metadata["dlq_attempt_count"] == 3
    assert dlq_env.metadata["dlq_topic"] == "connector-updates-dlq"
    assert dlq_env.metadata["retry_policy"] == "exponential"


@pytest.mark.asyncio
async def test_publish_envelope_dlq_uses_producer_ops_and_flushes() -> None:
    envelope = EventEnvelope(
        event_id="evt-2",
        event_type="SEARCH_PROJECTION_FAILED",
        aggregate_type="instance",
        aggregate_id="instance-123",
        data={"name": "demo"},
        metadata={"kind": "search_projection_dlq"},
    )
    spec = EnvelopeDlqSpec(
        dlq_topic="search-projection-dlq",
        service_name="search-projection-worker",
        kind="search_projection_dlq",
        failed_event_type="SEARCH_PROJECTION_FAILED",
        span_name="search_projection.dlq_produce",
        metric_event_name="SEARCH_PROJECTION_DLQ",
        flush_timeout_seconds=0.25,
    )
    producer_ops = _CaptureProducerOps()
    metrics = _CaptureMetrics()
    tracing = _NoopTracing()

    await publish_envelope_dlq(
        producer_ops=producer_ops,
        spec=spec,
        envelope=envelope,
        tracing=tracing,
        metrics=metrics,
        key=b"instance-123",
    )

    assert len(producer_ops.produce_calls) == 1
    call = producer_ops.produce_calls[0]
    assert call["topic"] == "search-projection-dlq"
    assert call["key"] == b"instance-123"
    assert producer_ops.flush_calls == [0.25]
    assert metrics.calls == [("SEARCH_PROJECTION_DLQ", "published")]

    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["event_id"] == "evt-2"
    assert payload["event_type"] == "SEARCH_PROJECTION_FAILED"
    assert payload["aggregate_id"] == "instance-123"
