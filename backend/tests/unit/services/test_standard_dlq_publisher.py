from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any

import pytest

from shared.services.kafka.dlq_publisher import DlqPublishSpec, publish_contextual_dlq_json, publish_standard_dlq


class _DummyMsg:
    def __init__(self) -> None:
        self._headers = [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]

    def topic(self) -> str:
        return "cmd-topic"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 44

    def value(self) -> bytes:
        return b'{"event_type":"X_REQUESTED"}'

    def key(self) -> bytes:
        return b"k1"

    def timestamp(self):
        return (0, 1700000000000)

    def headers(self):
        return list(self._headers)


class _CaptureProducer:
    def __init__(self) -> None:
        self.produce_calls: list[dict[str, Any]] = []
        self.flush_calls: list[float] = []

    def produce(self, topic: str, *, key: bytes, value: bytes, headers=None, **kwargs: Any) -> None:
        self.produce_calls.append({"topic": topic, "key": key, "value": value, "headers": headers})

    def flush(self, timeout_s: float) -> int:
        self.flush_calls.append(float(timeout_s))
        return 0


class _NoopTracing:
    @contextmanager
    def span(self, *_args: Any, **_kwargs: Any):
        yield None


@pytest.mark.asyncio
async def test_publish_standard_dlq_builds_payload_and_flushes() -> None:
    producer = _CaptureProducer()
    msg = _DummyMsg()
    spec = DlqPublishSpec(
        dlq_topic="commands-dlq",
        service_name="unit-worker",
        flush_timeout_seconds=0.5,
    )

    await publish_standard_dlq(
        producer=producer,
        msg=msg,
        worker="unit-worker",
        dlq_spec=spec,
        error="boom",
        attempt_count=3,
        stage="process",
        payload_text=None,
        payload_obj={"k": "v"},
        fallback_metadata={"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
    )

    assert producer.flush_calls == [0.5]
    assert len(producer.produce_calls) == 1
    call = producer.produce_calls[0]
    assert call["topic"] == "commands-dlq"
    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["worker"] == "unit-worker"
    assert payload["stage"] == "process"
    assert payload["attempt_count"] == 3
    assert payload["parsed_payload"] == {"k": "v"}


@pytest.mark.asyncio
async def test_publish_contextual_dlq_json_uses_synthetic_source_key() -> None:
    producer = _CaptureProducer()
    spec = DlqPublishSpec(
        dlq_topic="commands-dlq",
        service_name="unit-worker",
        span_name="unit.dlq",
        flush_timeout_seconds=0.5,
    )

    await publish_contextual_dlq_json(
        producer=producer,
        spec=spec,
        payload={"kind": "custom", "ok": True},
        message_key=None,
        msg=None,
        source_topic="dataset_ingest_outbox",
        tracing=_NoopTracing(),
    )

    assert producer.flush_calls == [0.5]
    assert len(producer.produce_calls) == 1
    call = producer.produce_calls[0]
    assert call["topic"] == "commands-dlq"
    assert call["key"] == b"dataset_ingest_outbox:-1:-1"
    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["kind"] == "custom"
    assert payload["ok"] is True
