from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Optional

import pytest


class _DummyMsg:
    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        value: bytes,
        key: Optional[bytes] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[list[tuple[str, bytes]]] = None,
    ) -> None:
        self._topic = topic
        self._partition = int(partition)
        self._offset = int(offset)
        self._value = value
        self._key = key
        self._timestamp_ms = timestamp_ms
        self._headers = headers or []

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> Optional[bytes]:
        return self._key

    def timestamp(self):
        if self._timestamp_ms is None:
            return None
        return (0, int(self._timestamp_ms))

    def headers(self):
        return list(self._headers)


class _CaptureProducer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def produce(self, topic: str, *, key: bytes, value: bytes, headers=None, **kwargs) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value, "headers": headers})

    def flush(self, *_args, **_kwargs) -> int:
        return 0


class _NoopTracing:
    @contextmanager
    def span(self, *_args, **_kwargs):
        yield None


class _NoopMetrics:
    def record_event(self, *_args, **_kwargs) -> None:
        return None


@pytest.mark.asyncio
async def test_action_worker_send_to_dlq_payload_shape() -> None:
    from action_worker.main import ActionWorker

    worker = ActionWorker()
    worker.tracing = _NoopTracing()
    worker.metrics = _NoopMetrics()

    producer = _CaptureProducer()
    worker.dlq_producer = producer  # type: ignore[assignment]
    worker.dlq_topic = "action-commands-dlq"
    worker.dlq_flush_timeout_seconds = 0.01

    msg = _DummyMsg(
        topic="action_commands",
        partition=1,
        offset=42,
        key=b"action-key",
        value=b'{"event_type":"EXECUTE_ACTION_REQUESTED","aggregate_id":"db:demo","metadata":{"kind":"command"}}',
        timestamp_ms=1700000000123,
        headers=[("traceparent", b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")],
    )

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        stage="execute_action",
        error="boom",
        attempt_count=3,
        payload_text=None,
        payload_obj={"hello": "world"},
    )

    assert producer.calls, "expected a DLQ produce call"
    call = producer.calls[-1]
    assert call["topic"] == "action-commands-dlq"

    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["original_topic"] == "action_commands"
    assert payload["original_partition"] == 1
    assert payload["original_offset"] == 42
    assert payload["original_key"] == "action-key"
    assert payload["stage"] == "execute_action"
    assert payload["attempt_count"] == 3
    assert payload["worker"] == "action-worker"


@pytest.mark.asyncio
async def test_ontology_worker_send_to_dlq_payload_shape() -> None:
    from ontology_worker.main import OntologyWorker

    worker = OntologyWorker()
    worker.tracing_service = _NoopTracing()
    worker.metrics_collector = _NoopMetrics()

    producer = _CaptureProducer()
    worker.dlq_producer = producer  # type: ignore[assignment]
    worker.dlq_topic = "ontology-commands-dlq"
    worker.dlq_flush_timeout_seconds = 0.01

    msg = _DummyMsg(
        topic="ontology_commands",
        partition=0,
        offset=7,
        key=b"onto-key",
        value=b'{"event_type":"ONTOLOGY_UPDATE_REQUESTED","aggregate_id":"db:demo","metadata":{"kind":"command"}}',
        timestamp_ms=1700000000456,
    )

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        stage="process_command",
        error="failed",
        attempt_count=2,
        payload_text=None,
        payload_obj={"x": 1},
        kafka_headers=msg.headers(),
        fallback_metadata={"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
    )

    assert producer.calls, "expected a DLQ produce call"
    call = producer.calls[-1]
    assert call["topic"] == "ontology-commands-dlq"
    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["original_topic"] == "ontology_commands"
    assert payload["stage"] == "process_command"
    assert payload["attempt_count"] == 2
    assert payload["worker"] == "ontology-worker"


@pytest.mark.asyncio
async def test_instance_worker_send_to_dlq_payload_shape() -> None:
    from instance_worker.main import StrictPalantirInstanceWorker

    worker = StrictPalantirInstanceWorker()
    worker.tracing = _NoopTracing()
    worker.metrics = _NoopMetrics()

    producer = _CaptureProducer()
    worker.dlq_producer = producer  # type: ignore[assignment]
    worker.dlq_topic = "instance-commands-dlq"
    worker.dlq_flush_timeout_seconds = 0.01

    msg = _DummyMsg(
        topic="instance_commands",
        partition=2,
        offset=99,
        key=b"inst-key",
        value=b'{"event_type":"CREATE_INSTANCE_REQUESTED","aggregate_id":"db:demo","metadata":{"kind":"command"}}',
        timestamp_ms=1700000000789,
    )

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        stage="process_command",
        error="failed",
        attempt_count=5,
        payload_text=None,
        payload_obj={"command_type": "CREATE_INSTANCE"},
        kafka_headers=msg.headers(),
        fallback_metadata={"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
    )

    assert producer.calls, "expected a DLQ produce call"
    call = producer.calls[-1]
    assert call["topic"] == "instance-commands-dlq"
    payload = json.loads(call["value"].decode("utf-8"))
    assert payload["original_topic"] == "instance_commands"
    assert payload["stage"] == "process_command"
    assert payload["attempt_count"] == 5
    assert payload["worker"] == "instance-worker"

    worker._consumer_executor.shutdown(wait=False, cancel_futures=True)

