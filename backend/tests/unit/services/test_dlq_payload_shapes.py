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
    from instance_worker.main import StrictInstanceWorker

    worker = StrictInstanceWorker()
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


@pytest.mark.asyncio
async def test_pipeline_worker_send_to_dlq_payload_shape() -> None:
    from pipeline_worker.main import PipelineWorker

    worker = PipelineWorker()
    producer = _CaptureProducer()
    worker.dlq_producer = producer  # type: ignore[assignment]

    msg = _DummyMsg(
        topic="pipeline_jobs",
        partition=3,
        offset=12,
        key=b"pipeline-key",
        value=b'{"job_id":"job-1","pipeline_id":"pipe-1","db_name":"demo","output_dataset_name":"orders"}',
        timestamp_ms=1700000000999,
    )
    payload = worker._parse_payload(msg.value())

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        payload=payload,
        raw_payload=msg.value().decode("utf-8"),
        error="failed",
        attempt_count=4,
    )

    assert producer.calls, "expected a DLQ produce call"
    call = producer.calls[-1]
    payload_json = json.loads(call["value"].decode("utf-8"))
    assert payload_json["stage"] == "execute"
    assert payload_json["attempt_count"] == 4
    assert payload_json["worker"] == worker._pipeline_worker_name
    assert payload_json["job"]["job_id"] == "job-1"
    assert payload_json["job"]["pipeline_id"] == "pipe-1"


@pytest.mark.asyncio
async def test_objectify_worker_send_to_dlq_payload_shape() -> None:
    from objectify_worker.main import ObjectifyWorker

    worker = ObjectifyWorker()
    producer = _CaptureProducer()
    worker.dlq_producer = producer  # type: ignore[assignment]

    msg = _DummyMsg(
        topic="objectify_jobs",
        partition=1,
        offset=22,
        key=b"objectify-key",
        value=(
            b'{"job_id":"job-7","db_name":"demo","dataset_id":"dataset-1","dataset_version_id":"ver-1",'
            b'"target_class_id":"Order"}'
        ),
        timestamp_ms=1700000001111,
    )
    payload = worker._parse_payload(msg.value())

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        payload=payload,
        raw_payload=msg.value().decode("utf-8"),
        error="failed",
        attempt_count=6,
    )

    assert producer.calls, "expected a DLQ produce call"
    call = producer.calls[-1]
    payload_json = json.loads(call["value"].decode("utf-8"))
    assert payload_json["stage"] == "process_message"
    assert payload_json["attempt_count"] == 6
    assert payload_json["worker"] == "objectify-worker"
    assert payload_json["job"]["job_id"] == "job-7"
    assert payload_json["job"]["dataset_id"] == "dataset-1"


class _EnvelopeProducerOps:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.flushes: list[float] = []

    async def produce(self, **kwargs: Any) -> None:
        self.calls.append(dict(kwargs))

    async def flush(self, timeout_s: float) -> int:
        self.flushes.append(float(timeout_s))
        return 0


@pytest.mark.asyncio
async def test_projection_worker_send_to_dlq_uses_envelope_shape() -> None:
    from projection_worker.main import ProjectionWorker
    from shared.models.event_envelope import EventEnvelope

    worker = ProjectionWorker()
    producer_ops = _EnvelopeProducerOps()
    worker.dlq_producer_ops = producer_ops

    envelope = EventEnvelope(
        event_id="evt-9",
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="inst-9",
        metadata={"kind": "domain"},
        data={"name": "Alice"},
    )
    msg = _DummyMsg(
        topic="instance_events",
        partition=0,
        offset=11,
        value=envelope.model_dump_json().encode("utf-8"),
    )

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        payload=envelope,
        raw_payload=msg.value().decode("utf-8"),
        error="projection failed",
        attempt_count=2,
    )

    assert producer_ops.calls, "expected an envelope DLQ produce call"
    call = producer_ops.calls[-1]
    payload_json = json.loads(call["value"].decode("utf-8"))
    assert call["topic"] == worker.dlq_topic
    assert payload_json["event_type"] == "PROJECTION_FAILED"
    assert payload_json["metadata"]["kind"] == "projection_dlq"
    assert payload_json["metadata"]["dlq_attempt_count"] == 2


@pytest.mark.asyncio
async def test_connector_sync_worker_send_to_dlq_uses_envelope_shape() -> None:
    from connector_sync_worker.main import ConnectorSyncWorker
    from shared.models.event_envelope import EventEnvelope

    worker = ConnectorSyncWorker()
    producer_ops = _EnvelopeProducerOps()
    worker.dlq_producer_ops = producer_ops

    envelope = EventEnvelope(
        event_id="",
        event_type="CONNECTOR_SYNC_REQUESTED",
        aggregate_type="connector",
        aggregate_id="",
        metadata={"kind": "domain"},
        data={"connector_id": "conn-1"},
    )
    msg = _DummyMsg(
        topic="connector_sync",
        partition=0,
        offset=3,
        value=envelope.model_dump_json().encode("utf-8"),
    )

    await worker._send_to_dlq(  # type: ignore[attr-defined]
        msg=msg,
        payload=envelope,
        raw_payload=msg.value().decode("utf-8"),
        error="connector failed",
        attempt_count=3,
    )

    assert producer_ops.calls, "expected an envelope DLQ produce call"
    call = producer_ops.calls[-1]
    payload_json = json.loads(call["value"].decode("utf-8"))
    assert call["topic"] == worker.dlq_topic
    assert call["key"] == b"connector_sync"
    assert payload_json["metadata"]["kind"] == "connector_update_dlq"
    assert payload_json["metadata"]["dlq_attempt_count"] == 3
