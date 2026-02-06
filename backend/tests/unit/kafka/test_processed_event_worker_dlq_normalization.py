from __future__ import annotations

from typing import Any

import pytest

from shared.services.kafka.dlq_publisher import DlqPublishSpec
from shared.services.kafka.processed_event_worker import ProcessedEventKafkaWorker, RegistryKey


class _StubWorker(ProcessedEventKafkaWorker[dict[str, Any], None]):
    def __init__(self) -> None:
        self.consumer = None
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub_handler"
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


class _MsgWithHeaders:
    def headers(self):
        return [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]


class _KafkaMsg:
    def __init__(self) -> None:
        self._headers = [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]

    def topic(self) -> str:
        return "demo.topic"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 9

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes:
        return b'{"a":1}'

    def headers(self):
        return list(self._headers)

    def timestamp(self):
        return (0, 1700000000000)


class _Producer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def produce(self, topic: str, *, key: bytes, value: bytes, headers=None, **_kwargs: Any) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value, "headers": headers})

    def flush(self, *_args: Any, **_kwargs: Any) -> int:
        return 0


class _ParseErr(Exception):
    def __init__(self) -> None:
        super().__init__("parse-failed")
        self.stage = "parse_json"
        self.payload_text = '{"broken": }'
        self.payload_obj = {"broken": True}
        self.fallback_metadata = {"kind": "command"}
        self.cause = ValueError("invalid json")


def test_normalize_dlq_publish_inputs_uses_inferred_defaults() -> None:
    worker = _StubWorker()

    stage, payload_text, payload_obj, headers, metadata = worker._normalize_dlq_publish_inputs(
        msg=_MsgWithHeaders(),
        stage="",
        default_stage="process_command",
        raw_payload='{"k":"v"}',
        payload_text=None,
        payload_obj=None,
        kafka_headers=None,
        fallback_metadata=None,
        inferred_payload_obj={"a": 1},
        inferred_metadata={"kind": "command"},
        inferred_stage="validated_stage",
    )

    assert stage == "validated_stage"
    assert payload_text == '{"k":"v"}'
    assert payload_obj == {"a": 1}
    assert headers == [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]
    assert metadata == {"kind": "command"}


def test_normalize_dlq_publish_inputs_preserves_explicit_values() -> None:
    worker = _StubWorker()

    stage, payload_text, payload_obj, headers, metadata = worker._normalize_dlq_publish_inputs(
        msg=_MsgWithHeaders(),
        stage="explicit_stage",
        default_stage="process_command",
        raw_payload='{"ignored":"raw"}',
        payload_text="explicit_text",
        payload_obj={"explicit": True},
        kafka_headers=[("x", b"1")],
        fallback_metadata={"explicit_meta": True},
        inferred_payload_obj={"ignored": True},
        inferred_metadata={"ignored_meta": True},
        inferred_stage="ignored_stage",
    )

    assert stage == "explicit_stage"
    assert payload_text == "explicit_text"
    assert payload_obj == {"explicit": True}
    assert headers == [("x", b"1")]
    assert metadata == {"explicit_meta": True}


def test_parse_error_context_extracts_custom_error_fields() -> None:
    worker = _StubWorker()
    context = worker._parse_error_context(raw_payload='{"raw":1}', error=_ParseErr())

    assert context.stage == "parse_json"
    assert context.payload_text == '{"broken": }'
    assert context.payload_obj == {"broken": True}
    assert context.fallback_metadata == {"kind": "command"}
    assert str(context.cause) == "invalid json"


def test_fallback_metadata_from_raw_payload_extracts_metadata_dict() -> None:
    worker = _StubWorker()
    metadata = worker._fallback_metadata_from_raw_payload(
        '{"event_id":"evt-1","metadata":{"kind":"command","trace_id":"abc"}}'
    )
    assert metadata == {"kind": "command", "trace_id": "abc"}


def test_fallback_metadata_from_raw_payload_returns_none_for_invalid_payload() -> None:
    worker = _StubWorker()
    assert worker._fallback_metadata_from_raw_payload(None) is None
    assert worker._fallback_metadata_from_raw_payload("not-json") is None
    assert worker._fallback_metadata_from_raw_payload('{"metadata": "invalid"}') is None


@pytest.mark.asyncio
async def test_publish_parse_error_to_dlq_calls_sender() -> None:
    worker = _StubWorker()
    calls: list[dict[str, Any]] = []

    async def _sender(
        stage: str,
        cause_text: str,
        payload_text: str | None,
        payload_obj: dict[str, Any] | None,
        kafka_headers: Any,
        fallback_metadata: dict[str, Any] | None,
    ) -> None:
        calls.append(
            {
                "stage": stage,
                "cause_text": cause_text,
                "payload_text": payload_text,
                "payload_obj": payload_obj,
                "kafka_headers": kafka_headers,
                "fallback_metadata": fallback_metadata,
            }
        )

    await worker._publish_parse_error_to_dlq(
        msg=_MsgWithHeaders(),
        raw_payload='{"raw":1}',
        error=_ParseErr(),
        dlq_sender=_sender,
        publish_failure_message="publish failed: %s",
        invalid_payload_message="invalid payload: %s",
        raise_on_publish_failure=True,
    )

    assert len(calls) == 1
    assert calls[0]["stage"] == "parse_json"
    assert calls[0]["cause_text"] == "invalid json"
    assert calls[0]["payload_obj"] == {"broken": True}


@pytest.mark.asyncio
async def test_publish_parse_error_to_dlq_raise_toggle() -> None:
    worker = _StubWorker()

    async def _raising_sender(
        stage: str,
        cause_text: str,
        payload_text: str | None,
        payload_obj: dict[str, Any] | None,
        kafka_headers: Any,
        fallback_metadata: dict[str, Any] | None,
    ) -> None:
        raise RuntimeError("dlq failed")

    await worker._publish_parse_error_to_dlq(
        msg=_MsgWithHeaders(),
        raw_payload='{"raw":1}',
        error=ValueError("bad"),
        dlq_sender=_raising_sender,
        publish_failure_message="publish failed: %s",
        invalid_payload_message="invalid payload: %s",
        raise_on_publish_failure=False,
    )

    with pytest.raises(RuntimeError):
        await worker._publish_parse_error_to_dlq(
            msg=_MsgWithHeaders(),
            raw_payload='{"raw":1}',
            error=ValueError("bad"),
            dlq_sender=_raising_sender,
            publish_failure_message="publish failed: %s",
            invalid_payload_message="invalid payload: %s",
            raise_on_publish_failure=True,
        )


@pytest.mark.asyncio
async def test_publish_standard_dlq_record_missing_producer_modes() -> None:
    worker = _StubWorker()
    msg = _KafkaMsg()
    spec = DlqPublishSpec(dlq_topic="demo.dlq", service_name="demo-service")

    with pytest.raises(RuntimeError):
        await worker._publish_standard_dlq_record(
            producer=None,
            msg=msg,
            worker="demo-worker",
            dlq_spec=spec,
            error="boom",
            attempt_count=1,
            stage="process",
            payload_text='{"raw":1}',
            payload_obj=None,
            raise_on_missing_producer=True,
            missing_producer_message="DLQ producer not configured",
        )

    sent = await worker._publish_standard_dlq_record(
        producer=None,
        msg=msg,
        worker="demo-worker",
        dlq_spec=spec,
        error="boom",
        attempt_count=1,
        stage="process",
        payload_text='{"raw":1}',
        payload_obj=None,
        raise_on_missing_producer=False,
        missing_producer_message="DLQ producer not configured",
    )
    assert sent is False


@pytest.mark.asyncio
async def test_publish_standard_dlq_record_success() -> None:
    worker = _StubWorker()
    msg = _KafkaMsg()
    producer = _Producer()
    spec = DlqPublishSpec(dlq_topic="demo.dlq", service_name="demo-service")

    sent = await worker._publish_standard_dlq_record(
        producer=producer,
        msg=msg,
        worker="demo-worker",
        dlq_spec=spec,
        error="boom",
        attempt_count=2,
        stage="process",
        payload_text='{"raw":1}',
        payload_obj={"x": 1},
    )

    assert sent is True
    assert len(producer.calls) == 1
    assert producer.calls[0]["topic"] == "demo.dlq"


@pytest.mark.asyncio
async def test_send_standard_dlq_record_normalizes_and_invokes_publisher() -> None:
    worker = _StubWorker()
    msg = _KafkaMsg()
    captured: dict[str, Any] = {}

    async def _publisher(**kwargs: Any) -> None:
        captured.update(kwargs)

    await worker._send_standard_dlq_record(
        msg=msg,
        error="boom",
        attempt_count=3,
        stage="",
        default_stage="process_command",
        raw_payload='{"raw":1}',
        payload_text=None,
        payload_obj=None,
        kafka_headers=None,
        fallback_metadata=None,
        publisher=_publisher,
        inferred_payload_obj={"inferred": True},
        inferred_metadata={"kind": "command"},
        inferred_stage="validated_stage",
    )

    assert captured["stage"] == "validated_stage"
    assert captured["attempt_count"] == 3
    assert captured["payload_text"] == '{"raw":1}'
    assert captured["payload_obj"] == {"inferred": True}
    assert captured["fallback_metadata"] == {"kind": "command"}
    assert captured["kafka_headers"] == [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]
