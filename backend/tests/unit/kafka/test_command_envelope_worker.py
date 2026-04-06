from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from shared.models.event_envelope import EventEnvelope
from shared.services.kafka.processed_event_worker import (
    CommandEnvelopeKafkaWorker,
    CommandEnvelopePayload,
    CommandParseError,
    RegistryKey,
)


@dataclass
class _StubPayload(CommandEnvelopePayload):
    pass


class _StubCommandWorker(CommandEnvelopeKafkaWorker[_StubPayload, None]):
    command_payload_cls = _StubPayload

    def __init__(self) -> None:
        self.consumer = None
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub_handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10
        self.published_calls: list[dict[str, Any]] = []

    async def _process_payload(self, payload: _StubPayload) -> None:  # type: ignore[override]
        _ = payload
        return None

    async def _publish_to_dlq(self, **kwargs: Any) -> None:
        self.published_calls.append(dict(kwargs))


def test_command_envelope_worker_unwraps_registry_command() -> None:
    worker = _StubCommandWorker()
    envelope = EventEnvelope(
        event_type="CREATE_INSTANCE_REQUESTED",
        aggregate_type="instance",
        aggregate_id="agg-1",
        metadata={"kind": "command"},
        data={"command_id": "cmd-1", "payload": {"name": "Alice"}},
        sequence_number=7,
    )

    payload = worker._parse_payload(envelope.model_dump_json().encode("utf-8"))
    registry_key = worker._registry_key(payload)

    assert payload.command["aggregate_id"] == "agg-1"
    assert payload.command["event_id"] == envelope.event_id
    assert payload.command["sequence_number"] == 7
    assert registry_key == RegistryKey(event_id=envelope.event_id, aggregate_id="agg-1", sequence_number=7)


def test_command_envelope_worker_rejects_unexpected_envelope_kind() -> None:
    worker = _StubCommandWorker()
    envelope = EventEnvelope(
        event_type="INSTANCE_UPDATED",
        aggregate_type="instance",
        aggregate_id="agg-1",
        metadata={"kind": "domain"},
        data={"command_id": "cmd-1"},
    )

    with pytest.raises(CommandParseError) as exc_info:
        worker._parse_payload(envelope.model_dump_json().encode("utf-8"))

    assert exc_info.value.stage == "validate_envelope"
    assert "unexpected_envelope_kind:domain" in str(exc_info.value.cause)


@pytest.mark.asyncio
async def test_action_worker_keeps_non_command_envelope_for_process_time_skip() -> None:
    from action_worker.main import ActionWorker

    worker = ActionWorker()
    envelope = EventEnvelope(
        event_type="ACTION_APPLIED",
        aggregate_type="action",
        aggregate_id="db:demo",
        metadata={"kind": "domain"},
        data={"command_type": "EXECUTE_ACTION"},
    )

    payload = worker._parse_payload(envelope.model_dump_json().encode("utf-8"))

    assert payload.envelope is not None
    assert payload.envelope.event_id == envelope.event_id
    await worker._process_payload(payload)


@pytest.mark.asyncio
async def test_command_envelope_worker_default_send_to_dlq_uses_shared_contract() -> None:
    worker = _StubCommandWorker()
    envelope = EventEnvelope(
        event_type="CREATE_INSTANCE_REQUESTED",
        aggregate_type="instance",
        aggregate_id="agg-2",
        metadata={"kind": "command", "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
        data={"command_id": "cmd-2", "payload": {"name": "Bob"}},
        sequence_number=3,
    )
    raw_payload = envelope.model_dump_json()
    payload = worker._parse_payload(raw_payload.encode("utf-8"))

    class _Msg:
        def headers(self):
            return [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]

    await worker._send_to_dlq(
        msg=_Msg(),
        payload=payload,
        raw_payload=raw_payload,
        error="boom",
        attempt_count=2,
    )

    assert len(worker.published_calls) == 1
    published = worker.published_calls[0]
    assert published["stage"] == "process_command"
    assert published["attempt_count"] == 2
    assert published["payload_text"] == raw_payload
    assert published["payload_obj"]["event_id"] == envelope.event_id
    assert published["payload_obj"]["aggregate_id"] == "agg-2"
    assert published["fallback_metadata"]["kind"] == "command"
