from __future__ import annotations

import pytest

from instance_worker.main import StrictInstanceWorker
from shared.models.event_envelope import EventEnvelope


@pytest.mark.asyncio
async def test_extract_payload_from_message_success() -> None:
    worker = StrictInstanceWorker()
    envelope = EventEnvelope(
        event_type="COMMAND",
        aggregate_type="instance",
        aggregate_id="agg-1",
        data={"aggregate_id": "agg-1", "payload": {"name": "x"}},
        metadata={"kind": "command"},
    )

    command = await worker.extract_payload_from_message(envelope.model_dump(mode="json"))
    assert command["aggregate_id"] == "agg-1"
    assert command["event_id"] == envelope.event_id


@pytest.mark.asyncio
async def test_extract_payload_from_message_rejects_non_command() -> None:
    worker = StrictInstanceWorker()
    envelope = EventEnvelope(
        event_type="DOMAIN",
        aggregate_type="instance",
        aggregate_id="agg-1",
        data={},
        metadata={"kind": "domain"},
    )

    with pytest.raises(ValueError):
        await worker.extract_payload_from_message(envelope.model_dump(mode="json"))


def test_primary_key_and_objectify_helpers() -> None:
    worker = StrictInstanceWorker()

    payload = {"customer_id": "c1"}
    assert worker.get_primary_key_value("Customer", payload) == "c1"

    payload = {"id": "x", "order_id": "o1"}
    assert worker.get_primary_key_value("Order", payload) == "o1"

    with pytest.raises(ValueError):
        worker.get_primary_key_value("Order", {"name": "x"}, allow_generate=False)

    assert worker._is_objectify_command({"metadata": {"mapping_spec_id": "m1"}}) is True
    assert worker._is_objectify_command({"metadata": {}}) is False


def test_retryable_error_detection() -> None:
    assert StrictInstanceWorker._is_retryable_error(Exception("ConnectionError: node unreachable")) is True
    assert StrictInstanceWorker._is_retryable_error(Exception("timeout waiting for response")) is True
    assert StrictInstanceWorker._is_retryable_error(Exception("409 conflict on version")) is True
    assert StrictInstanceWorker._is_retryable_error(Exception("invalid payload")) is False
