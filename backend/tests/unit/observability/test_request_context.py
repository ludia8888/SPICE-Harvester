import logging

import pytest


@pytest.mark.unit
def test_parse_baggage_header_best_effort():
    from shared.observability.request_context import parse_baggage_header

    assert parse_baggage_header(None) == {}
    assert parse_baggage_header("") == {}

    parsed = parse_baggage_header("correlation_id=abc,request_id=req-1;prop=1,foo=bar")
    assert parsed["correlation_id"] == "abc"
    assert parsed["request_id"] == "req-1"
    assert parsed["foo"] == "bar"


@pytest.mark.unit
def test_trace_context_filter_includes_request_context_fields():
    from shared.observability.logging import TraceContextFilter
    from shared.observability.request_context import request_context

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    with request_context(request_id="req-1", correlation_id="corr-1", db_name="db1", principal="user:1"):
        filt = TraceContextFilter()
        assert filt.filter(record) is True

    assert record.request_id == "req-1"
    assert record.correlation_id == "corr-1"
    assert record.db_name == "db1"
    assert record.principal == "user:1"


@pytest.mark.unit
def test_carrier_from_envelope_metadata_appends_baggage_from_fields():
    from shared.observability.context_propagation import carrier_from_envelope_metadata

    meta = {
        "correlation_id": "corr-123",
        "request_id": "req-123",
        "db_name": "db1",
        "baggage": "tenant=acme",
    }

    carrier = carrier_from_envelope_metadata(meta)
    assert carrier["correlation_id"] == "corr-123"
    assert carrier["request_id"] == "req-123"
    assert carrier["db_name"] == "db1"
    assert "tenant=acme" in carrier.get("baggage", "")
    assert "correlation_id=corr-123" in carrier.get("baggage", "")
    assert "request_id=req-123" in carrier.get("baggage", "")
    assert "db_name=db1" in carrier.get("baggage", "")


@pytest.mark.unit
def test_attach_context_from_kafka_uses_fallback_metadata_for_contextvars():
    from shared.observability.context_propagation import attach_context_from_kafka
    from shared.observability.request_context import get_correlation_id, get_db_name, get_request_id

    fallback = {"request_id": "req-1", "correlation_id": "corr-1", "db_name": "db1"}
    with attach_context_from_kafka(kafka_headers=None, fallback_metadata=fallback, service_name="test"):
        assert get_request_id() == "req-1"
        assert get_correlation_id() == "corr-1"
        assert get_db_name() == "db1"


@pytest.mark.unit
def test_event_envelope_from_command_includes_context_correlation_id():
    from shared.models.commands import CommandType, DatabaseCommand
    from shared.models.event_envelope import EventEnvelope
    from shared.observability.request_context import request_context

    command = DatabaseCommand(command_type=CommandType.CREATE_DATABASE, aggregate_id="db1", payload={})

    with request_context(request_id="req-ctx", correlation_id="corr-ctx"):
        envelope = EventEnvelope.from_command(command)

    assert envelope.metadata.get("request_id") == "req-ctx"
    assert envelope.metadata.get("correlation_id") == "corr-ctx"


@pytest.mark.unit
def test_event_envelope_from_command_prefers_command_metadata_over_context():
    from shared.models.commands import CommandType, DatabaseCommand
    from shared.models.event_envelope import EventEnvelope
    from shared.observability.request_context import request_context

    command = DatabaseCommand(
        command_type=CommandType.CREATE_DATABASE,
        aggregate_id="db1",
        payload={},
        metadata={"correlation_id": "corr-cmd", "request_id": "req-cmd"},
    )

    with request_context(request_id="req-ctx", correlation_id="corr-ctx"):
        envelope = EventEnvelope.from_command(command)

    assert envelope.metadata.get("request_id") == "req-cmd"
    assert envelope.metadata.get("correlation_id") == "corr-cmd"

