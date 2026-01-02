import importlib.util

import pytest


if importlib.util.find_spec("opentelemetry.propagate") is None:  # pragma: no cover
    pytest.skip("opentelemetry-api not installed", allow_module_level=True)


@pytest.mark.unit
def test_kafka_headers_roundtrip_via_attached_context():
    from shared.observability.context_propagation import (
        attach_context_from_carrier,
        carrier_from_kafka_headers,
        kafka_headers_from_current_context,
    )

    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    tracestate = "congo=t61rcWkgMzE"
    baggage = "tenant=acme,feature=ingest"

    carrier = {"traceparent": traceparent, "tracestate": tracestate, "baggage": baggage}
    with attach_context_from_carrier(carrier, service_name="test"):
        headers = kafka_headers_from_current_context()

    roundtrip = carrier_from_kafka_headers(headers)
    assert roundtrip.get("traceparent") == traceparent
    assert roundtrip.get("tracestate") == tracestate
    assert roundtrip.get("baggage") == baggage


@pytest.mark.unit
def test_kafka_headers_from_envelope_metadata_only_emits_known_keys():
    from shared.observability.context_propagation import kafka_headers_from_envelope_metadata

    meta = {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "tracestate": "congo=t61rcWkgMzE",
        "baggage": "k=v",
        "other": "ignored",
    }
    headers = kafka_headers_from_envelope_metadata(meta)
    assert {k for k, _ in headers} == {"traceparent", "tracestate", "baggage"}

