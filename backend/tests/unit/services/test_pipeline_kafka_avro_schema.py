from __future__ import annotations

import pytest

from shared.services.pipeline.pipeline_kafka_avro import (
    KafkaAvroSchemaRegistryReference,
    fetch_kafka_avro_schema_from_registry,
    resolve_inline_avro_schema,
    resolve_kafka_avro_schema_registry_reference,
    validate_kafka_avro_schema_config,
)


def test_resolve_inline_avro_schema_prefers_inline_metadata() -> None:
    schema = resolve_inline_avro_schema(
        read_config={"avroSchema": '{"type":"record","name":"Order","fields":[]}'}
    )
    assert schema.startswith("{")


def test_validate_kafka_avro_schema_requires_inline_or_registry() -> None:
    errors = validate_kafka_avro_schema_config(
        read_config={"value_format": "avro"},
        node_id="in",
    )
    assert (
        "input node in kafka value_format=avro requires read.avro_schema or read.schema_registry(url+subject+version)"
        in errors
    )


def test_validate_kafka_avro_schema_rejects_missing_registry_subject() -> None:
    errors = validate_kafka_avro_schema_config(
        read_config={
            "value_format": "avro",
            "schema_registry": {"url": "https://registry.example", "version": 3},
        },
        node_id="in",
    )
    assert "input node in kafka value_format=avro schema registry requires subject" in errors


def test_validate_kafka_avro_schema_rejects_invalid_registry_version() -> None:
    errors = validate_kafka_avro_schema_config(
        read_config={
            "value_format": "avro",
            "schema_registry": {"url": "https://registry.example", "subject": "orders-value", "version": "banana"},
        },
        node_id="in",
    )
    assert "input node in kafka value_format=avro schema registry version must be an integer >= 1" in errors


def test_validate_kafka_avro_schema_rejects_latest_registry_version_explicitly() -> None:
    errors = validate_kafka_avro_schema_config(
        read_config={
            "value_format": "avro",
            "schema_registry": {"url": "https://registry.example", "subject": "orders-value", "version": "latest"},
        },
        node_id="in",
    )
    assert (
        "input node in kafka value_format=avro schema registry version=latest is not allowed; pin an integer version >= 1"
        in errors
    )


def test_resolve_kafka_avro_schema_registry_reference_supports_options_aliases() -> None:
    reference = resolve_kafka_avro_schema_registry_reference(
        read_config={
            "options": {
                "schema.registry.url": "https://registry.example",
                "subject": "orders-value",
                "version": "9",
            }
        },
        node_id="in",
    )
    assert reference == KafkaAvroSchemaRegistryReference(
        url="https://registry.example",
        subject="orders-value",
        version=9,
    )
    assert reference.request_url.endswith("/subjects/orders-value/versions/9")


def test_fetch_kafka_avro_schema_from_registry_parses_schema_payload(monkeypatch) -> None:
    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):  # noqa: ANN201
            return {"schemaType": "AVRO", "schema": '{"type":"record","name":"Order","fields":[]}'}

    def _fake_get(url, timeout, headers):  # noqa: ANN001, ANN201
        assert url.endswith("/subjects/orders-value/versions/4")
        assert float(timeout) == 12.0
        assert headers == {"Authorization": "Bearer token"}
        return _Response()

    monkeypatch.setattr("shared.services.pipeline.pipeline_kafka_avro.httpx.get", _fake_get)
    schema = fetch_kafka_avro_schema_from_registry(
        reference=KafkaAvroSchemaRegistryReference(
            url="https://registry.example",
            subject="orders-value",
            version=4,
        ),
        timeout_seconds=12.0,
        headers={"Authorization": "Bearer token"},
    )
    assert "Order" in schema
