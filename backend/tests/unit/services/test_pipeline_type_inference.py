from __future__ import annotations

from shared.services.pipeline.pipeline_type_inference import common_join_key_type, infer_xsd_type_with_confidence


def test_infer_xsd_type_with_confidence_integer() -> None:
    result = infer_xsd_type_with_confidence(["1", "2", "003"])
    assert result.suggested_type == "xsd:integer"
    assert result.confidence >= 0.95


def test_infer_xsd_type_with_confidence_decimal() -> None:
    result = infer_xsd_type_with_confidence(["1.25", "2.00", "3.5"])
    assert result.suggested_type == "xsd:decimal"
    assert result.confidence >= 0.95


def test_infer_xsd_type_with_confidence_boolean() -> None:
    result = infer_xsd_type_with_confidence(["true", "false", "TRUE"])
    assert result.suggested_type == "xsd:boolean"
    assert result.confidence >= 0.95


def test_infer_xsd_type_with_confidence_datetime() -> None:
    result = infer_xsd_type_with_confidence(
        [
            "2024-01-01T12:00:00Z",
            "2024-01-02T00:00:00+00:00",
            "2024-01-03 10:30:00",
        ]
    )
    assert result.suggested_type == "xsd:dateTime"
    assert result.confidence >= 0.95


def test_infer_xsd_type_with_confidence_falls_back_to_string_for_mixed_values() -> None:
    result = infer_xsd_type_with_confidence(["1", "two", "3"])
    assert result.suggested_type == "xsd:string"
    assert 0.0 < result.confidence < 0.95


def test_common_join_key_type_biases_to_string_on_mismatch() -> None:
    assert common_join_key_type("xsd:integer", "xsd:string") == "xsd:string"
    assert common_join_key_type("xsd:integer", "xsd:decimal") == "xsd:string"

