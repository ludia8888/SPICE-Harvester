from __future__ import annotations

from shared.services.pipeline.pipeline_schema_casts import extract_schema_casts


def test_extract_schema_casts_returns_empty_for_non_dict_inputs() -> None:
    assert extract_schema_casts(None) == []
    assert extract_schema_casts([]) == []
    assert extract_schema_casts("not-a-dict") == []


def test_extract_schema_casts_parses_columns_dicts_and_strings() -> None:
    schema = {
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "created_at", "type": "dateTime"},
            {"name": " ", "type": "string"},
            {"name": "empty_type", "type": None},
            "raw_col",
        ]
    }
    casts = extract_schema_casts(schema)
    assert {"column": "id", "type": "xsd:integer"} in casts
    assert {"column": "created_at", "type": "xsd:dateTime"} in casts
    assert {"column": "empty_type", "type": "xsd:string"} in casts
    assert {"column": "raw_col", "type": "xsd:string"} in casts
    assert not any(cast["column"] == "" for cast in casts)


def test_extract_schema_casts_falls_back_to_fields() -> None:
    schema = {"fields": [{"name": "name", "data_type": "string"}, "status"]}
    casts = extract_schema_casts(schema)
    assert {"column": "name", "type": "xsd:string"} in casts
    assert {"column": "status", "type": "xsd:string"} in casts


def test_extract_schema_casts_extracts_from_properties() -> None:
    schema = {"properties": {"a": {"type": "string"}, "b": {}}}
    casts = extract_schema_casts(schema)
    assert {cast["column"] for cast in casts} == {"a", "b"}
    assert all(cast["type"] == "xsd:string" for cast in casts)

