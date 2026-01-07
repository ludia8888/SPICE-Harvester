from __future__ import annotations

from bff.routers import ontology as ontology_router


def test_localized_to_string() -> None:
    assert ontology_router._localized_to_string({"en": "Name", "ko": "이름"}) == "Name"
    assert ontology_router._localized_to_string({"ko": "이름"}) == "이름"
    assert ontology_router._localized_to_string(123) == "123"


def test_transform_properties_for_oms() -> None:
    payload = {
        "properties": [
            {"label": {"en": "Title"}, "type": "STRING"},
            {"name": "rel", "type": "link", "target": "Customer"},
            {"name": "tags", "type": "array", "items": {"type": "link", "target": "Tag"}},
        ]
    }

    ontology_router._transform_properties_for_oms(payload)

    props = payload["properties"]
    assert props[0]["name"]
    assert props[0]["type"] == "xsd:string"
    assert props[1]["linkTarget"] == "Customer"
    assert props[2]["items"]["linkTarget"] == "Tag"


def test_build_source_schema_and_samples() -> None:
    preview = {
        "columns": ["a", "b"],
        "sample_data": [[1, 2], [3, 4]],
        "inferred_schema": [
            {"column_name": "a", "inferred_type": {"type": "integer", "confidence": 0.9}},
            {"name": "b", "type": "string"},
        ],
    }

    schema = ontology_router._build_source_schema_from_preview(preview)
    assert schema[0]["type"] == "xsd:integer"
    assert schema[0]["confidence"] == 0.9

    samples = ontology_router._build_sample_data_from_preview(preview)
    assert samples[0]["a"] == 1


def test_normalize_mapping_type_and_import_target() -> None:
    assert ontology_router._normalize_mapping_type("money") == "xsd:decimal"
    assert ontology_router._normalize_mapping_type("xsd:integer") == "xsd:integer"

    fields = [ontology_router.ImportTargetField(name="count", type="INTEGER")]
    types = ontology_router._extract_target_field_types_from_import_schema(fields)
    assert types["count"] == "xsd:integer"
