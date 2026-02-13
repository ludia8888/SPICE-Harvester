from __future__ import annotations

import pytest

from shared.services.pipeline.output_plugins import (
    OUTPUT_KIND_DATASET,
    OUTPUT_KIND_GEOTEMPORAL,
    OUTPUT_KIND_MEDIA,
    OUTPUT_KIND_ONTOLOGY,
    OUTPUT_KIND_VIRTUAL,
    normalize_output_kind,
    validate_output_payload,
)


@pytest.mark.unit
def test_normalize_output_kind_supports_legacy_aliases() -> None:
    assert normalize_output_kind("unknown") == OUTPUT_KIND_DATASET
    assert normalize_output_kind("object") == OUTPUT_KIND_ONTOLOGY
    assert normalize_output_kind("link") == OUTPUT_KIND_ONTOLOGY


@pytest.mark.unit
def test_validate_output_payload_ontology_object_requires_target_class() -> None:
    errors = validate_output_payload(kind="ontology", payload={})
    assert errors == ["target_class_id is required"]


@pytest.mark.unit
def test_validate_output_payload_ontology_link_requires_full_metadata() -> None:
    errors = validate_output_payload(
        kind="ontology",
        payload={
            "relationship_spec_type": "link",
            "target_class_id": "Target",
            "source_class_id": "Source",
            "predicate": "relatedTo",
        },
    )
    assert errors
    assert "missing required link metadata" in errors[0]


@pytest.mark.unit
def test_validate_output_payload_dataset_has_no_required_fields() -> None:
    assert validate_output_payload(kind="dataset", payload={}) == []


@pytest.mark.unit
def test_validate_output_payload_geotemporal_requires_metadata() -> None:
    errors = validate_output_payload(kind=OUTPUT_KIND_GEOTEMPORAL, payload={})
    assert errors
    assert "missing required metadata" in errors[0]


@pytest.mark.unit
def test_validate_output_payload_geotemporal_accepts_camel_case() -> None:
    assert (
        validate_output_payload(
            kind=OUTPUT_KIND_GEOTEMPORAL,
            payload={
                "timeColumn": "event_time",
                "geometryColumn": "geom",
                "geometryFormat": "geojson",
            },
        )
        == []
    )


@pytest.mark.unit
def test_validate_output_payload_media_requires_type_enum() -> None:
    errors = validate_output_payload(
        kind=OUTPUT_KIND_MEDIA,
        payload={
            "media_uri_column": "uri",
            "media_type": "binary",
        },
    )
    assert errors == ["media_type must be one of: image|video|audio|document"]


@pytest.mark.unit
def test_validate_output_payload_virtual_requires_refresh_mode_enum() -> None:
    errors = validate_output_payload(
        kind=OUTPUT_KIND_VIRTUAL,
        payload={
            "query_sql": "select * from t",
            "refresh_mode": "manual",
        },
    )
    assert errors == ["refresh_mode must be one of: on_read|scheduled"]


@pytest.mark.unit
def test_validate_output_payload_virtual_accepts_required_values() -> None:
    assert (
        validate_output_payload(
            kind=OUTPUT_KIND_VIRTUAL,
            payload={
                "query_sql": "select 1",
                "refresh_mode": "scheduled",
            },
        )
        == []
    )


@pytest.mark.unit
def test_normalize_output_kind_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError):
        normalize_output_kind("not-a-kind")
