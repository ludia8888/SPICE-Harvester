from __future__ import annotations

import pytest

from shared.services.pipeline.output_plugins import (
    OUTPUT_KIND_DATASET,
    OUTPUT_KIND_GEOTEMPORAL,
    OUTPUT_KIND_MEDIA,
    OUTPUT_KIND_ONTOLOGY,
    OUTPUT_KIND_VIRTUAL,
    normalize_output_kind,
    resolve_output_kind,
    validate_output_payload,
)


@pytest.mark.unit
def test_normalize_output_kind_supports_legacy_aliases() -> None:
    assert normalize_output_kind("unknown") == OUTPUT_KIND_DATASET
    assert normalize_output_kind("object") == OUTPUT_KIND_ONTOLOGY
    assert normalize_output_kind("link") == OUTPUT_KIND_ONTOLOGY


@pytest.mark.unit
def test_resolve_output_kind_reports_alias_usage() -> None:
    resolved = resolve_output_kind("object")
    assert resolved.raw_kind == "object"
    assert resolved.normalized_kind == OUTPUT_KIND_ONTOLOGY
    assert resolved.used_alias is True


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
def test_validate_output_payload_dataset_requires_pk_for_append_only_new_rows() -> None:
    errors = validate_output_payload(
        kind="dataset",
        payload={"write_mode": "append_only_new_rows"},
    )
    assert errors == ["write_mode=append_only_new_rows requires primary_key_columns"]


@pytest.mark.unit
def test_validate_output_payload_dataset_requires_post_filtering_for_snapshot_remove() -> None:
    errors = validate_output_payload(
        kind="dataset",
        payload={
            "write_mode": "snapshot_replace_and_remove",
            "primary_key_columns": ["id"],
        },
    )
    assert errors == ["write_mode=snapshot_replace_and_remove requires post_filtering_column"]


@pytest.mark.unit
def test_validate_output_payload_dataset_rejects_unsupported_output_format() -> None:
    errors = validate_output_payload(
        kind="dataset",
        payload={"output_format": "xml"},
    )
    assert errors == ["output_format must be one of: avro|csv|json|orc|parquet"]


@pytest.mark.unit
def test_validate_output_payload_dataset_accepts_full_metadata() -> None:
    assert (
        validate_output_payload(
            kind="dataset",
            payload={
                "write_mode": "snapshot_replace_and_remove",
                "primary_key_columns": ["id"],
                "post_filtering_column": "is_deleted",
                "output_format": "avro",
                "partition_by": ["ds"],
            },
        )
        == []
    )


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
def test_validate_output_payload_virtual_rejects_dataset_write_settings() -> None:
    errors = validate_output_payload(
        kind=OUTPUT_KIND_VIRTUAL,
        payload={
            "query_sql": "select * from t",
            "refresh_mode": "on_read",
            "write_mode": "append_only_new_rows",
            "partition_by": ["ds"],
        },
    )
    assert errors == [
        "virtual output does not support dataset write settings: partition_by, write_mode"
    ]


@pytest.mark.unit
def test_normalize_output_kind_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError):
        normalize_output_kind("not-a-kind")
