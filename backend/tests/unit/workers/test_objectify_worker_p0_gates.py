import pytest

from objectify_worker.main import ObjectifyWorker
from shared.services.sheet_import_service import FieldMapping


def _build_instances(
    worker: ObjectifyWorker,
    *,
    columns,
    rows,
    mappings,
    target_field_types=None,
    mapping_sources=None,
    sources_by_target=None,
    required_targets=None,
    pk_targets=None,
    pk_fields=None,
    field_constraints=None,
    field_raw_types=None,
    seen_row_keys=None,
):
    if target_field_types is None:
        target_field_types = {m.target_field: "xsd:string" for m in mappings}
    if mapping_sources is None:
        mapping_sources = [m.source_field for m in mappings]
    if sources_by_target is None:
        sources_by_target = worker._map_mappings_by_target(mappings)
    if required_targets is None:
        required_targets = set()
    if pk_targets is None:
        pk_targets = []
    if pk_fields is None:
        pk_fields = []
    if field_constraints is None:
        field_constraints = {}
    if field_raw_types is None:
        field_raw_types = {}
    return worker._build_instances_with_validation(
        columns=columns,
        rows=rows,
        row_offset=0,
        mappings=mappings,
        target_field_types=target_field_types,
        mapping_sources=mapping_sources,
        sources_by_target=sources_by_target,
        required_targets=required_targets,
        pk_targets=pk_targets,
        pk_fields=pk_fields,
        field_constraints=field_constraints,
        field_raw_types=field_raw_types,
        seen_row_keys=seen_row_keys,
    )


def test_missing_source_column_is_fatal():
    worker = ObjectifyWorker()
    mappings = [FieldMapping(source_field="missing", target_field="id")]
    result = _build_instances(
        worker,
        columns=["id"],
        rows=[[1]],
        mappings=mappings,
    )

    assert result["fatal"] is True
    assert result["instances"] == []
    assert result["errors"][0]["code"] == "SOURCE_FIELD_MISSING"


def test_primary_key_missing_when_source_blank():
    worker = ObjectifyWorker()
    mappings = [
        FieldMapping(source_field="id", target_field="id"),
        FieldMapping(source_field="name", target_field="name"),
    ]
    result = _build_instances(
        worker,
        columns=["id", "name"],
        rows=[["", "Alice"]],
        mappings=mappings,
        pk_targets=["id"],
        pk_fields=["id"],
    )

    assert any(err.get("code") == "PRIMARY_KEY_MISSING" for err in result["errors"])
    assert 0 in result["error_row_indices"]


def test_required_field_missing_is_reported():
    worker = ObjectifyWorker()
    mappings = [
        FieldMapping(source_field="email", target_field="email"),
        FieldMapping(source_field="name", target_field="name"),
    ]
    result = _build_instances(
        worker,
        columns=["email", "name"],
        rows=[["", "Alice"]],
        mappings=mappings,
        required_targets={"email"},
    )

    assert any(err.get("code") == "REQUIRED_FIELD_MISSING" for err in result["errors"])
    assert 0 in result["error_row_indices"]


def test_value_constraints_fail_fast():
    worker = ObjectifyWorker()
    mappings = [FieldMapping(source_field="status", target_field="status")]
    result = _build_instances(
        worker,
        columns=["status"],
        rows=[["unknown"]],
        mappings=mappings,
        field_constraints={"status": {"enum": ["active", "inactive"]}},
        field_raw_types={"status": "string"},
    )

    assert any(err.get("code") == "VALUE_CONSTRAINT_FAILED" for err in result["errors"])
    assert 0 in result["error_row_indices"]


@pytest.mark.parametrize(
    ("raw_type", "format_hint", "value"),
    [
        ("email", "email", "not-an-email"),
        ("url", "uri", "not-a-url"),
        ("uuid", "uuid", "not-a-uuid"),
    ],
)
def test_value_constraints_format_failures(raw_type, format_hint, value):
    worker = ObjectifyWorker()
    mappings = [FieldMapping(source_field="field", target_field="field")]
    result = _build_instances(
        worker,
        columns=["field"],
        rows=[[value]],
        mappings=mappings,
        field_constraints={"field": {"format": format_hint}},
        field_raw_types={"field": raw_type},
    )

    assert any(err.get("code") == "VALUE_CONSTRAINT_FAILED" for err in result["errors"])
    assert 0 in result["error_row_indices"]


def test_value_constraints_min_length_enforced():
    worker = ObjectifyWorker()
    mappings = [FieldMapping(source_field="code", target_field="code")]
    result = _build_instances(
        worker,
        columns=["code"],
        rows=[["ab"]],
        mappings=mappings,
        field_constraints={"code": {"minLength": 3}},
        field_raw_types={"code": "string"},
    )

    assert any(err.get("code") == "VALUE_CONSTRAINT_FAILED" for err in result["errors"])
    assert 0 in result["error_row_indices"]


def test_value_constraints_pattern_enforced():
    worker = ObjectifyWorker()
    mappings = [FieldMapping(source_field="sku", target_field="sku")]
    result = _build_instances(
        worker,
        columns=["sku"],
        rows=[["aa11"]],
        mappings=mappings,
        field_constraints={"sku": {"pattern": "^[A-Z]{2}\\d{2}$"}},
        field_raw_types={"sku": "string"},
    )

    assert any(err.get("code") == "VALUE_CONSTRAINT_FAILED" for err in result["errors"])
    assert 0 in result["error_row_indices"]


def test_duplicate_primary_key_is_blocked():
    worker = ObjectifyWorker()
    mappings = [FieldMapping(source_field="id", target_field="id")]
    result = _build_instances(
        worker,
        columns=["id"],
        rows=[["1"], ["1"]],
        mappings=mappings,
        pk_targets=["id"],
        pk_fields=["id"],
        seen_row_keys=set(),
    )

    assert any(err.get("code") == "PRIMARY_KEY_DUPLICATE" for err in result["errors"])
    assert 1 in result["error_row_indices"]


def test_instance_id_requires_row_key():
    worker = ObjectifyWorker()
    instances = [{"name": "Alpha"}]

    with pytest.raises(ValueError):
        worker._ensure_instance_ids(
            instances,
            class_id="Item",
            stable_seed="seed",
            mapping_spec_version=1,
            row_keys=[None],
        )
