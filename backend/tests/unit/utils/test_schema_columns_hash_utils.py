from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.utils.schema_columns import (
    extract_schema_column_names,
    extract_schema_columns,
    extract_schema_type_map,
)
from shared.utils.schema_hash import compute_schema_hash, compute_schema_hash_from_payload


def test_extract_schema_columns_supports_list_payload() -> None:
    payload = [{"name": "id", "type": "integer"}, "name"]
    columns = extract_schema_columns(payload)
    assert columns == [{"name": "id", "type": "integer"}, {"name": "name", "type": None}]


def test_extract_schema_columns_supports_dict_shapes() -> None:
    payload = {"fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "integer"}]}
    assert extract_schema_columns(payload) == [{"name": "a", "type": "string"}, {"name": "b", "type": "integer"}]


def test_extract_schema_names_and_types() -> None:
    payload = {"columns": [{"name": "id", "type": "int"}, {"name": "created_at", "type": "datetime"}]}
    assert extract_schema_column_names(payload) == ["id", "created_at"]
    assert extract_schema_type_map(payload, normalizer=normalize_schema_type) == {
        "id": "xsd:integer",
        "created_at": "xsd:dateTime",
    }


def test_compute_schema_hash_from_payload_matches_columns_hash() -> None:
    payload = {"columns": [{"name": "id", "type": "xsd:integer"}]}
    assert compute_schema_hash_from_payload(payload) == compute_schema_hash(payload["columns"])
    assert compute_schema_hash_from_payload({}) is None

