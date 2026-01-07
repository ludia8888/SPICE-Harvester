from __future__ import annotations

from objectify_worker.main import ObjectifyWorker
from shared.services.sheet_import_service import FieldMapping


def test_objectify_worker_field_helpers() -> None:
    worker = ObjectifyWorker()

    payload = {
        "properties": [{"name": "id", "type": "xsd:string"}],
        "relationships": [{"predicate": "owned_by"}],
    }
    prop_map, rels = worker._extract_ontology_fields(payload)
    assert "id" in prop_map
    assert "owned_by" in rels

    constraints = worker._normalize_constraints({"min": 1, "enumValues": ["A"]}, raw_type="email")
    assert constraints["minimum"] == 1
    assert constraints["enum"] == ["A"]
    assert constraints["format"] == "email"

    assert worker._resolve_import_type("integer") == "xsd:integer"
    assert worker._resolve_import_type("unknown") is None

    message = worker._validate_value_constraints("B", constraints={"enum": ["A"]}, raw_type="string")
    assert message and "Value must be one of" in message

    mappings = [FieldMapping(source_field="a", target_field="id")]
    mapping = worker._map_mappings_by_target(mappings)
    assert mapping["id"] == ["a"]

    assert worker._normalize_pk_fields("a,b") == ["a", "b"]
    assert worker._normalize_pk_fields(["a", " "]) == ["a"]

    assert worker._hash_payload({"a": 1})


def test_objectify_worker_row_key_derivation() -> None:
    worker = ObjectifyWorker()
    row = ["pk-1", "value"]
    col_index = {"pk": 0}

    key = worker._derive_row_key(
        columns=["pk", "value"],
        col_index=col_index,
        row=row,
        instance={"pk": "pk-1"},
        pk_fields=["pk"],
        pk_targets=[],
    )
    assert key == "target:pk-1"

    key_from_row = worker._derive_row_key(
        columns=["pk", "value"],
        col_index=col_index,
        row=row,
        instance={},
        pk_fields=["pk"],
        pk_targets=[],
    )
    assert key_from_row == "source:pk-1"
