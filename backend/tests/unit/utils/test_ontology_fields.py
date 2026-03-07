from __future__ import annotations

from shared.utils.ontology_fields import (
    extract_ontology_fields,
    extract_ontology_properties,
    extract_ontology_relationships,
    list_ontology_properties,
)


def test_extract_ontology_fields_supports_data_wrapped_payload() -> None:
    payload = {
        "data": {
            "properties": [
                {"name": "id", "type": "xsd:string"},
                {"name": "name", "type": "xsd:string"},
            ],
            "relationships": [
                {"predicate": "owns", "target": "Asset"},
                {"name": "managed_by", "target": "User"},
            ],
        }
    }

    prop_map, rel_map = extract_ontology_fields(payload)

    assert list(prop_map) == ["id", "name"]
    assert list(rel_map) == ["owns", "managed_by"]
    assert prop_map["id"]["type"] == "xsd:string"
    assert rel_map["owns"]["target"] == "Asset"


def test_list_ontology_properties_preserves_insertion_order() -> None:
    payload = {
        "properties": [
            {"id": "first"},
            {"name": "second"},
            {"name": "third"},
        ]
    }

    properties = list_ontology_properties(payload)

    assert [item.get("name") or item.get("id") for item in properties] == ["first", "second", "third"]


def test_extract_ontology_helpers_ignore_invalid_entries() -> None:
    payload = {
        "properties": [{"id": "legacy_valid"}, "bad", {"type": "xsd:string"}],
        "relationships": [{"predicate": "linked_to"}, None, {"target": "User"}],
    }

    assert list(extract_ontology_properties(payload)) == ["legacy_valid"]
    assert list(extract_ontology_relationships(payload)) == ["linked_to"]


def test_extract_ontology_properties_uses_id_when_name_missing() -> None:
    payload = {
        "properties": [
            {"id": "order_id", "primaryKey": True},
            {"name": "tenant_id", "primary_key": True},
        ]
    }

    prop_map = extract_ontology_properties(payload)

    assert list(prop_map) == ["order_id", "tenant_id"]
