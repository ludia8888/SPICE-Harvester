from bff.services.object_type_contract_service import (
    _build_target_schema_from_ontology,
    _extract_ontology_property_names,
    _infer_pk_spec_from_ontology,
)


def test_infer_pk_spec_from_ontology_uses_shared_property_flags_with_id_fallback():
    ontology_payload = {
        "properties": [
            {"id": "customer_id", "primaryKey": True, "titleKey": True},
            {"name": "display_name", "titleKey": True},
        ]
    }

    pk_spec = _infer_pk_spec_from_ontology(ontology_payload=ontology_payload, class_id="Customer")

    assert pk_spec == {
        "primary_key": ["customer_id"],
        "title_key": ["customer_id", "display_name"],
    }


def test_infer_pk_spec_from_ontology_preserves_service_fallbacks_when_flags_absent():
    ontology_payload = {
        "properties": [
            {"name": "order_id"},
            {"name": "name"},
            {"name": "status"},
        ]
    }

    pk_spec = _infer_pk_spec_from_ontology(ontology_payload=ontology_payload, class_id="Order")

    assert pk_spec == {
        "primary_key": ["order_id"],
        "title_key": ["name"],
    }


def test_extract_ontology_property_names_uses_id_when_name_missing():
    ontology_payload = {
        "data": {
            "properties": [
                {"id": "order_id"},
                {"name": "display_name"},
            ]
        }
    }

    assert _extract_ontology_property_names(ontology_payload) == {"order_id", "display_name"}


def test_build_target_schema_from_ontology_uses_id_when_name_missing():
    ontology_payload = {
        "properties": [
            {"id": "order_id", "type": "xsd:string"},
            {"name": "display_name", "type": "xsd:string"},
        ]
    }

    assert _build_target_schema_from_ontology(ontology_payload) == [
        {"name": "order_id", "type": "xsd:string"},
        {"name": "display_name", "type": "xsd:string"},
    ]
