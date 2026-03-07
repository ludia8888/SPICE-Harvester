from bff.routers import object_types as object_types_router


def test_to_foundry_object_type_supports_camel_case_pk_spec_keys() -> None:
    resource = {
        "id": "Order",
        "spec": {
            "status": "ACTIVE",
            "pk_spec": {
                "primaryKey": ["order_id"],
                "titleKey": ["order_name"],
            },
        },
    }
    ontology_payload = {
        "properties": [
            {"name": "order_id", "type": "xsd:string"},
            {"name": "order_name", "type": "xsd:string"},
        ]
    }

    projected = object_types_router._to_foundry_object_type(resource, ontology_payload=ontology_payload)

    assert projected["primaryKey"] == "order_id"
    assert projected["titleProperty"] == "order_name"


def test_to_foundry_object_type_uses_shared_property_flags_when_pk_spec_missing() -> None:
    resource = {
        "id": "Order",
        "spec": {
            "status": "ACTIVE",
        },
    }
    ontology_payload = {
        "properties": [
            {"id": "order_id", "primaryKey": True, "type": "xsd:string"},
            {"name": "tenant_id", "primary_key": True, "type": "xsd:string"},
            {"name": "order_name", "titleKey": True, "type": "xsd:string"},
        ]
    }

    projected = object_types_router._to_foundry_object_type(resource, ontology_payload=ontology_payload)

    assert projected["primaryKey"] == "order_id"
    assert projected["titleProperty"] == "order_name"
