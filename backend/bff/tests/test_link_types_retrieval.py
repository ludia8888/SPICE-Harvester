import pytest
from bff.routers.link_types_read import _extract_resources, _to_foundry_outgoing_link_type


@pytest.mark.unit
def test_extract_resources_unwraps_nested_data():
    payload = {
        "status": "success",
        "data": {
            "resources": [
                {"id": "owned_by", "from": "Account", "to": "User"},
                {"id": "belongs_to", "from": "Order", "to": "Customer"},
            ]
        },
    }
    resources = _extract_resources(payload)
    assert len(resources) == 2
    assert resources[0]["id"] == "owned_by"
    assert resources[1]["id"] == "belongs_to"


@pytest.mark.unit
def test_to_foundry_outgoing_link_type_maps_link_type_fields():
    resource = {
        "id": "owned_by",
        "label": {"en": "Owned By"},
        "rid": "ri.spice.main.link-type.testdb.Account.owned_by",
        "spec": {
            "from": "Account",
            "to": "User",
            "cardinality": "n:1",
            "relationship_spec": {"fk_column": "user_id"},
        },
    }
    result = _to_foundry_outgoing_link_type(resource, source_object_type="Account")
    assert result == {
        "apiName": "owned_by",
        "objectTypeApiName": "User",
        "displayName": "Owned By",
        "status": "ACTIVE",
        "cardinality": "ONE",
        "foreignKeyPropertyApiName": "user_id",
        "linkTypeRid": "ri.spice.main.link-type.testdb.Account.owned_by",
    }


@pytest.mark.unit
def test_to_foundry_outgoing_link_type_filters_non_matching_source():
    resource = {
        "id": "owned_by",
        "spec": {
            "from": "Order",
            "to": "User",
            "cardinality": "n:1",
        },
    }
    assert _to_foundry_outgoing_link_type(resource, source_object_type="Account") is None
