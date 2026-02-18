import pytest
from bff.routers.link_types_read import (
    _extract_resources,
    _invert_cardinality,
    _to_foundry_incoming_link_type,
    _to_foundry_outgoing_link_type,
)


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


# ---------------------------------------------------------------------------
# _invert_cardinality
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1:1", "ONE"),
        ("n:1", "MANY"),   # many→one outgoing ⇒ MANY incoming
        ("m:1", "MANY"),
        ("1:n", "ONE"),    # one→many outgoing ⇒ ONE incoming
        ("n:n", "MANY"),
        ("n:m", "MANY"),
        ("many", "MANY"),
        ("one", "MANY"),   # 'one' outgoing cardinality ⇒ MANY incoming
        ("n:1+", "MANY"),
        (None, None),
        ("", None),
    ],
)
def test_invert_cardinality(raw, expected):
    assert _invert_cardinality(raw) == expected


# ---------------------------------------------------------------------------
# _to_foundry_incoming_link_type
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_to_foundry_incoming_link_type_maps_fields():
    """``owned_by`` goes from Account → User (n:1).

    Viewed from User's incoming perspective:
    - objectTypeApiName = Account (the source)
    - cardinality inverted: n:1 → MANY
    """
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
    result = _to_foundry_incoming_link_type(resource, target_object_type="User")
    assert result == {
        "apiName": "owned_by",
        "objectTypeApiName": "Account",
        "displayName": "Owned By",
        "status": "ACTIVE",
        "cardinality": "MANY",
        "linkTypeRid": "ri.spice.main.link-type.testdb.Account.owned_by",
    }


@pytest.mark.unit
def test_to_foundry_incoming_link_type_filters_non_matching_target():
    resource = {
        "id": "owned_by",
        "spec": {
            "from": "Account",
            "to": "User",
            "cardinality": "n:1",
        },
    }
    # Account is not the *target*, so this should return None.
    assert _to_foundry_incoming_link_type(resource, target_object_type="Account") is None


@pytest.mark.unit
def test_to_foundry_incoming_link_type_one_to_many_inverts_to_one():
    """``contains`` goes from Order → Product (1:n).

    Viewed from Product's incoming perspective:
    - objectTypeApiName = Order (the source)
    - cardinality inverted: 1:n → ONE
    """
    resource = {
        "id": "contains",
        "label": {"en": "Contains"},
        "rid": "ri.spice.main.link-type.testdb.Order.contains",
        "spec": {
            "from": "Order",
            "to": "Product",
            "cardinality": "1:n",
        },
    }
    result = _to_foundry_incoming_link_type(resource, target_object_type="Product")
    assert result is not None
    assert result["objectTypeApiName"] == "Order"
    assert result["cardinality"] == "ONE"


@pytest.mark.unit
def test_to_foundry_incoming_link_type_no_foreign_key():
    """Incoming link types should not expose foreignKeyPropertyApiName."""
    resource = {
        "id": "owned_by",
        "spec": {
            "from": "Account",
            "to": "User",
            "cardinality": "n:1",
            "relationship_spec": {"fk_column": "user_id"},
        },
    }
    result = _to_foundry_incoming_link_type(resource, target_object_type="User")
    assert result is not None
    # The incoming side does not hold the FK; it should not be present.
    assert "foreignKeyPropertyApiName" not in result
