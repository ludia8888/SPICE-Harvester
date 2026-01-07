from shared.models.ontology import Property
from oms.services.ontology_interface_contract import collect_interface_contract_issues


def test_interface_contract_missing_property_is_reported():
    interface_index = {
        "IUser": {"spec": {"required_properties": [{"name": "email", "type": "xsd:string"}]}}
    }
    issues = collect_interface_contract_issues(
        ontology_id="User",
        metadata={"interfaces": ["IUser"]},
        properties=[Property(name="id", type="xsd:string", label="ID")],
        relationships=[],
        interface_index=interface_index,
    )
    assert any(issue.get("code") == "IFACE_MISSING_PROPERTY" for issue in issues)


def test_interface_contract_missing_interface_is_reported():
    issues = collect_interface_contract_issues(
        ontology_id="User",
        metadata={"interfaces": ["IUser"]},
        properties=[],
        relationships=[],
        interface_index={},
    )
    assert any(issue.get("code") == "IFACE_NOT_FOUND" for issue in issues)
