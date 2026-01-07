import json

import pytest

from oms.routers.ontology import (
    _apply_shared_properties,
    _collect_interface_issues,
    _extract_group_refs,
    _validate_group_refs,
    _validate_relationships_gate,
    _validate_value_type_refs,
)
from shared.models.ontology import Property


class _FakeResourceService:
    def __init__(self, resources):
        self._resources = resources

    async def get_resource(self, db_name, *, branch, resource_type, resource_id):
        return self._resources.get((resource_type, resource_id))


class _FakeTerminus:
    def __init__(self, response):
        self._response = response

    async def validate_relationships(self, db_name, ontology_payload, *, branch="main"):
        return self._response


def test_extract_group_refs_dedupes():
    refs = _extract_group_refs(
        {
            "groups": ["g1", "g2", "g1"],
            "groupRef": "g3",
        }
    )
    assert refs == ["g1", "g2", "g3"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_validate_group_refs_reports_missing():
    service = _FakeResourceService({("group", "g1"): {"id": "g1"}})
    missing = await _validate_group_refs(
        terminus=None,
        db_name="demo",
        branch="main",
        metadata={"groups": ["g1", "g2"]},
        resource_service=service,
    )
    assert missing == ["g2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_apply_shared_properties_merges_and_tracks_duplicates():
    service = _FakeResourceService(
        {
            ("shared_property", "common"): {
                "spec": {
                    "properties": [
                        {"name": "region", "type": "xsd:string", "label": "Region"},
                        {"name": "id", "type": "xsd:string", "label": "ID"},
                    ]
                }
            }
        }
    )
    properties = [Property(name="id", type="xsd:string", label="ID")]
    merged, issues = await _apply_shared_properties(
        terminus=None,
        db_name="demo",
        branch="main",
        properties=properties,
        metadata={"shared_property_refs": ["common"]},
        resource_service=service,
    )

    assert any(p.name == "region" and p.shared_property_ref == "common" for p in merged)
    assert issues["duplicate_property_names"] == ["id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_validate_value_type_refs_detects_base_type_mismatch():
    service = _FakeResourceService(
        {
            ("value_type", "TextType"): {"spec": {"base_type": "xsd:string"}},
        }
    )
    properties = [
        Property(
            name="age",
            type="xsd:integer",
            label="Age",
            value_type_ref="TextType",
        )
    ]
    issues = await _validate_value_type_refs(
        terminus=None,
        db_name="demo",
        branch="main",
        properties=properties,
        resource_service=service,
    )
    assert issues
    assert issues[0]["code"] == "VALUE_TYPE_BASE_MISMATCH"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_collect_interface_issues_reports_missing_property():
    service = _FakeResourceService(
        {
            ("interface", "IUser"): {
                "spec": {"required_properties": [{"name": "email", "type": "xsd:string"}]}
            }
        }
    )
    issues = await _collect_interface_issues(
        terminus=None,
        db_name="demo",
        branch="main",
        ontology_id="User",
        metadata={"interfaces": ["IUser"]},
        properties=[Property(name="id", type="xsd:string", label="ID")],
        relationships=[],
        resource_service=service,
    )
    assert any(issue.get("code") == "IFACE_MISSING_PROPERTY" for issue in issues)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_validate_relationships_gate_returns_422():
    terminus = _FakeTerminus({"ok": False, "errors": [{"message": "boom"}]})
    response = await _validate_relationships_gate(
        terminus=terminus,
        db_name="demo",
        branch="main",
        ontology_payload={"id": "Thing"},
        enabled=True,
    )
    assert response is not None
    assert response.status_code == 422
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errors"] == ["boom"]
