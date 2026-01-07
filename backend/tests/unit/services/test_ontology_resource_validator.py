import pytest

from oms.services.ontology_resource_validator import (
    _collect_link_type_issues,
    _find_missing_link_type_refs,
    check_required_fields,
)


class _FakeTerminus:
    def __init__(self, existing):
        self._existing = existing

    async def get_ontology(self, db_name, class_id, *, branch="main"):
        return self._existing.get(class_id)


def test_action_type_requires_input_schema_and_policy():
    issues = check_required_fields("action_type", {})
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "input_schema" in missing
    assert "permission_policy" in missing


def test_object_type_requires_pk_spec_and_backing_source():
    issues = check_required_fields("object_type", {})
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "pk_spec" in missing
    assert "backing_source" in missing


def test_shared_property_requires_properties_list():
    issues = check_required_fields("shared_property", {})
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "properties" in missing


def test_function_requires_expression_and_return_type_ref():
    issues = check_required_fields("function", {})
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "expression" in missing
    assert "return_type_ref" in missing


def test_link_type_invalid_predicate_is_reported():
    issues = _collect_link_type_issues({"predicate": "HasOrder", "cardinality": "1:n"})
    assert issues
    invalid_fields = issues[0]["details"]["invalid_fields"]
    assert "predicate" in invalid_fields


@pytest.mark.unit
@pytest.mark.asyncio
async def test_link_type_missing_refs_are_reported():
    terminus = _FakeTerminus({"Customer": {"id": "Customer"}})
    missing = await _find_missing_link_type_refs(
        terminus=terminus,
        db_name="demo",
        branch="main",
        spec={"from": "Customer", "to": "Order", "predicate": "has_order", "cardinality": "1:n"},
    )
    assert missing == ["Order"]
