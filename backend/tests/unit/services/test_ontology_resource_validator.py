import pytest

from oms.services.ontology_resource_validator import (
    _collect_permission_policy_issues,
    _collect_relationship_spec_issues,
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
    assert "writeback_target" in missing
    assert "implementation" in missing


def test_object_type_requires_pk_spec_and_backing_source():
    issues = check_required_fields("object_type", {})
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "pk_spec" in missing
    assert "pk_spec.primary_key" in missing
    assert "pk_spec.title_key" in missing
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


def test_action_type_rejects_unsafe_submission_criteria_expression():
    spec = {
        "input_schema": {"fields": [{"name": "ticket", "type": "object_ref", "required": True}]},
        "permission_policy": {"effect": "ALLOW", "principals": ["role:DomainModeler"]},
        "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
        "implementation": {
            "type": "template_v1",
            "targets": [{"target": {"from": "input.ticket"}, "changes": {"set": {"status": "APPROVED"}}}],
        },
        "submission_criteria": "__import__('os')",
    }
    issues = check_required_fields("action_type", spec)
    invalid = {field for issue in issues for field in issue["details"]["invalid_fields"]}
    assert "submission_criteria" in invalid


def test_action_type_rejects_invalid_validation_rules():
    spec = {
        "input_schema": {"fields": [{"name": "ticket", "type": "object_ref", "required": True}]},
        "permission_policy": {"effect": "ALLOW", "principals": ["role:DomainModeler"]},
        "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
        "implementation": {
            "type": "template_v1",
            "targets": [{"target": {"from": "input.ticket"}, "changes": {"set": {"status": "APPROVED"}}}],
        },
        "validation_rules": [{"type": "assert", "scope": "each_target", "expr": "__import__('os')"}],
    }
    issues = check_required_fields("action_type", spec)
    invalid = {field for issue in issues for field in issue["details"]["invalid_fields"]}
    assert "validation_rules[0].expr" in invalid


def test_link_type_invalid_predicate_is_reported():
    issues = _collect_link_type_issues({"predicate": "HasOrder", "cardinality": "1:n"})
    assert issues
    invalid_fields = issues[0]["details"]["invalid_fields"]
    assert "predicate" in invalid_fields


def test_relationship_spec_missing_is_reported():
    issues = _collect_relationship_spec_issues({})
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "relationship_spec" in missing


def test_relationship_spec_invalid_type_is_reported():
    issues = _collect_relationship_spec_issues({"relationship_spec": {"type": "unknown"}})
    invalid = {field for issue in issues for field in issue["details"]["invalid_fields"]}
    assert "relationship_spec.type" in invalid


def test_relationship_spec_object_backed_requires_object_type():
    issues = _collect_relationship_spec_issues(
        {
            "relationship_spec": {
                "type": "object_backed",
                "relationship_spec_id": "rs-1",
                "source_key_column": "source_id",
                "target_key_column": "target_id",
            }
        }
    )
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "relationship_spec.relationship_object_type" in missing


def test_relationship_spec_join_table_requires_dataset_or_auto_create():
    issues = _collect_relationship_spec_issues(
        {
            "relationship_spec": {
                "type": "join_table",
                "relationship_spec_id": "rs-1",
                "source_key_column": "source_id",
                "target_key_column": "target_id",
            }
        }
    )
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "relationship_spec.join_dataset_id" in missing


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


@pytest.mark.unit
def test_permission_policy_allows_legacy_roles_alias_for_runtime_compat() -> None:
    issues = _collect_permission_policy_issues(
        {
            "effect": "ALLOW",
            "roles": ["DomainModeler"],
        }
    )
    assert issues == []


@pytest.mark.unit
def test_permission_policy_allows_legacy_users_alias_for_runtime_compat() -> None:
    issues = _collect_permission_policy_issues(
        {
            "effect": "ALLOW",
            "users": ["alice"],
        }
    )
    assert issues == []


@pytest.mark.unit
def test_permission_policy_rejects_unsupported_legacy_fields() -> None:
    issues = _collect_permission_policy_issues(
        {
            "effect": "ALLOW",
            "principals": ["role:DomainModeler"],
            "rules": [{"op": "always_true"}],
        }
    )
    invalid = {field for issue in issues for field in issue["details"]["invalid_fields"]}
    assert "permission_policy.rules" in invalid


@pytest.mark.unit
def test_action_type_datasource_derived_allows_missing_permission_policy() -> None:
    spec = {
        "permission_model": "datasource_derived",
        "input_schema": {"fields": [{"name": "ticket", "type": "object_ref", "required": True}]},
        "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
        "implementation": {
            "type": "template_v1",
            "targets": [{"target": {"from": "input.ticket"}, "changes": {"set": {"status": "APPROVED"}}}],
        },
    }
    issues = check_required_fields("action_type", spec)
    missing = {field for issue in issues for field in issue["details"]["missing_fields"]}
    assert "permission_policy" not in missing


@pytest.mark.unit
def test_action_type_rejects_invalid_permission_model() -> None:
    spec = {
        "permission_model": "invalid_model",
        "input_schema": {"fields": [{"name": "ticket", "type": "object_ref", "required": True}]},
        "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
        "implementation": {
            "type": "template_v1",
            "targets": [{"target": {"from": "input.ticket"}, "changes": {"set": {"status": "APPROVED"}}}],
        },
    }
    issues = check_required_fields("action_type", spec)
    invalid = {field for issue in issues for field in issue["details"]["invalid_fields"]}
    assert "permission_model" in invalid


@pytest.mark.unit
def test_action_type_rejects_non_boolean_edits_beyond_actions() -> None:
    spec = {
        "permission_model": "ontology_roles",
        "edits_beyond_actions": "yes",
        "input_schema": {"fields": [{"name": "ticket", "type": "object_ref", "required": True}]},
        "permission_policy": {"effect": "ALLOW", "principals": ["role:DomainModeler"]},
        "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
        "implementation": {
            "type": "template_v1",
            "targets": [{"target": {"from": "input.ticket"}, "changes": {"set": {"status": "APPROVED"}}}],
        },
    }
    issues = check_required_fields("action_type", spec)
    invalid = {field for issue in issues for field in issue["details"]["invalid_fields"]}
    assert "edits_beyond_actions" in invalid
