import pytest

from shared.utils.action_input_schema import (
    ActionInputSchemaError,
    ActionInputValidationError,
    validate_action_input,
)


def test_validate_action_input_validates_and_normalizes_object_ref():
    schema = {
        "fields": [
            {"name": "ticket", "type": "object_ref", "required": True, "object_type": "Ticket"},
            {"name": "comment", "type": "string", "required": False, "max_length": 10},
        ],
        "allow_extra_fields": False,
    }
    payload = {"ticket": {"class_id": "Ticket", "instance_id": "ticket-123"}, "comment": "hi"}
    out = validate_action_input(input_schema=schema, payload=payload)
    assert out == {"ticket": {"class_id": "Ticket", "instance_id": "ticket-123"}, "comment": "hi"}


def test_validate_action_input_rejects_unknown_fields_by_default():
    schema = {"fields": [{"name": "x", "type": "string", "required": True}]}
    with pytest.raises(ActionInputValidationError):
        validate_action_input(input_schema=schema, payload={"x": "ok", "y": "nope"})


def test_validate_action_input_rejects_reserved_internal_keys_anywhere():
    schema = {"fields": [{"name": "x", "type": "string", "required": True}]}
    with pytest.raises(ActionInputValidationError):
        validate_action_input(input_schema=schema, payload={"x": "ok", "nested": {"base_token": "nope"}})


def test_validate_action_input_reports_invalid_schema():
    with pytest.raises(ActionInputSchemaError):
        validate_action_input(input_schema={"fields": []}, payload={})

