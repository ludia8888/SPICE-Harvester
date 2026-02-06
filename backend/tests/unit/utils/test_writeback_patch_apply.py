import pytest

from shared.utils.writeback_patch_apply import apply_changes_to_payload


def test_apply_changes_set_unset() -> None:
    payload = {"a": 1, "b": 2}
    assert apply_changes_to_payload(payload, {"set": {"c": 3}, "unset": ["a"]}) is False
    assert payload == {"b": 2, "c": 3}


def test_apply_changes_link_add_and_remove() -> None:
    payload = {"links": ["x"]}
    assert apply_changes_to_payload(payload, {"link_add": [{"field": "links", "value": "y"}]}) is False
    assert payload["links"] == ["x", "y"]

    assert apply_changes_to_payload(payload, {"link_add": [{"field": "links", "value": "y"}]}) is False
    assert payload["links"] == ["x", "y"]

    assert apply_changes_to_payload(payload, {"link_remove": ["links:x"]}) is False
    assert payload["links"] == ["y"]


def test_apply_changes_link_scalar_coerces_to_list() -> None:
    payload = {"links": "x"}
    assert apply_changes_to_payload(payload, {"link_add": [{"field": "links", "value": "y"}]}) is False
    assert payload["links"] == ["x", "y"]


def test_apply_changes_delete_short_circuits() -> None:
    payload = {"a": 1}
    assert apply_changes_to_payload(payload, {"delete": True, "set": {"b": 2}}) is True
    assert payload == {"a": 1}


def test_apply_changes_type_validation() -> None:
    with pytest.raises(TypeError):
        apply_changes_to_payload({}, [])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        apply_changes_to_payload([], {})  # type: ignore[arg-type]

