from __future__ import annotations

from shared.utils.instance_properties import flatten_instance_properties, instance_property_name


def test_instance_property_name_uses_id_when_name_missing() -> None:
    assert instance_property_name({"id": "temperature"}) == "temperature"


def test_flatten_instance_properties_uses_id_when_name_missing() -> None:
    assert flatten_instance_properties(
        [
            {"id": "temperature", "value": "20.5"},
            {"name": "status", "value": "ACTIVE"},
        ]
    ) == {
        "temperature": "20.5",
        "status": "ACTIVE",
    }
