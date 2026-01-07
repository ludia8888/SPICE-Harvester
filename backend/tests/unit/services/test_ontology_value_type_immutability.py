import pytest

from fastapi import HTTPException

from oms.routers.ontology_extensions import _validate_value_type_immutability


def test_value_type_immutability_blocks_base_type_change():
    existing = {"spec": {"base_type": "xsd:string", "constraints": {}}}
    incoming = {"spec": {"base_type": "xsd:integer", "constraints": {}}}

    with pytest.raises(HTTPException) as exc:
        _validate_value_type_immutability(existing, incoming)

    assert exc.value.status_code == 409


def test_value_type_immutability_blocks_constraint_change():
    existing = {"spec": {"base_type": "xsd:string", "constraints": {"minLength": 2}}}
    incoming = {"spec": {"base_type": "xsd:string", "constraints": {}}}

    with pytest.raises(HTTPException) as exc:
        _validate_value_type_immutability(existing, incoming)

    assert exc.value.status_code == 409


def test_value_type_immutability_allows_same_spec():
    existing = {"spec": {"base_type": "xsd:string", "constraints": {"minLength": 2}}}
    incoming = {"spec": {"base_type": "xsd:string", "constraints": {"minLength": 2}}}

    _validate_value_type_immutability(existing, incoming)
