from __future__ import annotations

import pytest

from oms.routers.ontology import (
    _coerce_property_models,
    _coerce_relationship_models,
    _is_internal_ontology,
    _ontology_from_resource_payload,
)


@pytest.mark.unit
def test_ontology_from_resource_payload_maps_object_type_contract() -> None:
    payload = {
        "id": "Product",
        "label": {"en": "Product", "ko": "상품"},
        "description": "Product type",
        "metadata": {"source": "contract"},
        "spec": {
            "abstract": False,
            "parent_class": "Thing",
            "properties": [{"name": "id", "type": "string"}],
            "relationships": [{"predicate": "ownedBy", "target": "User"}],
        },
    }

    ontology = _ontology_from_resource_payload(payload)
    assert ontology["id"] == "Product"
    assert ontology["parent_class"] == "Thing"
    assert ontology["abstract"] is False
    assert isinstance(ontology["properties"], list)
    assert isinstance(ontology["relationships"], list)
    assert ontology["metadata"]["source"] == "contract"


@pytest.mark.unit
def test_is_internal_ontology_detects_internal_dict_payload() -> None:
    assert _is_internal_ontology({"id": "Product", "metadata": {"internal": True}})
    assert _is_internal_ontology({"id": "__system.Object", "metadata": {}})
    assert not _is_internal_ontology({"id": "Product", "metadata": {}})


@pytest.mark.unit
def test_coerce_property_models_backfills_label_for_resource_payload() -> None:
    props = _coerce_property_models(
        [
            {"name": "product_id", "type": "xsd:string", "required": True},
            {"name": "name", "type": "xsd:string", "label": {"en": "Name"}},
        ]
    )

    assert props[0].name == "product_id"
    assert props[0].label == "product_id"
    assert props[1].name == "name"


@pytest.mark.unit
def test_coerce_relationship_models_backfills_label_for_resource_payload() -> None:
    rels = _coerce_relationship_models(
        [
            {"predicate": "owned_by", "target": "User", "cardinality": "n:1"},
        ]
    )

    assert rels[0].predicate == "owned_by"
    assert rels[0].label == "owned_by"
