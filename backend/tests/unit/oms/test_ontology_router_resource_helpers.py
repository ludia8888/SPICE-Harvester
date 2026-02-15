from __future__ import annotations

import pytest

from oms.routers.ontology import _is_internal_ontology, _ontology_from_resource_payload


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
