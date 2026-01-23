from __future__ import annotations

import pytest

from bff.services.pipeline_cleansing_utils import apply_cleansing_transforms
from shared.services.pipeline_preflight_utils import SchemaInfo


@pytest.mark.unit
def test_cleansing_upstream_push_common_ancestor() -> None:
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left"}},
            {"id": "right", "type": "input", "metadata": {"datasetName": "right"}},
            {"id": "join", "type": "transform", "metadata": {"operation": "join", "leftKey": "id", "rightKey": "id"}},
            {"id": "compute_a", "type": "transform", "metadata": {"operation": "compute", "expression": "x = 1"}},
            {"id": "compute_b", "type": "transform", "metadata": {"operation": "compute", "expression": "y = 1"}},
            {"id": "out_a", "type": "output", "metadata": {"outputName": "out_a"}},
            {"id": "out_b", "type": "output", "metadata": {"outputName": "out_b"}},
        ],
        "edges": [
            {"from": "left", "to": "join"},
            {"from": "right", "to": "join"},
            {"from": "join", "to": "compute_a"},
            {"from": "join", "to": "compute_b"},
            {"from": "compute_a", "to": "out_a"},
            {"from": "compute_b", "to": "out_b"},
        ],
    }
    transforms = [{"operation": "normalize", "columns": ["email"], "trim": True}]
    schema_by_node = {
        "join": SchemaInfo(columns=["email"], type_map={"email": "xsd:string"}),
    }

    updated, warnings = apply_cleansing_transforms(definition, transforms, schema_by_node=schema_by_node)
    assert warnings == []

    cleanse_nodes = [
        node
        for node in (updated.get("nodes") or [])
        if (node.get("metadata") or {}).get("operation") == "normalize"
    ]
    assert len(cleanse_nodes) == 1
    cleanse_id = cleanse_nodes[0]["id"]

    edges = updated.get("edges") or []
    assert {"from": "join", "to": cleanse_id} in edges
    assert {"from": cleanse_id, "to": "compute_a"} in edges
    assert {"from": cleanse_id, "to": "compute_b"} in edges
    assert {"from": "join", "to": "compute_a"} not in edges
    assert {"from": "join", "to": "compute_b"} not in edges
