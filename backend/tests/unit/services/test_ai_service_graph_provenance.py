from __future__ import annotations

from bff.services.ai_service import _ground_graph_query_result
from shared.models.graph_query import GraphEdge, GraphNode, GraphQueryResponse


def _response_with_provenance(provenance: dict) -> GraphQueryResponse:
    return GraphQueryResponse(
        nodes=[
            GraphNode(
                id="Customer/c1",
                type="Customer",
                es_doc_id="c1",
                db_name="demo",
                es_ref={"index": "demo_instances", "id": "c1"},
                data_status="FULL",
                provenance=provenance,
            )
        ],
        edges=[GraphEdge(from_node="Customer/c1", to_node="Customer/c2", predicate="related_to")],
        query={"start_class": "Customer", "hops": []},
        count=1,
        warnings=[],
    )


def test_ground_graph_query_result_prefers_graph_provenance() -> None:
    grounded = _ground_graph_query_result(
        _response_with_provenance(
            {
                "graph": {"event_id": "evt-graph", "occurred_at": "2026-01-01T00:00:00+00:00"},
                "es": {"event_id": "evt-es", "occurred_at": "2026-01-01T00:01:00+00:00"},
            }
        )
    )

    sample_nodes = grounded["sample_nodes"]
    assert sample_nodes
    assert sample_nodes[0]["provenance"]["event_id"] == "evt-graph"


def test_ground_graph_query_result_ignores_legacy_terminus_key() -> None:
    grounded = _ground_graph_query_result(
        _response_with_provenance(
            {
                "terminus": {"event_id": "evt-legacy", "occurred_at": "2026-01-01T00:00:00+00:00"},
                "es": {"event_id": "evt-es", "occurred_at": "2026-01-01T00:01:00+00:00"},
            }
        )
    )

    sample_nodes = grounded["sample_nodes"]
    assert sample_nodes
    assert sample_nodes[0]["provenance"]["event_id"] == "evt-es"
