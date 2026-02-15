from __future__ import annotations

from shared.models.lineage import LineageEdge, LineageGraph, LineageNode

from bff.routers.lineage import _count_artifact_kinds, _edge_signature, _find_shortest_path


def test_find_shortest_path_downstream() -> None:
    graph = LineageGraph(
        root="event:e1",
        direction="downstream",
        max_depth=5,
        nodes=[
            LineageNode(node_id="event:e1", node_type="event"),
            LineageNode(node_id="agg:order:o1", node_type="aggregate"),
            LineageNode(node_id="artifact:es:demo:main:Order/o1", node_type="artifact"),
        ],
        edges=[
            LineageEdge(from_node_id="event:e1", to_node_id="agg:order:o1", edge_type="event_applied"),
            LineageEdge(
                from_node_id="agg:order:o1",
                to_node_id="artifact:es:demo:main:Order/o1",
                edge_type="event_materialized_es_document",
            ),
        ],
    )

    nodes, edges = _find_shortest_path(
        graph=graph,
        source="event:e1",
        target="artifact:es:demo:main:Order/o1",
        direction="downstream",
    )

    assert nodes == ["event:e1", "agg:order:o1", "artifact:es:demo:main:Order/o1"]
    assert [direction for _, direction in edges] == ["forward", "forward"]


def test_find_shortest_path_upstream_uses_reverse_traversal() -> None:
    graph = LineageGraph(
        root="artifact:graph:demo:main:Order/o1",
        direction="upstream",
        max_depth=5,
        nodes=[
            LineageNode(node_id="event:e1", node_type="event"),
            LineageNode(node_id="artifact:graph:demo:main:Order/o1", node_type="artifact"),
        ],
        edges=[
            LineageEdge(
                from_node_id="event:e1",
                to_node_id="artifact:graph:demo:main:Order/o1",
                edge_type="event_wrote_graph_document",
            )
        ],
    )

    nodes, edges = _find_shortest_path(
        graph=graph,
        source="artifact:graph:demo:main:Order/o1",
        target="event:e1",
        direction="upstream",
    )

    assert nodes == ["artifact:graph:demo:main:Order/o1", "event:e1"]
    assert [direction for _, direction in edges] == ["reverse"]


def test_edge_signature_uses_projection_name_metadata() -> None:
    edge = LineageEdge(
        from_node_id="event:e1",
        to_node_id="artifact:es:demo:main:Order/o1",
        edge_type="event_materialized_es_document",
        metadata={"projection_name": "orders_projection"},
    )

    assert (
        _edge_signature(edge)
        == "event:e1|artifact:es:demo:main:Order/o1|event_materialized_es_document|orders_projection"
    )


def test_count_artifact_kinds() -> None:
    counts = _count_artifact_kinds(
        [
            LineageNode(node_id="event:e1", node_type="event"),
            LineageNode(node_id="artifact:graph:demo:main:Order/o1", node_type="artifact"),
            LineageNode(node_id="artifact:es:demo:main:Order/o1", node_type="artifact"),
            LineageNode(node_id="artifact:es:demo:main:Order/o2", node_type="artifact"),
        ]
    )

    assert counts == {"graph": 1, "es": 2}
