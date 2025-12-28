from __future__ import annotations

from typing import Any, Dict, List


def normalize_nodes(nodes_raw: Any) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    if not isinstance(nodes_raw, list):
        return output
    for node in nodes_raw:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "")
        if not node_id:
            continue
        output[node_id] = node
    return output


def normalize_edges(edges_raw: Any) -> List[Dict[str, str]]:
    edges: List[Dict[str, str]] = []
    if not isinstance(edges_raw, list):
        return edges
    for edge in edges_raw:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "")
        dst = str(edge.get("to") or "")
        if not src or not dst:
            continue
        edges.append({"from": src, "to": dst})
    return edges


def build_incoming(edges: List[Dict[str, str]]) -> Dict[str, List[str]]:
    incoming: Dict[str, List[str]] = {}
    for edge in edges:
        incoming.setdefault(edge["to"], []).append(edge["from"])
    return incoming


def topological_sort(
    nodes: Dict[str, Dict[str, Any]],
    edges: List[Dict[str, str]],
    *,
    include_unordered: bool = False,
) -> List[str]:
    in_degree = {node_id: 0 for node_id in nodes.keys()}
    outgoing: Dict[str, List[str]] = {node_id: [] for node_id in nodes.keys()}
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        if src not in nodes or dst not in nodes:
            continue
        outgoing[src].append(dst)
        in_degree[dst] = in_degree.get(dst, 0) + 1

    queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
    order: List[str] = []
    while queue:
        node_id = queue.pop(0)
        order.append(node_id)
        for neighbor in outgoing.get(node_id, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if include_unordered:
        for node_id in nodes.keys():
            if node_id not in order:
                order.append(node_id)
    return order
