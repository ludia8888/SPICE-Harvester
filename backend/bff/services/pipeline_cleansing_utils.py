from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shared.services.pipeline.pipeline_graph_utils import normalize_edges, normalize_nodes, topological_sort, unique_node_id


def _transform_columns(transform: Dict[str, Any]) -> List[str]:
    operation = str(transform.get("operation") or "").strip().lower()
    if operation == "normalize":
        return [str(col).strip() for col in (transform.get("columns") or []) if str(col).strip()]
    if operation == "cast":
        columns: List[str] = []
        for item in transform.get("casts") or []:
            col = str(item.get("column") or "").strip()
            if col:
                columns.append(col)
        return columns
    if operation == "regexreplace":
        rules = transform.get("rules") or []
        if isinstance(rules, list) and rules:
            return [str(rule.get("column") or "").strip() for rule in rules if str(rule.get("column") or "").strip()]
        columns = transform.get("columns") or []
        return [str(col).strip() for col in columns if str(col).strip()]
    return []


def _columns_available(
    schema_by_node: Optional[Dict[str, Any]],
    node_id: str,
    columns: List[str],
) -> bool:
    if not columns:
        return True
    if not schema_by_node:
        return False
    schema = schema_by_node.get(node_id)
    if not schema or not getattr(schema, "columns", None):
        return False
    if getattr(schema, "dynamic_columns", False):
        return False
    return all(col in schema.columns for col in columns)


def _build_outgoing(edges: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    outgoing: Dict[str, List[str]] = {}
    for edge in edges:
        src = str(edge.get("from") or "")
        dst = str(edge.get("to") or "")
        if not src or not dst:
            continue
        outgoing.setdefault(src, []).append(dst)
    return outgoing


def _reachable_outputs(
    nodes: Dict[str, Dict[str, Any]],
    edges: List[Dict[str, Any]],
    output_nodes: List[str],
) -> tuple[Dict[str, set[str]], List[str]]:
    outgoing = _build_outgoing(edges)
    order = topological_sort(nodes, edges, include_unordered=True)
    reachable: Dict[str, set[str]] = {node_id: set() for node_id in nodes.keys()}
    for output_id in output_nodes:
        reachable.setdefault(output_id, set()).add(output_id)
    for node_id in reversed(order):
        for child in outgoing.get(node_id, []):
            reachable[node_id].update(reachable.get(child, set()))
    return reachable, order


def _select_anchor_groups(
    nodes: Dict[str, Dict[str, Any]],
    edges: List[Dict[str, Any]],
    output_nodes: List[str],
    *,
    required_columns: List[str],
    schema_by_node: Optional[Dict[str, Any]],
) -> tuple[List[tuple[str, set[str]]], set[str]]:
    reachable, order = _reachable_outputs(nodes, edges, output_nodes)
    depth_index = {node_id: idx for idx, node_id in enumerate(order)}
    candidates: Dict[frozenset[str], str] = {}
    for node_id, outputs in reachable.items():
        if len(outputs) <= 1:
            continue
        node = nodes.get(node_id) or {}
        if str(node.get("type") or "").strip().lower() == "output":
            continue
        if not _columns_available(schema_by_node, node_id, required_columns):
            continue
        key = frozenset(outputs)
        existing = candidates.get(key)
        if existing is None or depth_index.get(node_id, 0) > depth_index.get(existing, 0):
            candidates[key] = node_id

    groups = sorted(
        [(set(outputs), node_id) for outputs, node_id in candidates.items()],
        key=lambda item: (len(item[0]), depth_index.get(item[1], 0)),
        reverse=True,
    )
    assigned: set[str] = set()
    anchors: List[tuple[str, set[str]]] = []
    for outputs, node_id in groups:
        if outputs & assigned:
            continue
        anchors.append((node_id, outputs))
        assigned.update(outputs)
    return anchors, assigned


def _insert_chain(
    *,
    source_id: str,
    target_ids: List[str],
    transforms: List[Dict[str, Any]],
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    existing_ids: set[str],
) -> None:
    prev_id = source_id
    for transform in transforms:
        base_id = f"cleanse_{transform.get('operation')}_{source_id}"
        node_id = unique_node_id(base_id, existing_ids, start_index=2)
        existing_ids.add(node_id)
        nodes.append(
            {
                "id": node_id,
                "type": "transform",
                "metadata": transform,
            }
        )
        edges.append({"from": prev_id, "to": node_id})
        prev_id = node_id
    for target_id in target_ids:
        edges.append({"from": prev_id, "to": target_id})


def _apply_transforms_to_outputs(
    *,
    output_ids: List[str],
    transforms: List[Dict[str, Any]],
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    existing_ids: set[str],
    warnings: List[str],
) -> None:
    if not transforms or not output_ids:
        return
    outputs_by_source: Dict[str, List[str]] = {}
    for output_id in output_ids:
        incoming = [edge for edge in edges if edge.get("to") == output_id]
        if len(incoming) != 1:
            warnings.append(f"output {output_id} has {len(incoming)} incoming edges; skipping cleansing inserts")
            continue
        source_id = str(incoming[0].get("from") or "").strip()
        if not source_id:
            continue
        outputs_by_source.setdefault(source_id, []).append(output_id)

    for source_id, targets in outputs_by_source.items():
        edges[:] = [
            edge
            for edge in edges
            if not (edge.get("from") == source_id and edge.get("to") in targets)
        ]
        _insert_chain(
            source_id=source_id,
            target_ids=targets,
            transforms=transforms,
            nodes=nodes,
            edges=edges,
            existing_ids=existing_ids,
        )


def apply_cleansing_transforms(
    definition_json: Dict[str, Any],
    transforms: List[Dict[str, Any]],
    *,
    schema_by_node: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[str]]:
    if not transforms:
        return definition_json, []

    nodes_raw = definition_json.get("nodes")
    edges_raw = definition_json.get("edges")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json, ["definition_json has no nodes"]

    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    edges = normalize_edges(edges_raw)
    node_by_id = normalize_nodes(nodes_raw)
    existing_ids = set(node_by_id.keys())

    output_nodes = [node_id for node_id, node in node_by_id.items() if node.get("type") == "output"]
    warnings: List[str] = []
    updated_edges = list(edges)
    updated_nodes = list(nodes)

    pushable_ops = {"normalize", "cast", "regexreplace"}
    pushable = [
        t
        for t in transforms
        if str(t.get("operation") or "").strip().lower() in pushable_ops
    ]
    local = [
        t
        for t in transforms
        if str(t.get("operation") or "").strip().lower() not in pushable_ops
    ]

    if pushable:
        required_columns: List[str] = []
        for transform in pushable:
            required_columns.extend(_transform_columns(transform))
        required_columns = [col for col in dict.fromkeys(required_columns) if col]

        anchors, assigned_outputs = _select_anchor_groups(
            node_by_id,
            updated_edges,
            output_nodes,
            required_columns=required_columns,
            schema_by_node=schema_by_node,
        )
        reachable, _ = _reachable_outputs(node_by_id, updated_edges, output_nodes)
        outgoing = _build_outgoing(updated_edges)

        for anchor_id, outputs in anchors:
            children = [
                child
                for child in outgoing.get(anchor_id, [])
                if reachable.get(child, set()) & outputs
            ]
            if not children:
                warnings.append(f"no downstream targets found for upstream cleansing at {anchor_id}")
                continue
            updated_edges[:] = [
                edge
                for edge in updated_edges
                if not (edge.get("from") == anchor_id and edge.get("to") in children)
            ]
            _insert_chain(
                source_id=anchor_id,
                target_ids=children,
                transforms=pushable,
                nodes=updated_nodes,
                edges=updated_edges,
                existing_ids=existing_ids,
            )

        remaining_outputs = [output_id for output_id in output_nodes if output_id not in assigned_outputs]
        _apply_transforms_to_outputs(
            output_ids=remaining_outputs,
            transforms=pushable,
            nodes=updated_nodes,
            edges=updated_edges,
            existing_ids=existing_ids,
            warnings=warnings,
        )

    if local:
        _apply_transforms_to_outputs(
            output_ids=output_nodes,
            transforms=local,
            nodes=updated_nodes,
            edges=updated_edges,
            existing_ids=existing_ids,
            warnings=warnings,
        )

    updated_definition = dict(definition_json)
    updated_definition["nodes"] = updated_nodes
    updated_definition["edges"] = updated_edges
    return updated_definition, warnings
