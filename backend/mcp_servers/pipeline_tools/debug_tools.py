from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List

from shared.errors.error_types import ErrorCategory, ErrorCode

from mcp_servers.pipeline_mcp_errors import tool_error

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


async def _debug_inspect_node(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    node_id = str(arguments.get("node_id") or "").strip()
    if not node_id:
        return tool_error("node_id is required")

    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []
    edges = definition.get("edges") or []

    node = next((n for n in nodes if n.get("id") == node_id), None)
    if not node:
        available_nodes = [n.get("id") for n in nodes if n.get("id")]
        payload = tool_error(
            f"Node '{node_id}' not found in plan",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
        )
        payload["available_nodes"] = available_nodes[:20]
        return payload

    input_edges = [e for e in edges if e.get("target") == node_id]
    output_edges = [e for e in edges if e.get("source") == node_id]

    result: Dict[str, Any] = {
        "status": "success",
        "node_id": node_id,
        "node_type": node.get("type"),
        "node_config": node.get("data") or node.get("config") or {},
        "inputs": [{"from_node": e.get("source"), "handle": e.get("sourceHandle")} for e in input_edges],
        "outputs": [{"to_node": e.get("target"), "handle": e.get("targetHandle")} for e in output_edges],
        "input_count": len(input_edges),
        "output_count": len(output_edges),
    }

    if arguments.get("include_sample"):
        node_data = node.get("data") or {}
        if node_data.get("preview"):
            result["sample_data"] = node_data.get("preview")

    return result


async def _debug_dry_run(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    check_joins = arguments.get("check_joins", True)

    errors: List[str] = []
    warnings: List[str] = []

    if not plan:
        errors.append("Plan is empty")
        return {"status": "invalid", "errors": errors, "warnings": warnings, "dry_run_passed": False}

    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []
    edges = definition.get("edges") or []

    if not nodes:
        errors.append("Plan has no nodes")
    if not edges and len(nodes) > 1:
        warnings.append("Plan has multiple nodes but no edges connecting them")

    node_types = [n.get("type") for n in nodes]
    has_input = any(t in ("input", "dataset_input", "source") for t in node_types)
    has_output = any(t in ("output", "dataset_output", "sink") for t in node_types)

    if not has_input:
        errors.append("Plan has no input/source node")
    if not has_output:
        warnings.append("Plan has no output/sink node")

    node_ids = {n.get("id") for n in nodes if n.get("id")}
    for edge in edges:
        source = edge.get("source")
        target = edge.get("target")
        if source and source not in node_ids:
            errors.append(f"Edge references non-existent source node: {source}")
        if target and target not in node_ids:
            errors.append(f"Edge references non-existent target node: {target}")

    if check_joins:
        for node in nodes:
            if node.get("type") == "join":
                node_data = node.get("data") or {}
                left_key = node_data.get("left_key") or node_data.get("leftKey")
                right_key = node_data.get("right_key") or node_data.get("rightKey")
                if not left_key:
                    errors.append(f"Join node '{node.get('id')}' missing left_key")
                if not right_key:
                    errors.append(f"Join node '{node.get('id')}' missing right_key")

    for node in nodes:
        nid = node.get("id")
        ntype = node.get("type") or ""
        has_incoming = any(e.get("target") == nid for e in edges)
        has_outgoing = any(e.get("source") == nid for e in edges)

        if ntype in ("input", "dataset_input", "source") and not has_outgoing:
            warnings.append(f"Input node '{nid}' has no outgoing edges")
        elif ntype in ("output", "dataset_output", "sink") and not has_incoming:
            warnings.append(f"Output node '{nid}' has no incoming edges")
        elif ntype not in ("input", "dataset_input", "source", "output", "dataset_output", "sink"):
            if not has_incoming and not has_outgoing:
                warnings.append(f"Node '{nid}' is orphaned (no edges)")

    return {
        "status": "success" if not errors else "invalid",
        "errors": errors,
        "warnings": warnings,
        "dry_run_passed": not errors,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


DEBUG_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "debug_inspect_node": _debug_inspect_node,
    "debug_dry_run": _debug_dry_run,
}


def build_debug_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(DEBUG_TOOL_HANDLERS)

