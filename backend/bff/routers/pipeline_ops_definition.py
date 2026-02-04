"""Pipeline Builder definition helpers.

Stable hashes, commit resolution, location normalization, and diffing.
Extracted from `bff.routers.pipeline_ops`.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

from fastapi import HTTPException, status


def _stable_definition_hash(definition_json: Dict[str, Any]) -> str:
    try:
        payload = json.dumps(definition_json or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        payload = json.dumps(str(definition_json))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_definition_commit_id(
    definition_json: Dict[str, Any],
    latest_version: Optional[Any],
    definition_hash: Optional[str] = None,
) -> Optional[str]:
    if not latest_version:
        return None
    if definition_hash is None:
        definition_hash = _stable_definition_hash(definition_json)
    latest_hash = _stable_definition_hash(getattr(latest_version, "definition_json", {}) or {})
    if latest_hash == definition_hash:
        return str(getattr(latest_version, "lakefs_commit_id", "") or "").strip() or None
    return None


def _normalize_location(location: str) -> str:
    cleaned = (location or "").strip()
    if not cleaned:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="location is required")
    if "personal" in cleaned.lower():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="personal locations are not allowed",
        )
    return cleaned


def _extract_node_ids(definition_json: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(definition_json, dict):
        return set()
    nodes = definition_json.get("nodes") or []
    if not isinstance(nodes, list):
        return set()
    output: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or node.get("node_id") or "").strip()
        if node_id:
            output.add(node_id)
    return output


def _extract_edge_ids(definition_json: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(definition_json, dict):
        return set()
    edges = definition_json.get("edges") or []
    if not isinstance(edges, list):
        return set()
    output: set[str] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_id = str(edge.get("id") or "").strip()
        if not edge_id:
            source = str(edge.get("source") or edge.get("from") or "").strip()
            target = str(edge.get("target") or edge.get("to") or "").strip()
            if source or target:
                edge_id = f"{source}->{target}"
        if edge_id:
            output.add(edge_id)
    return output



def _definition_diff(
    previous: Optional[Dict[str, Any]],
    current: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    prev_nodes = _extract_node_ids(previous)
    next_nodes = _extract_node_ids(current)
    prev_edges = _extract_edge_ids(previous)
    next_edges = _extract_edge_ids(current)
    return {
        "nodes_added": sorted(next_nodes - prev_nodes),
        "nodes_removed": sorted(prev_nodes - next_nodes),
        "edges_added": sorted(next_edges - prev_edges),
        "edges_removed": sorted(prev_edges - next_edges),
        "node_count": len(next_nodes),
        "edge_count": len(next_edges),
    }
