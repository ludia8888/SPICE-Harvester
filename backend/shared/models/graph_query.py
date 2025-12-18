"""
Graph query models shared across routers/services.

These models were originally defined inside `bff/routers/graph.py` but are
shared now so other modules (e.g., AI/NLQ translation) can reuse the same
request/response contracts without duplicating schemas.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class GraphHop(BaseModel):
    """Represents a single hop in a graph traversal."""

    predicate: str  # Relationship predicate (e.g., "owned_by")
    target_class: str  # Target class id (e.g., "Client")


class GraphQueryRequest(BaseModel):
    """Request model for multi-hop graph queries."""

    start_class: str  # Starting class id (e.g., "Product")
    hops: List[GraphHop]  # List of hops to traverse
    filters: Optional[Dict[str, Any]] = None  # Optional filters on start class (best-effort)
    limit: int = 100  # Max bindings (page size)
    offset: int = 0  # Binding offset for pagination
    max_nodes: int = 500
    max_edges: int = 2000
    include_paths: bool = False
    max_paths: int = 100
    no_cycles: bool = False
    include_provenance: bool = False
    include_documents: bool = True
    include_audit: bool = False


class GraphNode(BaseModel):
    """Graph node with ES document reference."""

    id: str
    type: str
    es_doc_id: str
    db_name: str
    es_ref: Dict[str, str]
    data_status: str  # FULL|PARTIAL|MISSING
    display: Optional[Dict[str, Any]] = None  # Stable UI fields from Terminus graph
    data: Optional[Dict[str, Any]] = None  # Document payload from ES
    index_status: Optional[Dict[str, Any]] = None
    provenance: Optional[Dict[str, Any]] = None


class GraphEdge(BaseModel):
    """Graph edge between nodes."""

    from_node: str
    to_node: str
    predicate: str
    provenance: Optional[Dict[str, Any]] = None


class GraphQueryResponse(BaseModel):
    """Response model for graph queries."""

    nodes: List[GraphNode]
    edges: List[GraphEdge]
    query: Dict[str, Any]
    count: int
    warnings: List[str] = []
    page: Optional[Dict[str, Any]] = None
    paths: Optional[List[Dict[str, Any]]] = None
    index_summary: Optional[Dict[str, Any]] = None


class SimpleGraphQueryRequest(BaseModel):
    """Request for simple single-class queries."""

    class_name: str
    filters: Optional[Dict[str, Any]] = None
    limit: int = 100

