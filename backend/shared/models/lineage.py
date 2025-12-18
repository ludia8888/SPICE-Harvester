"""
Lineage (provenance) models.

Goal:
- Treat provenance as a first-class, queryable graph (not just log lines).
- Support upstream/downstream traversal for debugging + impact analysis.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


LineageDirection = Literal["upstream", "downstream", "both"]


class LineageNode(BaseModel):
    node_id: str = Field(..., description="Stable node id (e.g. event:<uuid>, agg:<type>:<id>)")
    node_type: str = Field(..., description="Node type (event, aggregate, artifact, projection, ...)")
    label: Optional[str] = Field(default=None, description="Human-friendly label")
    created_at: Optional[datetime] = None
    recorded_at: Optional[datetime] = Field(default=None, description="When this node was recorded")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")


class LineageEdge(BaseModel):
    edge_id: Optional[str] = Field(default=None, description="Edge id (uuid)")
    from_node_id: str
    to_node_id: str
    edge_type: str = Field(..., description="Edge type (caused, affects, indexed, wrote, ...)")
    occurred_at: Optional[datetime] = None
    recorded_at: Optional[datetime] = Field(default=None, description="When this edge was recorded")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")


class LineageGraph(BaseModel):
    root: str
    direction: LineageDirection
    max_depth: int
    nodes: List[LineageNode] = Field(default_factory=list)
    edges: List[LineageEdge] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")
