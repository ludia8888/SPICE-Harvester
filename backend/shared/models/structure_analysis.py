"""
Structure analysis models for SPICE HARVESTER.

These models represent the output of the "Structure Analysis" engine that
standardizes messy spreadsheet-like grids into table blocks and key-value pairs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from shared.models.type_inference import ColumnAnalysisResult


class CellAddress(BaseModel):
    """0-based cell address (row, col)."""

    row: int = Field(..., ge=0)
    col: int = Field(..., ge=0)

    model_config = ConfigDict(extra="ignore")


class BoundingBox(BaseModel):
    """0-based inclusive bounding box."""

    top: int = Field(..., ge=0)
    left: int = Field(..., ge=0)
    bottom: int = Field(..., ge=0)
    right: int = Field(..., ge=0)

    model_config = ConfigDict(extra="ignore")


class MergeRange(BoundingBox):
    """Merged cell range (inclusive)."""

    model_config = ConfigDict(extra="ignore")


class KeyValueItem(BaseModel):
    """Extracted key-value item from a sheet-like grid."""

    key: str
    value: Any
    value_type: str = Field(..., description="Inferred type id (e.g., money, xsd:date)")
    confidence: float = Field(..., ge=0.0, le=1.0)
    key_cell: Optional[CellAddress] = None
    value_cell: CellAddress
    reason: str = Field(default="", description="Why this key-value pairing was selected")

    model_config = ConfigDict(extra="ignore")


class HeaderTreeNode(BaseModel):
    """Hierarchical header node for multi-row/grouped headers."""

    label: str = Field(default="")
    start_col: int = Field(..., ge=0, description="0-based column start (inclusive) within the table bbox")
    end_col: int = Field(..., ge=0, description="0-based column end (inclusive) within the table bbox")
    children: Optional[List["HeaderTreeNode"]] = None

    model_config = ConfigDict(extra="ignore")


class CellEvidence(BaseModel):
    """Small evidence sample for explainability/debugging."""

    cell: CellAddress
    value: Any
    inferred_type: str
    score: float = Field(..., ge=0.0, le=1.0)
    style_hint: Optional[int] = Field(default=None, description="Optional style bitmask (Excel-only)")

    model_config = ConfigDict(extra="ignore")


class DetectedTable(BaseModel):
    """
    Detected table-like block.

    mode:
    - table: regular header-row + record-rows
    - transposed: row-as-field, needs pivot to become table
    - property: key-value form (label/value)
    """

    id: str
    bbox: BoundingBox
    mode: Literal["table", "transposed", "property"]
    confidence: float = Field(..., ge=0.0, le=1.0)
    reason: str = ""

    header_rows: int = Field(default=1, ge=0)
    header_cols: int = Field(default=0, ge=0)

    header_grid: Optional[List[List[str]]] = Field(
        default=None,
        description="Raw header cells (multi-row headers supported).",
    )
    header_tree: Optional[List[HeaderTreeNode]] = Field(
        default=None,
        description="Optional hierarchical header tree derived from header_grid/merges.",
    )
    headers: List[str] = Field(default_factory=list)
    sample_rows: List[List[Any]] = Field(default_factory=list)

    column_provenance: Optional[List["ColumnProvenance"]] = Field(
        default=None,
        description="Per-column provenance (header/data cell ranges) for lineage.",
    )

    inferred_schema: Optional[List[ColumnAnalysisResult]] = None
    key_values: Optional[List[KeyValueItem]] = None
    decision_trace: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Explainability trace: candidate scores, chosen mode, header candidates ranking.",
    )
    cell_evidence_samples: Optional[List[CellEvidence]] = Field(
        default=None,
        description="Small sample of evidence cells (typed seeds etc.) for debugging/UX.",
    )

    model_config = ConfigDict(extra="ignore")


class ColumnProvenance(BaseModel):
    """Lineage hook: where a field came from in the source grid."""

    field: str
    column_index: int = Field(..., ge=0)
    header_cells: List[CellAddress] = Field(default_factory=list)
    data_bbox: BoundingBox

    model_config = ConfigDict(extra="ignore")


class SheetStructureAnalysisRequest(BaseModel):
    """Request for structure analysis on a raw 2D grid."""

    grid: List[List[Any]] = Field(..., description="Raw sheet grid (rows x columns)")
    include_complex_types: bool = Field(default=True, description="Enable complex type hints")
    max_tables: int = Field(default=5, ge=1, le=50)
    merged_cells: Optional[List[MergeRange]] = Field(
        default=None,
        description="Optional merged cell ranges (0-based inclusive)",
    )
    cell_style_hints: Optional[List[List[int]]] = Field(
        default=None,
        description="Optional per-cell style hint bitmask grid (Excel-only). Must align with grid shape.",
    )
    options: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")


class SheetStructureAnalysisResponse(BaseModel):
    """Structure analysis output: table blocks + key-value metadata."""

    tables: List[DetectedTable] = Field(default_factory=list)
    key_values: List[KeyValueItem] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")
