"""
Structure analysis patch models.

These models support a human-in-the-loop workflow where users correct detected tables
and the server can re-apply those corrections to similar sheets (via sheet_signature).
"""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from shared.models.structure_analysis import BoundingBox


PatchOpType = Literal[
    "set_table_bbox",
    "set_table_mode",
    "set_header_rows",
    "set_header_cols",
    "remove_key_value",
    "remove_table",
]


class SheetStructurePatchOp(BaseModel):
    """Single patch operation (applied in order)."""

    op: PatchOpType

    # Identify target table
    table_id: Optional[str] = None
    table_index: Optional[int] = Field(default=None, ge=0)

    # Table edits
    bbox: Optional[BoundingBox] = None
    mode: Optional[Literal["table", "transposed", "property"]] = None
    header_rows: Optional[int] = Field(default=None, ge=0, le=50)
    header_cols: Optional[int] = Field(default=None, ge=0, le=50)

    # Key-value edits
    key: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class SheetStructurePatch(BaseModel):
    """Patch bundle stored per sheet_signature."""

    sheet_signature: str = Field(..., min_length=3)
    ops: List[SheetStructurePatchOp] = Field(default_factory=list)
    note: Optional[str] = None

    model_config = ConfigDict(extra="ignore")

