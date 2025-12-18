"""
Sheet grid extraction models.

Goal: represent a spreadsheet (Excel/Google Sheets) as a normalized grid + merged-cell ranges,
so downstream engines (structure analysis, type inference) can operate on one standard format.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field
from pydantic import HttpUrl

from shared.models.structure_analysis import MergeRange


class GoogleSheetGridRequest(BaseModel):
    """Request for extracting a full grid (values + merges) from a Google Sheet URL."""

    sheet_url: HttpUrl = Field(..., description="Google Sheets URL")
    worksheet_name: Optional[str] = Field(
        default=None, description="Optional worksheet title; falls back to gid/first sheet"
    )
    api_key: Optional[str] = Field(default=None, description="Optional per-request API key override")

    max_rows: Optional[int] = Field(default=None, ge=1, le=50000)
    max_cols: Optional[int] = Field(default=None, ge=1, le=2000)
    trim_trailing_empty: bool = Field(default=True, description="Trim trailing empty rows/cols")

    model_config = ConfigDict(extra="ignore")


class GoogleSheetStructureAnalysisRequest(GoogleSheetGridRequest):
    """Request for end-to-end Google Sheets structure analysis via grid extraction."""

    include_complex_types: bool = Field(default=True, description="Enable complex type hints")
    max_tables: int = Field(default=5, ge=1, le=50)
    options: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")


class SheetGrid(BaseModel):
    """Normalized sheet representation (0-based coordinates)."""

    source: Literal["excel", "google_sheets", "unknown"] = "unknown"
    sheet_name: Optional[str] = None

    grid: List[List[Any]] = Field(default_factory=list, description="Rectangular 2D grid")
    merged_cells: List[MergeRange] = Field(
        default_factory=list, description="Merged cell ranges (0-based, inclusive)"
    )

    metadata: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")
