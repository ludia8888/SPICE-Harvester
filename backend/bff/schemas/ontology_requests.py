"""
Ontology-related request schemas (BFF).

These schemas belong to the presentation layer and are shared across multiple
ontology routers. Keeping them in one module reduces router bloat and supports
router composition (Composite pattern).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from shared.models.structure_analysis import BoundingBox


class SchemaFromDataRequest(BaseModel):
    """Request model for schema suggestion from data."""

    data: List[List[Any]]
    columns: List[str]
    class_name: Optional[str] = None
    include_complex_types: bool = False


class SchemaFromGoogleSheetsRequest(BaseModel):
    """Request model for schema suggestion from Google Sheets."""

    sheet_url: str
    worksheet_name: Optional[str] = None
    class_name: Optional[str] = None
    api_key: Optional[str] = None
    connection_id: Optional[str] = None
    table_id: Optional[str] = None
    table_bbox: Optional[BoundingBox] = None


class MappingSuggestionRequest(BaseModel):
    """Request model for mapping suggestions between schemas."""

    source_schema: List[Dict[str, Any]]  # New data schema
    target_schema: List[Dict[str, Any]]  # Existing ontology schema
    sample_data: Optional[List[Dict[str, Any]]] = None  # Sample values for pattern matching
    target_sample_data: Optional[List[Dict[str, Any]]] = None  # Target sample values for distribution matching


class MappingFromGoogleSheetsRequest(BaseModel):
    """Request model for mapping suggestions from Google Sheets → existing ontology class."""

    sheet_url: str
    worksheet_name: Optional[str] = None
    api_key: Optional[str] = None
    connection_id: Optional[str] = None

    target_class_id: str
    # OMS integration is optional; when disabled, clients should supply the target schema directly.
    target_schema: List[Dict[str, Any]] = Field(default_factory=list)

    table_id: Optional[str] = None
    table_bbox: Optional[BoundingBox] = None

    include_relationships: bool = False
    enable_semantic_hints: bool = False


class ImportFieldMapping(BaseModel):
    """Field mapping for import (source column → target property)."""

    source_field: str
    target_field: str


class ImportTargetField(BaseModel):
    """Target field definition for import (name + type)."""

    name: str
    type: str = "xsd:string"


class ImportFromGoogleSheetsRequest(BaseModel):
    """Request model for dry-run/commit import from Google Sheets."""

    sheet_url: str
    worksheet_name: Optional[str] = None
    api_key: Optional[str] = None
    connection_id: Optional[str] = None

    target_class_id: str
    target_schema: List[ImportTargetField] = Field(default_factory=list)
    mappings: List[ImportFieldMapping] = Field(default_factory=list)

    table_id: Optional[str] = None
    table_bbox: Optional[BoundingBox] = None

    # Extraction controls
    max_tables: int = 5
    max_rows: Optional[int] = None
    max_cols: Optional[int] = None
    trim_trailing_empty: bool = True

    # Import behavior
    allow_partial: bool = False
    dry_run_rows: int = 100
    max_import_rows: Optional[int] = None
    batch_size: int = 500
    return_instances: bool = False
    max_return_instances: int = 1000
    options: Dict[str, Any] = Field(default_factory=dict)

