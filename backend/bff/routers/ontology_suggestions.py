"""Ontology suggestion endpoints (BFF).

This module contains schema inference and mapping suggestion endpoints.
Composed by `bff.routers.ontology` via router composition (Composite pattern).
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, File, Form, Query, UploadFile

from bff.dependencies import TerminusService, TerminusServiceDep
from bff.schemas.ontology_requests import (
    MappingFromGoogleSheetsRequest,
    MappingSuggestionRequest,
    SchemaFromDataRequest,
    SchemaFromGoogleSheetsRequest,
)
from bff.services import ontology_suggestions_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Ontology Management"])


@router.post("/suggest-schema-from-data")
@trace_endpoint("bff.ontology.suggest_schema_from_data")
async def suggest_schema_from_data(
    db_name: str,
    request: SchemaFromDataRequest,
    terminus: TerminusService = TerminusServiceDep,
):
    _ = terminus
    return await ontology_suggestions_service.suggest_schema_from_data(db_name=db_name, body=request)


@router.post("/suggest-mappings")
@trace_endpoint("bff.ontology.suggest_mappings")
async def suggest_mappings(
    db_name: str,
    request: MappingSuggestionRequest,
) -> Dict[str, Any]:
    return await ontology_suggestions_service.suggest_mappings(db_name=db_name, body=request)


@router.post("/suggest-mappings-from-google-sheets")
@trace_endpoint("bff.ontology.suggest_mappings_from_google_sheets")
async def suggest_mappings_from_google_sheets(
    db_name: str,
    request: MappingFromGoogleSheetsRequest,
):
    return await ontology_suggestions_service.suggest_mappings_from_google_sheets(db_name=db_name, body=request)


@router.post("/suggest-mappings-from-excel")
@trace_endpoint("bff.ontology.suggest_mappings_from_excel")
async def suggest_mappings_from_excel(
    db_name: str,
    target_class_id: str = Query(..., description="Target ontology class id"),
    file: UploadFile = File(...),
    target_schema_json: Optional[str] = Form(None, description="JSON array of target schema fields"),
    sheet_name: Optional[str] = None,
    table_id: Optional[str] = None,
    table_top: Optional[int] = None,
    table_left: Optional[int] = None,
    table_bottom: Optional[int] = None,
    table_right: Optional[int] = None,
    include_relationships: bool = False,
    enable_semantic_hints: bool = False,
    max_tables: int = 5,
    max_rows: Optional[int] = None,
    max_cols: Optional[int] = None,
):
    return await ontology_suggestions_service.suggest_mappings_from_excel(
        db_name=db_name,
        target_class_id=target_class_id,
        file=file,
        target_schema_json=target_schema_json,
        sheet_name=sheet_name,
        table_id=table_id,
        table_top=table_top,
        table_left=table_left,
        table_bottom=table_bottom,
        table_right=table_right,
        include_relationships=include_relationships,
        enable_semantic_hints=enable_semantic_hints,
        max_tables=max_tables,
        max_rows=max_rows,
        max_cols=max_cols,
    )


@router.post("/suggest-schema-from-google-sheets")
@trace_endpoint("bff.ontology.suggest_schema_from_google_sheets")
async def suggest_schema_from_google_sheets(
    db_name: str,
    request: SchemaFromGoogleSheetsRequest,
    terminus: TerminusService = TerminusServiceDep,
):
    _ = terminus
    return await ontology_suggestions_service.suggest_schema_from_google_sheets(db_name=db_name, body=request)


@router.post("/suggest-schema-from-excel")
@trace_endpoint("bff.ontology.suggest_schema_from_excel")
async def suggest_schema_from_excel(
    db_name: str,
    file: UploadFile = File(...),
    sheet_name: Optional[str] = None,
    class_name: Optional[str] = None,
    table_id: Optional[str] = None,
    table_top: Optional[int] = None,
    table_left: Optional[int] = None,
    table_bottom: Optional[int] = None,
    table_right: Optional[int] = None,
    include_complex_types: bool = True,
    max_tables: int = 5,
    max_rows: Optional[int] = None,
    max_cols: Optional[int] = None,
):
    return await ontology_suggestions_service.suggest_schema_from_excel(
        db_name=db_name,
        file=file,
        sheet_name=sheet_name,
        class_name=class_name,
        table_id=table_id,
        table_top=table_top,
        table_left=table_left,
        table_bottom=table_bottom,
        table_right=table_right,
        include_complex_types=include_complex_types,
        max_tables=max_tables,
        max_rows=max_rows,
        max_cols=max_cols,
    )

