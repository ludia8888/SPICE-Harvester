"""Ontology import endpoints (BFF).

This module contains endpoints that build instances from Sheets/Excel and kick
off bulk writes through OMS.

Business logic lives in `bff.services.ontology_imports_service` (Facade).
Composed by `bff.routers.ontology` via router composition (Composite pattern).
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from typing import Optional

from fastapi import APIRouter, File, Form, UploadFile

from bff.dependencies import OMSClientDep
from bff.schemas.ontology_requests import ImportFromGoogleSheetsRequest
from bff.services import ontology_imports_service
from bff.services.oms_client import OMSClient

router = APIRouter(tags=["Ontology Management"])


@router.post("/import-from-google-sheets/dry-run")
@trace_endpoint("bff.ontology.dry_run_import_from_google_sheets")
async def dry_run_import_from_google_sheets(
    db_name: str,
    request: ImportFromGoogleSheetsRequest,
):
    return await ontology_imports_service.dry_run_import_from_google_sheets(db_name=db_name, body=request)


@router.post("/import-from-google-sheets/commit")
@trace_endpoint("bff.ontology.commit_import_from_google_sheets")
async def commit_import_from_google_sheets(
    db_name: str,
    request: ImportFromGoogleSheetsRequest,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_imports_service.commit_import_from_google_sheets(
        db_name=db_name,
        body=request,
        oms_client=oms_client,
    )


@router.post("/import-from-excel/dry-run")
@trace_endpoint("bff.ontology.dry_run_import_from_excel")
async def dry_run_import_from_excel(
    db_name: str,
    file: UploadFile = File(...),
    target_class_id: str = Form(...),
    target_schema_json: Optional[str] = Form(None),
    mappings_json: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    max_tables: int = Form(5),
    max_rows: Optional[int] = Form(None),
    max_cols: Optional[int] = Form(None),
    dry_run_rows: int = Form(100),
    max_import_rows: Optional[int] = Form(None),
    options_json: Optional[str] = Form(None),
):
    return await ontology_imports_service.dry_run_import_from_excel(
        db_name=db_name,
        file=file,
        target_class_id=target_class_id,
        target_schema_json=target_schema_json,
        mappings_json=mappings_json,
        sheet_name=sheet_name,
        table_id=table_id,
        table_top=table_top,
        table_left=table_left,
        table_bottom=table_bottom,
        table_right=table_right,
        max_tables=max_tables,
        max_rows=max_rows,
        max_cols=max_cols,
        dry_run_rows=dry_run_rows,
        max_import_rows=max_import_rows,
        options_json=options_json,
    )


@router.post("/import-from-excel/commit")
@trace_endpoint("bff.ontology.commit_import_from_excel")
async def commit_import_from_excel(
    db_name: str,
    file: UploadFile = File(...),
    target_class_id: str = Form(...),
    target_schema_json: Optional[str] = Form(None),
    mappings_json: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    max_tables: int = Form(5),
    max_rows: Optional[int] = Form(None),
    max_cols: Optional[int] = Form(None),
    allow_partial: bool = Form(False),
    max_import_rows: Optional[int] = Form(None),
    batch_size: int = Form(500),
    return_instances: bool = Form(False),
    max_return_instances: int = Form(1000),
    options_json: Optional[str] = Form(None),
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_imports_service.commit_import_from_excel(
        db_name=db_name,
        file=file,
        target_class_id=target_class_id,
        target_schema_json=target_schema_json,
        mappings_json=mappings_json,
        sheet_name=sheet_name,
        table_id=table_id,
        table_top=table_top,
        table_left=table_left,
        table_bottom=table_bottom,
        table_right=table_right,
        max_tables=max_tables,
        max_rows=max_rows,
        max_cols=max_cols,
        allow_partial=allow_partial,
        max_import_rows=max_import_rows,
        batch_size=batch_size,
        return_instances=return_instances,
        max_return_instances=max_return_instances,
        options_json=options_json,
        oms_client=oms_client,
    )

