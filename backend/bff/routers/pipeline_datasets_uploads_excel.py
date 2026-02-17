"""Pipeline dataset Excel upload endpoints (BFF).

Composed by `bff.routers.pipeline_datasets_uploads` via router composition (Composite pattern).
"""

import asyncio
import io
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status

from bff.routers.pipeline_datasets_ops import (
    _build_tabular_analysis_payload,
    _build_schema_columns,
    _columns_from_schema,
    _convert_xls_to_xlsx_bytes,
    _default_dataset_name,
    _normalize_table_bbox,
    _rows_from_preview,
)
from bff.routers.pipeline_datasets_deps import get_objectify_job_queue
from bff.routers.pipeline_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry
from bff.services.pipeline_dataset_upload_context import _prepare_dataset_upload_context
from bff.services.pipeline_tabular_upload_facade import finalize_tabular_upload
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.dependencies.providers import LineageStoreDep
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])


@router.post("/datasets/excel-upload", response_model=ApiResponse)
@trace_endpoint("upload_excel_dataset")
async def upload_excel_dataset(
    db_name: str = Query(..., description="Database name"),
    branch: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    try:
        ctx = await _prepare_dataset_upload_context(
            request=request,
            db_name=db_name,
            branch=branch,
            pipeline_registry=pipeline_registry,
        )
        db_name = ctx.db_name
        dataset_branch = ctx.dataset_branch

        filename = file.filename or "upload.xlsx"
        lower_filename = filename.lower()
        is_xls = lower_filename.endswith(".xls")
        if not lower_filename.endswith((".xlsx", ".xlsm", ".xls")):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Only .xls, .xlsx, or .xlsm files are supported",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        sample_bytes = await asyncio.to_thread(file.file.read, 65536)
        if not sample_bytes:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Empty file", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        try:
            file.file.seek(0)
        except (AttributeError, OSError, ValueError):
            logging.getLogger(__name__).warning("Failed to rewind uploaded Excel stream", exc_info=True)
        upload_stream: Any = file.file
        if is_xls:
            raw_bytes = await asyncio.to_thread(file.file.read)
            if not raw_bytes:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Empty file", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            try:
                file.file.seek(0)
            except (AttributeError, OSError, ValueError):
                logging.getLogger(__name__).warning("Failed to rewind compatibility XLS stream", exc_info=True)
            converted = await asyncio.to_thread(_convert_xls_to_xlsx_bytes, raw_bytes)
            stem = filename.rsplit(".", 1)[0].strip() or "upload"
            filename = f"{stem}.xlsx"
            upload_stream = io.BytesIO(converted)

        resolved_name = (dataset_name or "").strip() or _default_dataset_name(filename)
        if not resolved_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "dataset_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        bbox = _normalize_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        from bff.services.funnel_client import FunnelClient

        preview: Dict[str, Any] = {}
        content_hash = ""
        columns: list[str] = []
        preview_rows: list[list[Any]] = []
        inferred_schema: list[Dict[str, Any]] = []
        analysis_payload: Optional[Dict[str, Any]] = None

        settings = get_settings()
        async with FunnelClient() as funnel_client:
            result, content_hash = await funnel_client.excel_to_structure_preview_stream(
                fileobj=upload_stream,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=bbox,
                include_complex_types=True,
            )
            preview = result.get("preview") or {}
            columns = [str(col) for col in (preview.get("columns") or []) if str(col)]
            preview_rows = preview.get("sample_data") or []
            inferred_schema = preview.get("inferred_schema") or []
            if columns and preview_rows:
                try:
                    analysis = await funnel_client.analyze_dataset(
                        {
                            "data": preview_rows,
                            "columns": columns,
                            "sample_size": min(len(preview_rows), 500),
                            "include_complex_types": True,
                        },
                        timeout_seconds=float(settings.services.funnel_infer_timeout_seconds),
                    )
                    analysis_payload = analysis if isinstance(analysis, dict) else None
                except Exception as exc:
                    logger.warning("Excel type inference risk assessment failed: %s", exc)

        schema_columns = _build_schema_columns(columns, inferred_schema)
        schema_json = {"columns": schema_columns}
        sample_rows = _rows_from_preview(columns, preview_rows)
        total_rows = preview.get("total_rows")
        row_count: Optional[int] = int(total_rows) if isinstance(total_rows, int) else None
        if row_count is None and sample_rows:
            row_count = len(sample_rows)

        tabular_analysis = _build_tabular_analysis_payload(analysis_payload, inferred_schema)

        sample_json = {"columns": _columns_from_schema(schema_columns), "rows": sample_rows}
        preview_payload: Dict[str, Any] = {
            "columns": _columns_from_schema(schema_columns),
            "rows": sample_rows,
        }
        source_metadata = preview.get("source_metadata") if isinstance(preview, dict) else None
        return await finalize_tabular_upload(
            ctx=ctx,
            dataset_name=resolved_name,
            description=description,
            source_type="excel_upload",
            source_ref=filename,
            request_fingerprint_payload={
                "db_name": db_name,
                "branch": dataset_branch,
                "dataset_name": resolved_name,
                "source_type": "excel_upload",
                "filename": filename,
                "sheet_name": sheet_name,
                "table_id": table_id,
                "table_bbox": bbox,
                "content_sha256": content_hash,
            },
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata if isinstance(source_metadata, dict) else None,
            artifact_fileobj=upload_stream,
            artifact_basename="source.xlsx",
            artifact_content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            content_sha256=content_hash,
            commit_message=f"Excel dataset upload {db_name}/{resolved_name}",
            commit_metadata_extra={},
            lineage_label="excel_upload",
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            objectify_job_queue=objectify_job_queue,
            lineage_store=lineage_store,
            preview_payload=preview_payload,
            tabular_analysis=tabular_analysis,
            success_message="Excel dataset created",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to upload excel dataset: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)
