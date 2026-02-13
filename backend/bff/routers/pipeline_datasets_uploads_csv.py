"""Pipeline dataset CSV upload endpoints (BFF).

Composed by `bff.routers.pipeline_datasets_uploads` via router composition (Composite pattern).
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status

from bff.routers.pipeline_datasets_ops import (
    _build_funnel_analysis_payload,
    _build_schema_columns,
    _columns_from_schema,
    _default_dataset_name,
    _detect_csv_delimiter,
    _parse_csv_file,
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


@router.post("/datasets/csv-upload", response_model=ApiResponse)
@trace_endpoint("upload_csv_dataset")
async def upload_csv_dataset(
    db_name: str = Query(..., description="Database name"),
    branch: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    delimiter: Optional[str] = Form(None),
    has_header: bool = Form(True),
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

        filename = file.filename or "upload.csv"
        if not filename.lower().endswith(".csv"):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Only .csv files are supported", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        sample_bytes = await asyncio.to_thread(file.file.read, 65536)
        if not sample_bytes:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Empty file", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        resolved_name = (dataset_name or "").strip() or _default_dataset_name(filename)
        if not resolved_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "dataset_name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        sample_text = sample_bytes.decode("utf-8", errors="replace")
        resolved_delimiter = delimiter or _detect_csv_delimiter(sample_text)
        columns, preview_rows, total_rows, content_hash = await asyncio.to_thread(
            _parse_csv_file,
            file.file,
            delimiter=resolved_delimiter,
            has_header=has_header,
        )
        if not columns:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "No columns detected", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        inferred_schema: list[Dict[str, Any]] = []
        analysis_payload: Optional[Dict[str, Any]] = None
        try:
            from bff.services.funnel_client import FunnelClient

            settings = get_settings()
            async with FunnelClient() as funnel_client:
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
            inferred_schema = (analysis_payload or {}).get("columns") or []
        except Exception as exc:
            logger.warning("CSV type inference failed: %s", exc)
            from shared.services.pipeline.pipeline_funnel_fallback import build_funnel_analysis_fallback

            analysis_payload = build_funnel_analysis_fallback(
                columns=columns,
                rows=preview_rows,
                include_complex_types=True,
                error=str(exc),
                stage="bff",
            )
            inferred_schema = (analysis_payload or {}).get("columns") or []

        funnel_analysis = _build_funnel_analysis_payload(analysis_payload, inferred_schema)
        schema_columns = _build_schema_columns(columns, inferred_schema)
        schema_json = {"columns": schema_columns}
        sample_rows = _rows_from_preview(columns, preview_rows)
        row_count = total_rows if total_rows > 0 else len(sample_rows)

        source_metadata = {
            "type": "csv",
            "filename": filename,
            "delimiter": resolved_delimiter,
            "has_header": has_header,
        }
        sample_json = {"columns": _columns_from_schema(schema_columns), "rows": sample_rows}
        preview_payload: Dict[str, Any] = {
            "columns": _columns_from_schema(schema_columns),
            "rows": sample_rows,
        }
        return await finalize_tabular_upload(
            ctx=ctx,
            dataset_name=resolved_name,
            description=description,
            source_type="csv_upload",
            source_ref=filename,
            request_fingerprint_payload={
                "db_name": db_name,
                "branch": dataset_branch,
                "dataset_name": resolved_name,
                "source_type": "csv_upload",
                "filename": filename,
                "delimiter": resolved_delimiter,
                "has_header": has_header,
                "content_sha256": content_hash,
            },
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
            artifact_fileobj=file.file,
            artifact_basename="source.csv",
            artifact_content_type="text/csv",
            content_sha256=content_hash,
            commit_message=f"CSV dataset upload {db_name}/{resolved_name}",
            commit_metadata_extra={
                "delimiter": resolved_delimiter,
                "has_header": has_header,
            },
            lineage_label="csv_upload",
            dataset_registry=dataset_registry,
            objectify_registry=objectify_registry,
            objectify_job_queue=objectify_job_queue,
            lineage_store=lineage_store,
            preview_payload=preview_payload,
            funnel_analysis=funnel_analysis,
            success_message="CSV dataset created",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to upload csv dataset")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)
