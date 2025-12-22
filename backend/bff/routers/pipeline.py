"""
Pipeline Builder API (BFF)

Provides CRUD for pipelines + preview/build entrypoints.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status

from shared.models.requests import ApiResponse
from shared.models.event_envelope import EventEnvelope
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_executor import PipelineExecutor
from shared.services.event_store import event_store
from shared.dependencies.providers import StorageServiceDep, LineageStoreDep
from shared.services.pipeline_artifact_store import PipelineArtifactStore
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.observability.tracing import trace_endpoint

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipelines", tags=["Pipeline Builder"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def get_pipeline_registry() -> PipelineRegistry:
    from bff.main import get_pipeline_registry as _get_pipeline_registry

    return await _get_pipeline_registry()


async def get_pipeline_executor() -> PipelineExecutor:
    from bff.main import get_pipeline_executor as _get_pipeline_executor

    return await _get_pipeline_executor()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_location(location: str) -> str:
    cleaned = (location or "").strip()
    if not cleaned:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="location is required")
    if "personal" in cleaned.lower():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="personal locations are not allowed",
        )
    return cleaned


def _default_dataset_name(filename: str) -> str:
    base = (filename or "").strip()
    if not base:
        return "excel_dataset"
    if "." in base:
        base = ".".join(base.split(".")[:-1]) or base
    return base or "excel_dataset"


def _normalize_table_bbox(
    *,
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
) -> Optional[Dict[str, int]]:
    parts = [table_top, table_left, table_bottom, table_right]
    if all(part is None for part in parts):
        return None
    if any(part is None for part in parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="table_top/table_left/table_bottom/table_right must be provided together",
        )
    return {
        "top": int(table_top),
        "left": int(table_left),
        "bottom": int(table_bottom),
        "right": int(table_right),
    }


def _build_schema_columns(columns: list[str], inferred_schema: list[Dict[str, Any]]) -> list[Dict[str, str]]:
    inferred_map: Dict[str, str] = {}
    for item in inferred_schema or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        inferred_map[name] = str(item.get("type") or item.get("data_type") or "String")

    schema_columns: list[Dict[str, str]] = []
    for col in columns:
        col_name = str(col)
        if not col_name:
            continue
        schema_columns.append({"name": col_name, "type": inferred_map.get(col_name, "String")})
    return schema_columns


def _columns_from_schema(schema_columns: list[Dict[str, str]]) -> list[Dict[str, str]]:
    return [
        {"name": str(col.get("name") or ""), "type": str(col.get("type") or "String")}
        for col in schema_columns
        if str(col.get("name") or "")
    ]


def _rows_from_preview(columns: list[str], sample_rows: list[list[Any]]) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for row in sample_rows or []:
        if not isinstance(row, list):
            continue
        payload: Dict[str, Any] = {}
        for index, col in enumerate(columns):
            payload[str(col)] = row[index] if index < len(row) else None
        rows.append(payload)
    return rows


def _build_event(
    *,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    data: Dict[str, Any],
    command_type: str,
    actor: Optional[str] = None,
) -> EventEnvelope:
    command_payload = {
        "command_id": str(uuid4()),
        "command_type": command_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "payload": data,
        "metadata": {},
        "created_at": _utcnow().isoformat(),
        "created_by": actor,
        "version": 1,
    }
    return EventEnvelope(
        event_id=command_payload["command_id"],
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=_utcnow(),
        actor=actor,
        data=command_payload,
        metadata={
            "kind": "command",
            "command_type": command_type,
            "command_version": 1,
            "command_id": command_payload["command_id"],
        },
    )


@router.get("", response_model=ApiResponse)
@trace_endpoint("list_pipelines")
async def list_pipelines(
    db_name: str,
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        pipelines = await pipeline_registry.list_pipelines(db_name=db_name)
        return ApiResponse.success(
            message="Pipelines fetched",
            data={"pipelines": pipelines, "count": len(pipelines)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("get_pipeline")
async def get_pipeline(
    pipeline_id: str,
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    try:
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        version = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id)
        return ApiResponse.success(
            message="Pipeline fetched",
            data={
                "pipeline": {
                    **pipeline.__dict__,
                    "definition_json": version.definition_json if version else {},
                    "version": version.version if version else None,
                }
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("", response_model=ApiResponse)
@trace_endpoint("create_pipeline")
async def create_pipeline(
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name is required")
        pipeline_type = str(sanitized.get("pipeline_type") or "batch").strip() or "batch"
        location = _normalize_location(str(sanitized.get("location") or ""))
        description = str(sanitized.get("description") or "").strip() or None
        definition_json = sanitized.get("definition_json") if isinstance(sanitized, dict) else {}
        if not isinstance(definition_json, dict):
            definition_json = {}

        record = await pipeline_registry.create_pipeline(
            db_name=db_name,
            name=name,
            description=description,
            pipeline_type=pipeline_type,
            location=location,
        )
        version = await pipeline_registry.add_version(
            pipeline_id=record.pipeline_id,
            definition_json=definition_json,
        )

        event = _build_event(
            event_type="PIPELINE_CREATED",
            aggregate_type="Pipeline",
            aggregate_id=record.pipeline_id,
            data={
                "db_name": db_name,
                "pipeline_id": record.pipeline_id,
                "name": name,
                "pipeline_type": pipeline_type,
                "location": location,
                "definition_json": definition_json,
                "version": version.version,
            },
            command_type="CREATE_PIPELINE",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline create event: {e}")

        return ApiResponse.success(
            message="Pipeline created",
            data={
                "pipeline": {**record.__dict__, "definition_json": definition_json, "version": version.version},
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("update_pipeline")
async def update_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        definition_json = sanitized.get("definition_json") if isinstance(sanitized, dict) else None
        if definition_json is not None and not isinstance(definition_json, dict):
            definition_json = {}
        update = await pipeline_registry.update_pipeline(
            pipeline_id=pipeline_id,
            name=str(sanitized.get("name") or "").strip() or None,
            description=str(sanitized.get("description") or "").strip() or None,
            location=(_normalize_location(str(sanitized.get("location") or "")) if "location" in sanitized else None),
            status=str(sanitized.get("status") or "").strip() or None,
        )
        version = None
        if definition_json is not None:
            version = await pipeline_registry.add_version(
                pipeline_id=pipeline_id,
                definition_json=definition_json,
            )

        event = _build_event(
            event_type="PIPELINE_UPDATED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={
                "pipeline_id": pipeline_id,
                "definition_json": definition_json,
                "version": version.version if version else None,
            },
            command_type="UPDATE_PIPELINE",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline update event: {e}")

        return ApiResponse.success(
            message="Pipeline updated",
            data={
                "pipeline": {
                    **update.__dict__,
                    "definition_json": definition_json if definition_json is not None else None,
                    "version": version.version if version else None,
                }
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/preview", response_model=ApiResponse)
@trace_endpoint("preview_pipeline")
async def preview_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_executor: PipelineExecutor = Depends(get_pipeline_executor),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        limit = int(sanitized.get("limit") or 200)
        limit = max(1, min(limit, 500))
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        db_name = str(sanitized.get("db_name") or "").strip()
        if not definition_json:
            latest = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id)
            definition_json = latest.definition_json if latest else {}
        if not db_name:
            pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")

        sample = await pipeline_executor.preview(
            definition=definition_json,
            db_name=db_name,
            node_id=node_id,
            limit=limit,
        )

        await pipeline_registry.record_preview(
            pipeline_id=pipeline_id,
            status="SUCCESS",
            row_count=limit,
            sample_json=sample,
        )

        event = _build_event(
            event_type="PIPELINE_PREVIEWED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={"pipeline_id": pipeline_id, "row_limit": limit},
            command_type="RUN_PIPELINE_PREVIEW",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline preview event: {e}")

        return ApiResponse.success(
            message="Preview generated",
            data={"pipeline_id": pipeline_id, "limit": limit, "sample": sample},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to preview pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{pipeline_id}/deploy", response_model=ApiResponse)
@trace_endpoint("deploy_pipeline")
async def deploy_pipeline(
    pipeline_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_executor: PipelineExecutor = Depends(get_pipeline_executor),
    storage_service: StorageServiceDep = Depends(),
    lineage_store: LineageStoreDep = Depends(),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        output = sanitized.get("output") if isinstance(sanitized.get("output"), dict) else {}
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        dataset_name = str(output.get("dataset_name") or "").strip()
        db_name = str(output.get("db_name") or "").strip()

        if not definition_json:
            latest = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id)
            definition_json = latest.definition_json if latest else {}
        if not db_name:
            pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")
        if dataset_name:
            db_name = validate_db_name(db_name)

        table, deploy_meta = await pipeline_executor.deploy(
            definition=definition_json,
            db_name=db_name,
            node_id=node_id,
            dataset_name=dataset_name or "pipeline_output",
        )

        artifact_bucket = os.getenv("PIPELINE_ARTIFACT_BUCKET", "pipeline-artifacts")
        artifact_object_key: Optional[str] = None
        artifact_uri: Optional[str] = None
        if storage_service:
            artifact_store = PipelineArtifactStore(storage_service, artifact_bucket)
            artifact_uri = await artifact_store.save_table(
                dataset_name=dataset_name or "pipeline_output",
                columns=table.columns,
                rows=table.rows,
                db_name=db_name,
                pipeline_id=pipeline_id,
            )
            deploy_meta["artifact_key"] = artifact_uri
            parsed = parse_s3_uri(artifact_uri)
            if parsed:
                artifact_bucket, artifact_object_key = parsed

        output_payload = {
            "db_name": db_name,
            "dataset_name": dataset_name or "pipeline_output",
            "row_count": deploy_meta.get("row_count"),
            "artifact_key": deploy_meta.get("artifact_key"),
            "schema_json": {"columns": [{"name": name, "type": "String"} for name in table.columns]},
            "sample_json": {"columns": [{"name": name, "type": "String"} for name in table.columns], "rows": table.rows},
        }

        dataset_id: Optional[str] = None
        if dataset_name:
            existing = await dataset_registry.get_dataset_by_name(db_name=db_name, name=dataset_name)
            if not existing:
                dataset = await dataset_registry.create_dataset(
                    db_name=db_name,
                    name=dataset_name,
                    description=None,
                    source_type="pipeline",
                    source_ref=pipeline_id,
                    schema_json=output_payload["schema_json"],
                )
                dataset_id = dataset.dataset_id
            else:
                dataset_id = existing.dataset_id

        if dataset_id:
            try:
                await dataset_registry.add_version(
                    dataset_id=dataset_id,
                    artifact_key=str(deploy_meta.get("artifact_key") or "").strip() or None,
                    row_count=int(deploy_meta.get("row_count") or 0) or None,
                    sample_json=output_payload["sample_json"],
                    schema_json=output_payload["schema_json"],
                )
            except ValueError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        latest_version = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id)
        await pipeline_registry.record_build(
            pipeline_id=pipeline_id,
            status="DEPLOYED",
            output_json=output,
            deployed_version=latest_version.version if latest_version else None,
        )

        event = _build_event(
            event_type="PIPELINE_DEPLOYED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={"pipeline_id": pipeline_id, "output": output_payload},
            command_type="DEPLOY_PIPELINE",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline deploy event: {e}")

        if lineage_store and artifact_object_key:
            await lineage_store.record_link(
                from_node_id=lineage_store.node_event(str(event.event_id)),
                to_node_id=lineage_store.node_artifact("s3", artifact_bucket, artifact_object_key),
                edge_type="pipeline_output_stored",
                occurred_at=_utcnow(),
                from_label="pipeline_deploy",
                to_label=build_s3_uri(artifact_bucket, artifact_object_key),
                db_name=db_name,
                edge_metadata={
                    "db_name": db_name,
                    "bucket": artifact_bucket,
                    "key": artifact_object_key,
                    "dataset_name": dataset_name or "pipeline_output",
                    "pipeline_id": pipeline_id,
                },
            )

        return ApiResponse.success(
            message="Pipeline deployed",
            data={
                "pipeline_id": pipeline_id,
                "output": output_payload,
                "deployed_version": latest_version.version if latest_version else None,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to deploy pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/datasets", response_model=ApiResponse)
@trace_endpoint("list_datasets")
async def list_datasets(
    db_name: str,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        datasets = await dataset_registry.list_datasets(db_name=db_name)
        return ApiResponse.success(
            message="Datasets fetched",
            data={"datasets": datasets, "count": len(datasets)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets", response_model=ApiResponse)
@trace_endpoint("create_dataset")
async def create_dataset(
    payload: Dict[str, Any],
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name is required")
        description = str(sanitized.get("description") or "").strip() or None
        source_type = str(sanitized.get("source_type") or "manual").strip() or "manual"
        source_ref = str(sanitized.get("source_ref") or "").strip() or None
        schema_json = sanitized.get("schema_json") if isinstance(sanitized.get("schema_json"), dict) else {}

        dataset = await dataset_registry.create_dataset(
            db_name=db_name,
            name=name,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            schema_json=schema_json,
        )

        event = _build_event(
            event_type="DATASET_CREATED",
            aggregate_type="Dataset",
            aggregate_id=dataset.dataset_id,
            data={"dataset_id": dataset.dataset_id, "db_name": db_name, "name": name},
            command_type="CREATE_DATASET",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append dataset create event: {e}")

        return ApiResponse.success(
            message="Dataset created",
            data={"dataset": dataset.__dict__},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/{dataset_id}/versions", response_model=ApiResponse)
@trace_endpoint("create_dataset_version")
async def create_dataset_version(
    dataset_id: str,
    payload: Dict[str, Any],
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        sample_json = sanitized.get("sample_json") if isinstance(sanitized.get("sample_json"), dict) else {}
        schema_json = sanitized.get("schema_json") if isinstance(sanitized.get("schema_json"), dict) else None
        row_count = None
        if sanitized.get("row_count") is not None:
            try:
                row_count = int(sanitized.get("row_count"))
            except Exception:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="row_count must be integer")
        try:
            version = await dataset_registry.add_version(
                dataset_id=dataset_id,
                artifact_key=str(sanitized.get("artifact_key") or "").strip() or None,
                row_count=row_count,
                sample_json=sample_json,
                schema_json=schema_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        event = _build_event(
            event_type="DATASET_VERSION_CREATED",
            aggregate_type="Dataset",
            aggregate_id=dataset_id,
            data={"dataset_id": dataset_id, "version": version.version},
            command_type="INGEST_DATASET_SNAPSHOT",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append dataset version event: {e}")

        return ApiResponse.success(
            message="Dataset version created",
            data={"version": version.__dict__},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create dataset version: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/excel-upload", response_model=ApiResponse)
@trace_endpoint("upload_excel_dataset")
async def upload_excel_dataset(
    db_name: str = Query(..., description="Database name"),
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    storage_service: StorageServiceDep = Depends(),
    lineage_store: LineageStoreDep = Depends(),
) -> ApiResponse:
    """
    Excel 업로드 → preview/스키마 추론 → dataset registry 저장 + artifact 저장

    Pipeline Builder 전용: raw 데이터를 canonical dataset으로 저장합니다.
    """
    try:
        db_name = validate_db_name(db_name)
        filename = file.filename or "upload.xlsx"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .xlsx/.xlsm files are supported",
            )

        content = await file.read()
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        resolved_name = (dataset_name or "").strip() or _default_dataset_name(filename)
        if not resolved_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_name is required")

        bbox = _normalize_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_structure_preview(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=bbox,
                include_complex_types=True,
            )

        preview = result.get("preview") or {}
        columns = [str(col) for col in (preview.get("columns") or []) if str(col)]
        inferred_schema = preview.get("inferred_schema") or []
        schema_columns = _build_schema_columns(columns, inferred_schema)
        schema_json = {"columns": schema_columns}
        sample_rows = _rows_from_preview(columns, preview.get("sample_data") or [])
        total_rows = preview.get("total_rows")
        row_count = int(total_rows) if isinstance(total_rows, int) else None
        if row_count is None and sample_rows:
            row_count = len(sample_rows)

        dataset = await dataset_registry.get_dataset_by_name(db_name=db_name, name=resolved_name)
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=resolved_name,
                description=description.strip() if description else None,
                source_type="excel_upload",
                source_ref=filename,
                schema_json=schema_json,
            )
            created_dataset = True

        if created_dataset:
            create_event = _build_event(
                event_type="DATASET_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset.dataset_id,
                data={"dataset_id": dataset.dataset_id, "db_name": db_name, "name": resolved_name},
                command_type="CREATE_DATASET",
            )
            try:
                await event_store.connect()
                await event_store.append_event(create_event)
            except Exception as e:
                logger.warning(f"Failed to append excel dataset create event: {e}")

        artifact_bucket = os.getenv("DATASET_ARTIFACT_BUCKET") or os.getenv(
            "PIPELINE_ARTIFACT_BUCKET", "pipeline-artifacts"
        )
        artifact_key = None
        if storage_service:
            safe_name = resolved_name.replace(" ", "_")
            timestamp = _utcnow().strftime("%Y%m%dT%H%M%SZ")
            object_key = f"datasets/{db_name}/{dataset.dataset_id}/{safe_name}/{timestamp}.xlsx"
            await storage_service.create_bucket(artifact_bucket)
            await storage_service.save_bytes(
                artifact_bucket,
                object_key,
                content,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                metadata={
                    "db_name": db_name,
                    "dataset_name": resolved_name,
                    "source": "excel_upload",
                },
            )
            artifact_key = build_s3_uri(artifact_bucket, object_key)

        try:
            version = await dataset_registry.add_version(
                dataset_id=dataset.dataset_id,
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json={"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                schema_json=schema_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        event = _build_event(
            event_type="DATASET_VERSION_CREATED",
            aggregate_type="Dataset",
            aggregate_id=dataset.dataset_id,
            data={
                "dataset_id": dataset.dataset_id,
                "db_name": db_name,
                "name": resolved_name,
                "version": version.version,
                "artifact_key": artifact_key,
            },
            command_type="INGEST_DATASET_SNAPSHOT",
        )
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append excel dataset event: {e}")

        if lineage_store and artifact_key:
            parsed = parse_s3_uri(artifact_key)
            if parsed:
                bucket, key = parsed
                try:
                    await lineage_store.record_link(
                        from_node_id=lineage_store.node_event(str(event.event_id)),
                        to_node_id=lineage_store.node_artifact("s3", bucket, key),
                        edge_type="dataset_artifact_stored",
                        occurred_at=_utcnow(),
                        from_label="excel_upload",
                        to_label=artifact_key,
                        db_name=db_name,
                        edge_metadata={
                            "db_name": db_name,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": resolved_name,
                            "bucket": bucket,
                            "key": key,
                            "source": "excel_upload",
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to record excel dataset lineage: {e}")

        return ApiResponse.success(
            message="Excel dataset created",
            data={
                "dataset": dataset.__dict__,
                "version": version.__dict__,
                "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                "source": preview.get("source_metadata"),
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload excel dataset: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
