"""
Pipeline Builder API (BFF)

Provides CRUD for pipelines + preview/build entrypoints.
"""

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status

from shared.models.requests import ApiResponse
from shared.models.event_envelope import EventEnvelope
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_executor import PipelineExecutor
from shared.services.pipeline_job_queue import PipelineJobQueue
from shared.models.pipeline_job import PipelineJob
from shared.services.event_store import event_store
from shared.dependencies.providers import StorageServiceDep, LineageStoreDep
from shared.services.pipeline_artifact_store import PipelineArtifactStore
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.observability.tracing import trace_endpoint
from shared.config.app_config import AppConfig

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


async def get_pipeline_job_queue() -> PipelineJobQueue:
    return PipelineJobQueue()


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


def _normalize_inferred_type(type_value: Any) -> str:
    if not type_value:
        return "xsd:string"
    raw = str(type_value).strip()
    if not raw:
        return "xsd:string"
    if raw.startswith("xsd:"):
        return raw
    lowered = raw.lower()
    if lowered == "money":
        return "xsd:decimal"
    if lowered in {"integer", "int"}:
        return "xsd:integer"
    if lowered in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if lowered in {"boolean", "bool"}:
        return "xsd:boolean"
    if lowered in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if lowered == "date":
        return "xsd:dateTime"
    return "xsd:string"


def _build_schema_columns(columns: list[str], inferred_schema: list[Dict[str, Any]]) -> list[Dict[str, str]]:
    inferred_map: Dict[str, str] = {}
    for item in inferred_schema or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("column_name") or "").strip()
        if not name:
            continue
        inferred_type = item.get("type") or item.get("data_type")
        if not inferred_type:
            inferred_payload = item.get("inferred_type")
            if isinstance(inferred_payload, dict):
                inferred_type = inferred_payload.get("type")
        inferred_map[name] = _normalize_inferred_type(inferred_type)

    schema_columns: list[Dict[str, str]] = []
    for col in columns:
        col_name = str(col)
        if not col_name:
            continue
        schema_columns.append({"name": col_name, "type": inferred_map.get(col_name, "xsd:string")})
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


def _detect_csv_delimiter(sample: str) -> str:
    candidates = [",", "\t", ";", "|"]
    line = next((line for line in sample.splitlines() if line.strip()), "")
    if not line:
        return ","
    scores = {delimiter: line.count(delimiter) for delimiter in candidates}
    best = max(scores, key=scores.get)
    return best if scores.get(best, 0) > 0 else ","


def _parse_csv_content(
    content: str,
    *,
    delimiter: str,
    has_header: bool,
    preview_limit: int = 200,
) -> tuple[list[str], list[list[str]], int]:
    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    columns: list[str] = []
    preview_rows: list[list[str]] = []
    total_rows = 0

    for row in reader:
        if not row or all(str(cell).strip() == "" for cell in row):
            continue
        if not columns:
            if has_header:
                columns = [str(cell).strip() or f"column_{index + 1}" for index, cell in enumerate(row)]
                continue
            columns = [f"column_{index + 1}" for index in range(len(row))]
        normalized = [str(cell).strip() for cell in row]
        if len(normalized) < len(columns):
            normalized.extend([""] * (len(columns) - len(normalized)))
        elif len(normalized) > len(columns):
            normalized = normalized[: len(columns)]
        total_rows += 1
        if len(preview_rows) < preview_limit:
            preview_rows.append(normalized)

    return columns, preview_rows, total_rows


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


def _sanitize_s3_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not metadata:
        return {}
    sanitized: Dict[str, str] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        raw = str(value)
        try:
            raw.encode("ascii")
            sanitized[key] = raw
        except UnicodeEncodeError:
            sanitized[key] = quote(raw, safe="")
    return sanitized


@router.get("", response_model=ApiResponse)
@trace_endpoint("list_pipelines")
async def list_pipelines(
    db_name: str,
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


@router.get("/datasets", response_model=ApiResponse)
@trace_endpoint("list_datasets")
async def list_datasets(
    db_name: str,
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


@router.get("/{pipeline_id}", response_model=ApiResponse)
@trace_endpoint("get_pipeline")
async def get_pipeline(
    pipeline_id: str,
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
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
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
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
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
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    pipeline_job_queue: PipelineJobQueue = Depends(get_pipeline_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        output = sanitized.get("output") if isinstance(sanitized.get("output"), dict) else {}
        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        node_id = str(sanitized.get("node_id") or "").strip() or None
        dataset_name = str(output.get("dataset_name") or "").strip()
        db_name = str(output.get("db_name") or "").strip()
        schedule_interval_seconds = None
        schedule = sanitized.get("schedule") or output.get("schedule")
        if isinstance(schedule, dict):
            schedule_interval_seconds = schedule.get("interval_seconds")
        elif isinstance(schedule, (int, float, str)):
            try:
                schedule_interval_seconds = int(schedule)
            except (TypeError, ValueError):
                schedule_interval_seconds = None

        if not definition_json:
            latest = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id)
            definition_json = latest.definition_json if latest else {}
        pipeline = None
        if not db_name:
            pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
            db_name = pipeline.db_name if pipeline else ""
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")
        db_name = validate_db_name(db_name)

        job = PipelineJob(
            job_id=uuid4().hex,
            pipeline_id=pipeline_id,
            db_name=db_name,
            pipeline_type=pipeline.pipeline_type if pipeline else "batch",
            definition_json=definition_json or {},
            node_id=node_id,
            output_dataset_name=dataset_name or "pipeline_output",
            mode="deploy",
            schedule_interval_seconds=schedule_interval_seconds,
        )
        await pipeline_job_queue.publish(job)

        latest_version = await pipeline_registry.get_latest_version(pipeline_id=pipeline_id)
        await pipeline_registry.record_build(
            pipeline_id=pipeline_id,
            status="QUEUED",
            output_json={"job_id": job.job_id, "output": output, "schedule": schedule_interval_seconds},
            deployed_version=latest_version.version if latest_version else None,
        )

        event = _build_event(
            event_type="PIPELINE_DEPLOYED",
            aggregate_type="Pipeline",
            aggregate_id=pipeline_id,
            data={"pipeline_id": pipeline_id, "job_id": job.job_id, "output": output},
            command_type="DEPLOY_PIPELINE",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append pipeline deploy event: {e}")

        if lineage_store:
            await lineage_store.record_link(
                from_node_id=lineage_store.node_event(str(event.event_id)),
                to_node_id=lineage_store.node_aggregate("Pipeline", pipeline_id),
                edge_type="pipeline_job_enqueued",
                occurred_at=_utcnow(),
                from_label="pipeline_deploy",
                to_label=job.job_id,
                db_name=db_name,
                edge_metadata={
                    "db_name": db_name,
                    "pipeline_id": pipeline_id,
                    "pipeline_version": latest_version.version if latest_version else None,
                    "job_id": job.job_id,
                },
            )

        return ApiResponse.success(
            message="Pipeline deploy queued",
            data={
                "pipeline_id": pipeline_id,
                "job_id": job.job_id,
                "deployed_version": latest_version.version if latest_version else None,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to deploy pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets", response_model=ApiResponse)
@trace_endpoint("create_dataset")
async def create_dataset(
    payload: Dict[str, Any],
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
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
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
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
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
    *,
    storage_service: StorageServiceDep,
    lineage_store: LineageStoreDep,
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
            create_event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
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
                metadata=_sanitize_s3_metadata(
                    {
                        "db_name": db_name,
                        "dataset_name": resolved_name,
                        "source": "excel_upload",
                    }
                ),
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
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
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


@router.post("/datasets/csv-upload", response_model=ApiResponse)
@trace_endpoint("upload_csv_dataset")
async def upload_csv_dataset(
    db_name: str = Query(..., description="Database name"),
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    delimiter: Optional[str] = Form(None),
    has_header: bool = Form(True),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    *,
    storage_service: StorageServiceDep,
    lineage_store: LineageStoreDep,
) -> ApiResponse:
    """
    CSV 업로드 → preview/타입 추론 → dataset registry 저장 + artifact 저장

    Pipeline Builder 전용: raw 데이터를 canonical dataset으로 저장합니다.
    """
    try:
        db_name = validate_db_name(db_name)
        filename = file.filename or "upload.csv"
        if not filename.lower().endswith(".csv"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only .csv files are supported")

        content = await file.read()
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        resolved_name = (dataset_name or "").strip() or _default_dataset_name(filename)
        if not resolved_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_name is required")

        decoded = content.decode("utf-8", errors="replace")
        resolved_delimiter = delimiter or _detect_csv_delimiter(decoded)
        columns, preview_rows, total_rows = _parse_csv_content(
            decoded,
            delimiter=resolved_delimiter,
            has_header=has_header,
        )
        if not columns:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No columns detected")

        inferred_schema: list[Dict[str, Any]] = []
        try:
            from bff.services.funnel_client import FunnelClient

            async with FunnelClient() as funnel_client:
                analysis = await funnel_client.analyze_dataset(
                    {
                        "data": preview_rows,
                        "columns": columns,
                        "sample_size": min(len(preview_rows), 500),
                        "include_complex_types": True,
                    }
                )
            inferred_schema = analysis.get("columns") or []
        except Exception as exc:
            logger.warning(f"CSV type inference failed: {exc}")

        schema_columns = _build_schema_columns(columns, inferred_schema)
        schema_json = {"columns": schema_columns}
        sample_rows = _rows_from_preview(columns, preview_rows)
        row_count = total_rows if total_rows > 0 else len(sample_rows)

        dataset = await dataset_registry.get_dataset_by_name(db_name=db_name, name=resolved_name)
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=resolved_name,
                description=description.strip() if description else None,
                source_type="csv_upload",
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
            create_event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
            try:
                await event_store.connect()
                await event_store.append_event(create_event)
            except Exception as e:
                logger.warning(f"Failed to append csv dataset create event: {e}")

        artifact_bucket = os.getenv("DATASET_ARTIFACT_BUCKET") or os.getenv(
            "PIPELINE_ARTIFACT_BUCKET", "pipeline-artifacts"
        )
        artifact_key = None
        if storage_service:
            safe_name = resolved_name.replace(" ", "_")
            timestamp = _utcnow().strftime("%Y%m%dT%H%M%SZ")
            object_key = f"datasets/{db_name}/{dataset.dataset_id}/{safe_name}/{timestamp}.csv"
            await storage_service.create_bucket(artifact_bucket)
            await storage_service.save_bytes(
                artifact_bucket,
                object_key,
                content,
                content_type="text/csv",
                metadata=_sanitize_s3_metadata(
                    {
                        "db_name": db_name,
                        "dataset_name": resolved_name,
                        "source": "csv_upload",
                    }
                ),
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
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await event_store.connect()
            await event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append csv dataset event: {e}")

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
                        from_label="csv_upload",
                        to_label=artifact_key,
                        db_name=db_name,
                        edge_metadata={
                            "db_name": db_name,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": resolved_name,
                            "bucket": bucket,
                            "key": key,
                            "source": "csv_upload",
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to record csv dataset lineage: {e}")

        return ApiResponse.success(
            message="CSV dataset created",
            data={
                "dataset": dataset.__dict__,
                "version": version.__dict__,
                "preview": {"columns": _columns_from_schema(schema_columns), "rows": sample_rows},
                "source": {
                    "type": "csv",
                    "filename": filename,
                    "delimiter": resolved_delimiter,
                    "has_header": has_header,
                },
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload csv dataset: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
