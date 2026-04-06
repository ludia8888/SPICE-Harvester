"""Pipeline dataset write/version endpoints (BFF).

Composed by `bff.routers.pipeline_datasets` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, status

import bff.routers.pipeline_datasets_ops as ops
from bff.routers.pipeline_datasets_deps import get_objectify_job_queue
from bff.routers.pipeline_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry
from bff.services import pipeline_dataset_version_service
from shared.config.app_config import AppConfig
from shared.dependencies.providers import get_lineage_store
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.responses import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.event_utils import build_command_event

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])

@router.post("/datasets", response_model=ApiResponse, deprecated=True, include_in_schema=False)
@trace_endpoint("create_dataset")
async def create_dataset(
    payload: Dict[str, Any],
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        dataset_id: Optional[str] = None
        raw_dataset_id = sanitized.get("dataset_id") or sanitized.get("datasetId")
        if raw_dataset_id:
            try:
                dataset_id = str(UUID(str(raw_dataset_id)))
            except Exception as exc:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "dataset_id must be a UUID", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        name = str(sanitized.get("name") or "").strip()
        if not name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "name is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        description = str(sanitized.get("description") or "").strip() or None
        source_type = str(sanitized.get("source_type") or "manual").strip() or "manual"
        source_ref = str(sanitized.get("source_ref") or "").strip() or None
        branch = str(sanitized.get("branch") or "main").strip() or "main"
        schema_json = sanitized.get("schema_json") if isinstance(sanitized.get("schema_json"), dict) else {}

        dataset = await dataset_registry.create_dataset(
            db_name=db_name,
            name=name,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            schema_json=schema_json,
            branch=branch,
            dataset_id=dataset_id,
        )

        event = build_command_event(
            event_type="DATASET_CREATED",
            aggregate_type="Dataset",
            aggregate_id=dataset.dataset_id,
            data={"dataset_id": dataset.dataset_id, "db_name": db_name, "name": name},
            command_type="CREATE_DATASET",
        )
        event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
        try:
            await ops.event_store.connect()
            await ops.event_store.append_event(event)
        except Exception as e:
            logger.warning(f"Failed to append dataset create event: {e}")

        return ApiResponse.success(
            message="Dataset created",
            data={"dataset": dataset.__dict__},
        ).to_dict()
    except HTTPException:
        raise
    except asyncpg.PostgresError:
        raise
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/datasets/{dataset_id}/versions", response_model=ApiResponse, deprecated=True, include_in_schema=False)
@trace_endpoint("create_dataset_version")
async def create_dataset_version(
    dataset_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    lineage_store: LineageStore = Depends(get_lineage_store),
) -> ApiResponse:
    return await pipeline_dataset_version_service.create_dataset_version(
        dataset_id=dataset_id,
        payload=payload,
        request=request,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        lineage_store=lineage_store,
        flush_dataset_ingest_outbox=ops.flush_dataset_ingest_outbox,
        build_dataset_event_payload=ops.build_dataset_event_payload,
    )
