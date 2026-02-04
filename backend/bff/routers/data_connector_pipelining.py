"""Google Sheets -> Pipeline Builder endpoints (BFF).

Composed by `bff.routers.data_connector` via router composition (Composite pattern).

This router is intentionally thin: business logic lives in
`bff.services.data_connector_pipelining_service` (Facade).
"""

from typing import Any, Dict

from fastapi import APIRouter, Depends, Request

from bff.routers.data_connector_deps import (
    get_connector_registry,
    get_dataset_registry,
    get_google_sheets_service,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
)
from bff.services import data_connector_pipelining_service
from data_connector.google_sheets.service import GoogleSheetsService
from shared.dependencies.providers import LineageStoreDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.observability.tracing import trace_endpoint
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Data Connectors"])


@router.post(
    "/{sheet_id}/start-pipelining",
    response_model=Dict[str, Any],
    summary="Start pipelining from a registered Google Sheet",
    description="Materialize a dataset entry for a registered connector and return it for Pipeline Builder.",
)
@rate_limit(**RateLimitPresets.WRITE)
@trace_endpoint("start_pipelining_google_sheet")
async def start_pipelining_google_sheet(
    sheet_id: str,
    payload: Dict[str, Any],
    http_request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    objectify_job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    *,
    lineage_store: LineageStoreDep,
) -> Dict[str, Any]:
    return await data_connector_pipelining_service.start_pipelining_google_sheet(
        sheet_id=sheet_id,
        payload=payload,
        http_request=http_request,
        google_sheets_service=google_sheets_service,
        connector_registry=connector_registry,
        pipeline_registry=pipeline_registry,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        lineage_store=lineage_store,
    )
