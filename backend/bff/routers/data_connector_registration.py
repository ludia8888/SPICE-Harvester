"""
Google Sheets registration/monitoring endpoints (BFF).

Composed by `bff.routers.data_connector` via router composition (Composite pattern).

This router is intentionally thin: business logic lives in
`bff.services.data_connector_registration_service` (Facade).
"""

from typing import Any, Dict

from fastapi import APIRouter, Depends, Request, status

from bff.routers.data_connector_deps import get_connector_registry, get_dataset_registry, get_google_sheets_service
from bff.services import data_connector_registration_service
from data_connector.google_sheets.service import GoogleSheetsService
from shared.dependencies.providers import LineageStoreDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.observability.tracing import trace_endpoint
from shared.services.registries.connector_registry import ConnectorRegistry
from shared.services.registries.dataset_registry import DatasetRegistry

router = APIRouter(tags=["Data Connectors"])


@router.post(
    "/register",
    response_model=Dict[str, Any],
    summary="Register Google Sheet for data monitoring",
    description="Register a Google Sheet URL for data change monitoring and automatic import",
)
@rate_limit(**RateLimitPresets.STANDARD)
@trace_endpoint("register_google_sheet")
async def register_google_sheet(
    sheet_data: Dict[str, Any],
    http_request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    *,
    lineage_store: LineageStoreDep,
) -> Dict[str, Any]:
    """
    Register a Google Sheet for data monitoring and automatic import.

    This endpoint:
    1. Validates the Google Sheet URL and accessibility
    2. Extracts sheet metadata (name, worksheets, data structure)
    3. Registers the sheet for monitoring via Kafka messaging
    4. Returns registration details and preview data
    """
    return await data_connector_registration_service.register_google_sheet(
        sheet_data=sheet_data,
        google_sheets_service=google_sheets_service,
        connector_registry=connector_registry,
        dataset_registry=dataset_registry,
        lineage_store=lineage_store,
    )


@router.get(
    "/{sheet_id}/preview",
    response_model=Dict[str, Any],
    summary="Preview registered Google Sheet data",
    description="Get a preview of data from a registered Google Sheet.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("preview_google_sheet")
async def preview_google_sheet(
    sheet_id: str,
    http_request: Request,
    worksheet_name: str | None = None,
    limit: int = 10,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> Dict[str, Any]:
    """Preview data from a registered Google Sheet."""
    return await data_connector_registration_service.preview_google_sheet(
        sheet_id=sheet_id,
        worksheet_name=worksheet_name,
        limit=limit,
        google_sheets_service=google_sheets_service,
        connector_registry=connector_registry,
    )


@router.get(
    "/registered",
    response_model=Dict[str, Any],
    summary="List registered Google Sheets",
    description="Get list of all registered Google Sheets for monitoring.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("list_registered_sheets")
async def list_registered_sheets(
    http_request: Request,
    database_name: str | None = None,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> Dict[str, Any]:
    """List all registered Google Sheets."""
    return await data_connector_registration_service.list_registered_sheets(
        database_name=database_name,
        connector_registry=connector_registry,
    )


@router.delete(
    "/{sheet_id}",
    response_model=Dict[str, str],
    summary="Unregister Google Sheet",
    description="Remove a Google Sheet from monitoring.",
)
@rate_limit(**RateLimitPresets.STANDARD)
@trace_endpoint("unregister_google_sheet")
async def unregister_google_sheet(
    sheet_id: str,
    http_request: Request,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> Dict[str, str]:
    """Unregister a Google Sheet from monitoring."""
    return await data_connector_registration_service.unregister_google_sheet(
        sheet_id=sheet_id,
        connector_registry=connector_registry,
    )
