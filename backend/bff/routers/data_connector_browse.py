"""
Google Sheets browsing endpoints (BFF).

Composed by `bff.routers.data_connector` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

from bff.routers.data_connector_deps import get_connector_registry, get_google_sheets_service
from bff.routers.data_connector_ops import _build_google_oauth_client, _resolve_google_connection
from data_connector.google_sheets.service import GoogleSheetsService
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.connector_registry import ConnectorRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Data Connectors"])


@router.get(
    "/drive/spreadsheets",
    response_model=Dict[str, Any],
    summary="List Google Sheets spreadsheets",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("list_google_sheets_spreadsheets")
async def list_google_sheets_spreadsheets(
    http_request: Request,
    connection_id: str,
    query: Optional[str] = None,
    limit: int = 50,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
) -> Dict[str, Any]:
    oauth_client = _build_google_oauth_client()
    _, access_token = await _resolve_google_connection(
        connector_registry=connector_registry,
        oauth_client=oauth_client,
        connection_id=connection_id,
    )
    if not access_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Connection access token missing")

    spreadsheets = await google_sheets_service.list_spreadsheets(access_token=access_token, query=query, page_size=limit)
    return ApiResponse.success(message="Spreadsheets retrieved", data={"spreadsheets": spreadsheets}).to_dict()


@router.get(
    "/spreadsheets/{sheet_id}/worksheets",
    response_model=Dict[str, Any],
    summary="List worksheets for a spreadsheet",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("list_google_sheets_worksheets")
async def list_google_sheets_worksheets(
    sheet_id: str,
    http_request: Request,
    connection_id: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
) -> Dict[str, Any]:
    oauth_client = _build_google_oauth_client()
    _, access_token = await _resolve_google_connection(
        connector_registry=connector_registry,
        oauth_client=oauth_client,
        connection_id=connection_id,
    )
    if not access_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Connection access token missing")

    metadata = await google_sheets_service.get_sheet_metadata(sheet_id, access_token=access_token)
    worksheets = []
    for sheet in metadata.sheets or []:
        props = sheet.get("properties", {}) if isinstance(sheet, dict) else {}
        worksheets.append({"worksheet_id": props.get("sheetId"), "title": props.get("title")})

    return ApiResponse.success(message="Worksheets retrieved", data={"worksheets": worksheets}).to_dict()

