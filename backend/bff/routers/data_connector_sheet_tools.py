"""
Google Sheets extraction/preview endpoints (BFF).

Composed by `bff.routers.data_connector` via router composition (Composite pattern).
"""

import logging

from fastapi import APIRouter, Depends, Request, status
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.data_connector_deps import get_connector_registry, get_google_sheets_service
from bff.routers.data_connector_ops import _resolve_optional_access_token
from data_connector.google_sheets.service import GoogleSheetsService
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.google_sheets import GoogleSheetPreviewRequest, GoogleSheetPreviewResponse
from shared.models.sheet_grid import GoogleSheetGridRequest, SheetGrid
from shared.observability.tracing import trace_endpoint
from shared.services.core.sheet_grid_parser import SheetGridParseOptions, SheetGridParser
from shared.services.registries.connector_registry import ConnectorRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Data Connectors"])


@router.post(
    "/grid",
    response_model=SheetGrid,
    summary="Extract Google Sheets grid + merges",
    description="Fetches values + metadata(merges) and returns normalized grid/merged_cells.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("extract_google_sheet_grid")
async def extract_google_sheet_grid(
    request: GoogleSheetGridRequest,
    http_request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> SheetGrid:
    try:
        access_token = await _resolve_optional_access_token(
            connector_registry=connector_registry,
            connection_id=str(request.connection_id) if request.connection_id else None,
        )
        sheet_id, metadata, worksheet_title, worksheet_sheet_id, values = await google_sheets_service.fetch_sheet_values(
            str(request.sheet_url),
            worksheet_name=request.worksheet_name,
            api_key=request.api_key,
            access_token=access_token,
        )

        merges = SheetGridParser.merged_cells_from_google_metadata(
            metadata.model_dump(),
            worksheet_name=worksheet_title,
            sheet_id=worksheet_sheet_id,
        )

        sheet_grid = SheetGridParser.from_google_sheets_values(
            values,
            merged_cells=merges,
            sheet_name=worksheet_title,
            options=SheetGridParseOptions(
                trim_trailing_empty=bool(request.trim_trailing_empty),
                max_rows=request.max_rows,
                max_cols=request.max_cols,
            ),
            metadata={
                "sheet_id": sheet_id,
                "sheet_title": metadata.title,
                "worksheet_title": worksheet_title,
                "sheet_url": str(request.sheet_url),
            },
        )

        return sheet_grid
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        logger.error("Failed to extract Google Sheets grid: %s", e)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Grid extraction failed: {e}", code=ErrorCode.CONNECTOR_ERROR) from e


@router.post(
    "/preview",
    response_model=GoogleSheetPreviewResponse,
    summary="Preview Google Sheet data (for Funnel)",
    description="Fetches formatted values and returns header + sample rows for type inference.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("preview_google_sheet_for_funnel")
async def preview_google_sheet_for_funnel(
    request: GoogleSheetPreviewRequest,
    http_request: Request,
    limit: int = 10,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> GoogleSheetPreviewResponse:
    try:
        access_token = await _resolve_optional_access_token(
            connector_registry=connector_registry,
            connection_id=str(request.connection_id) if request.connection_id else None,
        )
        preview = await google_sheets_service.preview_sheet(
            str(request.sheet_url),
            worksheet_name=request.worksheet_name,
            limit=limit,
            api_key=request.api_key,
            access_token=access_token,
        )
        return GoogleSheetPreviewResponse(
            sheet_id=preview.sheet_id,
            sheet_title=preview.sheet_title,
            worksheet_title=preview.worksheet_title,
            columns=preview.columns,
            sample_rows=preview.sample_rows,
            total_rows=preview.total_rows,
            total_columns=preview.total_columns,
        )
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        logger.error("Failed to preview Google Sheet: %s", e)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Preview failed: {e}", code=ErrorCode.CONNECTOR_ERROR) from e
