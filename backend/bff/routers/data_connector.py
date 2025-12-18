"""
Data Connector Router - Google Sheets Integration

This router handles data connector registration and management,
specifically for Google Sheets integration with Kafka messaging.
"""

import logging
from typing import Dict, Any, Union

from fastapi import APIRouter, Depends, HTTPException, Request, status
from confluent_kafka import Producer

from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.models import (
    GoogleSheetRegisterResponse,
    RegisteredSheet,
    SheetMetadata
)
from shared.models.google_sheets import GoogleSheetPreviewRequest, GoogleSheetPreviewResponse
from shared.models.requests import ApiResponse
from shared.models.sheet_grid import GoogleSheetGridRequest, SheetGrid
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets
from shared.observability.tracing import trace_endpoint
from shared.services.sheet_grid_parser import SheetGridParseOptions, SheetGridParser

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Data Connectors"])


# Import the dependency functions from main
# This avoids circular imports while maintaining clean dependency injection
async def get_kafka_producer() -> Producer:
    """Import here to avoid circular dependency"""
    from bff.main import get_kafka_producer as _get_kafka_producer

    return await _get_kafka_producer()


async def get_google_sheets_service() -> GoogleSheetsService:
    """Import here to avoid circular dependency"""
    from bff.main import get_google_sheets_service as _get_google_sheets_service

    return await _get_google_sheets_service()


@router.post(
    "/data-connectors/google-sheets/grid",
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
) -> SheetGrid:
    try:
        sheet_id, metadata, worksheet_title, worksheet_sheet_id, values = await google_sheets_service.fetch_sheet_values(
            str(request.sheet_url),
            worksheet_name=request.worksheet_name,
            api_key=request.api_key,
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to extract Google Sheets grid: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Grid extraction failed: {e}"
        )


@router.post(
    "/data-connectors/google-sheets/preview",
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
) -> GoogleSheetPreviewResponse:
    try:
        preview = await google_sheets_service.preview_sheet(
            str(request.sheet_url),
            worksheet_name=request.worksheet_name,
            limit=limit,
            api_key=request.api_key,
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to preview Google Sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Preview failed: {e}"
        )


@router.post(
    "/data-connectors/google-sheets/register",
    response_model=Dict[str, Any],
    summary="Register Google Sheet for data monitoring",
    description="Register a Google Sheet URL for data change monitoring and automatic import"
)
@rate_limit(**RateLimitPresets.STANDARD)
@trace_endpoint("register_google_sheet")
async def register_google_sheet(
    sheet_data: Dict[str, Any],
    http_request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service)
) -> Dict[str, Any]:
    """
    Register a Google Sheet for data monitoring and automatic import.
    
    This endpoint:
    1. Validates the Google Sheet URL and accessibility
    2. Extracts sheet metadata (name, worksheets, data structure)
    3. Registers the sheet for monitoring via Kafka messaging
    4. Returns registration details and preview data
    
    Args:
        sheet_data: Dictionary containing:
            - sheet_url: Google Sheet URL
            - database_name: Target database name for import
            - description: Optional description of the data source
            - monitoring_enabled: Whether to enable real-time monitoring (default: True)
    
    Returns:
        ApiResponse containing registration details and sheet metadata
    """
    try:
        sheet_url = sheet_data.get("sheet_url")
        worksheet_name = sheet_data.get("worksheet_name") or sheet_data.get("worksheet_title")
        polling_interval = int(sheet_data.get("polling_interval", 300))
        database_name = sheet_data.get("database_name")
        branch = sheet_data.get("branch") or "main"
        class_label = sheet_data.get("class_label")
        auto_import = bool(sheet_data.get("auto_import", False))
        max_import_rows = sheet_data.get("max_import_rows")
        try:
            max_import_rows = int(max_import_rows) if max_import_rows is not None else None
        except Exception:
            max_import_rows = None
        
        # Validate required fields
        if not sheet_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="sheet_url is required"
            )

        logger.info(f"Registering Google Sheet: {sheet_url} (worksheet={worksheet_name}, interval={polling_interval}s)")

        registration_result = await google_sheets_service.register_sheet(
            sheet_url=sheet_url,
            worksheet_name=worksheet_name,
            polling_interval=polling_interval,
            database_name=database_name,
            branch=branch,
            class_label=class_label,
            auto_import=auto_import,
            max_import_rows=max_import_rows,
        )
        
        logger.info(f"Successfully registered Google Sheet: {registration_result.sheet_id}")
        
        return ApiResponse.success(
            message="Google Sheet registered successfully",
            data=registration_result.model_dump(mode="json"),
        ).to_dict()
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Invalid Google Sheet URL or data: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid sheet data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to register Google Sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )


@router.get(
    "/data-connectors/google-sheets/{sheet_id}/preview",
    response_model=Dict[str, Any],
    summary="Preview registered Google Sheet data",
    description="Get a preview of data from a registered Google Sheet.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("preview_google_sheet")
async def preview_google_sheet(
    sheet_id: str,
    http_request: Request,
    worksheet_name: str = None,
    limit: int = 10,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service)
) -> Dict[str, Any]:
    """
    Preview data from a registered Google Sheet.
    
    Args:
        sheet_id: Google Sheet ID
        worksheet_name: Optional specific worksheet name
        limit: Maximum number of rows to preview (default: 10)
    
    Returns:
        ApiResponse containing preview data and metadata
    """
    try:
        logger.info(f"Previewing Google Sheet: {sheet_id}, worksheet: {worksheet_name}")

        # This endpoint is intended as "preview registered sheet".
        # If not registered, fail fast without external calls.
        registered = await google_sheets_service.get_registered_sheets()
        sheet = next((s for s in registered if s.sheet_id == sheet_id), None)
        if not sheet:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Sheet is not registered",
            )

        sheet_url = sheet.sheet_url
        preview_result = await google_sheets_service.preview_sheet(
            sheet_url,
            worksheet_name=worksheet_name or sheet.worksheet_name,
            limit=limit,
        )

        return ApiResponse.success(
            message="Sheet preview retrieved successfully",
            data=preview_result.model_dump(mode="json"),
        ).to_dict()
        
    except ValueError as e:
        logger.error(f"Invalid sheet ID or worksheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid parameters: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to preview Google Sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Preview failed: {str(e)}"
        )


@router.get(
    "/data-connectors/google-sheets/registered",
    response_model=Dict[str, Any],
    summary="List registered Google Sheets",
    description="Get list of all registered Google Sheets for monitoring.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("list_registered_sheets")
async def list_registered_sheets(
    http_request: Request,
    database_name: str = None,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service)
) -> Dict[str, Any]:
    """
    List all registered Google Sheets.
    
    Args:
        database_name: Optional filter by target database name
    
    Returns:
        ApiResponse containing list of registered sheets
    """
    try:
        logger.info(f"Listing registered sheets for database: {database_name}")

        registered_sheets = await google_sheets_service.get_registered_sheets()
        if database_name:
            registered_sheets = [s for s in registered_sheets if s.database_name == database_name]

        return ApiResponse.success(
            message="Registered sheets retrieved successfully",
            data={
                "sheets": [s.model_dump(mode="json") for s in registered_sheets],
                "count": len(registered_sheets),
                "database_filter": database_name,
            },
        ).to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list registered sheets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve sheets: {str(e)}"
        )


@router.delete(
    "/data-connectors/google-sheets/{sheet_id}",
    response_model=Dict[str, str],
    summary="Unregister Google Sheet",
    description="Remove a Google Sheet from monitoring.",
)
@rate_limit(**RateLimitPresets.STANDARD)
@trace_endpoint("unregister_google_sheet")
async def unregister_google_sheet(
    sheet_id: str,
    http_request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service)
) -> Dict[str, str]:
    """
    Unregister a Google Sheet from monitoring.
    
    Args:
        sheet_id: Google Sheet ID to unregister
    
    Returns:
        ApiResponse confirming removal
    """
    try:
        logger.info(f"Unregistering Google Sheet: {sheet_id}")

        ok = await google_sheets_service.unregister_sheet(sheet_id)
        if not ok:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Sheet is not registered",
            )

        return ApiResponse.success(
            message="Google Sheet unregistered successfully",
            data={"sheet_id": sheet_id, "status": "unregistered"},
        ).to_dict()
        
    except ValueError as e:
        logger.error(f"Invalid sheet ID: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid sheet ID: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unregister Google Sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unregistration failed: {str(e)}"
        )
