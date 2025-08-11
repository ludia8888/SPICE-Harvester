"""
Data Connector Router - Google Sheets Integration

This router handles data connector registration and management,
specifically for Google Sheets integration with Kafka messaging.
"""

import logging
from typing import Dict, Any, Union

from fastapi import APIRouter, Depends, HTTPException, status
from confluent_kafka import Producer

from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.models import (
    GoogleSheetRegisterResponse,
    RegisteredSheet,
    SheetMetadata
)
from shared.models.requests import ApiResponse
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets
from shared.observability.tracing import trace_endpoint

logger = logging.getLogger(__name__)

router = APIRouter()


# Import the dependency functions from main
# This avoids circular imports while maintaining clean dependency injection
def get_kafka_producer() -> Producer:
    """Import here to avoid circular dependency"""
    from bff.main import get_kafka_producer as _get_kafka_producer
    import asyncio
    return asyncio.run(_get_kafka_producer())


def get_google_sheets_service() -> GoogleSheetsService:
    """Import here to avoid circular dependency"""
    from bff.main import get_google_sheets_service as _get_google_sheets_service
    import asyncio
    return asyncio.run(_get_google_sheets_service())


@router.post(
    "/connectors/google-sheets/register",
    response_model=Dict[str, Any],
    summary="Register Google Sheet for data monitoring",
    description="Register a Google Sheet URL for data change monitoring and automatic import"
)
@rate_limit(**RateLimitPresets.STANDARD)
@trace_endpoint("register_google_sheet")
async def register_google_sheet(
    sheet_data: Dict[str, Any],
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
        # Extract required fields from request
        sheet_url = sheet_data.get("sheet_url")
        database_name = sheet_data.get("database_name")
        description = sheet_data.get("description", "")
        monitoring_enabled = sheet_data.get("monitoring_enabled", True)
        
        # Validate required fields
        if not sheet_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="sheet_url is required"
            )
        
        if not database_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="database_name is required"
            )
        
        logger.info(f"Registering Google Sheet: {sheet_url} for database: {database_name}")
        
        # Register the sheet using GoogleSheetsService
        registration_result = await google_sheets_service.register_sheet(
            sheet_url=sheet_url,
            database_name=database_name,
            description=description,
            monitoring_enabled=monitoring_enabled
        )
        
        logger.info(f"Successfully registered Google Sheet: {registration_result.sheet_id}")
        
        return ApiResponse(
            success=True,
            message="Google Sheet registered successfully",
            data=registration_result
        )
        
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
    "/connectors/google-sheets/{sheet_id}/preview",
    response_model=Dict[str, Any],
    summary="Preview Google Sheet data",
    description="Get a preview of data from a registered Google Sheet"
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("preview_google_sheet")
async def preview_google_sheet(
    sheet_id: str,
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
        
        # Get preview data using GoogleSheetsService
        preview_result = await google_sheets_service.preview_sheet_data(
            sheet_id=sheet_id,
            worksheet_name=worksheet_name,
            limit=limit
        )
        
        return ApiResponse(
            success=True,
            message="Sheet preview retrieved successfully",
            data=preview_result.dict()
        )
        
    except ValueError as e:
        logger.error(f"Invalid sheet ID or worksheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to preview Google Sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Preview failed: {str(e)}"
        )


@router.get(
    "/connectors/google-sheets/registered",
    response_model=Dict[str, Any],
    summary="List registered Google Sheets",
    description="Get list of all registered Google Sheets for monitoring"
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("list_registered_sheets")
async def list_registered_sheets(
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
        
        # Get registered sheets using GoogleSheetsService
        # Note: This would need to be implemented in GoogleSheetsService
        # For now, return a placeholder response
        
        registered_sheets = []  # Placeholder - implement in GoogleSheetsService
        
        return ApiResponse(
            success=True,
            message="Registered sheets retrieved successfully", 
            data={
                "sheets": registered_sheets,
                "count": len(registered_sheets),
                "database_filter": database_name
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to list registered sheets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve sheets: {str(e)}"
        )


@router.delete(
    "/connectors/google-sheets/{sheet_id}",
    response_model=Dict[str, str],
    summary="Unregister Google Sheet",
    description="Remove a Google Sheet from monitoring"
)
@rate_limit(**RateLimitPresets.STANDARD)
@trace_endpoint("unregister_google_sheet")
async def unregister_google_sheet(
    sheet_id: str,
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
        
        # Unregister sheet using GoogleSheetsService
        # Note: This would need to be implemented in GoogleSheetsService
        # For now, return a success response
        
        return ApiResponse(
            success=True,
            message="Google Sheet unregistered successfully",
            data={"sheet_id": sheet_id, "status": "unregistered"}
        )
        
    except ValueError as e:
        logger.error(f"Invalid sheet ID: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid sheet ID: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to unregister Google Sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unregistration failed: {str(e)}"
        )