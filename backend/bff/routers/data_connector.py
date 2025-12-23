"""
Data Connector Router - Google Sheets Integration

This router handles data connector registration and management,
specifically for Google Sheets integration with the connector runtime.
"""

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.models import (
    GoogleSheetRegisterResponse,
    RegisteredSheet,
)
from shared.models.google_sheets import GoogleSheetPreviewRequest, GoogleSheetPreviewResponse
from shared.models.requests import ApiResponse
from shared.models.sheet_grid import GoogleSheetGridRequest, SheetGrid
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.sheet_grid_parser import SheetGridParseOptions, SheetGridParser
from shared.services.connector_registry import ConnectorRegistry
from shared.services.dataset_registry import DatasetRegistry
from shared.dependencies.providers import LineageStoreDep, StorageServiceDep
from shared.utils.s3_uri import build_s3_uri
from shared.config.app_config import AppConfig
from shared.services.event_store import event_store
from shared.models.event_envelope import EventEnvelope
from data_connector.google_sheets.auth import GoogleOAuth2Client

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Data Connectors"])

_OAUTH_STATE_CACHE: Dict[str, Dict[str, Any]] = {}


# Import the dependency functions from main
# This avoids circular imports while maintaining clean dependency injection
async def get_google_sheets_service() -> GoogleSheetsService:
    """Import here to avoid circular dependency"""
    from bff.main import get_google_sheets_service as _get_google_sheets_service

    return await _get_google_sheets_service()


async def get_connector_registry() -> ConnectorRegistry:
    """Import here to avoid circular dependency"""
    from bff.main import get_connector_registry as _get_connector_registry

    return await _get_connector_registry()


async def get_dataset_registry() -> DatasetRegistry:
    """Import here to avoid circular dependency"""
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


def _build_google_oauth_client() -> GoogleOAuth2Client:
    return GoogleOAuth2Client()


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
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": actor,
        "version": 1,
    }
    return EventEnvelope(
        event_id=command_payload["command_id"],
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=datetime.now(timezone.utc),
        actor=actor,
        data=command_payload,
        metadata={
            "kind": "command",
            "command_type": command_type,
            "command_version": 1,
            "command_id": command_payload["command_id"],
        },
    )

def _connector_oauth_enabled(oauth_client: GoogleOAuth2Client) -> bool:
    return bool(oauth_client.client_id and oauth_client.client_secret and oauth_client.redirect_uri)

def _append_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params[key] = [value]
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


async def _resolve_google_connection(
    *,
    connector_registry: ConnectorRegistry,
    oauth_client: GoogleOAuth2Client,
    connection_id: str,
) -> tuple[Any, Optional[str]]:
    connection_id = (connection_id or "").strip()
    if not connection_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="connection_id is required")

    source = await connector_registry.get_source(
        source_type="google_sheets_connection",
        source_id=connection_id,
    )
    if not source or not source.enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    config = source.config_json or {}
    token = config.get("access_token")
    expires_at = config.get("expires_at")
    refresh_token = config.get("refresh_token")

    if token and expires_at:
        try:
            expiry = datetime.fromisoformat(str(expires_at))
        except ValueError:
            expiry = None
        if expiry and expiry > datetime.now(timezone.utc):
            return source, str(token)

    if refresh_token:
        refreshed = await oauth_client.refresh_access_token(str(refresh_token))
        config.update(
            {
                "access_token": refreshed.get("access_token"),
                "expires_at": refreshed.get("expires_at"),
                "refresh_token": refreshed.get("refresh_token", refresh_token),
            }
        )
        await connector_registry.upsert_source(
            source_type=source.source_type,
            source_id=source.source_id,
            enabled=True,
            config_json=config,
        )
        token = config.get("access_token")

    return source, str(token) if token else None


@router.post(
    "/data-connectors/google-sheets/oauth/start",
    response_model=Dict[str, Any],
    summary="Start Google Sheets OAuth flow",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("google_sheets_oauth_start")
async def start_google_sheets_oauth(
    payload: Dict[str, Any],
    http_request: Request,
) -> Dict[str, Any]:
    oauth_client = _build_google_oauth_client()
    if not _connector_oauth_enabled(oauth_client):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google OAuth is not configured")
    if not oauth_client.redirect_uri:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google OAuth redirect URI is missing")

    sanitized = sanitize_input(payload or {})
    state = uuid4().hex
    redirect_uri = str(sanitized.get("redirect_uri") or "").strip()
    label = str(sanitized.get("label") or "").strip() or None
    db_name = str(sanitized.get("db_name") or "").strip() or None
    requested_branch = str(sanitized.get("branch") or "").strip() or None

    _OAUTH_STATE_CACHE[state] = {
        "redirect_uri": redirect_uri,
        "label": label,
        "db_name": db_name,
        "branch": requested_branch,
    }

    auth_url = oauth_client.get_authorization_url(state)
    return ApiResponse.success(
        message="OAuth flow started",
        data={"authorization_url": auth_url, "state": state},
    ).to_dict()


@router.get(
    "/data-connectors/google-sheets/oauth/callback",
    summary="Google Sheets OAuth callback",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("google_sheets_oauth_callback")
async def google_sheets_oauth_callback(
    request: Request,
    code: str,
    state: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> RedirectResponse:
    oauth_client = _build_google_oauth_client()
    if not _connector_oauth_enabled(oauth_client):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google OAuth is not configured")

    state_payload = _OAUTH_STATE_CACHE.pop(state, None)
    if state_payload is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid OAuth state")

    token_data = await oauth_client.exchange_code_for_token(code)
    expires_at = token_data.get("expires_at")
    connection_id = uuid4().hex

    config = {
        "provider": "google_sheets",
        "access_token": token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "expires_at": expires_at,
        "label": state_payload.get("label"),
        "db_name": state_payload.get("db_name"),
        "branch": state_payload.get("branch"),
    }
    await connector_registry.upsert_source(
        source_type="google_sheets_connection",
        source_id=connection_id,
        enabled=True,
        config_json=config,
    )

    redirect_url = state_payload.get("redirect_uri") or "/"
    redirect_url = _append_query_param(redirect_url, "connection_id", connection_id)
    return RedirectResponse(url=redirect_url)


@router.get(
    "/data-connectors/google-sheets/connections",
    response_model=Dict[str, Any],
    summary="List Google Sheets connections",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("list_google_sheets_connections")
async def list_google_sheets_connections(
    http_request: Request,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> Dict[str, Any]:
    sources = await connector_registry.list_sources(source_type="google_sheets_connection", enabled=True, limit=200)
    connections = []
    for src in sources:
        cfg = src.config_json or {}
        connections.append(
            {
                "connection_id": src.source_id,
                "label": cfg.get("label") or "Google Sheets",
                "created_at": src.created_at.astimezone(timezone.utc).isoformat(),
            }
        )
    return ApiResponse.success(message="Connections retrieved", data={"connections": connections}).to_dict()


@router.delete(
    "/data-connectors/google-sheets/connections/{connection_id}",
    response_model=Dict[str, Any],
    summary="Remove Google Sheets connection",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("delete_google_sheets_connection")
async def delete_google_sheets_connection(
    connection_id: str,
    http_request: Request,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> Dict[str, Any]:
    updated = await connector_registry.set_source_enabled(
        source_type="google_sheets_connection",
        source_id=connection_id,
        enabled=False,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return ApiResponse.success(message="Connection removed", data={"connection_id": connection_id}).to_dict()


@router.get(
    "/data-connectors/google-sheets/drive/spreadsheets",
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

    spreadsheets = await google_sheets_service.list_spreadsheets(
        access_token=access_token,
        query=query,
        page_size=limit,
    )
    return ApiResponse.success(message="Spreadsheets retrieved", data={"spreadsheets": spreadsheets}).to_dict()


@router.get(
    "/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets",
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

    metadata = await google_sheets_service.get_sheet_metadata(
        sheet_id,
        access_token=access_token,
    )
    worksheets = []
    for sheet in metadata.sheets or []:
        props = sheet.get("properties", {}) if isinstance(sheet, dict) else {}
        worksheets.append(
            {
                "worksheet_id": props.get("sheetId"),
                "title": props.get("title"),
            }
        )
    return ApiResponse.success(message="Worksheets retrieved", data={"worksheets": worksheets}).to_dict()


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
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> SheetGrid:
    try:
        access_token = None
        if request.connection_id:
            oauth_client = _build_google_oauth_client()
            _, access_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(request.connection_id),
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
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> GoogleSheetPreviewResponse:
    try:
        access_token = None
        if request.connection_id:
            oauth_client = _build_google_oauth_client()
            _, access_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(request.connection_id),
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
        api_key = sheet_data.get("api_key")
        connection_id = sheet_data.get("connection_id")
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

        # Validate + resolve sheet_id/worksheet name via connector I/O (no durable state here).
        access_token = None
        refresh_token = None
        expires_at = None
        if connection_id:
            oauth_client = _build_google_oauth_client()
            connection_source, access_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(connection_id),
            )
            cfg = connection_source.config_json or {}
            refresh_token = cfg.get("refresh_token")
            expires_at = cfg.get("expires_at")

        preview = await google_sheets_service.preview_sheet(
            str(sheet_url),
            worksheet_name=worksheet_name,
            limit=25,
            api_key=api_key,
            access_token=access_token,
        )
        sheet_id = preview.sheet_id
        resolved_worksheet = preview.worksheet_name

        # Foundry policy: durable registry is Postgres (connector_sources + mappings).
        source = await connector_registry.upsert_source(
            source_type="google_sheets",
            source_id=str(sheet_id),
            enabled=True,
            config_json={
                "sheet_url": str(sheet_url),
                "sheet_title": preview.sheet_title,
                "worksheet_name": str(resolved_worksheet),
                "polling_interval": int(polling_interval),
                "max_import_rows": int(max_import_rows) if max_import_rows is not None else None,
                "connection_id": str(connection_id) if connection_id else None,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_at": expires_at,
            },
        )

        mapping_enabled = bool(auto_import and (database_name or "").strip() and (class_label or "").strip())
        mapping_status = "confirmed" if mapping_enabled else "draft"
        await connector_registry.upsert_mapping(
            source_type="google_sheets",
            source_id=str(sheet_id),
            enabled=mapping_enabled,
            status=mapping_status,
            target_db_name=database_name,
            target_branch=branch,
            target_class_label=class_label,
            field_mappings=[],
        )

        registered_sheet = RegisteredSheet(
            sheet_id=str(sheet_id),
            sheet_url=str(sheet_url),
            worksheet_name=str(resolved_worksheet),
            polling_interval=int(polling_interval),
            database_name=(database_name or "").strip() or None,
            branch=(branch or "main").strip() or "main",
            class_label=(class_label or "").strip() or None,
            auto_import=bool(mapping_enabled),
            max_import_rows=max_import_rows,
            last_polled=None,
            last_hash=None,
            is_active=True,
            registered_at=source.created_at.astimezone(timezone.utc).isoformat(),
        )

        registration_result = GoogleSheetRegisterResponse(
            sheet_id=str(sheet_id),
            status="success",
            message=f"Successfully registered sheet {sheet_id} for monitoring",
            registered_sheet=registered_sheet,
        )

        dataset_payload = None
        if (database_name or "").strip():
            db_name = validate_db_name(str(database_name))
            source_ref = f"google_sheets:{sheet_id}"
            existing_dataset = await dataset_registry.get_dataset_by_source_ref(
                db_name=db_name,
                source_type="connector",
                source_ref=source_ref,
            )
            if existing_dataset:
                dataset = existing_dataset
            else:
                dataset = await dataset_registry.create_dataset(
                    db_name=db_name,
                    name=f"gsheet_{sheet_id}",
                    description=f"Google Sheets sync: {sheet_url}",
                    source_type="connector",
                    source_ref=source_ref,
                    schema_json={"columns": [{"name": col, "type": "String"} for col in preview.columns]},
                )

            sample_rows = preview.sample_rows or []
            if sample_rows:
                await dataset_registry.add_version(
                    dataset_id=dataset.dataset_id,
                    artifact_key=None,
                    row_count=len(sample_rows),
                    sample_json={"columns": [{"name": col, "type": "String"} for col in preview.columns], "rows": sample_rows},
                    schema_json={"columns": [{"name": col, "type": "String"} for col in preview.columns]},
                )
            dataset_payload = {
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "db_name": dataset.db_name,
            }
            if lineage_store:
                try:
                    await lineage_store.record_link(
                        from_node_id=lineage_store.node_artifact("connector", "google_sheets", str(sheet_id)),
                        to_node_id=lineage_store.node_aggregate("Dataset", dataset.dataset_id),
                        edge_type="connector_registered_dataset",
                        db_name=db_name,
                        edge_metadata={
                            "db_name": db_name,
                            "source_type": "google_sheets",
                            "source_id": str(sheet_id),
                            "dataset_id": dataset.dataset_id,
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to record lineage for connector dataset: {e}")
        
        logger.info(f"Successfully registered Google Sheet: {registration_result.sheet_id} (postgres registry)")
        
        return ApiResponse.success(
            message="Google Sheet registered successfully",
            data={
                **registration_result.model_dump(mode="json"),
                "dataset": dataset_payload,
            },
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
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
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

        source = await connector_registry.get_source(source_type="google_sheets", source_id=sheet_id)
        if not source or not source.enabled:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Sheet is not registered",
            )

        sheet_url = (source.config_json or {}).get("sheet_url")
        if not sheet_url:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Registered sheet is missing sheet_url",
            )

        config = source.config_json or {}
        default_ws = config.get("worksheet_name")
        access_token = config.get("access_token")
        connection_id = config.get("connection_id")
        if connection_id:
            oauth_client = _build_google_oauth_client()
            _, refreshed_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(connection_id),
            )
            access_token = refreshed_token or access_token

        preview_result = await google_sheets_service.preview_sheet(
            sheet_url,
            worksheet_name=worksheet_name or default_ws,
            limit=limit,
            access_token=access_token,
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
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
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

        sources = await connector_registry.list_sources(source_type="google_sheets", enabled=True, limit=1000)
        registered_sheets: list[RegisteredSheet] = []
        for src in sources:
            mapping = await connector_registry.get_mapping(source_type=src.source_type, source_id=src.source_id)
            if database_name and mapping and mapping.target_db_name != database_name:
                continue
            state = await connector_registry.get_sync_state(source_type=src.source_type, source_id=src.source_id)
            cfg = src.config_json or {}
            sheet_title = cfg.get("sheet_title")
            registered_sheets.append(
                RegisteredSheet(
                    sheet_id=src.source_id,
                    sheet_url=str(cfg.get("sheet_url") or ""),
                    sheet_title=str(sheet_title) if sheet_title else None,
                    worksheet_name=str(cfg.get("worksheet_name") or ""),
                    polling_interval=int(cfg.get("polling_interval") or 300),
                    database_name=(mapping.target_db_name if mapping else None),
                    branch=(mapping.target_branch if mapping and mapping.target_branch else "main"),
                    class_label=(mapping.target_class_label if mapping else None),
                    auto_import=bool(mapping.enabled) if mapping else False,
                    max_import_rows=cfg.get("max_import_rows"),
                    last_polled=state.last_polled_at.astimezone(timezone.utc).isoformat() if state and state.last_polled_at else None,
                    last_hash=state.last_seen_cursor if state else None,
                    is_active=bool(src.enabled),
                    registered_at=src.created_at.astimezone(timezone.utc).isoformat(),
                )
            )

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


@router.post(
    "/data-connectors/google-sheets/{sheet_id}/start-pipelining",
    response_model=Dict[str, Any],
    summary="Start pipelining from a registered Google Sheet",
    description="Materialize a dataset entry for a registered connector and return it for Pipeline Builder.",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("start_pipelining_google_sheet")
async def start_pipelining_google_sheet(
    sheet_id: str,
    payload: Dict[str, Any],
    http_request: Request,
    google_sheets_service: GoogleSheetsService = Depends(get_google_sheets_service),
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    *,
    storage_service: StorageServiceDep,
    lineage_store: LineageStoreDep,
) -> Dict[str, Any]:
    try:
        sanitized = sanitize_input(payload or {})
        db_name = str(sanitized.get("db_name") or "").strip()
        worksheet_name = str(sanitized.get("worksheet_name") or "").strip() or None
        api_key = sanitized.get("api_key")
        limit = int(sanitized.get("limit") or 25)
        limit = max(1, min(limit, 500))

        source = await connector_registry.get_source(source_type="google_sheets", source_id=sheet_id)
        if not source or not source.enabled:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sheet is not registered")

        mapping = await connector_registry.get_mapping(source_type="google_sheets", source_id=sheet_id)
        if not db_name and mapping and mapping.target_db_name:
            db_name = str(mapping.target_db_name)
        if not db_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="db_name is required")
        db_name = validate_db_name(db_name)

        sheet_url = (source.config_json or {}).get("sheet_url")
        if not sheet_url:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Registered sheet missing URL")
        config = source.config_json or {}
        default_ws = config.get("worksheet_name")
        access_token = config.get("access_token")
        connection_id = config.get("connection_id")
        if connection_id:
            oauth_client = _build_google_oauth_client()
            _, refreshed_token = await _resolve_google_connection(
                connector_registry=connector_registry,
                oauth_client=oauth_client,
                connection_id=str(connection_id),
            )
            access_token = refreshed_token or access_token

        sheet_id_resolved, metadata, worksheet_title, _, values = await google_sheets_service.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name or default_ws,
            api_key=api_key,
            access_token=access_token,
        )
        columns = []
        rows: list[list[Any]] = []
        try:
            from data_connector.google_sheets.utils import normalize_sheet_data

            columns, rows = normalize_sheet_data(values)
        except Exception:
            columns = []
            rows = []

        sample_rows = rows[: max(1, limit)] if rows else []
        preview = {
            "sheet_id": sheet_id_resolved,
            "sheet_title": metadata.title,
            "worksheet_name": worksheet_title,
            "columns": columns,
            "sample_rows": sample_rows,
            "total_rows": len(rows),
        }

        inferred_schema: list[Dict[str, Any]] = []
        try:
            from bff.services.funnel_client import FunnelClient

            async with FunnelClient() as funnel_client:
                analysis = await funnel_client.analyze_dataset(
                    {
                        "data": sample_rows or [],
                        "columns": columns or [],
                        "sample_size": min(len(sample_rows or []), 500),
                        "include_complex_types": True,
                    }
                )
            inferred_schema = analysis.get("columns") or []
        except Exception as exc:
            logger.warning(f"Google Sheets type inference failed: {exc}")

        schema_columns = []
        for column in columns or []:
            inferred_type = None
            for item in inferred_schema:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("column_name") or item.get("name") or "").strip()
                if name != column:
                    continue
                inferred_type = item.get("type") or item.get("data_type")
                if not inferred_type:
                    inferred_payload = item.get("inferred_type")
                    if isinstance(inferred_payload, dict):
                        inferred_type = inferred_payload.get("type")
                break
            schema_columns.append({"name": column, "type": inferred_type or "xsd:string"})

        source_ref = f"google_sheets:{sheet_id_resolved}"
        dataset = await dataset_registry.get_dataset_by_source_ref(
            db_name=db_name,
            source_type="connector",
            source_ref=source_ref,
        )
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=f"gsheet_{sheet_id_resolved}",
                description=f"Google Sheets sync: {sheet_url}",
                source_type="connector",
                source_ref=source_ref,
                schema_json={"columns": schema_columns},
            )

            create_event = _build_event(
                event_type="DATASET_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset.dataset_id,
                data={"dataset_id": dataset.dataset_id, "db_name": db_name, "name": dataset.name},
                command_type="CREATE_DATASET",
            )
            create_event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
            try:
                await event_store.connect()
                await event_store.append_event(create_event)
            except Exception as e:
                logger.warning(f"Failed to append gsheet dataset create event: {e}")

        artifact_bucket = os.getenv("DATASET_ARTIFACT_BUCKET") or os.getenv(
            "PIPELINE_ARTIFACT_BUCKET", "pipeline-artifacts"
        )
        artifact_key = None
        if storage_service:
            safe_name = dataset.name.replace(" ", "_")
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            object_key = f"datasets/{db_name}/{dataset.dataset_id}/{safe_name}/{timestamp}.csv"
            await storage_service.create_bucket(artifact_bucket)
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            if columns:
                writer.writerow(columns)
            for row in rows:
                writer.writerow(row)
            await storage_service.save_bytes(
                artifact_bucket,
                object_key,
                csv_buffer.getvalue().encode("utf-8"),
                content_type="text/csv",
            )
            artifact_key = build_s3_uri(artifact_bucket, object_key)

        if rows:
            version = await dataset_registry.add_version(
                dataset_id=dataset.dataset_id,
                artifact_key=artifact_key,
                row_count=len(rows),
                sample_json={"columns": schema_columns, "rows": sample_rows},
                schema_json={"columns": schema_columns},
            )

            event = _build_event(
                event_type="DATASET_VERSION_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset.dataset_id,
                data={
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "name": dataset.name,
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
                logger.warning(f"Failed to append gsheet dataset version event: {e}")

        if lineage_store:
            try:
                await lineage_store.record_link(
                    from_node_id=lineage_store.node_artifact("connector", "google_sheets", str(sheet_id_resolved)),
                    to_node_id=lineage_store.node_aggregate("Dataset", dataset.dataset_id),
                    edge_type="connector_start_pipelining",
                    db_name=db_name,
                    edge_metadata={
                        "db_name": db_name,
                        "source_type": "google_sheets",
                        "source_id": str(sheet_id_resolved),
                        "dataset_id": dataset.dataset_id,
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to record start-pipelining lineage: {e}")

        return ApiResponse.success(
            message="Start pipelining completed",
            data={
                "dataset": {
                    "dataset_id": dataset.dataset_id,
                    "db_name": dataset.db_name,
                    "name": dataset.name,
                    "schema_json": {"columns": schema_columns},
                    "source_type": dataset.source_type,
                    "source_ref": dataset.source_ref,
                },
                "sample": {
                    "columns": schema_columns,
                    "rows": sample_rows,
                },
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start pipelining: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


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
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
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

        ok = await connector_registry.set_source_enabled(source_type="google_sheets", source_id=sheet_id, enabled=False)
        if not ok:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Sheet is not registered",
            )
        # Disable auto-import mapping as well (kept for audit/history).
        existing_mapping = await connector_registry.get_mapping(source_type="google_sheets", source_id=sheet_id)
        if existing_mapping:
            await connector_registry.upsert_mapping(
                source_type="google_sheets",
                source_id=sheet_id,
                enabled=False,
                status=existing_mapping.status,
                target_db_name=existing_mapping.target_db_name,
                target_branch=existing_mapping.target_branch,
                target_class_label=existing_mapping.target_class_label,
                field_mappings=existing_mapping.field_mappings,
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
