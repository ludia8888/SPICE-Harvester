"""Google Sheets registration/monitoring service (BFF).

Extracted from `bff.routers.data_connector_registration` to keep routers thin and
to centralize registry + dataset wiring (Facade pattern).
"""

from __future__ import annotations

import logging
from datetime import timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.data_connector_ops import _build_google_oauth_client, _resolve_google_connection
from data_connector.google_sheets.models import GoogleSheetRegisterResponse, RegisteredSheet
from data_connector.google_sheets.service import GoogleSheetsService
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.connector_registry import ConnectorRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.dataset_registry_get_or_create import get_or_create_dataset_record
from shared.services.registries.lineage_store import LineageStore
from shared.utils.number_utils import to_int_or_none
from shared.observability.tracing import trace_external_call, trace_db_operation

logger = logging.getLogger(__name__)


def _as_int_or_none(value: Any) -> Optional[int]:
    return to_int_or_none(value)


async def _resolve_tokens(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not connection_id:
        return None, None, None

    oauth_client = _build_google_oauth_client()
    connection_source, access_token = await _resolve_google_connection(
        connector_registry=connector_registry,
        oauth_client=oauth_client,
        connection_id=str(connection_id),
    )
    cfg = connection_source.config_json or {}
    refresh_token = cfg.get("refresh_token")
    expires_at = cfg.get("expires_at")
    return access_token, refresh_token, expires_at


@trace_external_call("bff.data_connector_registration.register_google_sheet")
async def register_google_sheet(
    *,
    sheet_data: Dict[str, Any],
    google_sheets_service: GoogleSheetsService,
    connector_registry: ConnectorRegistry,
    dataset_registry: DatasetRegistry,
    lineage_store: LineageStore,
) -> Dict[str, Any]:
    try:
        sheet_url = sheet_data.get("sheet_url")
        worksheet_name = sheet_data.get("worksheet_name") or sheet_data.get("worksheet_title")
        polling_interval = int(sheet_data.get("polling_interval", 300))
        database_name = sheet_data.get("database_name")
        branch = sheet_data.get("branch") or "main"
        class_label = sheet_data.get("class_label")
        auto_import = bool(sheet_data.get("auto_import", False))
        max_import_rows = _as_int_or_none(sheet_data.get("max_import_rows"))
        api_key = sheet_data.get("api_key")
        connection_id = sheet_data.get("connection_id")

        if not sheet_url:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "sheet_url is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        logger.info(
            "Registering Google Sheet: %s (worksheet=%s, interval=%ss)",
            sheet_url,
            worksheet_name,
            polling_interval,
        )

        access_token, _, _ = await _resolve_tokens(
            connector_registry=connector_registry,
            connection_id=str(connection_id) if connection_id else None,
        )

        preview = await google_sheets_service.preview_sheet(
            str(sheet_url),
            worksheet_name=worksheet_name,
            limit=25,
            api_key=api_key,
            access_token=access_token,
        )
        sheet_id = preview.sheet_id
        resolved_worksheet = preview.worksheet_name

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
            dataset, _ = await get_or_create_dataset_record(
                lookup=lambda: dataset_registry.get_dataset_by_source_ref(
                    db_name=db_name,
                    source_type="connector",
                    source_ref=source_ref,
                    branch=branch or "main",
                ),
                create=lambda: dataset_registry.create_dataset(
                    db_name=db_name,
                    name=f"gsheet_{sheet_id}",
                    description=f"Google Sheets sync: {sheet_url}",
                    source_type="connector",
                    source_ref=source_ref,
                    schema_json={"columns": [{"name": col, "type": "String"} for col in preview.columns]},
                    branch=branch or "main",
                ),
                conflict_context=f"{db_name}/{source_ref}@{branch or 'main'}",
            )

            dataset_payload = {"dataset_id": dataset.dataset_id, "name": dataset.name, "db_name": dataset.db_name}
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

        logger.info("Successfully registered Google Sheet: %s (postgres registry)", registration_result.sheet_id)

        return ApiResponse.success(
            message="Google Sheet registered successfully",
            data={**registration_result.model_dump(mode="json"), "dataset": dataset_payload},
        ).to_dict()

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid Google Sheet URL or data: %s", e)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid sheet data: {str(e)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        logger.error("Failed to register Google Sheet: %s", e)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Registration failed: {str(e)}", code=ErrorCode.CONNECTOR_ERROR) from e


@trace_external_call("bff.data_connector_registration.preview_google_sheet")
async def preview_google_sheet(
    *,
    sheet_id: str,
    worksheet_name: Optional[str],
    limit: int,
    google_sheets_service: GoogleSheetsService,
    connector_registry: ConnectorRegistry,
) -> Dict[str, Any]:
    try:
        logger.info("Previewing Google Sheet: %s, worksheet: %s", sheet_id, worksheet_name)

        source = await connector_registry.get_source(source_type="google_sheets", source_id=sheet_id)
        if not source or not source.enabled:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Sheet is not registered", code=ErrorCode.RESOURCE_NOT_FOUND)

        sheet_url = (source.config_json or {}).get("sheet_url")
        if not sheet_url:
            raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, "Registered sheet is missing sheet_url", code=ErrorCode.CONNECTOR_ERROR)

        config = source.config_json or {}
        default_ws = config.get("worksheet_name")
        access_token = None
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

        return ApiResponse.success(message="Sheet preview retrieved successfully", data=preview_result.model_dump(mode="json")).to_dict()

    except ValueError as e:
        logger.error("Invalid sheet ID or worksheet: %s", e)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid parameters: {str(e)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to preview Google Sheet: %s", e)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Preview failed: {str(e)}", code=ErrorCode.CONNECTOR_ERROR) from e


@trace_db_operation("bff.data_connector_registration.list_registered_sheets")
async def list_registered_sheets(
    *,
    database_name: Optional[str],
    connector_registry: ConnectorRegistry,
) -> Dict[str, Any]:
    try:
        logger.info("Listing registered sheets for database: %s", database_name)

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
                    last_polled=state.last_polled_at.astimezone(timezone.utc).isoformat()
                    if state and state.last_polled_at
                    else None,
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
        logger.error("Failed to list registered sheets: %s", e)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Failed to retrieve sheets: {str(e)}", code=ErrorCode.CONNECTOR_ERROR) from e


@trace_db_operation("bff.data_connector_registration.unregister_google_sheet")
async def unregister_google_sheet(*, sheet_id: str, connector_registry: ConnectorRegistry) -> Dict[str, str]:
    try:
        logger.info("Unregistering Google Sheet: %s", sheet_id)

        deleted = await connector_registry.delete_source(source_type="google_sheets", source_id=sheet_id)
        if not deleted:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Sheet is not registered", code=ErrorCode.RESOURCE_NOT_FOUND)

        return ApiResponse.success(
            message="Google Sheet unregistered successfully",
            data={"sheet_id": sheet_id, "status": "unregistered"},
        ).to_dict()

    except ValueError as e:
        logger.error("Invalid sheet ID: %s", e)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid sheet ID: {str(e)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to unregister Google Sheet: %s", e)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Unregistration failed: {str(e)}", code=ErrorCode.CONNECTOR_ERROR) from e
