"""
Google Sheets connection endpoints (BFF).

Composed by `bff.routers.data_connector` via router composition (Composite pattern).
"""

import logging
from datetime import timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status

from bff.routers.data_connector_deps import get_connector_registry
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.connector_registry import ConnectorRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Data Connectors"])


@router.get(
    "/connections",
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
    "/connections/{connection_id}",
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

