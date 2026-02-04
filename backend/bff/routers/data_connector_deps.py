"""
Data connector dependency providers (BFF).

Centralizes FastAPI dependency providers used across the data connector
subrouters to support router composition (Composite pattern).
"""

from fastapi import Depends

from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry
from data_connector.google_sheets.service import GoogleSheetsService
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.connector_registry import ConnectorRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry


async def get_google_sheets_service() -> GoogleSheetsService:
    """Import here to avoid circular dependency."""
    from bff.main import get_google_sheets_service as _get_google_sheets_service

    return await _get_google_sheets_service()


async def get_connector_registry() -> ConnectorRegistry:
    """Import here to avoid circular dependency."""
    from bff.main import get_connector_registry as _get_connector_registry

    return await _get_connector_registry()


async def get_objectify_job_queue(
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> ObjectifyJobQueue:
    return ObjectifyJobQueue(objectify_registry=objectify_registry)
