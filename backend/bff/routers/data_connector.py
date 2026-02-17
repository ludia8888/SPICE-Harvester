"""
Data connector API (BFF).

This module is the composition root for data connector endpoints.
Routers are composed via `include_router` (Composite pattern) to keep each
sub-router focused and maintainable.
"""

from fastapi import APIRouter

from bff.routers import (
    data_connector_browse,
    data_connector_connections,
    data_connector_oauth,
    data_connector_pipelining,
    data_connector_registration,
)
from bff.routers.data_connector_deps import (
    get_connector_registry,
    get_dataset_registry,
    get_google_sheets_service,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
)

router = APIRouter(tags=["Data Connectors"])

_GOOGLE_SHEETS_PREFIX = "/data-connectors/google-sheets"

router.include_router(data_connector_oauth.router, prefix=_GOOGLE_SHEETS_PREFIX)
router.include_router(data_connector_connections.router, prefix=_GOOGLE_SHEETS_PREFIX)
router.include_router(data_connector_browse.router, prefix=_GOOGLE_SHEETS_PREFIX)
router.include_router(data_connector_registration.router, prefix=_GOOGLE_SHEETS_PREFIX)
router.include_router(data_connector_pipelining.router, prefix=_GOOGLE_SHEETS_PREFIX)

__all__ = [
    "router",
    # Back-compat: local scripts may override these dependencies.
    "get_google_sheets_service",
    "get_connector_registry",
    "get_dataset_registry",
    "get_pipeline_registry",
    "get_objectify_registry",
    "get_objectify_job_queue",
]
