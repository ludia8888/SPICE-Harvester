"""
Pipeline datasets router composition (BFF).

This module composes dataset-related endpoints using router composition
(Composite pattern) to keep each router focused and maintainable.

Selected helpers and endpoints are re-exported for backwards compatibility with
existing tests/imports.
"""


from fastapi import APIRouter

from bff.routers import (
    pipeline_datasets_catalog,
    pipeline_datasets_ingest,
    pipeline_datasets_uploads,
    pipeline_datasets_versions,
)
from bff.routers.pipeline_datasets_uploads_csv import upload_csv_dataset
from bff.routers.pipeline_datasets_versions import create_dataset_version

# Re-export endpoint functions used directly by tests.

__all__ = ["router", "create_dataset_version", "upload_csv_dataset"]

router = APIRouter(tags=["Pipeline Builder"])

router.include_router(pipeline_datasets_catalog.router)
router.include_router(pipeline_datasets_versions.router)
router.include_router(pipeline_datasets_uploads.router)
router.include_router(pipeline_datasets_ingest.router)
