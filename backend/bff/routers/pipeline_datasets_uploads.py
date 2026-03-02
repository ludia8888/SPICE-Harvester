"""Pipeline dataset upload endpoints (BFF).

This module composes dataset upload routers via `include_router` (Composite pattern)
and re-exports selected endpoint callables used by tests.
"""


from fastapi import APIRouter

from bff.routers import (
    pipeline_datasets_uploads_csv,
    pipeline_datasets_uploads_excel,
    pipeline_datasets_uploads_media,
)

# Re-export endpoint functions used directly by callers/tests.
from bff.routers.pipeline_datasets_uploads_csv import upload_csv_dataset
from bff.routers.pipeline_datasets_uploads_excel import upload_excel_dataset
from bff.routers.pipeline_datasets_uploads_media import upload_media_dataset

router = APIRouter(tags=["Pipeline Builder"])

router.include_router(pipeline_datasets_uploads_excel.router)
router.include_router(pipeline_datasets_uploads_csv.router)
router.include_router(pipeline_datasets_uploads_media.router)

__all__ = [
    "router",
    "upload_csv_dataset",
    "upload_excel_dataset",
    "upload_media_dataset",
]
