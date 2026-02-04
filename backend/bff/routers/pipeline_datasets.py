"""
Pipeline datasets router composition (BFF).

This module composes dataset-related endpoints using router composition
(Composite pattern) to keep each router focused and maintainable.

Selected helpers and endpoints are re-exported for backwards compatibility with
existing tests/imports.
"""

from __future__ import annotations

from fastapi import APIRouter

from bff.routers import (
    pipeline_datasets_catalog,
    pipeline_datasets_ingest,
    pipeline_datasets_uploads,
    pipeline_datasets_versions,
)
from bff.routers.pipeline_datasets_ops import (
    _default_dataset_name,
    _detect_csv_delimiter,
    _maybe_enqueue_objectify_job,
    _normalize_table_bbox,
    _parse_csv_content,
    _sanitize_s3_metadata,
)

# Re-export endpoint functions used directly by tests.
from bff.routers.pipeline_datasets_ingest import approve_dataset_schema, get_dataset_ingest_request
from bff.routers.pipeline_datasets_uploads import upload_csv_dataset, upload_excel_dataset, upload_media_dataset
from bff.routers.pipeline_datasets_versions import create_dataset_version, reanalyze_dataset_version

router = APIRouter(tags=["Pipeline Builder"])

router.include_router(pipeline_datasets_catalog.router)
router.include_router(pipeline_datasets_versions.router)
router.include_router(pipeline_datasets_uploads.router)
router.include_router(pipeline_datasets_ingest.router)

