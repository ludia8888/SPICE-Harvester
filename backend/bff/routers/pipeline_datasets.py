"""
Pipeline datasets router composition (BFF).

This module composes dataset-related endpoints using router composition
(Composite pattern) to keep each router focused and maintainable.

Selected helpers are re-exported for direct test invocation.
"""


from fastapi import APIRouter

from bff.routers import (
    pipeline_datasets_catalog,
)
from bff.routers.pipeline_datasets_ops import _maybe_enqueue_objectify_job, _sanitize_s3_metadata
from bff.routers.pipeline_datasets_ops import _default_dataset_name, _detect_csv_delimiter, _normalize_table_bbox, _parse_csv_content

# Re-export helper functions used directly by tests.

__all__ = [
    "router",
    "_maybe_enqueue_objectify_job",
    "_default_dataset_name",
    "_detect_csv_delimiter",
    "_normalize_table_bbox",
    "_parse_csv_content",
    "_sanitize_s3_metadata",
]

router = APIRouter(tags=["Pipeline Builder"])

router.include_router(pipeline_datasets_catalog.router)
