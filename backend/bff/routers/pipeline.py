"""
Pipeline Builder API (BFF).

This module is the composition root for pipeline-related endpoints.
Routers are composed via `include_router` (Composite pattern) to keep each
sub-router focused and maintainable.
"""

from fastapi import APIRouter

from bff.routers import (
    pipeline_branches,
    pipeline_catalog,
    pipeline_datasets,
    pipeline_detail,
    pipeline_execution,
    pipeline_history,
    pipeline_proposals,
    pipeline_simulation,
    pipeline_udfs,
)

_PIPELINES_PREFIX = "/pipelines"

router = APIRouter(tags=["Pipeline Builder"])

# Include static/top-level routes first to avoid shadowing by dynamic `/{pipeline_id}` routes.
router.include_router(pipeline_catalog.router, prefix=_PIPELINES_PREFIX)
router.include_router(pipeline_simulation.router, prefix=_PIPELINES_PREFIX)
router.include_router(pipeline_udfs.router, prefix=_PIPELINES_PREFIX)
router.include_router(pipeline_branches.router, prefix=_PIPELINES_PREFIX)
router.include_router(pipeline_proposals.router, prefix=_PIPELINES_PREFIX)
router.include_router(pipeline_datasets.router, prefix=_PIPELINES_PREFIX)

# Dynamic sub-resources (safe: include before/after detail is fine).
router.include_router(pipeline_history.router, prefix=_PIPELINES_PREFIX)

# Detail routes include `GET /{pipeline_id}` (dynamic catch for one segment), include last.
router.include_router(pipeline_detail.router, prefix=_PIPELINES_PREFIX)
router.include_router(pipeline_execution.router, prefix=_PIPELINES_PREFIX)
