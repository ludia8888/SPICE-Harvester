"""
Objectify (Dataset -> Ontology) API (BFF).

This module is the composition root for objectify-related endpoints.
Routers are composed via `include_router` (Composite pattern) to keep each
sub-router focused and maintainable.
"""

from fastapi import APIRouter

from bff.routers import (
    objectify_dag,
    objectify_enterprise,
    objectify_incremental,
    objectify_mapping_specs,
    objectify_runs,
)
from bff.routers.objectify_deps import (
    get_dataset_registry,
    get_objectify_job_queue,
    get_objectify_registry,
    get_pipeline_registry,
)

router = APIRouter(prefix="/objectify", tags=["Objectify"])

router.include_router(objectify_mapping_specs.router)
router.include_router(objectify_runs.router)
router.include_router(objectify_dag.router)
router.include_router(objectify_enterprise.router)
router.include_router(objectify_incremental.router)

__all__ = [
    "router",
    # Back-compat: tests and local scripts override these dependencies.
    "get_dataset_registry",
    "get_objectify_registry",
    "get_pipeline_registry",
    "get_objectify_job_queue",
]
