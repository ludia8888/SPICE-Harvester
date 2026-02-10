"""
Admin API router (BFF).

This module is the composition root for admin-only operational endpoints.
Routers are composed via `include_router` (Composite pattern) to keep each
feature-focused and maintainable.
"""

from fastapi import APIRouter, Depends

from bff.routers import admin_instance_rebuild, admin_lakefs, admin_recompute_projection, admin_replay, admin_system
from bff.routers.admin_deps import require_admin

router = APIRouter(prefix="/admin", tags=["Admin Operations"], dependencies=[Depends(require_admin)])

router.include_router(admin_replay.router)
router.include_router(admin_recompute_projection.router)
router.include_router(admin_system.router)
router.include_router(admin_lakefs.router)
router.include_router(admin_instance_rebuild.router)

