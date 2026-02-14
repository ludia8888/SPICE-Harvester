"""
Objectify dependency providers (BFF).

Centralizes FastAPI dependencies used across objectify subrouters to support
router composition (Composite pattern) and keep subrouters focused.
"""

from fastapi import Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.role_deps import enforce_required_database_role
from bff.routers.objectify_job_queue_deps import get_objectify_job_queue
from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry, get_pipeline_registry
from shared.security.database_access import enforce_database_role as _enforce_database_role

enforce_database_role = _enforce_database_role
__all__ = [
    "get_dataset_registry",
    "get_objectify_registry",
    "get_pipeline_registry",
    "get_objectify_job_queue",
    "_require_db_role",
]


async def _require_db_role(request: Request, *, db_name: str, roles) -> None:  # noqa: ANN001
    if enforce_database_role is _enforce_database_role:
        await enforce_required_database_role(request, db_name=db_name, roles=roles)
        return
    try:
        await enforce_database_role(headers=request.headers, db_name=db_name, required_roles=roles)
    except ValueError as exc:
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc
