"""
Objectify Changelog API.

Provides endpoints to query objectify change history — what instances were
added, modified, or deleted per objectify job execution. Enables audit
tracking similar to Palantir Foundry's Changelog Datasets.
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, status
from shared.errors.error_types import ErrorCode, classified_http_exception

router = APIRouter(tags=["Objectify Changelog"])


def _get_changelog_store():
    """Dependency placeholder — wired by objectify router composition."""
    raise NotImplementedError("changelog_store dependency not configured")


@router.get(
    "/changelog",
    summary="List objectify changelogs",
    response_model=Dict[str, Any],
)
@trace_endpoint("bff.objectify.list_changelogs")
async def list_changelogs(
    db_name: str = Query(..., description="Database name"),
    branch: str = Query(default="main", description="Branch"),
    target_class_id: Optional[str] = Query(default=None, description="Filter by class ID"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """List recent objectify changelogs for a database."""
    try:
        store = _get_changelog_store()
    except NotImplementedError:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Changelog store not configured",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )

    changelogs = await store.list_changelogs(
        db_name=db_name,
        branch=branch,
        target_class_id=target_class_id,
        limit=limit,
        offset=offset,
    )
    return {
        "data": changelogs,
        "count": len(changelogs),
        "db_name": db_name,
        "branch": branch,
    }


@router.get(
    "/changelog/{changelog_id}",
    summary="Get objectify changelog detail",
    response_model=Dict[str, Any],
)
@trace_endpoint("bff.objectify.get_changelog")
async def get_changelog(
    changelog_id: str,
) -> Dict[str, Any]:
    """Get a single objectify changelog entry."""
    try:
        store = _get_changelog_store()
    except NotImplementedError:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Changelog store not configured",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )

    entry = await store.get_changelog(changelog_id=changelog_id)
    if not entry:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"Changelog {changelog_id} not found",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )
    return {"data": entry}
