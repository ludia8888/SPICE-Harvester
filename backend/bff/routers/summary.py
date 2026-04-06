"""
Frontend-facing system summary router (BFF).

Goal: provide a single, stable endpoint that summarizes cross-store health and context
signals so the UI doesn't need to reconstruct state from multiple backends.
"""

from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from shared.dependencies.providers import ElasticsearchServiceDep, RedisServiceDep
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.utils.branch_utils import get_protected_branches

router = APIRouter(prefix="/summary", tags=["Summary"])


@router.get("")
@trace_endpoint("bff.summary.get_summary")
async def get_summary(
    db: Optional[str] = Query(None, description="Database (project) name"),
    branch: Optional[str] = Query(None, description="Branch name"),
    *,
    redis_service: RedisServiceDep,
    es_service: ElasticsearchServiceDep,
) -> Dict[str, Any]:
    """
    Summarize context + cross-service health for UI.

    This is intentionally a small, stable contract. Add fields here instead of
    leaking storage-specific details into the frontend.
    """
    db_name = validate_db_name(db) if db else None
    branch_name = validate_branch_name(branch) if branch else None

    protected_branches = get_protected_branches()
    is_protected_branch = bool(branch_name and branch_name in protected_branches)
    ontology_backend = "postgres"

    # Deprecated branch-info API is removed in Foundry-style profile.
    branch_info: Optional[Dict[str, Any]] = None

    redis_ok = await redis_service.ping()

    es_ok = False
    es_health: Optional[Dict[str, Any]] = None
    es_error: Optional[str] = None
    try:
        # Service container provides a singleton ES service. Keep the pooled
        # client open and reuse it across requests to avoid churn/leaks.
        await es_service.connect()
        es_ok = await es_service.ping()
        if es_ok:
            es_health = await es_service.get_cluster_health()
    except Exception as e:
        logging.getLogger(__name__).warning("Exception fallback at bff/routers/summary.py:77", exc_info=True)
        es_error = str(e)

    return ApiResponse.success(
        message="Summary fetched",
        data={
            "context": {"db_name": db_name, "branch": branch_name},
            "policy": {
                "protected_branches": sorted(protected_branches),
                "is_protected_branch": is_protected_branch,
                "ontology_backend": ontology_backend,
            },
            "services": {
                "redis": {"ok": bool(redis_ok)},
                "elasticsearch": {"ok": bool(es_ok), "health": es_health, "error": es_error},
            },
            "branch_info": branch_info,
        },
    ).to_dict()
