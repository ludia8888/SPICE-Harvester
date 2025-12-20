"""
Frontend-facing system summary router (BFF).

Goal: provide a single, stable endpoint that summarizes cross-store health and context
signals so the UI doesn't need to reconstruct state from multiple backends.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, status

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from shared.dependencies.providers import ElasticsearchServiceDep, RedisServiceDep
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import validate_branch_name, validate_db_name

router = APIRouter(prefix="/summary", tags=["Summary"])

_PROTECTED_BRANCHES_DEFAULT = {"main", "master", "production", "prod"}


def _get_protected_branches() -> set[str]:
    raw = (os.getenv("ONTOLOGY_PROTECTED_BRANCHES") or "").strip()
    if not raw:
        return set(_PROTECTED_BRANCHES_DEFAULT)
    branches = {b.strip() for b in raw.split(",") if b.strip()}
    return branches or set(_PROTECTED_BRANCHES_DEFAULT)


@router.get("")
async def get_summary(
    db: Optional[str] = Query(None, description="Database (project) name"),
    branch: Optional[str] = Query(None, description="Branch name"),
    *,
    oms: OMSClient = OMSClientDep,
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

    protected_branches = _get_protected_branches()
    is_protected_branch = bool(branch_name and branch_name in protected_branches)

    terminus_info: Optional[Dict[str, Any]] = None
    if db_name and branch_name:
        try:
            terminus_info = await oms.get(f"/api/v1/branch/{db_name}/branch/{branch_name}/info")
        except httpx.HTTPStatusError as e:
            resp = getattr(e, "response", None)
            detail: Any = None
            if resp is not None:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                raise HTTPException(status_code=resp.status_code, detail=detail) from e
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS branch info 조회 실패") from e
        except httpx.HTTPError as e:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS branch info 조회 실패") from e

    redis_ok = await redis_service.ping()

    es_ok = False
    es_health: Optional[Dict[str, Any]] = None
    es_error: Optional[str] = None
    try:
        await es_service.connect()
        es_ok = await es_service.ping()
        if es_ok:
            es_health = await es_service.get_cluster_health()
    except Exception as e:
        es_error = str(e)
    finally:
        try:
            await es_service.disconnect()
        except Exception:
            pass

    return ApiResponse.success(
        message="Summary fetched",
        data={
            "context": {"db_name": db_name, "branch": branch_name},
            "policy": {
                "protected_branches": sorted(protected_branches),
                "is_protected_branch": is_protected_branch,
            },
            "services": {
                "redis": {"ok": bool(redis_ok)},
                "elasticsearch": {"ok": bool(es_ok), "health": es_health, "error": es_error},
            },
            "terminus": terminus_info,
        },
    ).to_dict()
