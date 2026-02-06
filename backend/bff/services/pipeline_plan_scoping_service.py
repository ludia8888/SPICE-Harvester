"""Pipeline plan scoping helpers (BFF).

Extracted from `bff.routers.pipeline_plans_ops` to keep router modules focused
and to centralize plan scoping + access enforcement (Facade pattern).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple
from uuid import UUID

from fastapi import HTTPException, Request, status

from bff.services.input_validation_service import enforce_db_scope_or_403
from bff.services.pipeline_plan_tenant_service import resolve_tenant_id
from shared.models.pipeline_plan import PipelinePlan
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry


def _parse_plan_id(plan_id: str) -> str:
    try:
        return str(UUID(plan_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc


async def _get_plan_record_or_404(
    *, plan_id: str, request: Request, plan_registry: PipelinePlanRegistry
) -> Tuple[str, str, Any]:
    resolved_plan_id = _parse_plan_id(plan_id)
    tenant_id = resolve_tenant_id(request)
    record = await plan_registry.get_plan(plan_id=resolved_plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline plan not found")
    return resolved_plan_id, tenant_id, record


def _load_plan_or_400(record: Any) -> PipelinePlan:
    try:
        return PipelinePlan.model_validate(record.plan)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Stored plan invalid: {exc}") from exc


def _extract_db_name_or_400(plan: PipelinePlan) -> str:
    if not plan.data_scope or not plan.data_scope.db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Plan missing db_name")
    return db_name


@dataclass(frozen=True)
class PipelinePlanRequestContext:
    plan_id: str
    tenant_id: str
    db_name: str
    branch: Optional[str]
    plan: PipelinePlan


async def _load_scoped_plan(
    *, plan_id: str, request: Request, plan_registry: PipelinePlanRegistry
) -> PipelinePlanRequestContext:
    resolved_plan_id, tenant_id, record = await _get_plan_record_or_404(
        plan_id=plan_id,
        request=request,
        plan_registry=plan_registry,
    )
    plan = _load_plan_or_400(record)
    db_name = _extract_db_name_or_400(plan)
    _enforce_db_scope_or_403(request, db_name=db_name)
    branch = str(plan.data_scope.branch or "").strip() or None if plan.data_scope else None
    return PipelinePlanRequestContext(
        plan_id=resolved_plan_id,
        tenant_id=tenant_id,
        db_name=db_name,
        branch=branch,
        plan=plan,
    )
