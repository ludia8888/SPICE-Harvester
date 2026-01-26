"""
CI integration endpoints (webhook/polling ingestion).

These endpoints are designed for CI systems to push standardized results into
agent sessions (CHG-004 / INT-004) without requiring an end-user JWT.

Security model:
- Protected by the BFF shared token (admin/write) via middleware.
- Tenant isolation is enforced by requiring `tenant_id` and scoping all writes
  to that tenant in storage.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.agent_session_registry import AgentSessionRegistry

router = APIRouter(prefix="/admin/ci", tags=["Admin"])


async def get_agent_session_registry() -> AgentSessionRegistry:
    from bff.main import get_agent_session_registry as _get_agent_session_registry

    return await _get_agent_session_registry()


class AgentSessionCIResultIngestRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=200)
    session_id: str = Field(..., min_length=1, max_length=64)
    ci_result_id: Optional[str] = Field(default=None, description="Optional idempotency id (UUID)")
    job_id: Optional[str] = Field(default=None)
    plan_id: Optional[str] = Field(default=None)
    run_id: Optional[str] = Field(default=None)
    provider: Optional[str] = Field(default=None, max_length=100)
    status: str = Field(default="unknown", max_length=50)
    details_url: Optional[str] = Field(default=None, max_length=2000)
    summary: Optional[str] = Field(default=None, max_length=5000)
    checks: list | None = Field(default=None, description="Provider-normalized check list")
    raw: dict | None = Field(default=None, description="Raw provider payload (safe subset)")


_CI_STATUSES = {"queued", "in_progress", "success", "failure", "cancelled", "skipped", "unknown"}


def _safe_uuid(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return str(UUID(str(value)))
    except Exception:
        return None


@router.post("/ci-results", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def ingest_ci_result(
    body: AgentSessionCIResultIngestRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    payload = sanitize_input(body.model_dump(exclude_none=True))
    tenant_id = str(payload.get("tenant_id") or "").strip() or "default"

    try:
        session_id = str(UUID(str(payload.get("session_id") or "")))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    status_value = str(payload.get("status") or "").strip().lower() or "unknown"
    if status_value not in _CI_STATUSES:
        status_value = "unknown"

    ci_result_id = _safe_uuid(payload.get("ci_result_id")) or str(uuid4())
    ci_record = await sessions.record_ci_result(
        ci_result_id=ci_result_id,
        session_id=session_id,
        tenant_id=tenant_id,
        job_id=_safe_uuid(payload.get("job_id")),
        plan_id=_safe_uuid(payload.get("plan_id")),
        run_id=_safe_uuid(payload.get("run_id")),
        provider=str(payload.get("provider") or "").strip() or None,
        status=status_value,
        details_url=str(payload.get("details_url") or "").strip() or None,
        summary=str(payload.get("summary") or "").strip() or None,
        checks=payload.get("checks") if isinstance(payload.get("checks"), list) else None,
        raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else None,
        created_at=datetime.now(timezone.utc),
    )

    with contextlib.suppress(Exception):
        await sessions.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="CI_RESULT",
            occurred_at=datetime.now(timezone.utc),
            data={
                "ci_result_id": ci_record.ci_result_id,
                "status": ci_record.status,
                "provider": ci_record.provider,
                "details_url": ci_record.details_url,
                "summary": ci_record.summary,
                "job_id": ci_record.job_id,
                "plan_id": ci_record.plan_id,
                "run_id": ci_record.run_id,
                "source": "ci_webhook",
            },
        )

    return ApiResponse.created(
        message="CI result ingested",
        data={
            "ci_result": {
                "ci_result_id": ci_record.ci_result_id,
                "session_id": ci_record.session_id,
                "tenant_id": ci_record.tenant_id,
                "job_id": ci_record.job_id,
                "plan_id": ci_record.plan_id,
                "run_id": ci_record.run_id,
                "provider": ci_record.provider,
                "status": ci_record.status,
                "details_url": ci_record.details_url,
                "summary": ci_record.summary,
                "checks": ci_record.checks,
                "raw": ci_record.raw,
                "created_at": ci_record.created_at.isoformat(),
            }
        },
    )
