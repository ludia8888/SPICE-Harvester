"""
CI integration endpoints (webhook/polling ingestion).

These endpoints are designed for CI systems to push standardized results into
agent sessions (CHG-004 / INT-004) without requiring an end-user JWT.

Security model:
- Protected by the BFF shared token (admin/write) via middleware.
- Tenant isolation is enforced by requiring `tenant_id` and scoping all writes
  to that tenant in storage.
"""

from shared.observability.tracing import trace_endpoint

import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception
from pydantic import BaseModel, Field

from bff.routers.registry_deps import get_agent_session_registry
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.agent_session_registry import AgentSessionRegistry
from shared.utils.uuid_utils import safe_uuid as _safe_uuid

router = APIRouter(prefix="/admin/ci", tags=["Admin"])
logger = logging.getLogger(__name__)


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


@router.post("/ci-results", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
@trace_endpoint("bff.ci_webhooks.ingest_ci_result")
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
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "session_id must be a UUID", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Agent session not found", code=ErrorCode.RESOURCE_NOT_FOUND)

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

    try:
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
    except Exception as exc:
        logger.warning(
            "Failed to append CI_RESULT event session_id=%s tenant_id=%s: %s",
            session_id,
            tenant_id,
            exc,
            exc_info=True,
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
