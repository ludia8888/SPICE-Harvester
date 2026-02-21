"""
Audit log query router for BFF.

Exposes first-class, structured audit logs (Postgres-backed, hash-chained).
"""

from shared.observability.tracing import trace_endpoint

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.dependencies.providers import AuditLogStoreDep
from shared.models.requests import ApiResponse

router = APIRouter(prefix="/audit", tags=["Audit"])
_AUDIT_STATUS_VALUES = {"success", "failure"}


def _normalize_audit_status_filter(raw_value: Optional[str]) -> Optional[str]:
    text = str(raw_value or "").strip().lower()
    if not text:
        return None
    if text not in _AUDIT_STATUS_VALUES:
        raise ValueError("status must be one of: success, failure")
    return text


@router.get("/logs")
@trace_endpoint("bff.audit.list_audit_logs")
async def list_audit_logs(
    partition_key: Optional[str] = Query(None, description="Audit partition key (e.g. db:<db_name>)"),
    action: Optional[str] = Query(None, description="Action filter"),
    status_filter: Optional[str] = Query(None, alias="status", description="Status filter (success|failure)"),
    resource_type: Optional[str] = Query(None),
    resource_id: Optional[str] = Query(None),
    event_id: Optional[str] = Query(None),
    command_id: Optional[str] = Query(None),
    actor: Optional[str] = Query(None),
    since: Optional[datetime] = Query(None),
    until: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    *,
    audit_store: AuditLogStoreDep,
):
    try:
        normalized_status_filter = _normalize_audit_status_filter(status_filter)
        if since and until and since > until:
            raise ValueError("since must be less than or equal to until")
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e

    try:
        logs = await audit_store.list_logs(
            partition_key=partition_key,
            action=action,
            status=normalized_status_filter,
            resource_type=resource_type,
            resource_id=resource_id,
            event_id=event_id,
            command_id=command_id,
            actor=actor,
            since=since,
            until=until,
            limit=limit,
            offset=offset,
        )
        return ApiResponse.success(
            message="Audit logs fetched",
            data={"items": [item.model_dump(mode="json") for item in logs], "count": len(logs)},
        ).to_dict()
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/chain-head")
@trace_endpoint("bff.audit.get_chain_head")
async def get_chain_head(
    partition_key: str = Query(..., description="Audit partition key (e.g. db:<db_name>)"),
    *,
    audit_store: AuditLogStoreDep,
):
    try:
        head = await audit_store.get_chain_head(partition_key=partition_key)
        return ApiResponse.success(message="Audit chain head fetched", data=head).to_dict()
    except ValueError as e:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED) from e
    except Exception as e:
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e
