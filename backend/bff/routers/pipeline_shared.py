"""
Shared helpers for Pipeline Builder routers.

This module exists to support router composition (Composite pattern) by
centralizing cross-cutting helpers (permissions/audit/principal resolution)
without creating import cycles.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.services.http_idempotency import get_idempotency_key as _get_idempotency_key
from bff.services.http_idempotency import require_idempotency_key as _require_idempotency_key_impl
from shared.dependencies.providers import AuditLogStoreDep
from shared.security.principal_utils import actor_label, resolve_principal_from_headers
from shared.services.registries.pipeline_registry import PipelineRegistry


def _resolve_principal(request: Optional[Request]) -> tuple[str, str]:
    headers = request.headers if request else None
    principal_id_headers = (
        "X-Principal-Id",
        "X-Principal-ID",
        "X-Actor",
        "X-User-Id",
        "X-User-ID",
    )
    if not headers or not any(str(headers.get(key) or "").strip() for key in principal_id_headers):
        return "service", "bff"
    return resolve_principal_from_headers(
        headers,
        principal_id_headers=principal_id_headers,
        principal_type_headers=(
            "X-Principal-Type",
            "X-Principal-TYPE",
            "X-Actor-Type",
        ),
        default_principal_type="user",
        default_principal_id="bff",
        allowed_principal_types={"user", "service"},
    )


def _actor_label(principal_type: str, principal_id: str) -> str:
    return actor_label(principal_type, principal_id)


async def _filter_pipeline_records_for_read_access(
    pipeline_registry: PipelineRegistry,
    *,
    records: list[dict[str, Any]],
    request: Optional[Request],
    required_role: str = "read",
    pipeline_id_keys: tuple[str, ...] = ("pipeline_id", "pipelineId"),
) -> list[dict[str, Any]]:
    principal_type, principal_id = _resolve_principal(request)
    filtered: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        pipeline_id: str = ""
        for key in pipeline_id_keys:
            pipeline_id = str(record.get(key) or "").strip()
            if pipeline_id:
                break
        if not pipeline_id:
            continue

        if not await pipeline_registry.has_any_permissions(pipeline_id=pipeline_id):
            await pipeline_registry.grant_permission(
                pipeline_id=pipeline_id,
                principal_type=principal_type,
                principal_id=principal_id,
                role="admin",
            )
            filtered.append(record)
            continue

        if await pipeline_registry.has_permission(
            pipeline_id=pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
            required_role=required_role,
        ):
            filtered.append(record)
    return filtered


async def _ensure_pipeline_permission(
    pipeline_registry: PipelineRegistry,
    *,
    pipeline_id: str,
    request: Optional[Request],
    required_role: str,
) -> tuple[str, str]:
    principal_type, principal_id = _resolve_principal(request)
    try:
        UUID(str(pipeline_id))
    except ValueError:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.RESOURCE_NOT_FOUND)
    has_any = await pipeline_registry.has_any_permissions(pipeline_id=pipeline_id)
    if not has_any:
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        await pipeline_registry.grant_permission(
            pipeline_id=pipeline_id,
            principal_type=principal_type,
            principal_id=principal_id,
            role="admin",
        )
        return principal_type, principal_id
    allowed = await pipeline_registry.has_permission(
        pipeline_id=pipeline_id,
        principal_type=principal_type,
        principal_id=principal_id,
        required_role=required_role,
    )
    if not allowed:
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, "Permission denied", code=ErrorCode.PERMISSION_DENIED)
    return principal_type, principal_id


async def _log_pipeline_audit(
    audit_store: AuditLogStoreDep,
    *,
    request: Optional[Request],
    action: str,
    status: str,
    pipeline_id: Optional[str] = None,
    db_name: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    principal_type, principal_id = _resolve_principal(request)
    actor = _actor_label(principal_type, principal_id)
    partition_key = f"pipeline:{pipeline_id}" if pipeline_id else f"db:{db_name or 'unknown'}"
    await audit_store.log(
        partition_key=partition_key,
        actor=actor,
        action=action,
        status="success" if status.lower() == "success" else "failure",
        resource_type="pipeline" if pipeline_id else "pipeline_branch",
        resource_id=str(pipeline_id) if pipeline_id else None,
        metadata=metadata or {},
        error=error,
    )


def _require_idempotency_key(request: Optional[Request]) -> str:
    return _require_idempotency_key_impl(request)


def _require_pipeline_idempotency_key(request: Optional[Request], *, operation: str) -> str:
    """Require idempotency key for pipeline mutation operations."""
    key = _get_idempotency_key(request)
    if not key:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            f"Idempotency-Key header is required for {operation} operations",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return key
