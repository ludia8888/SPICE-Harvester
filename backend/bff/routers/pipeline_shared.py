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

from bff.services.http_idempotency import get_idempotency_key as _get_idempotency_key
from bff.services.http_idempotency import require_idempotency_key as _require_idempotency_key_impl
from shared.dependencies.providers import AuditLogStoreDep
from shared.services.registries.pipeline_registry import PipelineRegistry


def _resolve_principal(request: Optional[Request]) -> tuple[str, str]:
    if request is None:
        return "service", "bff"
    headers = request.headers
    principal_id = (
        headers.get("X-Principal-Id")
        or headers.get("X-Principal-ID")
        or headers.get("X-Actor")
        or headers.get("X-User-Id")
        or headers.get("X-User-ID")
        or ""
    ).strip()
    principal_type = (
        headers.get("X-Principal-Type")
        or headers.get("X-Principal-TYPE")
        or headers.get("X-Actor-Type")
        or ""
    ).strip()
    if not principal_id:
        return "service", "bff"
    normalized_type = principal_type.lower()
    if normalized_type not in {"user", "service"}:
        normalized_type = "user"
    return normalized_type, principal_id


def _actor_label(principal_type: str, principal_id: str) -> str:
    principal_type = principal_type or "user"
    principal_id = principal_id or "unknown"
    return f"{principal_type}:{principal_id}"


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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    has_any = await pipeline_registry.has_any_permissions(pipeline_id=pipeline_id)
    if not has_any:
        pipeline = await pipeline_registry.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
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
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Idempotency-Key header is required for {operation} operations",
        )
    return key
