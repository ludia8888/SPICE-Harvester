"""Ontology extensions service (BFF).

Extracted from `bff.routers.ontology_extensions` to keep routers thin and to
deduplicate error-handling + input sanitization (Template Method + Facade).
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

import httpx
from fastapi import HTTPException
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.schemas.ontology_extensions_requests import OntologyResourceRequest
from bff.services.oms_client import OMSClient
from bff.services.ontology_occ_guard_service import resolve_expected_head_commit
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)

_SECURITY_VIOLATION_DETAIL = "입력 데이터에 보안 위반이 감지되었습니다"

T = TypeVar("T")


def _default_expected_head_commit(branch: str) -> str:
    normalized = str(branch or "").strip() or "main"
    if normalized.lower().startswith("branch:"):
        return normalized
    return f"branch:{normalized}"


async def _call_oms(*, action: str, func: Callable[[], Awaitable[T]]) -> T:
    try:
        return await func()
    except HTTPException:
        raise
    except SecurityViolationError as exc:
        logger.warning("Security violation in %s: %s", action, exc)
        raise classified_http_exception(400, _SECURITY_VIOLATION_DETAIL, code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
        raise
    except Exception as exc:
        logger.error("Failed to %s: %s", action, exc)
        raise classified_http_exception(500, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.ontology_extensions.list_resources")
async def list_resources(
    *,
    oms_client: OMSClient,
    db_name: str,
    resource_type: Optional[str],
    branch: str,
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    return await _call_oms(
        action="list ontology resources",
        func=lambda: oms_client.list_ontology_resources(
            validate_db_name(db_name),
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        ),
    )


@trace_external_call("bff.ontology_extensions.create_resource")
async def create_resource(
    *,
    oms_client: OMSClient,
    db_name: str,
    resource_type: str,
    payload: OntologyResourceRequest,
    branch: str,
    expected_head_commit: Optional[str],
) -> Dict[str, Any]:
    resolved_db_name = validate_db_name(db_name)
    expected_head_commit = expected_head_commit or _default_expected_head_commit(branch)
    resolved_expected_head = await _call_oms(
        action="resolve ontology expected head commit",
        func=lambda: resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=resolved_db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        ),
    )
    return await _call_oms(
        action="create ontology resource",
        func=lambda: oms_client.create_ontology_resource(
            resolved_db_name,
            resource_type=resource_type,
            payload=sanitize_input(payload.model_dump(exclude_unset=True)),
            branch=branch,
            expected_head_commit=resolved_expected_head,
        ),
    )


@trace_external_call("bff.ontology_extensions.update_resource")
async def update_resource(
    *,
    oms_client: OMSClient,
    db_name: str,
    resource_type: str,
    resource_id: str,
    payload: OntologyResourceRequest,
    branch: str,
    expected_head_commit: Optional[str],
) -> Dict[str, Any]:
    resolved_db_name = validate_db_name(db_name)
    expected_head_commit = expected_head_commit or _default_expected_head_commit(branch)
    resolved_expected_head = await _call_oms(
        action="resolve ontology expected head commit",
        func=lambda: resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=resolved_db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        ),
    )
    return await _call_oms(
        action="update ontology resource",
        func=lambda: oms_client.update_ontology_resource(
            resolved_db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            payload=sanitize_input(payload.model_dump(exclude_unset=True)),
            branch=branch,
            expected_head_commit=resolved_expected_head,
        ),
    )


@trace_external_call("bff.ontology_extensions.get_resource")
async def get_resource(
    *,
    oms_client: OMSClient,
    db_name: str,
    resource_type: str,
    resource_id: str,
    branch: str,
) -> Dict[str, Any]:
    return await _call_oms(
        action="get ontology resource",
        func=lambda: oms_client.get_ontology_resource(
            validate_db_name(db_name),
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        ),
    )


@trace_external_call("bff.ontology_extensions.delete_resource")
async def delete_resource(
    *,
    oms_client: OMSClient,
    db_name: str,
    resource_type: str,
    resource_id: str,
    branch: str,
    expected_head_commit: Optional[str],
) -> Dict[str, Any]:
    resolved_db_name = validate_db_name(db_name)
    expected_head_commit = expected_head_commit or _default_expected_head_commit(branch)
    resolved_expected_head = await _call_oms(
        action="resolve ontology expected head commit",
        func=lambda: resolve_expected_head_commit(
            oms_client=oms_client,
            db_name=resolved_db_name,
            branch=branch,
            expected_head_commit=expected_head_commit,
        ),
    )
    return await _call_oms(
        action="delete ontology resource",
        func=lambda: oms_client.delete_ontology_resource(
            resolved_db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
            expected_head_commit=resolved_expected_head,
        ),
    )


@trace_external_call("bff.ontology_extensions.record_deployment")
async def record_deployment(
    *,
    oms_client: OMSClient,
    db_name: str,
    target_branch: str,
    ontology_commit_id: Optional[str],
    snapshot_rid: Optional[str],
    deployed_by: str,
    metadata: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    resolved_db_name = validate_db_name(db_name)
    normalized_branch = str(target_branch or "").strip() or "main"
    normalized_commit = str(ontology_commit_id or "").strip() or _default_expected_head_commit(normalized_branch)
    normalized_deployed_by = str(deployed_by or "").strip() or "system"
    normalized_snapshot = str(snapshot_rid or "").strip() or None
    return await _call_oms(
        action="record ontology deployment",
        func=lambda: oms_client.record_ontology_deployment(
            resolved_db_name,
            target_branch=normalized_branch,
            ontology_commit_id=normalized_commit,
            snapshot_rid=normalized_snapshot,
            deployed_by=normalized_deployed_by,
            metadata=sanitize_input(metadata or {}),
        ),
    )
