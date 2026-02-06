"""Ontology extensions service (BFF).

Extracted from `bff.routers.ontology_extensions` to keep routers thin and to
deduplicate error-handling + input sanitization (Template Method + Facade).
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

import httpx
from fastapi import HTTPException, status

from bff.schemas.ontology_extensions_requests import (
    OntologyApproveRequest,
    OntologyDeployRequest,
    OntologyProposalRequest,
    OntologyResourceRequest,
)
from bff.services.oms_client import OMSClient
from bff.services.ontology_occ_guard_service import resolve_expected_head_commit
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.models.requests import BranchCreateRequest
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name

logger = logging.getLogger(__name__)

_SECURITY_VIOLATION_DETAIL = "입력 데이터에 보안 위반이 감지되었습니다"

T = TypeVar("T")


async def _call_oms(*, action: str, func: Callable[[], Awaitable[T]]) -> T:
    try:
        return await func()
    except HTTPException:
        raise
    except SecurityViolationError as exc:
        logger.warning("Security violation in %s: %s", action, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_SECURITY_VIOLATION_DETAIL) from exc
    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
        raise
    except Exception as exc:
        logger.error("Failed to %s: %s", action, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


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


async def list_ontology_branches(*, oms_client: OMSClient, db_name: str) -> Dict[str, Any]:
    return await _call_oms(
        action="list ontology branches",
        func=lambda: oms_client.list_ontology_branches(validate_db_name(db_name)),
    )


async def create_ontology_branch(*, oms_client: OMSClient, db_name: str, request: BranchCreateRequest) -> Dict[str, Any]:
    return await _call_oms(
        action="create ontology branch",
        func=lambda: oms_client.create_ontology_branch(
            validate_db_name(db_name),
            sanitize_input(request.model_dump(mode="json")),
        ),
    )


async def list_ontology_proposals(
    *,
    oms_client: OMSClient,
    db_name: str,
    status_filter: Optional[str],
    limit: int,
) -> Dict[str, Any]:
    return await _call_oms(
        action="list ontology proposals",
        func=lambda: oms_client.list_ontology_proposals(
            validate_db_name(db_name),
            status_filter=status_filter,
            limit=limit,
        ),
    )


async def create_ontology_proposal(
    *,
    oms_client: OMSClient,
    db_name: str,
    request: OntologyProposalRequest,
) -> Dict[str, Any]:
    return await _call_oms(
        action="create ontology proposal",
        func=lambda: oms_client.create_ontology_proposal(
            validate_db_name(db_name),
            sanitize_input(request.model_dump(mode="json")),
        ),
    )


async def approve_ontology_proposal(
    *,
    oms_client: OMSClient,
    db_name: str,
    proposal_id: str,
    request: OntologyApproveRequest,
) -> Dict[str, Any]:
    return await _call_oms(
        action="approve ontology proposal",
        func=lambda: oms_client.approve_ontology_proposal(
            validate_db_name(db_name),
            proposal_id,
            sanitize_input(request.model_dump(mode="json")),
        ),
    )


async def deploy_ontology(
    *,
    oms_client: OMSClient,
    db_name: str,
    request: OntologyDeployRequest,
) -> Dict[str, Any]:
    return await _call_oms(
        action="deploy ontology",
        func=lambda: oms_client.deploy_ontology(
            validate_db_name(db_name),
            sanitize_input(request.model_dump(mode="json")),
        ),
    )


async def ontology_health(*, oms_client: OMSClient, db_name: str, branch: str) -> Dict[str, Any]:
    return await _call_oms(
        action="fetch ontology health",
        func=lambda: oms_client.get_ontology_health(validate_db_name(db_name), branch=branch),
    )
