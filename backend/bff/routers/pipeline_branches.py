"""
Pipeline branch endpoints (BFF).

Composed by `bff.routers.pipeline` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.routers.pipeline_deps import get_pipeline_registry
from bff.routers.pipeline_shared import _ensure_pipeline_permission, _log_pipeline_audit
from shared.dependencies.providers import AuditLogStoreDep
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.registries.pipeline_registry import PipelineAlreadyExistsError, PipelineRegistry
from shared.services.storage.lakefs_client import LakeFSConflictError, LakeFSError
from shared.utils.path_utils import safe_lakefs_ref

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])


@router.get("/branches", response_model=ApiResponse)
@trace_endpoint("list_pipeline_branches")
async def list_pipeline_branches(
    db_name: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        branches = await pipeline_registry.list_pipeline_branches(db_name=db_name)
        return ApiResponse.success(
            message="Pipeline branches fetched",
            data={"branches": branches, "count": len(branches)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list pipeline branches: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


@router.post("/branches/{branch}/archive", response_model=ApiResponse)
@trace_endpoint("archive_pipeline_branch")
async def archive_pipeline_branch(
    branch: str,
    db_name: str,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        resolved_branch = safe_lakefs_ref(branch)
        if resolved_branch == "main":
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "main branch cannot be archived", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        existing = await pipeline_registry.get_pipeline_branch(db_name=db_name, branch=resolved_branch)
        if not existing:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Branch not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        record = await pipeline_registry.archive_pipeline_branch(db_name=db_name, branch=resolved_branch)
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_ARCHIVED",
            status="success",
            db_name=db_name,
            metadata={"branch": resolved_branch},
        )
        return ApiResponse.success(message="Branch archived", data={"branch": record}).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_ARCHIVED",
            status="failure",
            db_name=db_name,
            error=str(exc),
        )
        logger.error("Failed to archive pipeline branch: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


@router.post("/branches/{branch}/restore", response_model=ApiResponse)
@trace_endpoint("restore_pipeline_branch")
async def restore_pipeline_branch(
    branch: str,
    db_name: str,
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        resolved_branch = safe_lakefs_ref(branch)
        existing = await pipeline_registry.get_pipeline_branch(db_name=db_name, branch=resolved_branch)
        if not existing:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Branch not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        record = await pipeline_registry.restore_pipeline_branch(db_name=db_name, branch=resolved_branch)
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_RESTORED",
            status="success",
            db_name=db_name,
            metadata={"branch": resolved_branch},
        )
        return ApiResponse.success(message="Branch restored", data={"branch": record}).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_RESTORED",
            status="failure",
            db_name=db_name,
            error=str(exc),
        )
        logger.error("Failed to restore pipeline branch: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR)


@router.post("/{pipeline_id}/branches", response_model=ApiResponse)
@trace_endpoint("create_pipeline_branch")
async def create_pipeline_branch(
    pipeline_id: str,
    payload: Dict[str, Any],
    audit_store: AuditLogStoreDep,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="edit",
        )
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        actor_user_id = actor_user_id or None

        sanitized = sanitize_input(payload)
        requested_branch = str(sanitized.get("branch") or "").strip()
        if not requested_branch:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "branch is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        branch = safe_lakefs_ref(requested_branch)
        if branch == "main" and requested_branch.lower() != "main":
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Invalid branch name", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        record = await pipeline_registry.create_branch(pipeline_id=pipeline_id, new_branch=branch, user_id=actor_user_id)
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_CREATED",
            status="success",
            pipeline_id=pipeline_id,
            metadata={"branch": branch},
        )
        return ApiResponse.success(
            message="Pipeline branch created",
            data={"branch": {**record.__dict__}},
        ).to_dict()
    except HTTPException:
        raise
    except (PipelineAlreadyExistsError,) as exc:
        raise classified_http_exception(status.HTTP_409_CONFLICT, str(exc), code=ErrorCode.RESOURCE_ALREADY_EXISTS)
    except Exception as exc:
        await _log_pipeline_audit(
            audit_store,
            request=request,
            action="PIPELINE_BRANCH_CREATED",
            status="failure",
            pipeline_id=pipeline_id,
            error=str(exc),
        )
        message = str(exc)
        if isinstance(exc, LakeFSConflictError):
            raise classified_http_exception(status.HTTP_409_CONFLICT, "Branch already exists", code=ErrorCode.RESOURCE_ALREADY_EXISTS)
        if isinstance(exc, LakeFSError):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, message, code=ErrorCode.REQUEST_VALIDATION_FAILED)
        if "not found" in message.lower():
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Pipeline not found", code=ErrorCode.PIPELINE_NOT_FOUND)
        logger.error("Failed to create pipeline branch: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, message, code=ErrorCode.INTERNAL_ERROR)
