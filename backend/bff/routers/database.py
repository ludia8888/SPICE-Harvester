"""
Database management router for BFF
Handles database creation, deletion, and listing
"""

from typing import Optional
from uuid import UUID
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, Query, Request, status

from bff.routers.role_deps import enforce_required_database_role
from bff.schemas.database_access_requests import UpsertDatabaseAccessRequest
from bff.services.input_validation_service import enforce_db_scope_or_403, validated_db_name
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.security.database_access import (
    DATABASE_ACCESS_ROLES,
    SECURITY_ROLES,
    fetch_database_access_entries,
    normalize_database_role,
    upsert_database_access_entry,
)
from shared.services.registries.action_log_registry import ActionLogRegistry

from bff.dependencies import OMSClientDep
from bff.routers.registry_deps import get_dataset_registry
from bff.services import database_service
from bff.services.oms_client import OMSClient
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.models.responses import ApiResponse
from shared.models.requests import DatabaseCreateRequest

router = APIRouter(prefix="/databases", tags=["Database Management"])

def _is_platform_admin_request(request: Request) -> bool:
    principal = getattr(request.state, "user", None)
    claims = getattr(principal, "claims", None)
    if isinstance(claims, dict) and bool(claims.get("admin_token")):
        return True
    if bool(getattr(request.state, "dev_master_auth", False)):
        return True
    return False


@router.get("", response_model=ApiResponse)
@trace_endpoint("bff.database.list_databases")
async def list_databases(
    request: Request,
    oms: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    """데이터베이스 목록 조회"""
    return await database_service.list_databases(request=request, oms=oms, dataset_registry=dataset_registry)


@router.post(
    "",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_201_CREATED: {"model": ApiResponse, "description": "Direct mode"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
@trace_endpoint("bff.database.create_database")
async def create_database(request: DatabaseCreateRequest, http_request: Request, oms: OMSClient = OMSClientDep):
    """데이터베이스 생성"""
    return await database_service.create_database(body=request, http_request=http_request, oms=oms)


@router.delete(
    "/{db_name}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
@trace_endpoint("bff.database.delete_database")
async def delete_database(
    db_name: str,
    http_request: Request,
    expected_seq: Optional[int] = Query(
        default=None,
        ge=0,
        description="Expected current aggregate sequence (OCC). When omitted, server resolves the current value.",
    ),
    oms: OMSClient = OMSClientDep,
):
    """데이터베이스 삭제"""
    return await database_service.delete_database(
        db_name=db_name,
        http_request=http_request,
        expected_seq=expected_seq,
        oms=oms,
    )


@router.get("/{db_name}")
@trace_endpoint("bff.database.get_database")
async def get_database(db_name: str, oms: OMSClient = OMSClientDep):
    """데이터베이스 정보 조회"""
    return await database_service.get_database(db_name=db_name, oms=oms)


@router.get("/{db_name}/expected-seq", response_model=ApiResponse)
@trace_endpoint("bff.database.get_database_expected_seq")
async def get_database_expected_seq(
    db_name: str,
):
    """
    Resolve the current `expected_seq` for database (aggregate) operations.

    Frontend policy: OCC tokens should be treated as resource versions, not user input.
    """
    return await database_service.get_database_expected_seq(db_name=db_name)


@router.get("/{db_name}/access", response_model=ApiResponse)
@trace_endpoint("bff.database.list_database_access")
async def list_database_access(
    db_name: str,
    http_request: Request,
) -> ApiResponse:
    """List database access entries (RBAC)."""
    resolved = validated_db_name(db_name)
    enforce_db_scope_or_403(http_request, db_name=resolved)
    if not _is_platform_admin_request(http_request):
        await enforce_required_database_role(http_request, db_name=resolved, roles=SECURITY_ROLES)
    grouped = await fetch_database_access_entries(db_names=[resolved])
    return ApiResponse.success(
        message="Database access entries",
        data={"db_name": resolved, "entries": grouped.get(resolved, [])},
    ).to_dict()


@router.post("/{db_name}/access", response_model=ApiResponse)
@trace_endpoint("bff.database.upsert_database_access")
async def upsert_database_access(
    db_name: str,
    body: UpsertDatabaseAccessRequest,
    http_request: Request,
) -> ApiResponse:
    """Upsert database access entries (RBAC). Requires Owner/Security."""
    resolved = validated_db_name(db_name)
    enforce_db_scope_or_403(http_request, db_name=resolved)
    if not _is_platform_admin_request(http_request):
        await enforce_required_database_role(http_request, db_name=resolved, roles=SECURITY_ROLES)

    entries = body.entries or []
    if not entries:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "entries is required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    updated = 0
    for entry in entries:
        principal_type = str(entry.principal_type or "user").strip().lower() or "user"
        principal_id = str(entry.principal_id or "").strip()
        if not principal_id:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "principal_id is required",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        principal_name = (str(entry.principal_name or "").strip() or principal_id).strip()
        role = normalize_database_role(entry.role) or str(entry.role or "").strip()
        if role not in set(DATABASE_ACCESS_ROLES):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"Invalid role: {entry.role}",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        await upsert_database_access_entry(
            db_name=resolved,
            principal_type=principal_type,
            principal_id=principal_id,
            principal_name=principal_name,
            role=role,
        )
        updated += 1

    return ApiResponse.success(
        message="Database access updated",
        data={"db_name": resolved, "updated": updated},
    ).to_dict()


@router.get("/{db_name}/action-logs/{action_log_id}", response_model=ApiResponse)
@trace_endpoint("bff.database.get_action_log")
async def get_action_log(
    db_name: str,
    action_log_id: str,
    http_request: Request,
) -> ApiResponse:
    """Get an action log record (writeback status + targets) for observability/debugging."""
    resolved = validated_db_name(db_name)
    enforce_db_scope_or_403(http_request, db_name=resolved)
    if not _is_platform_admin_request(http_request):
        await enforce_required_database_role(http_request, db_name=resolved, roles=DATABASE_ACCESS_ROLES)

    try:
        UUID(str(action_log_id))
    except Exception:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Invalid action_log_id (must be UUID)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    registry = ActionLogRegistry()
    await registry.connect()
    try:
        record = await registry.get_log(action_log_id=str(action_log_id))
        if not record or str(record.db_name or "").strip() != resolved:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                "Action log not found",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        submit_ctx = None
        if isinstance(record.metadata, dict):
            submit_ctx = record.metadata.get("__submit_context")
        overlay_branch = None
        if isinstance(submit_ctx, dict):
            overlay_branch = str(submit_ctx.get("overlay_branch") or "").strip() or None

        return ApiResponse.success(
            message="Action log retrieved",
            data={
                "action_log_id": record.action_log_id,
                "db_name": record.db_name,
                "status": record.status,
                "action_type_id": record.action_type_id,
                "ontology_commit_id": record.ontology_commit_id,
                "overlay_branch": overlay_branch,
                "writeback_target": record.writeback_target,
                "writeback_commit_id": record.writeback_commit_id,
                "submitted_at": record.submitted_at.isoformat() if record.submitted_at else None,
                "finished_at": record.finished_at.isoformat() if record.finished_at else None,
            },
        ).to_dict()
    finally:
        await registry.close()
