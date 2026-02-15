"""
Database management router for BFF
Handles database creation, deletion, and listing
"""

from typing import Any, Dict, Optional
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, Query, Request, status

from bff.dependencies import OMSClientDep
from bff.routers.registry_deps import get_dataset_registry
from bff.services import database_service
from bff.services.oms_client import OMSClient
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.models.requests import ApiResponse, DatabaseCreateRequest

router = APIRouter(prefix="/databases", tags=["Database Management"])


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


@router.get("/{db_name}/branches/{branch_name:path}")
@trace_endpoint("bff.database.get_branch_info")
async def get_branch_info(db_name: str, branch_name: str, oms: OMSClient = OMSClientDep) -> Dict[str, Any]:
    """브랜치 정보 조회 (프론트엔드용 BFF 래핑)"""
    return await database_service.get_branch_info(db_name=db_name, branch_name=branch_name, oms=oms)


@router.delete("/{db_name}/branches/{branch_name:path}")
@trace_endpoint("bff.database.delete_branch")
async def delete_branch(
    db_name: str,
    branch_name: str,
    force: bool = False,
    oms: OMSClient = OMSClientDep,
) -> Dict[str, Any]:
    """브랜치 삭제 (프론트엔드용 BFF 래핑)"""
    return await database_service.delete_branch(db_name=db_name, branch_name=branch_name, force=force, oms=oms)


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


@router.get("/{db_name}/classes")
@trace_endpoint("bff.database.list_classes")
async def list_classes(
    db_name: str,
    type: Optional[str] = "Class",
    limit: Optional[int] = None,
    oms: OMSClient = OMSClientDep,
):
    """데이터베이스의 클래스 목록 조회"""
    return await database_service.list_classes(db_name=db_name, type=type, limit=limit, oms=oms)


@router.post("/{db_name}/classes")
@trace_endpoint("bff.database.create_class")
async def create_class(
    db_name: str, class_data: Dict[str, Any], oms: OMSClient = OMSClientDep
):
    """데이터베이스에 새 클래스 생성"""
    return await database_service.create_class(db_name=db_name, class_data=class_data, oms=oms)


@router.get("/{db_name}/classes/{class_id}")
@trace_endpoint("bff.database.get_class")
async def get_class(
    db_name: str, class_id: str, request: Request, oms: OMSClient = OMSClientDep
):
    """특정 클래스 조회"""
    _ = request
    return await database_service.get_class(db_name=db_name, class_id=class_id, oms=oms)


@router.get("/{db_name}/branches")
@trace_endpoint("bff.database.list_branches")
async def list_branches(db_name: str, oms: OMSClient = OMSClientDep):
    """브랜치 목록 조회"""
    return await database_service.list_branches(db_name=db_name, oms=oms)


@router.post("/{db_name}/branches")
@trace_endpoint("bff.database.create_branch")
async def create_branch(
    db_name: str, branch_data: Dict[str, Any], oms: OMSClient = OMSClientDep
):
    """새 브랜치 생성"""
    return await database_service.create_branch(db_name=db_name, branch_data=branch_data, oms=oms)


@router.get("/{db_name}/versions")
@trace_endpoint("bff.database.get_versions")
async def get_versions(db_name: str, oms: OMSClient = OMSClientDep):
    """버전 히스토리 조회"""
    return await database_service.get_versions(db_name=db_name, oms=oms)
