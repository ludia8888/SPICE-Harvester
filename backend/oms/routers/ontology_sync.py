"""
OMS 동기 온톨로지 라우터 - 동기 API 래퍼
비동기 Command를 동기적으로 실행하는 편의 API
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status

from oms.dependencies import (
    get_jsonld_converter, 
    get_label_mapper, 
    get_terminus_service,
    get_outbox_service,
    get_redis_service,
    get_command_status_service,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)
from oms.database.postgres import db as postgres_db
from oms.database.outbox import OutboxService
from shared.services import CommandStatusService
from shared.services.sync_wrapper_service import SyncWrapperService
from shared.models.sync_wrapper import SyncOptions, SyncResult
from shared.models.ontology import (
    OntologyCreateRequest,
    OntologyUpdateRequest,
    OntologyResponse,
)
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_class_id,
    validate_db_name,
)

# 비동기 라우터 import (내부 호출용)
from oms.routers.ontology_async import (
    create_ontology_async,
    update_ontology_async,
    delete_ontology_async
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ontology/{db_name}/sync", 
    tags=["Sync Ontology Management"],
    responses={
        408: {"description": "Request Timeout"},
        503: {"description": "Service Unavailable"}
    }
)


def get_sync_wrapper_service(
    command_status_service: CommandStatusService = Depends(get_command_status_service)
) -> SyncWrapperService:
    """동기 래퍼 서비스 의존성"""
    return SyncWrapperService(command_status_service)


@router.post("/create", response_model=ApiResponse)
async def create_ontology_sync(
    db_name: str = Depends(ensure_database_exists),
    request: OntologyCreateRequest = ...,
    timeout: float = Query(default=30.0, ge=1.0, le=300.0, description="최대 대기 시간(초)"),
    poll_interval: float = Query(default=0.5, ge=0.1, le=5.0, description="상태 확인 간격(초)"),
    outbox_service: Optional[OutboxService] = Depends(get_outbox_service),
    command_status_service: CommandStatusService = Depends(get_command_status_service),
    sync_wrapper: SyncWrapperService = Depends(get_sync_wrapper_service),
) -> ApiResponse:
    """
    동기식 온톨로지 생성 - Command를 제출하고 완료될 때까지 대기
    
    비동기 API를 내부적으로 호출하고 결과를 기다립니다.
    타임아웃이 발생하면 408 Request Timeout을 반환합니다.
    """
    if not outbox_service or not postgres_db.pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sync processing is not available. Required services not configured."
        )
    
    try:
        # 실행 옵션 설정
        options = SyncOptions(
            timeout=timeout,
            poll_interval=poll_interval,
            include_progress=True
        )
        
        # 비동기 함수 실행 및 대기
        result = await sync_wrapper.execute_sync(
            async_func=create_ontology_async,
            request_data={
                "db_name": db_name,
                "request": request,
                "outbox_service": outbox_service,
                "command_status_service": command_status_service
            },
            options=options
        )
        
        # 결과 처리
        if result.success:
            return ApiResponse(
                status="success",
                message=f"Successfully created ontology class '{request.id}'",
                data={
                    "command_id": result.command_id,
                    "class_id": request.id,
                    "execution_time": result.execution_time,
                    **result.data
                }
            )
        elif result.final_status == "TIMEOUT":
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={
                    "message": f"Operation timed out after {timeout} seconds",
                    "command_id": result.command_id,
                    "hint": f"You can check the status using GET /api/v1/ontology/{db_name}/async/command/{result.command_id}/status"
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "message": "Operation failed",
                    "error": result.error,
                    "command_id": result.command_id
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ontology synchronously: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.put("/{class_id}", response_model=ApiResponse)
async def update_ontology_sync(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    request: OntologyUpdateRequest = ...,
    timeout: float = Query(default=30.0, ge=1.0, le=300.0, description="최대 대기 시간(초)"),
    poll_interval: float = Query(default=0.5, ge=0.1, le=5.0, description="상태 확인 간격(초)"),
    outbox_service: Optional[OutboxService] = Depends(get_outbox_service),
    command_status_service: CommandStatusService = Depends(get_command_status_service),
    sync_wrapper: SyncWrapperService = Depends(get_sync_wrapper_service),
) -> ApiResponse:
    """
    동기식 온톨로지 업데이트 - Command를 제출하고 완료될 때까지 대기
    """
    if not outbox_service or not postgres_db.pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sync processing is not available. Required services not configured."
        )
    
    try:
        # 실행 옵션 설정
        options = SyncOptions(
            timeout=timeout,
            poll_interval=poll_interval,
            include_progress=True
        )
        
        # 비동기 함수 실행 및 대기
        result = await sync_wrapper.execute_sync(
            async_func=update_ontology_async,
            request_data={
                "db_name": db_name,
                "class_id": class_id,
                "request": request,
                "outbox_service": outbox_service,
                "command_status_service": command_status_service
            },
            options=options
        )
        
        # 결과 처리
        if result.success:
            return ApiResponse(
                status="success",
                message=f"Successfully updated ontology class '{class_id}'",
                data={
                    "command_id": result.command_id,
                    "class_id": class_id,
                    "execution_time": result.execution_time,
                    **result.data
                }
            )
        elif result.final_status == "TIMEOUT":
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={
                    "message": f"Operation timed out after {timeout} seconds",
                    "command_id": result.command_id,
                    "hint": f"You can check the status using GET /api/v1/ontology/{db_name}/async/command/{result.command_id}/status"
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "message": "Operation failed",
                    "error": result.error,
                    "command_id": result.command_id
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update ontology synchronously: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/{class_id}", response_model=ApiResponse)
async def delete_ontology_sync(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    timeout: float = Query(default=30.0, ge=1.0, le=300.0, description="최대 대기 시간(초)"),
    poll_interval: float = Query(default=0.5, ge=0.1, le=5.0, description="상태 확인 간격(초)"),
    outbox_service: Optional[OutboxService] = Depends(get_outbox_service),
    command_status_service: CommandStatusService = Depends(get_command_status_service),
    sync_wrapper: SyncWrapperService = Depends(get_sync_wrapper_service),
) -> ApiResponse:
    """
    동기식 온톨로지 삭제 - Command를 제출하고 완료될 때까지 대기
    """
    if not outbox_service or not postgres_db.pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sync processing is not available. Required services not configured."
        )
    
    try:
        # 실행 옵션 설정
        options = SyncOptions(
            timeout=timeout,
            poll_interval=poll_interval,
            include_progress=True
        )
        
        # 비동기 함수 실행 및 대기
        result = await sync_wrapper.execute_sync(
            async_func=delete_ontology_async,
            request_data={
                "db_name": db_name,
                "class_id": class_id,
                "outbox_service": outbox_service,
                "command_status_service": command_status_service
            },
            options=options
        )
        
        # 결과 처리
        if result.success:
            return ApiResponse(
                status="success",
                message=f"Successfully deleted ontology class '{class_id}'",
                data={
                    "command_id": result.command_id,
                    "class_id": class_id,
                    "execution_time": result.execution_time
                }
            )
        elif result.final_status == "TIMEOUT":
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={
                    "message": f"Operation timed out after {timeout} seconds",
                    "command_id": result.command_id,
                    "hint": f"You can check the status using GET /api/v1/ontology/{db_name}/async/command/{result.command_id}/status"
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "message": "Operation failed",
                    "error": result.error,
                    "command_id": result.command_id
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete ontology synchronously: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/command/{command_id}/wait", response_model=ApiResponse)
async def wait_for_command(
    command_id: str,
    timeout: float = Query(default=30.0, ge=1.0, le=300.0, description="최대 대기 시간(초)"),
    poll_interval: float = Query(default=0.5, ge=0.1, le=5.0, description="상태 확인 간격(초)"),
    sync_wrapper: SyncWrapperService = Depends(get_sync_wrapper_service),
) -> ApiResponse:
    """
    기존 Command의 완료를 대기
    
    이미 제출된 Command ID를 사용하여 완료될 때까지 대기합니다.
    비동기 API로 제출한 Command를 동기적으로 기다릴 때 유용합니다.
    """
    try:
        # 실행 옵션 설정
        options = SyncOptions(
            timeout=timeout,
            poll_interval=poll_interval,
            include_progress=True
        )
        
        # Command 완료 대기
        result = await sync_wrapper.wait_for_command(command_id, options)
        
        # 결과 처리
        if result.success:
            return ApiResponse(
                status="success",
                message="Command completed successfully",
                data={
                    "command_id": result.command_id,
                    "execution_time": result.execution_time,
                    "progress_history": result.progress_history,
                    **result.data
                }
            )
        elif result.final_status == "TIMEOUT":
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={
                    "message": f"Waiting timed out after {timeout} seconds",
                    "command_id": result.command_id,
                    "progress_history": result.progress_history
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "message": "Command failed",
                    "error": result.error,
                    "command_id": result.command_id
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to wait for command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )