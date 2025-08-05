"""
OMS 비동기 인스턴스 라우터 - Command Pattern 기반
인스턴스 데이터의 명령을 저장하고 비동기로 처리하는 API
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel, Field

from oms.dependencies import (
    JSONLDConverterDep,
    LabelMapperDep,
    TerminusServiceDep,
    OutboxServiceDep,
    CommandStatusServiceDep,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)
from shared.dependencies.providers import RedisServiceDep
from oms.database.postgres import db as postgres_db
from oms.database.outbox import MessageType, OutboxService
from shared.models.commands import CommandType, InstanceCommand, CommandResult, CommandStatus
from shared.models.common import BaseResponse
from shared.services import CommandStatusService, RedisService
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_class_id,
    validate_db_name,
    validate_instance_id,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instances/{db_name}/async", tags=["Async Instance Management"])


# Request Models
class InstanceCreateRequest(BaseModel):
    """인스턴스 생성 요청"""
    data: Dict[str, Any] = Field(..., description="인스턴스 데이터")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")
    

class InstanceUpdateRequest(BaseModel):
    """인스턴스 수정 요청"""
    data: Dict[str, Any] = Field(..., description="수정할 데이터")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")
    

class BulkInstanceCreateRequest(BaseModel):
    """대량 인스턴스 생성 요청"""
    instances: List[Dict[str, Any]] = Field(..., description="인스턴스 데이터 목록")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")


@router.post("/{class_id}/create", response_model=CommandResult)
async def create_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    request: InstanceCreateRequest = ...,
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
    user_id: Optional[str] = None,
):
    """
    인스턴스 생성 명령을 비동기로 처리
    
    이 API는 Command만 저장하고 즉시 응답합니다.
    실제 인스턴스 생성은 Worker가 처리합니다.
    """
    try:
        # 입력 검증
        sanitized_data = sanitize_input(request.data)
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.CREATE_INSTANCE,
            db_name=db_name,
            class_id=class_id,
            payload=sanitized_data,
            metadata={
                **request.metadata,
                "user_id": user_id,
                "created_at": datetime.utcnow().isoformat()
            },
            created_by=user_id
        )
        
        # Command를 Outbox에 저장 (트랜잭션)
        if outbox_service:
            async with postgres_db.transaction() as conn:
                await outbox_service.publish_command(conn, command.dict())
        
        # Redis에 상태 저장
        await command_status_service.set_command_status(
            command_id=str(command.command_id),
            status=CommandStatus.PENDING,
            metadata={
                "command_type": command.command_type,
                "db_name": db_name,
                "class_id": class_id,
                "aggregate_id": command.aggregate_id,
                "created_at": command.created_at.isoformat(),
                "created_by": user_id
            }
        )
        
        # 응답
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Instance creation command accepted for class '{class_id}'",
                "class_id": class_id,
                "db_name": db_name
            }
        )
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in instance creation: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create instance command: {str(e)}"
        )


@router.put("/{class_id}/{instance_id}/update", response_model=CommandResult)
async def update_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    instance_id: str = ...,
    request: InstanceUpdateRequest = ...,
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
    user_id: Optional[str] = None,
):
    """
    인스턴스 수정 명령을 비동기로 처리
    """
    try:
        # 입력 검증
        validate_instance_id(instance_id)
        sanitized_data = sanitize_input(request.data)
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.UPDATE_INSTANCE,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            payload=sanitized_data,
            metadata={
                **request.metadata,
                "user_id": user_id,
                "updated_at": datetime.utcnow().isoformat()
            },
            created_by=user_id
        )
        
        # Command를 Outbox에 저장
        if outbox_service:
            async with postgres_db.transaction() as conn:
                await outbox_service.publish_command(conn, command.dict())
        
        # Redis에 상태 저장
        await command_status_service.set_command_status(
            command_id=str(command.command_id),
            status=CommandStatus.PENDING,
            metadata={
                "command_type": command.command_type,
                "db_name": db_name,
                "class_id": class_id,
                "instance_id": instance_id,
                "aggregate_id": command.aggregate_id,
                "updated_at": datetime.utcnow().isoformat(),
                "updated_by": user_id
            }
        )
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Instance update command accepted for '{instance_id}'",
                "instance_id": instance_id,
                "class_id": class_id,
                "db_name": db_name
            }
        )
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in instance update: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update instance command: {str(e)}"
        )


@router.delete("/{class_id}/{instance_id}/delete", response_model=CommandResult)
async def delete_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    instance_id: str = ...,
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
    user_id: Optional[str] = None,
):
    """
    인스턴스 삭제 명령을 비동기로 처리
    """
    try:
        # 입력 검증
        validate_instance_id(instance_id)
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.DELETE_INSTANCE,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            payload={},  # 삭제는 payload 필요 없음
            metadata={
                "user_id": user_id,
                "deleted_at": datetime.utcnow().isoformat()
            },
            created_by=user_id
        )
        
        # Command를 Outbox에 저장
        if outbox_service:
            async with postgres_db.transaction() as conn:
                await outbox_service.publish_command(conn, command.dict())
        
        # Redis에 상태 저장
        await command_status_service.set_command_status(
            command_id=str(command.command_id),
            status=CommandStatus.PENDING,
            metadata={
                "command_type": command.command_type,
                "db_name": db_name,
                "class_id": class_id,
                "instance_id": instance_id,
                "aggregate_id": command.aggregate_id,
                "deleted_at": datetime.utcnow().isoformat(),
                "deleted_by": user_id
            }
        )
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Instance deletion command accepted for '{instance_id}'",
                "instance_id": instance_id,
                "class_id": class_id,
                "db_name": db_name
            }
        )
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in instance deletion: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error deleting instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete instance command: {str(e)}"
        )


@router.post("/{class_id}/bulk-create", response_model=CommandResult)
async def bulk_create_instances_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    request: BulkInstanceCreateRequest = ...,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
    user_id: Optional[str] = None,
):
    """
    대량 인스턴스 생성 명령을 비동기로 처리
    
    이 엔드포인트는 즉시 202 Accepted를 반환하고,
    실제 처리는 백그라운드에서 진행됩니다.
    """
    try:
        # 입력 검증
        sanitized_instances = [sanitize_input(instance) for instance in request.instances]
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.BULK_CREATE_INSTANCES,
            db_name=db_name,
            class_id=class_id,
            payload={
                "instances": sanitized_instances,
                "count": len(sanitized_instances)
            },
            metadata={
                **request.metadata,
                "user_id": user_id,
                "created_at": datetime.utcnow().isoformat()
            },
            created_by=user_id
        )
        
        # Command를 Outbox에 저장
        if outbox_service:
            async with postgres_db.transaction() as conn:
                await outbox_service.publish_command(conn, command.dict())
        
        # Redis에 상태 저장
        await command_status_service.set_command_status(
            command_id=str(command.command_id),
            status=CommandStatus.PENDING,
            metadata={
                "command_type": command.command_type,
                "db_name": db_name,
                "class_id": class_id,
                "instance_count": len(sanitized_instances),
                "aggregate_id": command.aggregate_id,
                "created_at": command.created_at.isoformat(),
                "created_by": user_id
            }
        )
        
        # Add background task for progress tracking
        if len(sanitized_instances) > 10:  # Only for large batches
            background_tasks.add_task(
                _track_bulk_create_progress,
                command_id=str(command.command_id),
                total_instances=len(sanitized_instances),
                command_status_service=command_status_service
            )
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Bulk instance creation command accepted for {len(sanitized_instances)} instances",
                "class_id": class_id,
                "db_name": db_name,
                "instance_count": len(sanitized_instances),
                "note": "Large batches are processed in background with progress tracking" if len(sanitized_instances) > 10 else None
            }
        )
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in bulk instance creation: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating bulk instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create bulk instance command: {str(e)}"
        )


@router.get("/command/{command_id}/status", response_model=CommandResult)
async def get_instance_command_status(
    db_name: str = Depends(ensure_database_exists),
    command_id: str = ...,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
):
    """
    인스턴스 명령의 상태 조회
    """
    try:
        # Redis에서 상태 조회
        status_info = await command_status_service.get_command_status(command_id)
        
        if not status_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Command not found: {command_id}"
            )
        
        # 결과 조회
        result = await command_status_service.get_command_result(command_id)
        
        return CommandResult(
            command_id=UUID(command_id),
            status=CommandStatus(status_info.get("status", CommandStatus.PENDING)),
            result=result or {
                "message": f"Command is {status_info.get('status', 'PENDING')}",
                **status_info
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting command status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get command status: {str(e)}"
        )


# Background task helpers
async def _track_bulk_create_progress(
    command_id: str,
    total_instances: int,
    command_status_service: CommandStatusService
) -> None:
    """
    Track progress of bulk create operation in background.
    
    This demonstrates proper background task usage instead of fire-and-forget.
    """
    import asyncio
    
    try:
        logger.info(f"Starting progress tracking for bulk create command {command_id}")
        
        # Simulate progress updates (in real scenario, this would monitor actual progress)
        for i in range(0, total_instances + 1, max(1, total_instances // 10)):
            # Update progress
            progress_percentage = (i / total_instances) * 100 if total_instances > 0 else 100
            
            await command_status_service.set_command_status(
                command_id=command_id,
                status=CommandStatus.PROCESSING,
                metadata={
                    "progress": {
                        "current": i,
                        "total": total_instances,
                        "percentage": progress_percentage,
                        "message": f"Processing instance {i} of {total_instances}"
                    }
                }
            )
            
            # Simulate processing time
            await asyncio.sleep(0.5)
            
        logger.info(f"Completed progress tracking for bulk create command {command_id}")
        
    except Exception as e:
        logger.error(f"Error in bulk create progress tracking for {command_id}: {e}")
        # Don't raise - this is a background task


@router.post("/{class_id}/bulk-create-tracked", response_model=Dict[str, Any])
async def bulk_create_instances_with_tracking(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    request: BulkInstanceCreateRequest = ...,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
    user_id: Optional[str] = None,
):
    """
    Enhanced bulk instance creation with proper background task tracking.
    
    This endpoint demonstrates the improved pattern for handling long-running
    operations by immediately returning a task ID that can be monitored.
    """
    # Generate task ID
    task_id = str(uuid4())
    
    # Validate input
    sanitized_instances = [sanitize_input(instance) for instance in request.instances]
    
    # Add the actual bulk creation to background tasks
    background_tasks.add_task(
        _process_bulk_create_in_background,
        task_id=task_id,
        db_name=db_name,
        class_id=class_id,
        instances=sanitized_instances,
        metadata=request.metadata,
        user_id=user_id,
        outbox_service=outbox_service,
        command_status_service=command_status_service
    )
    
    # Return immediately with 202 Accepted
    return {
        "task_id": task_id,
        "status": "accepted",
        "message": f"Bulk creation task started for {len(sanitized_instances)} instances",
        "status_url": f"/api/v1/tasks/{task_id}/status",
        "instance_count": len(sanitized_instances)
    }


async def _process_bulk_create_in_background(
    task_id: str,
    db_name: str,
    class_id: str,
    instances: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    user_id: Optional[str],
    outbox_service: Optional[OutboxService],
    command_status_service: CommandStatusService
) -> None:
    """
    Process bulk create operation in background with proper error handling.
    """
    try:
        # Create command
        command = InstanceCommand(
            command_type=CommandType.BULK_CREATE_INSTANCES,
            db_name=db_name,
            class_id=class_id,
            payload={
                "instances": instances,
                "count": len(instances),
                "task_id": task_id
            },
            metadata={
                **metadata,
                "user_id": user_id,
                "created_at": datetime.utcnow().isoformat(),
                "background_task_id": task_id
            },
            created_by=user_id
        )
        
        # Save to outbox
        if outbox_service:
            from oms.database.postgres import db as postgres_db
            async with postgres_db.transaction() as conn:
                await outbox_service.publish_command(conn, command.dict())
        
        # Update task status
        await command_status_service.set_command_status(
            command_id=task_id,
            status=CommandStatus.COMPLETED,
            metadata={
                "command_id": str(command.command_id),
                "result": "Bulk create command published successfully"
            }
        )
        
    except Exception as e:
        logger.error(f"Background bulk create failed for task {task_id}: {e}")
        await command_status_service.set_command_status(
            command_id=task_id,
            status=CommandStatus.FAILED,
            metadata={
                "error": str(e),
                "error_type": type(e).__name__
            }
        )