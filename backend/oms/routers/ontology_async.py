"""
OMS 비동기 온톨로지 라우터 - Command Pattern 기반
명령을 저장하고 비동기로 처리하는 새로운 API
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

# Modernized dependency injection imports
from oms.dependencies import (
    get_jsonld_converter, 
    get_label_mapper, 
    get_terminus_service,
    get_outbox_service,
    get_redis_service,
    get_command_status_service,
    TerminusServiceDep,
    JSONLDConverterDep,
    LabelMapperDep,
    OutboxServiceDep,
    CommandStatusServiceDep,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)
from shared.dependencies.providers import RedisServiceDep
from oms.database.postgres import db as postgres_db
from oms.database.outbox import MessageType, OutboxService
from shared.models.commands import CommandType, OntologyCommand, CommandResult, CommandStatus
from shared.models.common import BaseResponse
from shared.services import CommandStatusService, RedisService
from shared.config.app_config import AppConfig

# Import service types for type annotations
from oms.services.async_terminus import AsyncTerminusService
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.utils.label_mapper import LabelMapper
from shared.models.ontology import (
    OntologyCreateRequest,
    OntologyUpdateRequest,
)
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_class_id,
    validate_db_name,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ontology/{db_name}/async", tags=["Async Ontology Management"])


@router.post("/create", response_model=CommandResult)
async def create_ontology_async(
    db_name: str = Depends(ensure_database_exists),
    request: OntologyCreateRequest = ...,
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
) -> CommandResult:
    """
    비동기 온톨로지 생성 - Command를 저장하고 즉시 응답
    실제 생성은 Worker Service가 처리
    """
    if not outbox_service or not postgres_db.pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Async processing is not available. Outbox service not configured."
        )
    
    try:
        # 요청 데이터를 dict로 변환
        ontology_data = request.model_dump()
        
        # 클래스 ID 검증
        class_id = ontology_data.get("id")
        if class_id:
            ontology_data["id"] = validate_class_id(class_id)
        
        # 기본 데이터 타입 검증
        if not ontology_data.get("id"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Ontology ID is required"
            )
        
        # Command 생성
        command = OntologyCommand(
            command_type=CommandType.CREATE_ONTOLOGY_CLASS,
            aggregate_id=ontology_data.get("id"),
            db_name=db_name,
            payload=ontology_data,
            created_by="system",  # TODO: 실제 사용자 정보 추가
            metadata={
                "source": "oms_api",
                "api_version": "v1"
            }
        )
        
        # Command를 Outbox에 저장 (단일 트랜잭션)
        async with postgres_db.transaction() as conn:
            command_id = await outbox_service.publish_command(
                connection=conn,
                command=command,
                topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC
            )
        
        # Redis에 command 상태 저장
        await command_status_service.create_command_status(
            command_id=str(command.command_id),
            command_type=command.command_type,
            aggregate_id=command.aggregate_id,
            payload=command.payload,
            user_id=command.created_by
        )
        
        logger.info(f"Created command {command_id} for ontology creation: {class_id}")
        
        # 즉시 응답 반환
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Ontology creation command accepted for '{class_id}'",
                "class_id": class_id,
                "db_name": db_name
            }
        )
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_ontology_async: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ontology command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )


@router.put("/{class_id}", response_model=CommandResult)
async def update_ontology_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    request: OntologyUpdateRequest = ...,
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
) -> CommandResult:
    """
    비동기 온톨로지 업데이트 - Command를 저장하고 즉시 응답
    """
    if not outbox_service or not postgres_db.pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Async processing is not available. Outbox service not configured."
        )
    
    try:
        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.dict(exclude_unset=True))
        
        # Command 생성
        command = OntologyCommand(
            command_type=CommandType.UPDATE_ONTOLOGY_CLASS,
            aggregate_id=class_id,
            db_name=db_name,
            payload={
                "class_id": class_id,
                "updates": sanitized_data
            },
            created_by="system",  # TODO: 실제 사용자 정보 추가
            metadata={
                "source": "oms_api",
                "api_version": "v1"
            }
        )
        
        # Command를 Outbox에 저장
        async with postgres_db.transaction() as conn:
            command_id = await outbox_service.publish_command(
                connection=conn,
                command=command,
                topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC
            )
        
        # Redis에 command 상태 저장
        await command_status_service.create_command_status(
            command_id=str(command.command_id),
            command_type=command.command_type,
            aggregate_id=command.aggregate_id,
            payload=command.payload,
            user_id=command.created_by
        )
        
        logger.info(f"Created command {command_id} for ontology update: {class_id}")
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Ontology update command accepted for '{class_id}'",
                "class_id": class_id,
                "db_name": db_name
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to create update command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )


@router.delete("/{class_id}", response_model=CommandResult)
async def delete_ontology_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    outbox_service: Optional[OutboxService] = OutboxServiceDep,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
) -> CommandResult:
    """
    비동기 온톨로지 삭제 - Command를 저장하고 즉시 응답
    """
    if not outbox_service or not postgres_db.pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Async processing is not available. Outbox service not configured."
        )
    
    try:
        # Command 생성
        command = OntologyCommand(
            command_type=CommandType.DELETE_ONTOLOGY_CLASS,
            aggregate_id=class_id,
            db_name=db_name,
            payload={
                "class_id": class_id
            },
            created_by="system",  # TODO: 실제 사용자 정보 추가
            metadata={
                "source": "oms_api",
                "api_version": "v1"
            }
        )
        
        # Command를 Outbox에 저장
        async with postgres_db.transaction() as conn:
            command_id = await outbox_service.publish_command(
                connection=conn,
                command=command,
                topic=AppConfig.ONTOLOGY_COMMANDS_TOPIC
            )
        
        # Redis에 command 상태 저장
        await command_status_service.create_command_status(
            command_id=str(command.command_id),
            command_type=command.command_type,
            aggregate_id=command.aggregate_id,
            payload=command.payload,
            user_id=command.created_by
        )
        
        logger.info(f"Created command {command_id} for ontology deletion: {class_id}")
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Ontology deletion command accepted for '{class_id}'",
                "class_id": class_id,
                "db_name": db_name
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to create delete command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )


@router.get("/command/{command_id}/status", response_model=CommandResult)
async def get_command_status(
    command_id: UUID,
    command_status_service: CommandStatusService = CommandStatusServiceDep,
) -> CommandResult:
    """
    Command 실행 상태 조회
    
    Redis에서 command의 현재 상태와 이력을 조회합니다.
    """
    try:
        # Redis에서 command 상태 조회
        command_details = await command_status_service.get_command_details(str(command_id))
        
        if not command_details:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Command {command_id} not found"
            )
        
        # 상태에 따라 응답 구성
        result_data = {
            "message": f"Command is {command_details['status']}",
            "command_type": command_details.get("command_type"),
            "aggregate_id": command_details.get("aggregate_id"),
            "created_at": command_details.get("created_at"),
            "updated_at": command_details.get("updated_at"),
            "progress": command_details.get("progress", 0)
        }
        
        # 이력 정보 추가
        if "history" in command_details:
            result_data["history"] = command_details["history"]
        
        # 완료된 경우 결과 포함
        if command_details["status"] == CommandStatus.COMPLETED and "result" in command_details:
            result_data["result"] = command_details["result"]
        
        # 실패한 경우 에러 정보 포함
        if command_details["status"] == CommandStatus.FAILED and "error" in command_details:
            result_data["error"] = command_details["error"]
        
        return CommandResult(
            command_id=command_id,
            status=command_details["status"],
            result=result_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get command status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve command status: {str(e)}"
        )