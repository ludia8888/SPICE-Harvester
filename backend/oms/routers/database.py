"""
데이터베이스 관리 라우터
데이터베이스 생성, 삭제, 목록 조회 등을 담당
Event Sourcing: S3/MinIO Event Store(SSoT)에 Command 이벤트를 저장
"""

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse

from oms.dependencies import TerminusServiceDep, EventStoreDep, CommandStatusServiceDep
from oms.services.async_terminus import AsyncTerminusService
from shared.models.requests import ApiResponse
from shared.models.commands import DatabaseCommand, CommandType, CommandStatus
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input, validate_db_name
from shared.config.app_config import AppConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.aggregate_sequence_allocator import OptimisticConcurrencyError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database", tags=["Database Management"])


@router.get("/list")
async def list_databases(terminus_service: AsyncTerminusService = TerminusServiceDep):
    """
    데이터베이스 목록 조회

    모든 데이터베이스의 이름 목록을 반환합니다.
    """
    try:
        databases = await terminus_service.list_databases()
        return ApiResponse.success(
            message=f"데이터베이스 목록 조회 완료 ({len(databases)}개)",
            data={"databases": databases}
        ).to_dict()
    except Exception as e:
        logger.error(f"Failed to list databases: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"데이터베이스 목록 조회 실패: {str(e)}",
        )


@router.post("/create")
async def create_database(
    request: dict, 
    event_store=EventStoreDep,
    command_status_service=CommandStatusServiceDep,
    terminus_service: AsyncTerminusService = TerminusServiceDep,
):
    """
    새 데이터베이스 생성

    지정된 이름으로 새 데이터베이스를 생성하고 Event Sourcing을 위한 Command를 발행합니다.
    """
    try:
        # 입력 데이터 보안 검증 및 정화
        sanitized_request = sanitize_input(request)

        # 요청 데이터 검증
        db_name = sanitized_request.get("name")
        if not db_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="데이터베이스 이름이 필요합니다"
            )

        # 데이터베이스 이름 보안 검증
        db_name = validate_db_name(db_name)

        # 설명 정화
        description = sanitized_request.get("description")
        if description and len(description) > 500:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="설명이 너무 깁니다 (500자 이하)"
            )

        command = DatabaseCommand(
            command_type=CommandType.CREATE_DATABASE,
            aggregate_id=db_name,
            expected_seq=0,
            payload={"database_name": db_name, "description": description},
            metadata={"source": "OMS", "user": "system"},
        )

        envelope = EventEnvelope.from_command(
            command,
            actor="system",
            kafka_topic=AppConfig.DATABASE_COMMANDS_TOPIC,
            metadata={"service": "oms", "mode": "event_sourcing"},
        )
        try:
            await event_store.append_event(envelope)
        except OptimisticConcurrencyError as e:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "optimistic_concurrency_conflict",
                    "aggregate_id": e.aggregate_id,
                    "expected_seq": e.expected_last_sequence,
                    "actual_seq": e.actual_last_sequence,
                },
            )

        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=str(command.command_id),
                    status=CommandStatus.PENDING,
                    metadata={
                        "command_type": command.command_type,
                        "aggregate_id": command.aggregate_id,
                        "db_name": db_name,
                        "created_at": command.created_at.isoformat(),
                        "created_by": command.created_by or "system",
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to persist command status (continuing without Redis): {e}")

        # Best-effort synchronous fallback to keep E2E flows unblocked even if workers lag.
        sync_fallback_enabled = os.getenv("ENABLE_SYNC_DATABASE_CREATE_FALLBACK", "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if sync_fallback_enabled:
            try:
                created = await terminus_service.create_database(db_name, description or "")
                if not created:
                    exists = await terminus_service.database_exists(db_name)
                    if not exists:
                        raise RuntimeError(f"Database '{db_name}' did not materialize")
                if command_status_service:
                    try:
                        await command_status_service.start_processing(command_id=str(command.command_id))
                    except Exception:
                        # Ignore best-effort start update
                        pass
                    await command_status_service.complete_command(
                        command_id=str(command.command_id),
                        result={
                            "db_name": db_name,
                            "message": f"Successfully created database: {db_name} (sync fallback)",
                        },
                    )
            except Exception as e:
                logger.warning(f"Sync database create fallback failed (non-fatal): {e}")

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=ApiResponse.accepted(
                message=f"데이터베이스 '{db_name}' 생성 명령이 접수되었습니다",
                data={
                    "command_id": str(command.command_id),
                    "database_name": db_name,
                    "status": "processing",
                    "mode": "event_sourcing",
                },
            ).to_dict(),
        )
    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_database: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        # 변수를 안전하게 처리
        db_name_for_error = "unknown"
        try:
            db_name_for_error = validate_db_name(request.get("name", "unknown"))
        except (ValueError, KeyError):
            # 검증 실패시 기본값 사용
            pass
        except Exception as validation_error:
            logger.debug(f"Error validating db_name for error message: {validation_error}")

        logger.error(f"Failed to create database '{db_name_for_error}': {e}")

        # Check for duplicate database error
        if (
            "already exists" in str(e).lower()
            or "이미 존재합니다" in str(e)
            or "duplicate" in str(e).lower()
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail="데이터베이스가 이미 존재합니다"
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"데이터베이스 생성 실패: {str(e)}",
        )


@router.delete("/{db_name}")
async def delete_database(
    db_name: str,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    event_store=EventStoreDep,
    command_status_service=CommandStatusServiceDep,
):
    """
    데이터베이스 삭제

    지정된 데이터베이스를 삭제하고 Event Sourcing을 위한 Command를 발행합니다.
    주의: 이 작업은 되돌릴 수 없습니다!
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 시스템 데이터베이스 보호
        protected_dbs = ["_system", "_meta"]
        if db_name in protected_dbs:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"시스템 데이터베이스 '{db_name}'은(는) 삭제할 수 없습니다",
            )

        # Event Sourcing mode: emit command only (async). Existence is checked by workers.
        command = DatabaseCommand(
            command_type=CommandType.DELETE_DATABASE,
            aggregate_id=db_name,
            expected_seq=expected_seq,
            payload={"database_name": db_name},
            metadata={"source": "OMS", "user": "system"},
        )

        envelope = EventEnvelope.from_command(
            command,
            actor="system",
            kafka_topic=AppConfig.DATABASE_COMMANDS_TOPIC,
            metadata={"service": "oms", "mode": "event_sourcing"},
        )
        try:
            await event_store.append_event(envelope)
        except OptimisticConcurrencyError as e:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "optimistic_concurrency_conflict",
                    "aggregate_id": e.aggregate_id,
                    "expected_seq": e.expected_last_sequence,
                    "actual_seq": e.actual_last_sequence,
                },
            )

        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=str(command.command_id),
                    status=CommandStatus.PENDING,
                    metadata={
                        "command_type": command.command_type,
                        "aggregate_id": command.aggregate_id,
                        "db_name": db_name,
                        "created_at": command.created_at.isoformat(),
                        "created_by": command.created_by or "system",
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to persist command status (continuing without Redis): {e}")

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=ApiResponse.accepted(
                message=f"데이터베이스 '{db_name}' 삭제 명령이 접수되었습니다",
                data={
                    "command_id": str(command.command_id),
                    "database_name": db_name,
                    "status": "processing",
                    "mode": "event_sourcing",
                },
            ).to_dict(),
        )
    except SecurityViolationError as e:
        logger.warning(f"Security violation in delete_database: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete database '{db_name}': {e}")

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"데이터베이스 삭제 실패: {str(e)}",
        )


@router.get("/exists/{db_name}")
async def database_exists(
    db_name: str, terminus_service: AsyncTerminusService = TerminusServiceDep
):
    """
    데이터베이스 존재 여부 확인

    지정된 데이터베이스가 존재하는지 확인합니다.
    항상 200 상태코드와 함께 exists 필드로 결과를 반환합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        exists = await terminus_service.database_exists(db_name)
        
        return ApiResponse.success(
            message=f"데이터베이스 '{db_name}' 존재 여부: {exists}",
            data={"exists": exists}
        ).to_dict()
    except SecurityViolationError as e:
        logger.warning(f"Security violation in database_exists: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        logger.error(f"Failed to check database existence for '{db_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"데이터베이스 존재 확인 실패: {str(e)}",
        )
