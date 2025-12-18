"""
OMS 비동기 인스턴스 라우터 - Command Pattern 기반
인스턴스 데이터의 명령을 저장하고 비동기로 처리하는 API
"""

import logging
from datetime import datetime, timezone
import os
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field

from oms.dependencies import (
    JSONLDConverterDep,
    LabelMapperDep,
    TerminusServiceDep,
    EventStoreDep,  # Added for S3/MinIO Event Store
    CommandStatusServiceDep,
    ProcessedEventRegistryDep,
    ValidatedDatabaseName,
    ValidatedClassId,
    ensure_database_exists
)
from shared.dependencies.providers import RedisServiceDep
from shared.config.app_config import AppConfig
from shared.models.commands import CommandType, InstanceCommand, CommandResult, CommandStatus
from shared.models.common import BaseResponse
from shared.services.command_status_service import CommandStatusService
from shared.services.processed_event_registry import ProcessedEventRegistry
from shared.services.redis_service import RedisService
from shared.models.event_envelope import EventEnvelope
from shared.services.aggregate_sequence_allocator import OptimisticConcurrencyError
from shared.utils.ontology_version import build_ontology_version, normalize_ontology_version
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
    validate_instance_id,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instances/{db_name}/async", tags=["Async Instance Management"])


def _map_registry_status(status_value: str) -> CommandStatus:
    status_value = (status_value or "").lower()
    if status_value == "processing":
        return CommandStatus.PROCESSING
    if status_value == "done":
        return CommandStatus.COMPLETED
    if status_value == "failed":
        return CommandStatus.FAILED
    if status_value == "skipped_stale":
        return CommandStatus.FAILED
    return CommandStatus.PENDING


async def _fallback_from_registry(
    *,
    command_uuid: UUID,
    registry: Optional[ProcessedEventRegistry],
) -> Optional[CommandResult]:
    if not registry:
        return None

    try:
        record = await registry.get_event_record(event_id=str(command_uuid))
    except Exception as e:
        logger.warning(f"ProcessedEventRegistry lookup failed for {command_uuid}: {e}")
        return None

    if not record:
        return None

    status_value = str(record.get("status") or "")
    parsed_status = _map_registry_status(status_value)
    error = record.get("last_error")
    if status_value == "skipped_stale" and not error:
        error = "stale_event"

    return CommandResult(
        command_id=command_uuid,
        status=parsed_status,
        error=error,
        result={
            "message": f"Command status derived from processed_event_registry ({status_value})",
            "source": "processed_event_registry",
            "handler": record.get("handler"),
            "status": status_value,
            "attempt_count": record.get("attempt_count"),
            "started_at": record.get("started_at"),
            "processed_at": record.get("processed_at"),
            "heartbeat_at": record.get("heartbeat_at"),
        },
    )


def _coerce_commit_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for key in ("commit", "commit_id", "identifier", "id", "@id", "head"):
            candidate = value.get(key)
            if candidate:
                return str(candidate).strip() or None
    return str(value).strip() or None


async def _resolve_ontology_version(terminus, *, db_name: str, branch: str) -> Dict[str, str]:
    """
    Best-effort ontology semantic contract stamp (ref + commit).

    Command acceptance should not fail on temporary TerminusDB outages, so missing
    commit is allowed; workers will still stamp domain events.
    """
    commit: Optional[str] = None
    try:
        branches = await terminus.version_control_service.list_branches(db_name)
        for item in branches or []:
            if isinstance(item, dict) and item.get("name") == branch:
                commit = _coerce_commit_id(item.get("head"))
                break
    except Exception as e:
        logger.debug(f"Failed to resolve ontology version (db={db_name}, branch={branch}): {e}")
    return build_ontology_version(branch=branch, commit=commit)


def _merge_ontology_stamp(existing: Any, resolved: Dict[str, str]) -> Dict[str, str]:
    existing_norm = normalize_ontology_version(existing)
    if not existing_norm:
        return dict(resolved)

    merged = dict(existing_norm)
    if "ref" not in merged and resolved.get("ref"):
        merged["ref"] = resolved["ref"]
    if "commit" not in merged and resolved.get("commit"):
        merged["commit"] = resolved["commit"]
    return merged


async def _append_command_event(command: InstanceCommand, *, event_store, topic: str, actor: Optional[str]) -> None:
    """Store command as an immutable event in S3/MinIO for the publisher to relay to Kafka."""
    envelope = EventEnvelope.from_command(
        command,
        actor=actor,
        kafka_topic=topic,
        metadata={"service": "oms", "mode": "event_sourcing"},
    )
    await event_store.append_event(envelope)
    logger.info(f"Stored command event {envelope.event_id} (seq={envelope.sequence_number}) to Event Store")


def _derive_instance_id(class_id: str, payload: Dict[str, Any]) -> str:
    """Derive a stable instance_id for CREATE_INSTANCE so command/event aggregate_id matches."""
    if not isinstance(payload, dict):
        raise SecurityViolationError("Instance payload must be an object")

    expected_key = f"{class_id.lower()}_id"
    candidate = payload.get(expected_key)
    if not candidate:
        for key in sorted(payload.keys()):
            if key.endswith("_id") and payload.get(key):
                candidate = payload.get(key)
                break

    if not candidate:
        candidate = f"{class_id.lower()}_{uuid4().hex[:8]}"

    instance_id = str(candidate)
    validate_instance_id(instance_id)
    return instance_id


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


@router.post("/{class_id}/create", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
async def create_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    request: InstanceCreateRequest = ...,
    terminus=TerminusServiceDep,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    event_store=EventStoreDep,
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
        branch = validate_branch_name(branch)

        # CREATE_INSTANCE는 aggregate_id 정합성을 위해 instance_id를 포함해야 함
        instance_id = _derive_instance_id(class_id, sanitized_data)

        ontology_version = _merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await _resolve_ontology_version(terminus, db_name=db_name, branch=branch),
        )
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.CREATE_INSTANCE,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            branch=branch,
            expected_seq=0,
            payload=sanitized_data,
            metadata={
                **(request.metadata or {}),
                "user_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "ontology": ontology_version,
            },
            created_by=user_id
        )
        
        try:
            await _append_command_event(
                command,
                event_store=event_store,
                topic=AppConfig.INSTANCE_COMMANDS_TOPIC,
                actor=user_id,
            )
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
        
        # Redis에 상태 저장 (if available)
        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=str(command.command_id),
                    status=CommandStatus.PENDING,
                    metadata={
                        "command_type": command.command_type,
                        "db_name": db_name,
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "branch": branch,
                        "aggregate_id": command.aggregate_id,
                        "created_at": command.created_at.isoformat(),
                        "created_by": user_id,
                    },
                )
            except Exception as e:
                logger.warning(
                    f"Failed to persist command status (continuing without Redis): {e}"
                )
        else:
            logger.warning(f"Command status tracking disabled for command {command.command_id}")
        
        # 응답
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Instance creation command accepted for class '{class_id}'",
                "instance_id": instance_id,
                "class_id": class_id,
                "db_name": db_name,
                "branch": branch,
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


@router.put("/{class_id}/{instance_id}/update", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
async def update_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    instance_id: str = ...,
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    request: InstanceUpdateRequest = ...,
    terminus=TerminusServiceDep,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    event_store=EventStoreDep,
    user_id: Optional[str] = None,
):
    """
    인스턴스 수정 명령을 비동기로 처리
    """
    try:
        # 입력 검증
        validate_instance_id(instance_id)
        sanitized_data = sanitize_input(request.data)
        branch = validate_branch_name(branch)

        ontology_version = _merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await _resolve_ontology_version(terminus, db_name=db_name, branch=branch),
        )
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.UPDATE_INSTANCE,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            branch=branch,
            expected_seq=expected_seq,
            payload=sanitized_data,
            metadata={
                **(request.metadata or {}),
                "user_id": user_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "ontology": ontology_version,
            },
            created_by=user_id
        )
        
        try:
            await _append_command_event(
                command,
                event_store=event_store,
                topic=AppConfig.INSTANCE_COMMANDS_TOPIC,
                actor=user_id,
            )
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
        
        # Redis에 상태 저장
        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=str(command.command_id),
                    status=CommandStatus.PENDING,
                    metadata={
                        "command_type": command.command_type,
                        "db_name": db_name,
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "branch": branch,
                        "aggregate_id": command.aggregate_id,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "updated_by": user_id,
                    },
                )
            except Exception as e:
                logger.warning(
                    f"Failed to persist command status (continuing without Redis): {e}"
                )
        else:
            logger.warning(f"Command status tracking disabled for command {command.command_id}")
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Instance update command accepted for '{instance_id}'",
                "instance_id": instance_id,
                "class_id": class_id,
                "db_name": db_name,
                "branch": branch,
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


@router.delete("/{class_id}/{instance_id}/delete", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
async def delete_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    instance_id: str = ...,
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    terminus=TerminusServiceDep,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    event_store=EventStoreDep,
    user_id: Optional[str] = None,
):
    """
    인스턴스 삭제 명령을 비동기로 처리
    """
    try:
        # 입력 검증
        validate_instance_id(instance_id)
        branch = validate_branch_name(branch)

        ontology_version = await _resolve_ontology_version(terminus, db_name=db_name, branch=branch)
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.DELETE_INSTANCE,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            branch=branch,
            expected_seq=expected_seq,
            payload={},  # 삭제는 payload 필요 없음
            metadata={
                "user_id": user_id,
                "deleted_at": datetime.now(timezone.utc).isoformat(),
                "ontology": ontology_version,
            },
            created_by=user_id
        )
        
        try:
            await _append_command_event(
                command,
                event_store=event_store,
                topic=AppConfig.INSTANCE_COMMANDS_TOPIC,
                actor=user_id,
            )
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
        
        # Redis에 상태 저장
        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=str(command.command_id),
                    status=CommandStatus.PENDING,
                    metadata={
                        "command_type": command.command_type,
                        "db_name": db_name,
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "branch": branch,
                        "aggregate_id": command.aggregate_id,
                        "deleted_at": datetime.now(timezone.utc).isoformat(),
                        "deleted_by": user_id,
                    },
                )
            except Exception as e:
                logger.warning(
                    f"Failed to persist command status (continuing without Redis): {e}"
                )
        else:
            logger.warning(f"Command status tracking disabled for command {command.command_id}")
        
        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Instance deletion command accepted for '{instance_id}'",
                "instance_id": instance_id,
                "class_id": class_id,
                "db_name": db_name,
                "branch": branch,
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


@router.post("/{class_id}/bulk-create", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
async def bulk_create_instances_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    request: BulkInstanceCreateRequest = ...,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    terminus=TerminusServiceDep,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    event_store=EventStoreDep,
    user_id: Optional[str] = None,
):
    """
    대량 인스턴스 생성 명령을 비동기로 처리
    
    이 엔드포인트는 즉시 202 Accepted를 반환하고,
    실제 처리는 백그라운드에서 진행됩니다.
    """
    try:
        # 입력 검증
        branch = validate_branch_name(branch)
        sanitized_instances = [sanitize_input(instance) for instance in request.instances]

        ontology_version = _merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await _resolve_ontology_version(terminus, db_name=db_name, branch=branch),
        )
        
        # Command 생성
        command = InstanceCommand(
            command_type=CommandType.BULK_CREATE_INSTANCES,
            db_name=db_name,
            class_id=class_id,
            branch=branch,
            payload={
                "instances": sanitized_instances,
                "count": len(sanitized_instances)
            },
            metadata={
                **(request.metadata or {}),
                "user_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "ontology": ontology_version,
            },
            created_by=user_id
        )
        
        try:
            await _append_command_event(
                command,
                event_store=event_store,
                topic=AppConfig.INSTANCE_COMMANDS_TOPIC,
                actor=user_id,
            )
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
        
        # Redis에 상태 저장
        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=str(command.command_id),
                    status=CommandStatus.PENDING,
                    metadata={
                        "command_type": command.command_type,
                        "db_name": db_name,
                        "class_id": class_id,
                        "branch": branch,
                        "instance_count": len(sanitized_instances),
                        "aggregate_id": command.aggregate_id,
                        "created_at": command.created_at.isoformat(),
                        "created_by": user_id,
                    },
                )
            except Exception as e:
                logger.warning(
                    f"Failed to persist command status (continuing without Redis): {e}"
                )
        else:
            logger.warning(f"Command status tracking disabled for command {command.command_id}")
        
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
                "branch": branch,
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
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    processed_event_registry: Optional[ProcessedEventRegistry] = ProcessedEventRegistryDep,
    event_store=EventStoreDep,
):
    """
    인스턴스 명령의 상태 조회
    """
    try:
        try:
            command_uuid = UUID(command_id)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid command_id (must be UUID)",
            )

        status_info = None
        if command_status_service:
            status_info = await command_status_service.get_command_status(command_id)
            if status_info:
                # 결과 조회
                result = await command_status_service.get_command_result(command_id)

                raw_status = status_info.get("status", CommandStatus.PENDING)
                if hasattr(raw_status, "value"):
                    raw_status = raw_status.value

                try:
                    parsed_status = CommandStatus(raw_status)
                except Exception:
                    parsed_status = CommandStatus.PENDING

                return CommandResult(
                    command_id=command_uuid,
                    status=parsed_status,
                    error=status_info.get("error"),
                    result=result or {
                        "message": f"Command is {raw_status}",
                        **status_info
                    }
                )

        fallback = await _fallback_from_registry(
            command_uuid=command_uuid,
            registry=processed_event_registry,
        )
        if fallback:
            return fallback

        try:
            key = await event_store.get_event_object_key(event_id=str(command_uuid))
            if key:
                return CommandResult(
                    command_id=command_uuid,
                    status=CommandStatus.PENDING,
                    error=None,
                    result={
                        "message": "Command accepted (event store lookup)",
                        "source": "event_store",
                        "event_key": key,
                    },
                )
        except Exception as e:
            logger.warning(f"Event store lookup failed for {command_uuid}: {e}")

        if not command_status_service and not processed_event_registry:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Command status tracking is unavailable (Redis/Postgres unavailable)",
            )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Command not found: {command_id}"
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
        if not command_status_service:
            return
        logger.info(f"Starting progress tracking for bulk create command {command_id}")
        
        # Simulate progress updates (in real scenario, this would monitor actual progress)
        for i in range(0, total_instances + 1, max(1, total_instances // 10)):
            # Update progress
            progress_percentage = (i / total_instances) * 100 if total_instances > 0 else 100
            
            try:
                await command_status_service.set_command_status(
                    command_id=command_id,
                    status=CommandStatus.PROCESSING,
                    metadata={
                        "progress": {
                            "current": i,
                            "total": total_instances,
                            "percentage": progress_percentage,
                            "message": f"Processing instance {i} of {total_instances}",
                        }
                    },
                )
            except Exception as e:
                logger.warning(f"Progress status update failed (continuing): {e}")
            
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
    branch: str = Query("main", description="Target branch (default: main)"),
    request: BulkInstanceCreateRequest = ...,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    terminus=TerminusServiceDep,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    event_store=EventStoreDep,
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
    branch = validate_branch_name(branch)
    sanitized_instances = [sanitize_input(instance) for instance in request.instances]

    ontology_version = _merge_ontology_stamp(
        (request.metadata or {}).get("ontology"),
        await _resolve_ontology_version(terminus, db_name=db_name, branch=branch),
    )
    
    # Add the actual bulk creation to background tasks
    background_tasks.add_task(
        _process_bulk_create_in_background,
        task_id=task_id,
        db_name=db_name,
        class_id=class_id,
        branch=branch,
        instances=sanitized_instances,
        metadata=request.metadata or {},
        user_id=user_id,
        ontology_version=ontology_version,
        event_store=event_store,
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
    branch: str,
    instances: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    user_id: Optional[str],
    ontology_version: Dict[str, str],
    event_store,
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
            branch=branch,
            payload={
                "instances": instances,
                "count": len(instances),
                "task_id": task_id
            },
            metadata={
                **(metadata or {}),
                "user_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "background_task_id": task_id,
                "ontology": ontology_version,
            },
            created_by=user_id
        )
        
        await _append_command_event(
            command,
            event_store=event_store,
            topic=AppConfig.INSTANCE_COMMANDS_TOPIC,
            actor=user_id,
        )
        
        # Update task status (best-effort; Redis may be unavailable)
        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=task_id,
                    status=CommandStatus.COMPLETED,
                    metadata={
                        "command_id": str(command.command_id),
                        "result": "Bulk create command published successfully",
                    },
                )
            except Exception as e:
                logger.warning(f"Task status update failed (continuing): {e}")
        
    except Exception as e:
        logger.error(f"Background bulk create failed for task {task_id}: {e}")
        if command_status_service:
            try:
                await command_status_service.set_command_status(
                    command_id=task_id,
                    status=CommandStatus.FAILED,
                    metadata={
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
            except Exception as status_err:
                logger.warning(f"Task failure status update failed (continuing): {status_err}")
