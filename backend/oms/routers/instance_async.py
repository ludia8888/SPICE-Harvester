"""
OMS 비동기 인스턴스 라우터 - Command Pattern 기반
인스턴스 데이터의 명령을 저장하고 비동기로 처리하는 API
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field

from oms.dependencies import (
    EventStoreDep,  # Added for S3/MinIO Event Store
    CommandStatusServiceDep,
    ProcessedEventRegistryDep,
    ValidatedClassId,
    ensure_database_exists
)
from oms.routers._event_sourcing import append_event_sourcing_command, build_command_status_metadata
from shared.config.app_config import AppConfig
from shared.models.commands import CommandType, InstanceCommand, CommandResult, CommandStatus
from shared.services.core.command_status_service import CommandStatusService
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.utils.ontology_version import resolve_ontology_version
from oms.utils.command_status_utils import map_registry_status
from oms.utils.ontology_stamp import merge_ontology_stamp
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_instance_id,
)
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.observability.tracing import trace_endpoint

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instances/{db_name}/async", tags=["Async Instance Management"])


def _enforce_ingest_only_if_writeback_enabled(
    *,
    class_id: str,
    metadata: Optional[Dict[str, Any]],
) -> None:
    if not AppConfig.WRITEBACK_ENFORCE:
        return
    if not AppConfig.is_writeback_enabled_object_type(class_id):
        return
    kind = ""
    if isinstance(metadata, dict):
        kind = str(metadata.get("kind") or "").strip().lower()
    if kind == "ingest":
        return
    raise classified_http_exception(
        status.HTTP_409_CONFLICT,
        "Direct CRUD instance writes are ingestion-only for writeback-enabled object types. Use Actions.",
        code=ErrorCode.CONFLICT,
        extra={
            "error": "writeback_enforced",
            "class_id": class_id,
            "hint": "Set metadata.kind=ingest for ingestion paths, or submit an Action for operational edits.",
        },
    )


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
    parsed_status = map_registry_status(status_value)
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


def _derive_ordering_key(command: InstanceCommand) -> str:
    branch = validate_branch_name(command.branch or "main")
    if isinstance(command.metadata, dict):
        value = str(command.metadata.get("ordering_key") or "").strip()
        if value:
            return value
    return f"{command.db_name}:{branch}"


async def _publish_instance_command(
    *,
    command: InstanceCommand,
    event_store: Any,
    command_status_service: Optional[CommandStatusService],
    actor: Optional[str],
    status_extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Append an InstanceCommand to the Event Store + best-effort Redis status."""
    ordering_key = _derive_ordering_key(command)

    extra: Dict[str, Any] = {
        "db_name": command.db_name,
        "class_id": command.class_id,
        "branch": command.branch,
    }
    if getattr(command, "instance_id", None):
        extra["instance_id"] = command.instance_id
    if status_extra:
        extra.update(status_extra)

    envelope = await append_event_sourcing_command(
        event_store=event_store,
        command=command,
        actor=actor,
        kafka_topic=AppConfig.INSTANCE_COMMANDS_TOPIC,
        envelope_metadata={"ordering_key": ordering_key},
        command_status_service=command_status_service,
        command_status_metadata=build_command_status_metadata(command=command, extra=extra),
    )
    logger.info(
        "Stored %s command event %s (seq=%s) to Event Store",
        command.command_type,
        envelope.event_id,
        envelope.sequence_number,
    )

    if not command_status_service:
        logger.warning("Command status tracking disabled for command %s", command.command_id)


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


class InstanceDeleteRequest(BaseModel):
    """인스턴스 삭제 요청 (optional body for metadata/ingest marker)."""

    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")


class BulkInstanceCreateRequest(BaseModel):
    """대량 인스턴스 생성 요청"""
    instances: List[Dict[str, Any]] = Field(..., description="인스턴스 데이터 목록")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")


class BulkInstanceUpdateRequest(BaseModel):
    """대량 인스턴스 수정 요청"""
    instances: List[Dict[str, Any]] = Field(..., description="instance_id + data payloads")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")


@router.post("/{class_id}/create", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
@trace_endpoint("oms.instance_async.create")
async def create_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    request: InstanceCreateRequest = ...,
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
        branch = validate_branch_name(branch)
        _enforce_ingest_only_if_writeback_enabled(class_id=class_id, metadata=request.metadata)
        sanitized_data = sanitize_input(request.data)

        # CREATE_INSTANCE는 aggregate_id 정합성을 위해 instance_id를 포함해야 함
        instance_id = _derive_instance_id(class_id, sanitized_data)

        ontology_version = merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await resolve_ontology_version(None, db_name=db_name, branch=branch, logger=logger),
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

        await _publish_instance_command(
            command=command,
            event_store=event_store,
            command_status_service=command_status_service,
            actor=user_id,
        )
        
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
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(e),
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating instance command: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Failed to create instance command: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.put("/{class_id}/{instance_id}/update", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
@trace_endpoint("oms.instance_async.update")
async def update_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    instance_id: str = ...,
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    request: InstanceUpdateRequest = ...,
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
        branch = validate_branch_name(branch)
        _enforce_ingest_only_if_writeback_enabled(class_id=class_id, metadata=request.metadata)
        sanitized_data = sanitize_input(request.data)

        ontology_version = merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await resolve_ontology_version(None, db_name=db_name, branch=branch, logger=logger),
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

        await _publish_instance_command(
            command=command,
            event_store=event_store,
            command_status_service=command_status_service,
            actor=user_id,
            status_extra={
                "updated_at": command.metadata.get("updated_at") if isinstance(command.metadata, dict) else None,
                "updated_by": user_id,
            },
        )
        
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
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(e),
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating instance command: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Failed to update instance command: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.delete("/{class_id}/{instance_id}/delete", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
@trace_endpoint("oms.instance_async.delete")
async def delete_instance_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    instance_id: str = ...,
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    request: Optional[InstanceDeleteRequest] = None,
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
        _enforce_ingest_only_if_writeback_enabled(
            class_id=class_id,
            metadata=(request.metadata if request else None),
        )

        ontology_version = await resolve_ontology_version(None, db_name=db_name, branch=branch, logger=logger)
        
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
                **(((request.metadata or {}) if request else {}) or {}),
                "user_id": user_id,
                "deleted_at": datetime.now(timezone.utc).isoformat(),
                "ontology": ontology_version,
            },
            created_by=user_id
        )

        await _publish_instance_command(
            command=command,
            event_store=event_store,
            command_status_service=command_status_service,
            actor=user_id,
            status_extra={
                "deleted_at": command.metadata.get("deleted_at") if isinstance(command.metadata, dict) else None,
                "deleted_by": user_id,
            },
        )
        
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
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(e),
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting instance command: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Failed to delete instance command: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/{class_id}/bulk-create", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
@trace_endpoint("oms.instance_async.bulk_create")
async def bulk_create_instances_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    request: BulkInstanceCreateRequest = ...,
    background_tasks: BackgroundTasks = BackgroundTasks(),
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
        _enforce_ingest_only_if_writeback_enabled(class_id=class_id, metadata=request.metadata)
        sanitized_instances = [sanitize_input(instance) for instance in request.instances]

        ontology_version = merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await resolve_ontology_version(None, db_name=db_name, branch=branch, logger=logger),
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

        await _publish_instance_command(
            command=command,
            event_store=event_store,
            command_status_service=command_status_service,
            actor=user_id,
            status_extra={"instance_count": len(sanitized_instances)},
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
                "branch": branch,
                "instance_count": len(sanitized_instances),
                "note": "Large batches are processed in background with progress tracking" if len(sanitized_instances) > 10 else None
            }
        )
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in bulk instance creation: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            str(e),
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating bulk instance command: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Failed to create bulk instance command: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/{class_id}/bulk-update", response_model=CommandResult, status_code=status.HTTP_202_ACCEPTED)
@trace_endpoint("oms.instance_async.bulk_update")
async def bulk_update_instances_async(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    request: BulkInstanceUpdateRequest = ...,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    event_store=EventStoreDep,
    user_id: Optional[str] = None,
):
    """
    대량 인스턴스 수정 명령을 비동기로 처리
    """
    try:
        branch = validate_branch_name(branch)
        _enforce_ingest_only_if_writeback_enabled(class_id=class_id, metadata=request.metadata)
        sanitized_instances: List[Dict[str, Any]] = []

        for idx, inst in enumerate(request.instances):
            if not isinstance(inst, dict):
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"instances[{idx}] must be object", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            instance_id = inst.get("instance_id")
            if not instance_id:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "instance_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            instance_id = str(instance_id)
            validate_instance_id(instance_id)
            data = inst.get("data")
            if not isinstance(data, dict):
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "data must be object", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            sanitized_instances.append(
                {
                    "instance_id": instance_id,
                    "data": sanitize_input(data),
                }
            )

        ontology_version = merge_ontology_stamp(
            (request.metadata or {}).get("ontology"),
            await resolve_ontology_version(None, db_name=db_name, branch=branch, logger=logger),
        )

        command = InstanceCommand(
            command_type=CommandType.BULK_UPDATE_INSTANCES,
            db_name=db_name,
            class_id=class_id,
            branch=branch,
            payload={
                "instances": sanitized_instances,
                "count": len(sanitized_instances),
            },
            metadata={
                **(request.metadata or {}),
                "user_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "ontology": ontology_version,
            },
            created_by=user_id,
        )

        await _publish_instance_command(
            command=command,
            event_store=event_store,
            command_status_service=command_status_service,
            actor=user_id,
            status_extra={"count": len(sanitized_instances)},
        )

        return CommandResult(
            command_id=command.command_id,
            status=CommandStatus.PENDING,
            result={
                "message": f"Bulk update accepted for {len(sanitized_instances)} instances",
                "count": len(sanitized_instances),
                "db_name": db_name,
                "class_id": class_id,
                "branch": branch,
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating bulk update command: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Failed to create bulk update command: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/command/{command_id}/status", response_model=CommandResult)
@trace_endpoint("oms.instance_async.get_command_status")
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
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Invalid command_id (must be UUID)",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
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
                    logging.getLogger(__name__).warning("Broad exception fallback at oms/routers/instance_async.py:706", exc_info=True)
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
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "Command status tracking is unavailable (Redis/Postgres unavailable)",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
            )

        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"Command not found: {command_id}",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting command status: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Failed to get command status: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
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
        terminal_statuses = {
            CommandStatus.COMPLETED.value,
            CommandStatus.FAILED.value,
            CommandStatus.CANCELLED.value,
        }
        for i in range(0, total_instances + 1, max(1, total_instances // 10)):
            status_info = await command_status_service.get_command_status(command_id)
            if status_info:
                raw_status = status_info.get("status")
                if hasattr(raw_status, "value"):
                    raw_status = raw_status.value
                if str(raw_status).upper() in terminal_statuses:
                    logger.info(
                        "Stopping bulk create progress tracking for %s (status=%s)",
                        command_id,
                        raw_status,
                    )
                    break
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
@trace_endpoint("oms.instance_async.bulk_create_tracked")
async def bulk_create_instances_with_tracking(
    db_name: str = Depends(ensure_database_exists),
    class_id: str = Depends(ValidatedClassId),
    branch: str = Query("main", description="Target branch (default: main)"),
    request: BulkInstanceCreateRequest = ...,
    background_tasks: BackgroundTasks = BackgroundTasks(),
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
    _enforce_ingest_only_if_writeback_enabled(class_id=class_id, metadata=request.metadata)
    sanitized_instances = [sanitize_input(instance) for instance in request.instances]

    ontology_version = merge_ontology_stamp(
        (request.metadata or {}).get("ontology"),
        await resolve_ontology_version(None, db_name=db_name, branch=branch, logger=logger),
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
    command_status_service: Optional[CommandStatusService],
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
        
        await _publish_instance_command(
            command=command,
            event_store=event_store,
            command_status_service=command_status_service,
            actor=user_id,
            status_extra={"task_id": task_id, "instance_count": len(instances)},
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
