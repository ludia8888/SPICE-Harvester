"""
Command status router (read-only).

Provides a generic way to query asynchronous command execution results regardless of command type.
Backed by Redis CommandStatusService.
"""

from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from oms.dependencies import CommandStatusServiceDep, EventStoreDep, ProcessedEventRegistryDep
from shared.models.commands import CommandResult, CommandStatus
from shared.services.command_status_service import CommandStatusService
from shared.services.processed_event_registry import ProcessedEventRegistry
from oms.services.event_store import EventStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/commands", tags=["Command Status"])


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
) -> tuple[Optional[CommandResult], bool]:
    if not registry:
        return None, False

    try:
        record = await registry.get_event_record(event_id=str(command_uuid))
    except Exception as e:
        logger.warning(f"ProcessedEventRegistry lookup failed for {command_uuid}: {e}")
        return None, False

    if not record:
        return None, True

    status_value = str(record.get("status") or "")
    parsed_status = _map_registry_status(status_value)
    error = record.get("last_error")
    if status_value == "skipped_stale" and not error:
        error = "stale_event"

    return (
        CommandResult(
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
        ),
        True,
    )


@router.get("/{command_id}/status", response_model=CommandResult)
async def get_command_status(
    command_id: str,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
    processed_event_registry: Optional[ProcessedEventRegistry] = ProcessedEventRegistryDep,
    event_store: EventStore = EventStoreDep,
):
    """
    Get command execution status/result.

    This endpoint is the user-facing observability contract for async (202 Accepted) flows.
    """
    try:
        command_uuid = UUID(command_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid command_id (must be UUID)",
        )

    try:
        status_info = None
        status_available = False
        if command_status_service:
            try:
                status_info = await command_status_service.get_command_status(command_id)
                status_available = True
            except Exception as e:
                logger.warning(f"CommandStatusService failed, falling back (command_id={command_id}): {e}")
                status_info = None
                command_status_service = None

            if status_info:
                result = None
                try:
                    result = await command_status_service.get_command_result(command_id)
                except Exception as e:
                    logger.warning(
                        f"CommandStatusService result lookup failed, returning status only "
                        f"(command_id={command_id}): {e}"
                    )

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
                    result=result
                    or {
                        "message": f"Command is {raw_status}",
                        **status_info,
                    },
                )

        fallback, registry_available = await _fallback_from_registry(
            command_uuid=command_uuid,
            registry=processed_event_registry,
        )
        if fallback:
            return fallback

        if not status_available and not registry_available:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Command status tracking is unavailable (Redis/Postgres unavailable)",
            )

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

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Command not found: {command_id}",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting command status for {command_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get command status: {str(e)}",
        )
