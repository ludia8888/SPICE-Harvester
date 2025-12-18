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

from oms.dependencies import CommandStatusServiceDep
from shared.models.commands import CommandResult, CommandStatus
from shared.services.command_status_service import CommandStatusService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/commands", tags=["Command Status"])


@router.get("/{command_id}/status", response_model=CommandResult)
async def get_command_status(
    command_id: str,
    command_status_service: Optional[CommandStatusService] = CommandStatusServiceDep,
):
    """
    Get command execution status/result.

    This endpoint is the user-facing observability contract for async (202 Accepted) flows.
    """
    if not command_status_service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Command status tracking is disabled (Redis unavailable)",
        )

    try:
        command_uuid = UUID(command_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid command_id (must be UUID)",
        )

    try:
        status_info = await command_status_service.get_command_status(command_id)
        if not status_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Command not found: {command_id}",
            )

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
            result=result
            or {
                "message": f"Command is {raw_status}",
                **status_info,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting command status for {command_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get command status: {str(e)}",
        )
