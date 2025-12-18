"""
Command status router (BFF).

Frontend should be able to observe async (202 Accepted) command execution without
calling internal services directly. This router proxies OMS' command status API.
"""

from __future__ import annotations

import logging
from uuid import UUID

import httpx
from fastapi import APIRouter, HTTPException, status

from bff.dependencies import OMSClientDep
from bff.services.oms_client import OMSClient
from shared.models.commands import CommandResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/commands", tags=["Command Status"])


@router.get("/{command_id}/status", response_model=CommandResult)
async def get_command_status(command_id: str, oms: OMSClient = OMSClientDep) -> CommandResult:
    """
    Proxy OMS: `GET /api/v1/commands/{command_id}/status`.

    This is part of the async observability contract for all write-side commands.
    """
    try:
        UUID(command_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid command_id (must be UUID)",
        )

    try:
        payload = await oms.get(f"/api/v1/commands/{command_id}/status")
        return CommandResult(**payload)
    except httpx.HTTPStatusError as e:
        resp = getattr(e, "response", None)
        if resp is not None:
            detail = None
            try:
                detail = resp.json().get("detail")
            except Exception:
                detail = resp.text
            raise HTTPException(status_code=resp.status_code, detail=detail) from e
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="OMS command status 조회 실패",
        ) from e
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="OMS command status 조회 실패",
        ) from e
    except Exception as e:
        logger.error(f"Error proxying command status ({command_id}): {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Command status 조회 실패",
        ) from e
