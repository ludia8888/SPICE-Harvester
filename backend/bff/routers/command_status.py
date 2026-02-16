"""
Command status router (BFF).

Frontend should be able to observe async (202 Accepted) command execution without
calling internal services directly. This router proxies OMS' command status API.
"""

from shared.observability.tracing import trace_endpoint

import logging
from uuid import UUID

import httpx
from fastapi import APIRouter, status

from bff.dependencies import OMSClientDep
from shared.errors.error_types import ErrorCode, classified_http_exception
from bff.services.oms_client import OMSClient
from shared.models.commands import CommandResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/commands", tags=["Command Status"])


@router.get("/{command_id}/status", response_model=CommandResult)
@trace_endpoint("bff.command_status.get_command_status")
async def get_command_status(command_id: str, oms: OMSClient = OMSClientDep) -> CommandResult:
    """
    Proxy OMS: `GET /api/v1/commands/{command_id}/status`.

    This is part of the async observability contract for all write-side commands.
    """
    try:
        UUID(command_id)
    except Exception:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Invalid command_id (must be UUID)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
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
                logging.getLogger(__name__).warning("Exception fallback at bff/routers/command_status.py:53", exc_info=True)
                detail = resp.text
            raise classified_http_exception(resp.status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from e
        raise classified_http_exception(
            status.HTTP_502_BAD_GATEWAY,
            "OMS command status 조회 실패",
            code=ErrorCode.UPSTREAM_ERROR,
        ) from e
    except httpx.HTTPError as e:
        raise classified_http_exception(
            status.HTTP_502_BAD_GATEWAY,
            "OMS command status 조회 실패",
            code=ErrorCode.UPSTREAM_ERROR,
        ) from e
    except Exception as e:
        logger.error(f"Error proxying command status ({command_id}): {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Command status 조회 실패",
            code=ErrorCode.INTERNAL_ERROR,
        ) from e
