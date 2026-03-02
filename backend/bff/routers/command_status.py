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


def _map_upstream_status_to_error_code(status_code: int) -> ErrorCode:
    if status_code in {400, 422}:
        return ErrorCode.REQUEST_VALIDATION_FAILED
    if status_code == 401:
        return ErrorCode.AUTH_REQUIRED
    if status_code == 403:
        return ErrorCode.PERMISSION_DENIED
    if status_code == 404:
        return ErrorCode.RESOURCE_NOT_FOUND
    if status_code == 409:
        return ErrorCode.CONFLICT
    if status_code in {502, 503, 504}:
        return ErrorCode.UPSTREAM_UNAVAILABLE
    if status_code >= 500:
        return ErrorCode.INTERNAL_ERROR
    return ErrorCode.UPSTREAM_ERROR


@router.get(
    "/{command_id}/status",
    response_model=CommandResult,
    responses={
        400: {"description": "Invalid command_id"},
        404: {"description": "Command not found"},
        503: {"description": "Upstream command status unavailable"},
    },
)
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
            detail: str | dict = ""
            try:
                payload = resp.json()
                if isinstance(payload, dict):
                    detail_obj = payload.get("detail")
                    if isinstance(detail_obj, dict):
                        detail = detail_obj
                    elif detail_obj is not None:
                        detail = str(detail_obj)
                    else:
                        detail = str(payload)
                else:
                    detail = str(payload)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at bff/routers/command_status.py:53", exc_info=True)
                detail = resp.text
            raise classified_http_exception(
                resp.status_code,
                detail if isinstance(detail, dict) else str(detail),
                code=_map_upstream_status_to_error_code(resp.status_code),
            ) from e
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "OMS command status 조회 실패",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        ) from e
    except httpx.HTTPError as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "OMS command status 조회 실패",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        ) from e
    except Exception as e:
        logger.error(f"Error proxying command status ({command_id}): {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Command status 조회 실패",
            code=ErrorCode.INTERNAL_ERROR,
        ) from e
