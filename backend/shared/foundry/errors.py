from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi.responses import JSONResponse
from starlette.requests import Request


def foundry_error(
    status: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=int(status),
        content={
            "errorCode": str(error_code),
            "errorName": str(error_name),
            "errorInstanceId": str(uuid4()),
            "parameters": dict(parameters or {}),
        },
    )


@dataclass(frozen=True, slots=True)
class FoundryAPIError(Exception):
    status: int
    error_code: str
    error_name: str
    parameters: Dict[str, Any]


async def foundry_exception_handler(request: Request, exc: FoundryAPIError) -> JSONResponse:  # noqa: ARG001
    return foundry_error(
        exc.status,
        error_code=exc.error_code,
        error_name=exc.error_name,
        parameters=exc.parameters,
    )

