import pytest
import httpx
from fastapi import HTTPException

from bff.routers.command_status import get_command_status
from shared.errors.error_types import ErrorCode


class DummyOMSClient:
    def __init__(self):
        self.last_path = None

    async def get(self, path: str, **kwargs):
        self.last_path = path
        return {
            "command_id": "00000000-0000-0000-0000-000000000001",
            "status": "PENDING",
            "result": {"message": "ok"},
            "error": None,
            "retry_count": 0,
        }


class ErrorOMSClient:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self.payload = payload

    async def get(self, path: str, **kwargs):
        _ = kwargs
        request = httpx.Request("GET", f"http://oms.local{path}")
        response = httpx.Response(self.status_code, json=self.payload, request=request)
        raise httpx.HTTPStatusError("upstream error", request=request, response=response)


@pytest.mark.asyncio
async def test_command_status_proxies_to_api_v1_commands_status_path():
    oms = DummyOMSClient()
    await get_command_status(command_id="00000000-0000-0000-0000-000000000001", oms=oms)
    assert oms.last_path == "/api/v1/commands/00000000-0000-0000-0000-000000000001/status"


@pytest.mark.asyncio
async def test_command_status_maps_upstream_404_to_resource_not_found():
    oms = ErrorOMSClient(
        status_code=404,
        payload={
            "status": "error",
            "code": "RESOURCE_NOT_FOUND",
            "detail": {
                "message": "Command not found: 00000000-0000-0000-0000-000000000000",
                "code": "RESOURCE_NOT_FOUND",
            },
        },
    )

    with pytest.raises(HTTPException) as exc:
        await get_command_status(command_id="00000000-0000-0000-0000-000000000000", oms=oms)

    assert exc.value.status_code == 404
    assert isinstance(exc.value.detail, dict)
    assert exc.value.detail.get("code") == ErrorCode.RESOURCE_NOT_FOUND.value
