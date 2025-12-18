import pytest

from bff.routers.command_status import get_command_status


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


@pytest.mark.asyncio
async def test_command_status_proxies_to_api_v1_commands_status_path():
    oms = DummyOMSClient()
    await get_command_status(command_id="00000000-0000-0000-0000-000000000001", oms=oms)
    assert oms.last_path == "/api/v1/commands/00000000-0000-0000-0000-000000000001/status"
