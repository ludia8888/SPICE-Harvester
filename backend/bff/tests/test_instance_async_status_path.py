import pytest

from bff.routers.instance_async import get_instance_command_status


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
async def test_instance_command_status_calls_api_v1_path():
    oms = DummyOMSClient()
    res = await get_instance_command_status(
        db_name="testdb",
        command_id="00000000-0000-0000-0000-000000000001",
        oms_client=oms,
    )

    assert oms.last_path == "/api/v1/instances/testdb/async/command/00000000-0000-0000-0000-000000000001/status"
    assert res.status.value == "PENDING"
