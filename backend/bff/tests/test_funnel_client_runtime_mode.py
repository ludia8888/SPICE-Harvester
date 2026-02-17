import pytest

from bff.services.funnel_client import FunnelClient


@pytest.mark.asyncio
async def test_funnel_client_internal_mode_uses_inprocess_asgi():
    async with FunnelClient() as client:
        assert client.runtime_mode == "internal"

        result = await client.analyze_dataset(
            {
                "data": [["1"], ["2"], ["3"]],
                "columns": ["id"],
                "sample_size": 3,
                "include_complex_types": False,
            }
        )
        assert isinstance(result.get("columns"), list)
        assert result["columns"][0]["column_name"] == "id"
