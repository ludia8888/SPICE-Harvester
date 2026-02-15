from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from bff.dependencies import TerminusService


@pytest.mark.asyncio
async def test_get_database_info_uses_oms_get_database() -> None:
    oms = AsyncMock()
    oms.get_database = AsyncMock(return_value={"status": "success", "data": {"exists": True}})
    service = TerminusService(oms)

    result = await service.get_database_info("demo_db")

    assert result == {"status": "success", "data": {"exists": True}}
    oms.get_database.assert_awaited_once_with("demo_db")
