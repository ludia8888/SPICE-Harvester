from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from oms.services.async_terminus import AsyncTerminusService


@pytest.mark.asyncio
async def test_get_branch_info_returns_current_and_protection_flags() -> None:
    service = AsyncTerminusService.__new__(AsyncTerminusService)
    service.list_branches = AsyncMock(return_value=["main", "feature-a"])
    service.get_current_branch = AsyncMock(return_value="feature-a")

    info = await AsyncTerminusService.get_branch_info(service, "db1", "feature-a")

    assert info == {"name": "feature-a", "current": True, "protected": False}


@pytest.mark.asyncio
async def test_get_branch_info_raises_for_unknown_branch() -> None:
    service = AsyncTerminusService.__new__(AsyncTerminusService)
    service.list_branches = AsyncMock(return_value=["main"])
    service.get_current_branch = AsyncMock(return_value="main")

    with pytest.raises(ValueError):
        await AsyncTerminusService.get_branch_info(service, "db1", "feature-x")
