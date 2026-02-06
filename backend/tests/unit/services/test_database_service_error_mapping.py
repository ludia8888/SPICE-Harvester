from __future__ import annotations

import pytest
from fastapi import HTTPException

from bff.services.database_service import create_branch, get_versions, list_branches


class _FailingOms:
    def __init__(self, message: str) -> None:
        self.message = message

    async def list_branches(self, db_name: str):  # noqa: ANN201
        _ = db_name
        raise RuntimeError(self.message)

    async def create_branch(self, db_name: str, branch_name: str, *, from_branch: str = "main"):  # noqa: ANN201
        _ = db_name, branch_name, from_branch
        raise RuntimeError(self.message)

    async def get_version_history(self, db_name: str):  # noqa: ANN201
        _ = db_name
        raise RuntimeError(self.message)


@pytest.mark.asyncio
async def test_list_branches_maps_not_found_to_404() -> None:
    oms = _FailingOms("database not found")
    with pytest.raises(HTTPException) as raised:
        await list_branches(db_name="db1", oms=oms)
    assert raised.value.status_code == 404


@pytest.mark.asyncio
async def test_create_branch_maps_duplicate_to_409() -> None:
    oms = _FailingOms("branch already exists")
    with pytest.raises(HTTPException) as raised:
        await create_branch(db_name="db1", branch_data={"name": "feature-a"}, oms=oms)
    assert raised.value.status_code == 409


@pytest.mark.asyncio
async def test_get_versions_returns_empty_on_empty_history() -> None:
    oms = _FailingOms("empty history")
    result = await get_versions(db_name="db1", oms=oms)
    assert result == {"versions": [], "count": 0}
