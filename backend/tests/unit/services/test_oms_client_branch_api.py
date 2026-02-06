from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest

from bff.services.oms_client import OMSClient


class _FakeResponse:
    def __init__(self, payload: Dict[str, Any] | None = None) -> None:
        self._payload = payload or {"status": "success"}
        self.text = "ok"

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return dict(self._payload)


class _FakeAsyncClient:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, str, Dict[str, Any]]] = []

    async def get(self, path: str, **kwargs) -> _FakeResponse:
        self.calls.append(("get", path, kwargs))
        return _FakeResponse({"method": "get", "path": path, "kwargs": kwargs})

    async def post(self, path: str, **kwargs) -> _FakeResponse:
        self.calls.append(("post", path, kwargs))
        return _FakeResponse({"method": "post", "path": path, "kwargs": kwargs})

    async def delete(self, path: str, **kwargs) -> _FakeResponse:
        self.calls.append(("delete", path, kwargs))
        return _FakeResponse({"method": "delete", "path": path, "kwargs": kwargs})


def _build_client() -> OMSClient:
    client = OMSClient.__new__(OMSClient)
    client.client = _FakeAsyncClient()
    client.base_url = "http://oms.local"
    return client


@pytest.mark.asyncio
async def test_create_branch_accepts_string_input() -> None:
    client = _build_client()

    response = await client.create_branch("db1", "feature-a", from_branch="main")

    assert response["path"] == "/api/v1/branch/db1/create"
    assert response["kwargs"]["json"] == {"branch_name": "feature-a", "from_branch": "main"}


@pytest.mark.asyncio
async def test_create_branch_accepts_dict_input_for_backward_compatibility() -> None:
    client = _build_client()
    payload = {"branch_name": "feature-b", "from_branch": "dev"}

    response = await client.create_branch("db1", payload)

    assert response["kwargs"]["json"] == payload


@pytest.mark.asyncio
async def test_get_and_delete_branch_use_typed_paths() -> None:
    client = _build_client()

    info = await client.get_branch_info("db1", "feature-c")
    deleted = await client.delete_branch("db1", "feature-c", force=True)

    assert info["path"] == "/api/v1/branch/db1/branch/feature-c/info"
    assert deleted["path"] == "/api/v1/branch/db1/branch/feature-c"
    assert deleted["kwargs"]["params"] == {"force": True}
