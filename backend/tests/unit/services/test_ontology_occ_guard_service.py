from __future__ import annotations

import pytest
from fastapi import HTTPException
import httpx

from bff.services.ontology_occ_guard_service import (
    fetch_branch_head_commit_id,
    resolve_branch_head_commit_with_bootstrap,
    resolve_expected_head_commit,
)


class _FakeOMSClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0
        self.last_db_name = None
        self.last_branch = None

    async def get_version_head(self, db_name: str, *, branch: str = "main"):  # noqa: ANN201
        self.calls += 1
        self.last_db_name = db_name
        self.last_branch = branch
        return self.payload


class _BootstrapOMSClient:
    def __init__(self, *, heads, create_branch_status: int = 200):
        self._heads = list(heads)
        self.create_branch_status = int(create_branch_status)
        self.get_head_calls = 0
        self.create_branch_calls = 0

    async def get_version_head(self, db_name: str, *, branch: str = "main"):  # noqa: ANN201
        _ = db_name, branch
        self.get_head_calls += 1
        if self._heads:
            item = self._heads.pop(0)
        else:
            item = {"data": {}}
        if isinstance(item, Exception):
            raise item
        return item

    async def create_branch(self, db_name: str, branch_data: dict[str, str]):  # noqa: ANN201
        _ = db_name, branch_data
        self.create_branch_calls += 1
        if self.create_branch_status in {200, 201, 202}:
            return {"status": "success"}
        if self.create_branch_status == 409:
            response = httpx.Response(409, request=httpx.Request("POST", "http://oms.local/branch"))
            raise httpx.HTTPStatusError("conflict", request=response.request, response=response)
        response = httpx.Response(self.create_branch_status, request=httpx.Request("POST", "http://oms.local/branch"))
        raise httpx.HTTPStatusError("create branch failed", request=response.request, response=response)


def _http_404() -> httpx.HTTPStatusError:
    response = httpx.Response(404, request=httpx.Request("GET", "http://oms.local/head"))
    return httpx.HTTPStatusError("not found", request=response.request, response=response)


@pytest.mark.asyncio
async def test_resolve_expected_head_commit_uses_given_value_without_calling_oms() -> None:
    oms_client = _FakeOMSClient(payload={"data": {"head_commit_id": "c-main"}})

    resolved = await resolve_expected_head_commit(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
        expected_head_commit="  c-fixed  ",
    )

    assert resolved == "c-fixed"
    assert oms_client.calls == 0


@pytest.mark.asyncio
async def test_fetch_branch_head_commit_id_extracts_from_head_payload() -> None:
    oms_client = _FakeOMSClient(payload={"data": {"head_commit_id": "c-123"}})

    resolved = await fetch_branch_head_commit_id(
        oms_client=oms_client,
        db_name="test_db",
        branch="feature",
    )

    assert resolved == "c-123"
    assert oms_client.calls == 1
    assert oms_client.last_db_name == "test_db"
    assert oms_client.last_branch == "feature"


@pytest.mark.asyncio
async def test_resolve_expected_head_commit_falls_back_to_commit_keys() -> None:
    oms_client = _FakeOMSClient(payload={"data": {"commit": "c-commit"}})
    resolved = await resolve_expected_head_commit(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
        expected_head_commit=None,
    )
    assert resolved == "c-commit"

    oms_client = _FakeOMSClient(payload={"data": {"head_commit": "c-head"}})
    resolved = await resolve_expected_head_commit(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
        expected_head_commit=None,
    )
    assert resolved == "c-head"


@pytest.mark.asyncio
async def test_resolve_expected_head_commit_raises_when_unresolved() -> None:
    oms_client = _FakeOMSClient(payload={"data": {}})

    with pytest.raises(HTTPException) as exc_info:
        await resolve_expected_head_commit(
            oms_client=oms_client,
            db_name="test_db",
            branch="main",
            expected_head_commit=None,
        )

    assert exc_info.value.status_code == 409
    assert "expected_head_commit is required" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_resolve_expected_head_commit_allows_none_when_configured() -> None:
    oms_client = _FakeOMSClient(payload={"data": {}})

    resolved = await resolve_expected_head_commit(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
        expected_head_commit=None,
        allow_none=True,
    )

    assert resolved is None


@pytest.mark.asyncio
async def test_resolve_branch_head_commit_with_bootstrap_retries_after_branch_create() -> None:
    oms_client = _BootstrapOMSClient(
        heads=[
            _http_404(),
            {"status": "success", "data": {"head_commit_id": "c-boot"}},
        ],
    )

    resolved = await resolve_branch_head_commit_with_bootstrap(
        oms_client=oms_client,
        db_name="test_db",
        branch="feature-x",
        initial_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
    )

    assert resolved == "c-boot"
    assert oms_client.create_branch_calls == 1
    assert oms_client.get_head_calls == 2


@pytest.mark.asyncio
async def test_resolve_branch_head_commit_with_bootstrap_accepts_branch_conflict() -> None:
    oms_client = _BootstrapOMSClient(
        heads=[
            _http_404(),
            {"status": "success", "data": {"head_commit_id": "c-after-conflict"}},
        ],
        create_branch_status=409,
    )

    resolved = await resolve_branch_head_commit_with_bootstrap(
        oms_client=oms_client,
        db_name="test_db",
        branch="feature-y",
        initial_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
    )

    assert resolved == "c-after-conflict"
    assert oms_client.create_branch_calls == 1


@pytest.mark.asyncio
async def test_resolve_branch_head_commit_with_bootstrap_returns_none_when_unresolved() -> None:
    oms_client = _BootstrapOMSClient(
        heads=[
            _http_404(),
            {"status": "success", "data": {}},
            {"status": "success", "data": {}},
        ],
    )

    resolved = await resolve_branch_head_commit_with_bootstrap(
        oms_client=oms_client,
        db_name="test_db",
        branch="feature-z",
        max_attempts=2,
        initial_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
    )

    assert resolved is None
    assert oms_client.create_branch_calls == 1
