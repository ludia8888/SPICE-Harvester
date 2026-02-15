from __future__ import annotations

import pytest

from bff.services.ontology_occ_guard_service import (
    fetch_branch_head_commit_id,
    resolve_branch_head_commit_with_bootstrap,
    resolve_expected_head_commit,
)


class _UnusedOMSClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get_version_head(self, db_name: str, *, branch: str = "main"):  # noqa: ANN201
        _ = db_name, branch
        self.calls += 1
        return {"status": "success", "data": {"head_commit_id": "legacy-head"}}

    async def create_branch(self, db_name: str, branch_data: dict[str, str]):  # noqa: ANN201
        _ = db_name, branch_data
        self.calls += 1
        return {"status": "success"}


@pytest.mark.asyncio
async def test_resolve_expected_head_commit_prefers_explicit_value() -> None:
    oms_client = _UnusedOMSClient()

    resolved = await resolve_expected_head_commit(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
        expected_head_commit="  commit-123  ",
    )

    assert resolved == "commit-123"
    assert oms_client.calls == 0


@pytest.mark.asyncio
async def test_resolve_expected_head_commit_returns_none_without_legacy_lookup() -> None:
    oms_client = _UnusedOMSClient()

    resolved = await resolve_expected_head_commit(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
        expected_head_commit=None,
    )

    assert resolved is None
    assert oms_client.calls == 0


@pytest.mark.asyncio
async def test_fetch_branch_head_commit_id_is_disabled_in_foundry_profile() -> None:
    oms_client = _UnusedOMSClient()

    resolved = await fetch_branch_head_commit_id(
        oms_client=oms_client,
        db_name="test_db",
        branch="main",
    )

    assert resolved is None
    assert oms_client.calls == 0


@pytest.mark.asyncio
async def test_resolve_branch_head_commit_with_bootstrap_is_disabled() -> None:
    oms_client = _UnusedOMSClient()

    resolved = await resolve_branch_head_commit_with_bootstrap(
        oms_client=oms_client,
        db_name="test_db",
        branch="feature-x",
    )

    assert resolved is None
    assert oms_client.calls == 0
