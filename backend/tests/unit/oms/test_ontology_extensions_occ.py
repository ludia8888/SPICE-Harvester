from __future__ import annotations

import pytest
from fastapi import HTTPException

from oms.routers.ontology_extensions import _assert_expected_head_commit


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_skips_when_unset() -> None:
    resolved = await _assert_expected_head_commit(
        db_name="test_db",
        branch="main",
        expected_head_commit=None,
        strict_mode=False,
    )
    assert resolved is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_skips_when_blank() -> None:
    resolved = await _assert_expected_head_commit(
        db_name="test_db",
        branch="main",
        expected_head_commit="   ",
        strict_mode=False,
    )
    assert resolved is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_returns_trimmed_token_when_allowed_by_branch() -> None:
    resolved = await _assert_expected_head_commit(
        db_name="test_db",
        branch="main",
        expected_head_commit="  branch:main  ",
        strict_mode=True,
    )
    assert resolved == "branch:main"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_strict_mode_rejects_blank() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await _assert_expected_head_commit(
            db_name="test_db",
            branch="main",
            expected_head_commit="   ",
            strict_mode=True,
        )
    assert exc_info.value.status_code == 400


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_strict_mode_accepts_latest_deployed_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_latest_deployed_commit(self, *, db_name: str, target_branch: str = "main"):
        _ = self, db_name, target_branch
        return {"ontology_commit_id": "c-main-1"}

    monkeypatch.setattr(
        "oms.services.ontology_deployment_registry_v2.OntologyDeploymentRegistryV2.get_latest_deployed_commit",
        _fake_latest_deployed_commit,
    )

    resolved = await _assert_expected_head_commit(
        db_name="test_db",
        branch="main",
        expected_head_commit="c-main-1",
        strict_mode=True,
    )
    assert resolved == "c-main-1"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_strict_mode_rejects_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_latest_deployed_commit(self, *, db_name: str, target_branch: str = "main"):
        _ = self, db_name, target_branch
        return {"ontology_commit_id": "c-main-1"}

    monkeypatch.setattr(
        "oms.services.ontology_deployment_registry_v2.OntologyDeploymentRegistryV2.get_latest_deployed_commit",
        _fake_latest_deployed_commit,
    )

    with pytest.raises(HTTPException) as exc_info:
        await _assert_expected_head_commit(
            db_name="test_db",
            branch="main",
            expected_head_commit="c-main-2",
            strict_mode=True,
        )
    assert exc_info.value.status_code == 409
