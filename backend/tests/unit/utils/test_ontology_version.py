from __future__ import annotations

import pytest

from shared.utils.ontology_version import resolve_ontology_version


class _LegacyLikeSource:
    async def list_branches(self, db_name: str):  # noqa: ANN201
        _ = db_name
        return [
            {"name": "main", "commit": "commit-main"},
            {"name": "feature-a", "commit": "commit-feature"},
        ]


class _BranchInfoLikeSource:
    async def get_branch_info(self, db_name: str, branch_name: str):  # noqa: ANN201
        _ = (db_name, branch_name)
        return {
            "status": "success",
            "data": {
                "name": "main",
                "head": "commit-main",
            },
        }


class _FailingSource:
    async def list_branches(self, db_name: str):  # noqa: ANN201
        _ = db_name
        raise RuntimeError("boom")

    async def get_branch_info(self, db_name: str, branch_name: str):  # noqa: ANN201
        _ = (db_name, branch_name)
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_resolve_ontology_version_without_source_returns_ref_only() -> None:
    result = await resolve_ontology_version(
        None,
        db_name="demo",
        branch="main",
    )

    assert result == {"ref": "branch:main"}


@pytest.mark.asyncio
async def test_resolve_ontology_version_ignores_legacy_like_source() -> None:
    result = await resolve_ontology_version(
        _LegacyLikeSource(),
        db_name="demo",
        branch="main",
    )

    assert result == {"ref": "branch:main"}


@pytest.mark.asyncio
async def test_resolve_ontology_version_ignores_branch_list_source() -> None:
    result = await resolve_ontology_version(
        _LegacyLikeSource(),
        db_name="demo",
        branch="feature-a",
    )

    assert result == {"ref": "branch:feature-a"}


@pytest.mark.asyncio
async def test_resolve_ontology_version_ignores_branch_info_source() -> None:
    result = await resolve_ontology_version(
        _BranchInfoLikeSource(),
        db_name="demo",
        branch="main",
    )

    assert result == {"ref": "branch:main"}


@pytest.mark.asyncio
async def test_resolve_ontology_version_falls_back_to_ref_on_source_failures() -> None:
    result = await resolve_ontology_version(
        _FailingSource(),
        db_name="demo",
        branch="main",
    )

    assert result == {"ref": "branch:main"}
