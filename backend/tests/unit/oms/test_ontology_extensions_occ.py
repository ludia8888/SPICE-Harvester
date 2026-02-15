from __future__ import annotations

import pytest

from oms.routers.ontology_extensions import _assert_expected_head_commit


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_skips_when_unset() -> None:
    resolved = await _assert_expected_head_commit(expected_head_commit=None)
    assert resolved is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_skips_when_blank() -> None:
    resolved = await _assert_expected_head_commit(expected_head_commit="   ")
    assert resolved is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_assert_expected_head_commit_returns_trimmed_token() -> None:
    resolved = await _assert_expected_head_commit(expected_head_commit="  c-head-1  ")
    assert resolved == "c-head-1"
