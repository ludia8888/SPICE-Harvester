from __future__ import annotations

import pytest

from bff.routers.summary import get_summary


class _FakeRedis:
    async def ping(self) -> bool:
        return True


class _FakeEs:
    async def connect(self) -> None:
        return None

    async def ping(self) -> bool:
        return True

    async def get_cluster_health(self):  # noqa: ANN201
        return {"status": "green"}

    async def disconnect(self) -> None:
        return None


@pytest.mark.asyncio
async def test_summary_skips_legacy_branch_info_in_postgres_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ONTOLOGY_RESOURCE_STORAGE_BACKEND", "postgres")

    result = await get_summary(
        db="demo",
        branch="main",
        redis_service=_FakeRedis(),
        es_service=_FakeEs(),
    )

    assert result["data"]["branch_info"] is None
    assert result["data"]["policy"]["ontology_backend"] == "postgres"


@pytest.mark.asyncio
async def test_summary_keeps_branch_info_disabled_in_hybrid_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ONTOLOGY_RESOURCE_STORAGE_BACKEND", "hybrid")

    result = await get_summary(
        db="demo",
        branch="main",
        redis_service=_FakeRedis(),
        es_service=_FakeEs(),
    )

    assert result["data"]["branch_info"] is None
    assert result["data"]["policy"]["ontology_backend"] == "postgres"
