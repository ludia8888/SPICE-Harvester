from __future__ import annotations

from uuid import uuid4

import pytest

from shared.config.service_config import ServiceConfig
from shared.services.pipeline_registry import PipelineRegistry


@pytest.mark.asyncio
async def test_pipeline_branch_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = PipelineRegistry(dsn=ServiceConfig.get_postgres_url())
    await registry.connect()

    db_name = f"test_branch_{uuid4().hex}"
    pipeline = await registry.create_pipeline(
        db_name=db_name,
        name=f"pipeline_{uuid4().hex}",
        description=None,
        pipeline_type="batch",
        location="/pipelines",
        status="draft",
        branch="main",
    )

    class _Client:
        async def create_branch(self, **kwargs):  # noqa: ANN003
            return None

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        return _Client()

    monkeypatch.setattr(registry, "get_lakefs_client", _get_lakefs_client)

    created = await registry.create_branch(
        pipeline_id=pipeline.pipeline_id,
        new_branch="feature",
    )
    assert created.branch == "feature"

    branches = await registry.list_pipeline_branches(db_name=db_name)
    branch_names = {branch["branch"] for branch in branches}
    assert "main" in branch_names
    assert "feature" in branch_names

    archived = await registry.archive_pipeline_branch(db_name=db_name, branch="feature")
    assert archived["archived"] is True

    restored = await registry.restore_pipeline_branch(db_name=db_name, branch="feature")
    assert restored["archived"] is False

    await registry.close()
