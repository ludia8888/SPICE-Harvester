from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from shared.services.pipeline_registry import PipelineAlreadyExistsError, PipelineRecord, PipelineRegistry


def _pipeline_record(*, pipeline_id: str, db_name: str, name: str, branch: str) -> PipelineRecord:
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return PipelineRecord(
        pipeline_id=pipeline_id,
        db_name=db_name,
        name=name,
        description=None,
        pipeline_type="batch",
        location="/pipelines",
        status="draft",
        branch=branch,
        lakefs_repository="pipeline-artifacts",
        proposal_status=None,
        proposal_title=None,
        proposal_description=None,
        proposal_submitted_at=None,
        proposal_reviewed_at=None,
        proposal_review_comment=None,
        proposal_bundle={},
        last_preview_status=None,
        last_preview_at=None,
        last_preview_rows=None,
        last_preview_job_id=None,
        last_preview_node_id=None,
        last_preview_sample={},
        last_preview_nodes={},
        last_build_status=None,
        last_build_at=None,
        last_build_output={},
        deployed_at=None,
        deployed_commit_id=None,
        schedule_interval_seconds=None,
        schedule_cron=None,
        last_scheduled_at=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_branch_is_idempotent_when_db_unique_violation_races(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If branch creation is raced (or retried), the DB may return a unique constraint violation.
    The registry should treat this as idempotent and return the existing pipeline record.
    """

    source = _pipeline_record(
        pipeline_id="11111111-1111-1111-1111-111111111111",
        db_name="db",
        name="pipeline",
        branch="main",
    )
    existing = _pipeline_record(
        pipeline_id="22222222-2222-2222-2222-222222222222",
        db_name=source.db_name,
        name=source.name,
        branch="feature",
    )

    registry = PipelineRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = SimpleNamespace()  # bypass connection guard; we mock all DB calls

    async def _get_pipeline(*, pipeline_id: str):
        assert pipeline_id == source.pipeline_id
        return source

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        class _Client:
            async def create_branch(self, **kwargs):  # noqa: ANN003
                return None

        return _Client()

    call_count = {"get_by_name": 0, "create_pipeline": 0}

    async def _get_pipeline_by_name(*, db_name: str, name: str, branch: str):
        call_count["get_by_name"] += 1
        assert db_name == source.db_name
        assert name == source.name
        assert branch == existing.branch
        # Simulate TOCTOU: not found before create, found after unique violation.
        return existing if call_count["get_by_name"] >= 2 else None

    async def _create_pipeline(**kwargs):  # noqa: ANN003
        call_count["create_pipeline"] += 1
        raise PipelineAlreadyExistsError(db_name=source.db_name, name=source.name, branch=existing.branch)

    async def _get_latest_version(**kwargs):  # noqa: ANN003
        return None

    monkeypatch.setattr(registry, "get_pipeline", _get_pipeline)
    monkeypatch.setattr(registry, "get_lakefs_client", _get_lakefs_client)
    monkeypatch.setattr(registry, "get_pipeline_by_name", _get_pipeline_by_name)
    monkeypatch.setattr(registry, "create_pipeline", _create_pipeline)
    monkeypatch.setattr(registry, "get_latest_version", _get_latest_version)

    result = await registry.create_branch(pipeline_id=source.pipeline_id, new_branch=existing.branch)

    assert result.pipeline_id == existing.pipeline_id
    assert call_count["create_pipeline"] == 1
    assert call_count["get_by_name"] == 2
