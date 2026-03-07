from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from shared.services.registries.pipeline_registry import PipelineAlreadyExistsError, PipelineRecord, PipelineRegistry


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


class _FetchrowConnection:
    def __init__(self, row: dict[str, object]) -> None:
        self._row = row

    async def __aenter__(self) -> "_FetchrowConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def fetchrow(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self._row


class _FetchrowPool:
    def __init__(self, row: dict[str, object]) -> None:
        self._row = row

    def acquire(self) -> _FetchrowConnection:
        return _FetchrowConnection(self._row)


class _ExecuteConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def __aenter__(self) -> "_ExecuteConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def execute(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.calls.append((args, kwargs))
        return "INSERT 0 1"


class _ExecutePool:
    def __init__(self, conn: _ExecuteConnection) -> None:
        self._conn = conn

    def acquire(self) -> _ExecuteConnection:
        return self._conn


@pytest.mark.unit
@pytest.mark.asyncio
async def test_merge_branch_is_idempotent_when_target_pipeline_create_races(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _pipeline_record(
        pipeline_id="11111111-1111-1111-1111-111111111111",
        db_name="db",
        name="pipeline",
        branch="feature",
    )
    target = _pipeline_record(
        pipeline_id="22222222-2222-2222-2222-222222222222",
        db_name=source.db_name,
        name=source.name,
        branch="main",
    )
    now = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    registry = PipelineRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FetchrowPool(
        {
            "version_id": "33333333-3333-3333-3333-333333333333",
            "pipeline_id": target.pipeline_id,
            "branch": target.branch,
            "lakefs_commit_id": "merge-commit",
            "definition_json": {"nodes": []},
            "created_at": now,
        }
    )

    async def _get_pipeline(*, pipeline_id: str):
        assert pipeline_id == source.pipeline_id
        return source

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        class _Client:
            async def merge(self, **kwargs):  # noqa: ANN003
                return "merge-commit"

        return _Client()

    async def _get_lakefs_storage(**kwargs):  # noqa: ANN003
        class _Storage:
            async def load_json(self, **kwargs):  # noqa: ANN003
                return {"nodes": []}

        return _Storage()

    call_count = {"get_by_name": 0, "create_pipeline": 0}

    async def _get_pipeline_by_name(*, db_name: str, name: str, branch: str):
        call_count["get_by_name"] += 1
        assert db_name == source.db_name
        assert name == source.name
        assert branch == target.branch
        return target if call_count["get_by_name"] >= 2 else None

    async def _create_pipeline(**kwargs):  # noqa: ANN003
        call_count["create_pipeline"] += 1
        raise PipelineAlreadyExistsError(db_name=source.db_name, name=source.name, branch=target.branch)

    monkeypatch.setattr(registry, "get_pipeline", _get_pipeline)
    monkeypatch.setattr(registry, "get_lakefs_client", _get_lakefs_client)
    monkeypatch.setattr(registry, "get_lakefs_storage", _get_lakefs_storage)
    monkeypatch.setattr(registry, "get_pipeline_by_name", _get_pipeline_by_name)
    monkeypatch.setattr(registry, "create_pipeline", _create_pipeline)

    result = await registry.merge_branch(
        pipeline_id=source.pipeline_id,
        from_branch=source.branch,
        to_branch=target.branch,
        user_id="test-user",
    )

    assert result.pipeline_id == target.pipeline_id
    assert result.branch == target.branch
    assert result.lakefs_commit_id == "merge-commit"
    assert call_count["create_pipeline"] == 1
    assert call_count["get_by_name"] == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_branch_repairs_missing_seed_version_after_create_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    latest_source = SimpleNamespace(
        version_id="33333333-3333-3333-3333-333333333333",
        pipeline_id=source.pipeline_id,
        branch=source.branch,
        lakefs_commit_id="source-commit",
        definition_json={"nodes": [{"id": "n1"}]},
        created_at=datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
    )

    execute_conn = _ExecuteConnection()
    registry = PipelineRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _ExecutePool(execute_conn)

    async def _get_pipeline(*, pipeline_id: str):
        assert pipeline_id == source.pipeline_id
        return source

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        class _Client:
            async def create_branch(self, **kwargs):  # noqa: ANN003
                return None

        return _Client()

    call_count = {"get_by_name": 0, "create_pipeline": 0, "latest": 0}

    async def _get_pipeline_by_name(*, db_name: str, name: str, branch: str):
        call_count["get_by_name"] += 1
        assert db_name == source.db_name
        assert name == source.name
        assert branch == existing.branch
        return existing if call_count["get_by_name"] >= 2 else None

    async def _create_pipeline(**kwargs):  # noqa: ANN003
        call_count["create_pipeline"] += 1
        raise PipelineAlreadyExistsError(db_name=source.db_name, name=source.name, branch=existing.branch)

    async def _get_latest_version(*, pipeline_id: str, branch: str | None = None):
        call_count["latest"] += 1
        if pipeline_id == existing.pipeline_id and branch == existing.branch:
            return None
        if pipeline_id == source.pipeline_id and branch == source.branch:
            return latest_source
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
    assert call_count["latest"] == 2
    assert len(execute_conn.calls) == 1
    execute_args, _ = execute_conn.calls[0]
    assert existing.pipeline_id in execute_args
    assert existing.branch in execute_args
    assert latest_source.lakefs_commit_id in execute_args
