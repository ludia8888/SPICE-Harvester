from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from shared.services.lakefs_client import LakeFSError
from shared.services.pipeline_registry import PipelineRecord, PipelineRegistry


class _Acquire:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class _FakePool:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self._conn)


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
async def test_add_version_handles_lakefs_predicate_failed_by_resolving_head_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    lakeFS commits apply to the entire branch working copy. If another actor commits concurrently,
    a subsequent commit may fail with 'predicate failed' (no staged changes left).

    The registry should treat this as idempotent *iff* the branch head commit contains the
    just-written definition snapshot.
    """

    pipeline = _pipeline_record(
        pipeline_id="11111111-1111-1111-1111-111111111111",
        db_name="db",
        name="pipeline",
        branch="main",
    )

    registry = PipelineRegistry(dsn="postgresql://example.invalid/db")

    inserted: dict = {}

    class _Conn:
        async def fetchrow(self, query: str, *args):  # noqa: ANN001
            # Capture the inserted commit id for assertion.
            inserted["commit_id"] = args[3]
            return {
                "version_id": args[0],
                "pipeline_id": args[1],
                "branch": args[2],
                "lakefs_commit_id": args[3],
                "definition_json": args[4],
                "created_at": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            }

    registry._pool = _FakePool(_Conn())

    async def _get_pipeline(*, pipeline_id: str):  # noqa: ANN001
        assert pipeline_id == pipeline.pipeline_id
        return pipeline

    monkeypatch.setattr(registry, "get_pipeline", _get_pipeline)

    definition = {"nodes": [{"id": "node-0", "type": "input"}]}
    checksum = "checksum-123"
    head_commit_id = "deadbeef" * 8

    class _Storage:
        async def save_json(self, **kwargs):  # noqa: ANN003
            return checksum

        async def load_json(self, *, bucket: str, key: str):  # noqa: ANN001
            assert bucket == "pipeline-artifacts"
            if key == f"{head_commit_id}/pipeline-definitions/db/pipeline/definition.json":
                return definition
            raise FileNotFoundError(key)

    class _Client:
        def __init__(self) -> None:
            self.commit_calls = 0
            self.head_calls = 0

        async def commit(self, **kwargs):  # noqa: ANN003
            self.commit_calls += 1
            raise LakeFSError('lakeFS commit failed (500): {"message":"predicate failed"}')

        async def get_branch_head_commit_id(self, **kwargs):  # noqa: ANN003
            self.head_calls += 1
            return head_commit_id

    client = _Client()

    async def _get_lakefs_storage(**kwargs):  # noqa: ANN003
        return _Storage()

    async def _get_lakefs_client(**kwargs):  # noqa: ANN003
        return client

    monkeypatch.setattr(registry, "get_lakefs_storage", _get_lakefs_storage)
    monkeypatch.setattr(registry, "get_lakefs_client", _get_lakefs_client)

    result = await registry.add_version(pipeline_id=pipeline.pipeline_id, definition_json=definition, branch="main")

    assert result.lakefs_commit_id == head_commit_id
    assert inserted["commit_id"] == head_commit_id
    assert client.commit_calls == 1
    assert client.head_calls == 1
