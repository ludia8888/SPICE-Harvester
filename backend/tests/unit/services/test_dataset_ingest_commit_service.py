from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from bff.services.dataset_ingest_commit_service import (
    ensure_lakefs_commit_artifact,
    persist_ingest_commit_state,
)


@dataclass
class _IngestRequest:
    ingest_request_id: str
    lakefs_commit_id: str | None = None
    artifact_key: str | None = None


@dataclass
class _IngestTransaction:
    transaction_id: str


class _Registry:
    def __init__(self) -> None:
        self.mark_calls: list[dict[str, Any]] = []
        self.transaction_calls: list[dict[str, Any]] = []

    async def mark_ingest_committed(self, *, ingest_request_id: str, lakefs_commit_id: str, artifact_key: str) -> _IngestRequest:
        self.mark_calls.append(
            {
                "ingest_request_id": ingest_request_id,
                "lakefs_commit_id": lakefs_commit_id,
                "artifact_key": artifact_key,
            }
        )
        return _IngestRequest(
            ingest_request_id=ingest_request_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: str,
    ) -> None:
        self.transaction_calls.append(
            {
                "ingest_request_id": ingest_request_id,
                "lakefs_commit_id": lakefs_commit_id,
                "artifact_key": artifact_key,
            }
        )


@pytest.mark.asyncio
async def test_ensure_lakefs_commit_artifact_uses_existing_values(monkeypatch: pytest.MonkeyPatch) -> None:
    ingest_request = _IngestRequest(
        ingest_request_id="ing-1",
        lakefs_commit_id="c1",
        artifact_key="s3://repo/c1/path.json",
    )
    ensure_calls: list[dict[str, Any]] = []
    commit_calls: list[dict[str, Any]] = []

    async def _fake_ensure(**kwargs: Any) -> None:
        ensure_calls.append(dict(kwargs))

    async def _fake_commit(**kwargs: Any) -> str:
        commit_calls.append(dict(kwargs))
        return "c-should-not-run"

    monkeypatch.setattr("bff.services.dataset_ingest_commit_service.ops._ensure_lakefs_branch_exists", _fake_ensure)
    monkeypatch.setattr("bff.services.dataset_ingest_commit_service.ops._commit_lakefs_with_predicate_fallback", _fake_commit)

    result = await ensure_lakefs_commit_artifact(
        ingest_request=ingest_request,
        lakefs_client=object(),
        lakefs_storage_service=object(),
        repository="repo",
        branch="main",
        commit_message="msg",
        commit_metadata={"x": 1},
        object_key="datasets/path.json",
    )

    assert result.created_commit is False
    assert result.commit_id == "c1"
    assert result.artifact_key == "s3://repo/c1/path.json"
    assert ensure_calls == []
    assert commit_calls == []


@pytest.mark.asyncio
async def test_ensure_lakefs_commit_artifact_commits_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    ingest_request = _IngestRequest(ingest_request_id="ing-2")
    ensure_calls: list[dict[str, Any]] = []
    commit_calls: list[dict[str, Any]] = []

    async def _fake_ensure(**kwargs: Any) -> None:
        ensure_calls.append(dict(kwargs))

    async def _fake_commit(**kwargs: Any) -> str:
        commit_calls.append(dict(kwargs))
        return "commit-22"

    monkeypatch.setattr("bff.services.dataset_ingest_commit_service.ops._ensure_lakefs_branch_exists", _fake_ensure)
    monkeypatch.setattr("bff.services.dataset_ingest_commit_service.ops._commit_lakefs_with_predicate_fallback", _fake_commit)

    result = await ensure_lakefs_commit_artifact(
        ingest_request=ingest_request,
        lakefs_client="lakefs-client",
        lakefs_storage_service="lakefs-storage",
        repository="repo-a",
        branch="feature",
        commit_message="commit message",
        commit_metadata={"dataset_id": "d1"},
        object_key="datasets/d1/staging/data.json",
        expected_checksum="abc123",
    )

    assert result.created_commit is True
    assert result.commit_id == "commit-22"
    assert result.artifact_key == "s3://repo-a/commit-22/datasets/d1/staging/data.json"
    assert len(ensure_calls) == 1
    assert ensure_calls[0]["repository"] == "repo-a"
    assert len(commit_calls) == 1
    assert commit_calls[0]["branch"] == "feature"
    assert commit_calls[0]["expected_checksum"] == "abc123"


@pytest.mark.asyncio
async def test_persist_ingest_commit_state_skips_when_unchanged() -> None:
    ingest_request = _IngestRequest(
        ingest_request_id="ing-3",
        lakefs_commit_id="c3",
        artifact_key="s3://repo/c3/a.json",
    )
    registry = _Registry()

    updated = await persist_ingest_commit_state(
        dataset_registry=registry,
        ingest_request=ingest_request,
        ingest_transaction=None,
        commit_id="c3",
        artifact_key="s3://repo/c3/a.json",
        force=False,
    )

    assert updated is ingest_request
    assert registry.mark_calls == []
    assert registry.transaction_calls == []


@pytest.mark.asyncio
async def test_persist_ingest_commit_state_marks_and_updates_transaction() -> None:
    ingest_request = _IngestRequest(ingest_request_id="ing-4", lakefs_commit_id=None, artifact_key=None)
    ingest_transaction = _IngestTransaction(transaction_id="tx-4")
    registry = _Registry()

    updated = await persist_ingest_commit_state(
        dataset_registry=registry,
        ingest_request=ingest_request,
        ingest_transaction=ingest_transaction,
        commit_id="c4",
        artifact_key="s3://repo/c4/a.json",
        force=False,
    )

    assert updated.ingest_request_id == "ing-4"
    assert updated.lakefs_commit_id == "c4"
    assert updated.artifact_key == "s3://repo/c4/a.json"
    assert len(registry.mark_calls) == 1
    assert len(registry.transaction_calls) == 1
