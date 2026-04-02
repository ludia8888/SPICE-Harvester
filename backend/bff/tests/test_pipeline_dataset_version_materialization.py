from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from bff.services import pipeline_dataset_version_service
from bff.services.pipeline_dataset_version_service import create_dataset_version


@dataclass
class _Request:
    headers: dict[str, str]


@dataclass
class _Dataset:
    dataset_id: str
    db_name: str
    name: str
    source_type: str = "manual"
    branch: str = "main"


class _DatasetRegistry:
    def __init__(self, *, dataset: _Dataset) -> None:
        self._dataset = dataset
        self.received_artifact_key: Optional[str] = None
        self.received_lakefs_commit_id: Optional[str] = None
        self.received_row_count: Optional[int] = None
        self.received_sample_json: Optional[dict[str, Any]] = None

    async def get_dataset(self, *, dataset_id: str) -> Optional[_Dataset]:
        if dataset_id == self._dataset.dataset_id:
            return self._dataset
        return None

    async def add_version(
        self,
        *,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: dict[str, Any],
        schema_json: Optional[dict[str, Any]],
        promoted_from_artifact_id: Optional[str] = None,
    ) -> Any:
        assert dataset_id == self._dataset.dataset_id
        self.received_artifact_key = artifact_key
        self.received_lakefs_commit_id = lakefs_commit_id
        self.received_row_count = row_count
        self.received_sample_json = sample_json
        return type("_Version", (), {"lakefs_commit_id": lakefs_commit_id})()


class _LakeFSStorageService:
    def __init__(self) -> None:
        self.saved: list[dict[str, Any]] = []

    async def save_json(
        self,
        bucket: str,
        key: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        self.saved.append(
            {
                "bucket": bucket,
                "key": key,
                "data": data,
                "metadata": metadata or {},
            }
        )
        return "checksum"

class _PipelineRegistry:
    def __init__(self, *, storage: _LakeFSStorageService, client: Any) -> None:
        self._storage = storage
        self._client = client

    async def get_lakefs_storage(self, *, user_id: Optional[str] = None) -> _LakeFSStorageService:
        return self._storage

    async def get_lakefs_client(self, *, user_id: Optional[str] = None) -> Any:
        return self._client


class _LineageStore:
    @staticmethod
    def node_event(event_id: str) -> str:
        return f"event:{event_id}"

    @staticmethod
    def node_artifact(kind: str, *parts: str) -> str:
        return "artifact:" + ":".join([kind, *parts])

    async def record_link(self, **kwargs: Any) -> None:  # noqa: ARG002
        return None


@dataclass
class _IngestRequest:
    ingest_request_id: str


@dataclass
class _IngestTransaction:
    transaction_id: str


class _IdempotentDatasetRegistry(_DatasetRegistry):
    def __init__(self, *, dataset: _Dataset) -> None:
        super().__init__(dataset=dataset)
        self.publish_calls = 0

    async def create_ingest_request(self, **kwargs: Any) -> tuple[_IngestRequest, bool]:
        _ = kwargs
        return _IngestRequest(ingest_request_id="ing-1"), True

    async def publish_ingest_request(self, **kwargs: Any) -> Any:
        _ = kwargs
        self.publish_calls += 1
        return type(
            "_PublishedVersion",
            (),
            {
                "version_id": "version-1",
                "lakefs_commit_id": "commit-1",
                "artifact_key": "s3://dataset-artifacts-test/commit-1/data.json",
            },
        )()


@pytest.mark.asyncio
async def test_create_dataset_version_materializes_manual_sample_to_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    class _EventStore:
        async def connect(self) -> None:
            return None

        async def append_event(self, event: Any) -> None:
            return None

    import bff.routers.pipeline_datasets_ops as pipeline_datasets_ops

    monkeypatch.setattr(pipeline_datasets_ops, "event_store", _EventStore())

    class _LakeFSClient:
        async def commit(self, **kwargs: Any) -> str:
            return "c-manual"

    monkeypatch.setenv("PIPELINE_LOCKS_ENABLED", "false")
    monkeypatch.setenv("PIPELINE_LOCKS_REQUIRED", "false")
    monkeypatch.setenv("LAKEFS_RAW_REPOSITORY", "dataset-artifacts-test")

    dataset = _Dataset(dataset_id="ds-1", db_name="testdb", name="My Dataset", source_type="manual")
    registry = _DatasetRegistry(dataset=dataset)
    storage = _LakeFSStorageService()
    pipeline_registry = _PipelineRegistry(storage=storage, client=_LakeFSClient())

    sample_json = {
        "columns": [{"name": "id", "type": "String"}, {"name": "name", "type": "String"}],
        "rows": [{"id": "1", "name": "A"}],
    }

    async def _noop_flush(**_: Any) -> None:
        return None

    async def _noop_build_event(**_: Any) -> dict[str, Any]:
        return {}

    response = await create_dataset_version(
        dataset_id=dataset.dataset_id,
        payload={
            "sample_json": sample_json,
            "schema_json": {"columns": sample_json["columns"]},
        },
        request=_Request(headers={"X-DB-Name": "testdb"}),
        pipeline_registry=pipeline_registry,  # type: ignore[arg-type]
        dataset_registry=registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=_LineageStore(),  # type: ignore[arg-type]
        flush_dataset_ingest_outbox=_noop_flush,
        build_dataset_event_payload=_noop_build_event,
    )

    assert response["status"] == "success"
    assert registry.received_row_count == 1
    assert registry.received_artifact_key is not None
    assert registry.received_lakefs_commit_id == "c-manual"
    assert registry.received_artifact_key == "s3://dataset-artifacts-test/c-manual/datasets/testdb/ds-1/My_Dataset/data.json"

    assert len(storage.saved) == 1
    saved = storage.saved[0]
    assert saved["bucket"] == "dataset-artifacts-test"
    assert saved["key"] == "main/datasets/testdb/ds-1/My_Dataset/data.json"
    assert saved["data"] == sample_json


@pytest.mark.asyncio
async def test_create_dataset_version_does_not_mark_failed_after_successful_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _Dataset(dataset_id="ds-2", db_name="testdb", name="My Dataset", source_type="manual")
    registry = _IdempotentDatasetRegistry(dataset=dataset)
    storage = _LakeFSStorageService()
    pipeline_registry = _PipelineRegistry(storage=storage, client=object())
    marked_failed: list[dict[str, Any]] = []

    async def _fake_resolve_existing_version_or_raise(**kwargs: Any) -> None:
        _ = kwargs
        return None

    async def _fake_persist_ingest_commit_state(**kwargs: Any) -> _IngestRequest:
        return kwargs["ingest_request"]

    async def _fake_flush_inline(**kwargs: Any) -> None:
        _ = kwargs
        raise RuntimeError("flush failed")

    async def _fake_mark_ingest_failed(**kwargs: Any) -> None:
        marked_failed.append(kwargs)

    class _FakeOutboxBuilder:
        def __init__(self, **kwargs: Any) -> None:
            _ = kwargs

        def version_created(self, **kwargs: Any) -> dict[str, Any]:
            return dict(kwargs)

    async def _fake_ensure_ingest_transaction(*args: Any, **kwargs: Any) -> _IngestTransaction:
        _ = args, kwargs
        return _IngestTransaction(transaction_id="txn-1")

    async def _fake_maybe_enqueue_objectify_job(**kwargs: Any) -> None:
        _ = kwargs
        raise RuntimeError("enqueue failed")

    monkeypatch.setattr(pipeline_dataset_version_service, "resolve_existing_version_or_raise", _fake_resolve_existing_version_or_raise)
    monkeypatch.setattr(pipeline_dataset_version_service, "persist_ingest_commit_state", _fake_persist_ingest_commit_state)
    monkeypatch.setattr(pipeline_dataset_version_service, "maybe_flush_dataset_ingest_outbox_inline", _fake_flush_inline)
    monkeypatch.setattr(pipeline_dataset_version_service, "mark_ingest_failed", _fake_mark_ingest_failed)
    monkeypatch.setattr(pipeline_dataset_version_service, "DatasetIngestOutboxBuilder", _FakeOutboxBuilder)
    monkeypatch.setattr(pipeline_dataset_version_service.ops, "_ensure_ingest_transaction", _fake_ensure_ingest_transaction)
    monkeypatch.setattr(pipeline_dataset_version_service.ops, "_maybe_enqueue_objectify_job", _fake_maybe_enqueue_objectify_job)

    response = await create_dataset_version(
        dataset_id=dataset.dataset_id,
        payload={
            "sample_json": {"rows": [{"id": "1"}]},
            "artifact_key": "s3://dataset-artifacts-test/commit-1/datasets/testdb/ds-2/My_Dataset/data.json",
            "lakefs_commit_id": "commit-1",
        },
        request=_Request(headers={"X-DB-Name": "testdb", "Idempotency-Key": "idem-ds-2"}),
        pipeline_registry=pipeline_registry,  # type: ignore[arg-type]
        dataset_registry=registry,
        objectify_registry=None,
        objectify_job_queue=None,
        lineage_store=_LineageStore(),  # type: ignore[arg-type]
        flush_dataset_ingest_outbox=lambda **_: None,
        build_dataset_event_payload=lambda **_: {},
    )

    assert response["status"] == "success"
    assert response["data"]["objectify_job_id"] is None
    assert registry.publish_calls == 1
    assert marked_failed == []
