from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from bff.routers.pipeline import create_dataset_version


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


@pytest.mark.asyncio
async def test_create_dataset_version_materializes_manual_sample_to_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    class _EventStore:
        async def connect(self) -> None:
            return None

        async def append_event(self, event: Any) -> None:
            return None

    import bff.routers.pipeline as pipeline_router

    monkeypatch.setattr(pipeline_router, "event_store", _EventStore())
    monkeypatch.setattr(pipeline_router, "_utcnow", lambda: datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc))

    class _LakeFSClient:
        async def commit(self, **kwargs: Any) -> str:
            return "c-manual"

    monkeypatch.setenv("LAKEFS_RAW_REPOSITORY", "dataset-artifacts-test")

    dataset = _Dataset(dataset_id="ds-1", db_name="testdb", name="My Dataset", source_type="manual")
    registry = _DatasetRegistry(dataset=dataset)
    storage = _LakeFSStorageService()
    pipeline_registry = _PipelineRegistry(storage=storage, client=_LakeFSClient())

    sample_json = {
        "columns": [{"name": "id", "type": "String"}, {"name": "name", "type": "String"}],
        "rows": [{"id": "1", "name": "A"}],
    }

    response = await create_dataset_version(
        dataset_id=dataset.dataset_id,
        payload={
            "sample_json": sample_json,
            "schema_json": {"columns": sample_json["columns"]},
        },
        request=_Request(headers={"X-DB-Name": "testdb"}),
        pipeline_registry=pipeline_registry,  # type: ignore[arg-type]
        dataset_registry=registry,
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
