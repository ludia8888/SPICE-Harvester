import os
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from bff.main import app
from bff.routers import pipeline as pipeline_router
from shared.services.dataset_registry import DatasetRegistry


class _FakeLakeFSStorage:
    async def save_bytes(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def save_json(self, *args: Any, **kwargs: Any) -> None:
        return None


class _FakeLakeFSClient:
    def __init__(self) -> None:
        self.commit_calls = 0

    async def commit(self, *args: Any, **kwargs: Any) -> str:
        self.commit_calls += 1
        return f"commit-{self.commit_calls}"

    async def create_branch(self, *args: Any, **kwargs: Any) -> None:
        return None


class _FakePipelineRegistry:
    def __init__(self) -> None:
        self.storage = _FakeLakeFSStorage()
        self.client = _FakeLakeFSClient()

    async def get_lakefs_storage(self, *args: Any, **kwargs: Any) -> _FakeLakeFSStorage:
        return self.storage

    async def get_lakefs_client(self, *args: Any, **kwargs: Any) -> _FakeLakeFSClient:
        return self.client


async def _noop_flush_outbox(*args: Any, **kwargs: Any) -> None:
    return None


def test_csv_upload_idempotency_key_reuses_version(monkeypatch):
    monkeypatch.setenv("BFF_ADMIN_TOKEN", "testtoken")
    monkeypatch.setenv(
        "POSTGRES_URL",
        "postgresql://spiceadmin:spicepass123@127.0.0.1:55433/spicedb",
    )
    monkeypatch.setattr(pipeline_router, "flush_dataset_ingest_outbox", _noop_flush_outbox)

    dataset_registry = DatasetRegistry(
        dsn=os.environ.get("POSTGRES_URL"),
        schema="spice_datasets_test_ingest",
    )
    pipeline_registry = _FakePipelineRegistry()

    async def _get_dataset_registry():
        await dataset_registry.connect()
        return dataset_registry

    async def _get_pipeline_registry():
        return pipeline_registry

    app.dependency_overrides[pipeline_router.get_dataset_registry] = _get_dataset_registry
    app.dependency_overrides[pipeline_router.get_pipeline_registry] = _get_pipeline_registry

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            key = f"idem-csv-{uuid4().hex}"
            headers = {"X-Admin-Token": "testtoken", "Idempotency-Key": key}
            file_content = b"id,name\n1,A\n2,B\n"
            files = {"file": ("input.csv", file_content, "text/csv")}
            data = {"dataset_name": "csv_input"}

            res1 = client.post(
                "/api/v1/pipelines/datasets/csv-upload",
                params={"db_name": "testdb"},
                headers=headers,
                files=files,
                data=data,
            )
            assert res1.status_code == 200, res1.text
            version1 = res1.json()["data"]["version"]

            res2 = client.post(
                "/api/v1/pipelines/datasets/csv-upload",
                params={"db_name": "testdb"},
                headers=headers,
                files={"file": ("input.csv", file_content, "text/csv")},
                data=data,
            )
            assert res2.status_code == 200, res2.text
            version2 = res2.json()["data"]["version"]
            assert version1["version_id"] == version2["version_id"]
            assert version1["lakefs_commit_id"] == version2["lakefs_commit_id"]
            assert pipeline_registry.client.commit_calls == 1

            res3 = client.post(
                "/api/v1/pipelines/datasets/csv-upload",
                params={"db_name": "testdb"},
                headers=headers,
                files={"file": ("input.csv", b"id,name\n3,C\n", "text/csv")},
                data=data,
            )
            assert res3.status_code == 409
    finally:
        app.dependency_overrides.clear()
