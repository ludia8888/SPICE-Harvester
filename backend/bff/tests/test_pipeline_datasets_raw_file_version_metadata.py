from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from bff.routers import pipeline_datasets_catalog


class _FakeStorage:
    async def load_bytes(self, bucket: str, key: str) -> bytes:
        _ = bucket, key
        return b"order_id,order_status\norder-1,PENDING\n"


class _FakePipelineRegistry:
    async def get_lakefs_storage(self, *, user_id=None):  # noqa: ANN001
        _ = user_id
        return _FakeStorage()


class _FakeDatasetRegistry:
    async def get_dataset(self, *, dataset_id: str):
        if dataset_id != "ds-1":
            return None
        return SimpleNamespace(
            dataset_id="ds-1",
            db_name="demo_db",
            branch="main",
            name="orders",
            source_type="csv",
        )

    async def get_latest_version(self, *, dataset_id: str):
        if dataset_id != "ds-1":
            return None
        return SimpleNamespace(
            version_id="ver-1",
            dataset_id="ds-1",
            lakefs_commit_id="commit-1",
            artifact_key="s3://raw-datasets/main/demo_db/ds-1/orders.csv",
            sample_json={},
            created_at=datetime(2026, 2, 20, 0, 0, 0, tzinfo=timezone.utc),
        )


@pytest.mark.asyncio
async def test_get_dataset_raw_file_includes_version_metadata() -> None:
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-db-name", b"demo_db"),
                (b"x-project", b"demo_db"),
            ],
        }
    )

    result = await pipeline_datasets_catalog.get_dataset_raw_file(
        dataset_id="ds-1",
        request=request,
        pipeline_registry=_FakePipelineRegistry(),
        dataset_registry=_FakeDatasetRegistry(),
    )

    assert result["status"] == "success"
    payload = result.get("data") if isinstance(result.get("data"), dict) else {}
    file_payload = payload.get("file") if isinstance(payload.get("file"), dict) else {}
    assert file_payload.get("dataset_id") == "ds-1"
    assert file_payload.get("version_id") == "ver-1"
    assert file_payload.get("lakefs_commit_id") == "commit-1"
    assert isinstance(file_payload.get("version_created_at"), str)
