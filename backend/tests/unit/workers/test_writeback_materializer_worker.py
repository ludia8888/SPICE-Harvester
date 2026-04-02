from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.config.app_config import AppConfig
from writeback_materializer_worker import main as materializer_module
from writeback_materializer_worker.main import WritebackMaterializerWorker


class _FakeLakeFSStorage:
    def __init__(self, objects: list[str]) -> None:
        self.objects = objects
        self.saved: list[tuple[str, str, dict]] = []

    async def iter_objects(self, *, bucket: str, prefix: str):
        _ = bucket
        for key in self.objects:
            if key.startswith(prefix):
                yield {"Key": key}

    async def save_json(self, *, bucket: str, key: str, data: dict) -> None:
        self.saved.append((bucket, key, dict(data)))


class _FakeLakeFSClient:
    async def create_branch(self, **kwargs) -> None:  # noqa: ANN003
        return None

    async def commit(self, **kwargs) -> str:  # noqa: ANN003
        return "commit-1"

    async def merge(self, **kwargs) -> None:  # noqa: ANN003
        return None

    async def delete_branch(self, **kwargs) -> None:  # noqa: ANN003
        return None


class _FakeMergeService:
    def __init__(self, *, base_storage, lakefs_storage) -> None:  # noqa: ANN001
        _ = (base_storage, lakefs_storage)

    async def merge_instance(self, **kwargs):  # noqa: ANN003
        instance_id = kwargs["instance_id"]
        lifecycle_id = "lc-success" if instance_id == "ok-1" else "lc-blocked"
        if instance_id == "blocked-1":
            raise RuntimeError("merge failed")
        return SimpleNamespace(
            lifecycle_id=lifecycle_id,
            document={"data": {"instance_id": instance_id}},
            last_ontology_commit_id=None,
        )


@pytest.mark.asyncio
async def test_materializer_watermark_does_not_advance_past_failed_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_name = "demo"
    branch = AppConfig.get_ontology_writeback_branch(db_name)
    objects = [
        f"{branch}/writeback_edits_queue/queue/by_object/Ticket/ok-1/lc-success/10_action-ok.json",
        f"{branch}/writeback_edits_queue/queue/by_object/Ticket/blocked-1/lc-blocked/5_action-blocked.json",
    ]

    worker = WritebackMaterializerWorker()
    worker.lakefs_client = _FakeLakeFSClient()
    worker.lakefs_storage = _FakeLakeFSStorage(objects)
    worker.base_storage = object()

    monkeypatch.setattr(materializer_module, "WritebackMergeService", _FakeMergeService)

    await worker._materialize_db_inner(db_name=db_name)

    manifest_payloads = [
        data for _, key, data in worker.lakefs_storage.saved if key.endswith("/manifest.json")
    ]
    assert manifest_payloads
    manifest = manifest_payloads[-1]
    assert manifest["queue_high_watermark"] == 4
    assert manifest["snapshot_revision"] == 4
