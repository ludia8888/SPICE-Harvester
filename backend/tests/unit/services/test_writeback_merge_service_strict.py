from __future__ import annotations

import pytest

from shared.services.core.writeback_merge_service import WritebackMergeService


class _BaseStorage:
    async def list_command_files(self, *, bucket: str, prefix: str):  # noqa: ANN201
        _ = bucket, prefix
        return ["cmd-1.json"]

    async def replay_instance_state(self, *, bucket: str, command_files: list[str], strict: bool = False):  # noqa: ANN201
        _ = bucket, command_files
        return {"_metadata": {}} if strict else None


class _LakeFSStorage:
    async def load_json(self, *args, **kwargs):  # noqa: ANN003, ANN201
        _ = args, kwargs
        raise FileNotFoundError

    async def iter_objects(self, *args, **kwargs):  # noqa: ANN003, ANN201
        _ = args, kwargs
        if False:
            yield None


@pytest.mark.asyncio
async def test_merge_instance_uses_strict_replay_for_base_state() -> None:
    service = WritebackMergeService(base_storage=_BaseStorage(), lakefs_storage=_LakeFSStorage())

    merged = await service.merge_instance(
        db_name="demo",
        base_branch="main",
        overlay_branch="writeback/main",
        class_id="Order",
        instance_id="order-1",
        writeback_repo="repo",
        writeback_branch="wb",
    )

    assert merged.document["data"]["_metadata"] == {}
