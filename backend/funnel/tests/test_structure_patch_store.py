from __future__ import annotations

import pytest

from funnel.services import structure_patch_store
from shared.models.structure_patch import SheetStructurePatch


class _FakeRedisService:
    def __init__(self) -> None:
        self.data: dict[str, dict] = {}
        self.disconnected = False

    async def set_json(self, key: str, value: dict, ttl: int | None = None, ex: int | None = None) -> None:
        _ = ttl, ex
        self.data[key] = dict(value)

    async def get_json(self, key: str) -> dict | None:
        value = self.data.get(key)
        return dict(value) if value is not None else None

    async def delete(self, key: str) -> bool:
        return self.data.pop(key, None) is not None

    async def disconnect(self) -> None:
        self.disconnected = True


@pytest.mark.asyncio
async def test_structure_patch_store_round_trips_via_shared_redis_backend() -> None:
    redis_service = _FakeRedisService()
    await structure_patch_store.initialize_patch_store(redis_service)
    patch = SheetStructurePatch(
        sheet_signature="sheet-1",
        ops=[{"op": "remove_table", "table_id": "t1"}],
    )

    stored = await structure_patch_store.upsert_patch(patch)
    loaded = await structure_patch_store.get_patch("sheet-1")
    deleted = await structure_patch_store.delete_patch("sheet-1")
    missing = await structure_patch_store.get_patch("sheet-1")

    assert stored == patch
    assert loaded == patch
    assert deleted is True
    assert missing is None

    await structure_patch_store.close_patch_store()
    assert redis_service.disconnected is True


@pytest.mark.asyncio
async def test_structure_patch_store_raises_when_backend_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    await structure_patch_store.close_patch_store()

    async def _fail_init(redis_service=None):  # type: ignore[no-untyped-def]
        _ = redis_service
        raise RuntimeError("redis down")

    monkeypatch.setattr(structure_patch_store, "initialize_patch_store", _fail_init)

    with pytest.raises(structure_patch_store.StructurePatchStoreUnavailableError):
        await structure_patch_store.get_patch("sheet-1")
