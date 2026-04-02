from __future__ import annotations

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from shared.errors.infra_errors import RegistryUnavailableError
from shared.foundry.temporary_object_set_store import FoundryTemporaryObjectSetStore


class _FakeRedisService:
    def __init__(self) -> None:
        self._values: dict[str, dict] = {}

    async def set_json(self, key: str, value: dict, ttl: int | None = None, ex: int | None = None) -> None:
        _ = ttl, ex
        self._values[key] = dict(value)

    async def get_json(self, key: str) -> dict | None:
        value = self._values.get(key)
        return dict(value) if isinstance(value, dict) else None


class _FailingRedisService:
    async def set_json(self, key: str, value: dict, ttl: int | None = None, ex: int | None = None) -> None:
        _ = key, value, ttl, ex
        raise RedisConnectionError("redis down")

    async def get_json(self, key: str) -> dict | None:
        _ = key
        raise RedisConnectionError("redis down")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_temporary_object_set_store_round_trip() -> None:
    store = FoundryTemporaryObjectSetStore(redis_service=_FakeRedisService(), ttl_seconds=60)

    rid = await store.create({"objectType": "Order", "where": {"field": "status", "value": "ACTIVE"}})
    loaded = await store.get(rid)

    assert rid.startswith("ri.object-set.main.versioned-object-set.")
    assert loaded["objectType"] == "Order"
    assert loaded["where"]["field"] == "status"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_temporary_object_set_store_wraps_redis_failures() -> None:
    store = FoundryTemporaryObjectSetStore(redis_service=_FailingRedisService(), ttl_seconds=60)

    with pytest.raises(RegistryUnavailableError):
        await store.create({"objectType": "Order"})

    with pytest.raises(RegistryUnavailableError):
        await store.get("ri.object-set.main.versioned-object-set.demo")
