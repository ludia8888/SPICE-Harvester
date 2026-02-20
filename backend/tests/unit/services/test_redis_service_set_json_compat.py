from __future__ import annotations

import pytest

from shared.services.storage.redis_service import RedisService


class _FakeRedisClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self.calls.append(("setex", (key, ttl, value)))
        return True

    async def set(self, key: str, value: str) -> bool:
        self.calls.append(("set", (key, value)))
        return True


@pytest.mark.asyncio
async def test_set_json_accepts_ex_alias_for_ttl() -> None:
    service = RedisService()
    fake = _FakeRedisClient()
    service._client = fake  # noqa: SLF001 - test seam for injected fake client

    await service.set_json("k1", {"a": 1}, ex=30)

    assert fake.calls
    assert fake.calls[0][0] == "setex"
    assert fake.calls[0][1][0] == "k1"
    assert fake.calls[0][1][1] == 30


@pytest.mark.asyncio
async def test_set_json_prefers_ttl_over_ex_when_both_provided() -> None:
    service = RedisService()
    fake = _FakeRedisClient()
    service._client = fake  # noqa: SLF001 - test seam for injected fake client

    await service.set_json("k2", {"b": 2}, ttl=10, ex=99)

    assert fake.calls
    assert fake.calls[0][0] == "setex"
    assert fake.calls[0][1][1] == 10
