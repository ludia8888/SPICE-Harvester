from __future__ import annotations

from types import SimpleNamespace

import pytest

from projection_worker.main import ProjectionWorker


class _FakeRedisClient:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.lock_key: str | None = None
        self.eval_calls: list[tuple[str, str, str | None]] = []

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, *, ex: int, nx: bool):  # noqa: ANN001
        _ = ex
        if nx and key in self.store:
            return False
        self.store[key] = value
        self.lock_key = key
        return True

    async def setex(self, key: str, ttl: int, value: str) -> None:  # noqa: ANN001
        self.store[key] = value
        if self.lock_key is not None:
            self.store[self.lock_key] = "new-owner-token"

    async def eval(self, script: str, numkeys: int, key: str, token: str):  # noqa: ANN001
        _ = (script, numkeys)
        self.eval_calls.append((key, token, self.store.get(key)))
        if self.store.get(key) == token:
            self.store.pop(key, None)
            return 1
        return 0


class _FakeElasticsearchService:
    async def get_document(self, index_name: str, doc_id: str):  # noqa: ANN001
        return {"label": "Account"}


@pytest.mark.asyncio
async def test_get_class_label_releases_lock_only_when_it_still_owns_it() -> None:
    worker = object.__new__(ProjectionWorker)
    redis_client = _FakeRedisClient()
    worker.redis_service = SimpleNamespace(client=redis_client)
    worker.elasticsearch_service = _FakeElasticsearchService()
    worker.cache_metrics = {
        "negative_cache_hits": 0,
        "cache_hits": 0,
        "lock_acquisitions": 0,
        "cache_misses": 0,
        "elasticsearch_queries": 0,
        "lock_failures": 0,
        "total_lock_wait_time": 0.0,
        "fallback_queries": 0,
    }

    label = await worker._get_class_label("Account", "demo", branch="main")

    assert label == "Account"
    assert redis_client.lock_key is not None
    assert redis_client.store[redis_client.lock_key] == "new-owner-token"
    assert redis_client.eval_calls
