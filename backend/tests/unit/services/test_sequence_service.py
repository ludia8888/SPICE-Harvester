from __future__ import annotations

import fnmatch

import pytest

from shared.services.sequence_service import SequenceService


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, int] = {}

    def _coerce_key(self, key: str | bytes) -> str:
        if isinstance(key, bytes):
            return key.decode()
        return key

    async def incr(self, key: str | bytes) -> int:
        key = self._coerce_key(key)
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def get(self, key: str | bytes):
        key = self._coerce_key(key)
        value = self.store.get(key)
        if value is None:
            return None
        return str(value).encode()

    async def eval(self, _script: str, _numkeys: int, key: str | bytes, sequence: int) -> int:
        key = self._coerce_key(key)
        current = self.store.get(key)
        if current is None or sequence > int(current):
            self.store[key] = int(sequence)
            return 1
        return 0

    async def delete(self, key: str | bytes) -> int:
        key = self._coerce_key(key)
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    async def incrby(self, key: str | bytes, count: int) -> int:
        key = self._coerce_key(key)
        self.store[key] = int(self.store.get(key, 0)) + count
        return self.store[key]

    async def scan_iter(self, match: str):
        for key in list(self.store.keys()):
            if fnmatch.fnmatch(key, match):
                yield key.encode()


@pytest.mark.asyncio
async def test_sequence_service_increments_and_caches() -> None:
    redis = _FakeRedis()
    service = SequenceService(redis, namespace="seq")

    seq1 = await service.get_next_sequence("agg-1")
    seq2 = await service.get_next_sequence("agg-1")

    assert seq1 == 1
    assert seq2 == 2
    assert await service.get_current_sequence("agg-1") == 2

    assert await service.get_current_sequence("missing") == 0


@pytest.mark.asyncio
async def test_sequence_service_set_reset_and_batch() -> None:
    redis = _FakeRedis()
    service = SequenceService(redis, namespace="seq")

    assert await service.set_sequence("agg-1", 5) is True
    assert await service.set_sequence("agg-1", 4) is False
    assert await service.get_current_sequence("agg-1") == 5

    start, end = await service.get_batch_sequences("agg-1", 3)
    assert (start, end) == (6, 8)

    assert await service.reset_aggregate("agg-1") is True
    assert await service.get_current_sequence("agg-1") == 0


@pytest.mark.asyncio
async def test_sequence_service_lists_sequences() -> None:
    redis = _FakeRedis()
    service = SequenceService(redis, namespace="seq")

    await service.get_next_sequence("agg-1")
    await service.get_next_sequence("agg-2")

    all_sequences = await service.get_all_sequences()
    assert all_sequences == {"agg-1": 1, "agg-2": 1}

    filtered = await service.get_all_sequences(pattern="agg-1")
    assert filtered == {"agg-1": 1}
