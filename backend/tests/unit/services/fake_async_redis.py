from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _to_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode()


@dataclass
class _ValueEntry:
    value: bytes
    ttl_seconds: Optional[int] = None


class FakeAsyncRedis:
    """
    Minimal async Redis stub for unit tests.

    Implements only the subset of the redis.asyncio API used by unit tests:
    - get/set/setex/delete
    - hset/hgetall/expire
    - lpush/ltrim/lrange
    - aclose
    """

    def __init__(self) -> None:
        self._kv: Dict[str, _ValueEntry] = {}
        self._hashes: Dict[str, Dict[str, bytes]] = {}
        self._lists: Dict[str, List[bytes]] = {}

    async def aclose(self) -> None:  # pragma: no cover
        return None

    async def delete(self, key: str) -> int:
        removed = 0
        if key in self._kv:
            del self._kv[key]
            removed += 1
        if key in self._hashes:
            del self._hashes[key]
            removed += 1
        if key in self._lists:
            del self._lists[key]
            removed += 1
        return removed

    async def get(self, key: str) -> Optional[bytes]:
        entry = self._kv.get(key)
        if entry is None:
            return None
        return entry.value

    async def set(self, key: str, value: Any, nx: bool = False, ex: Optional[int] = None) -> bool:
        if nx and key in self._kv:
            return False
        self._kv[key] = _ValueEntry(value=_to_bytes(value), ttl_seconds=ex)
        return True

    async def setex(self, key: str, time: int, value: Any) -> bool:  # noqa: A002
        self._kv[key] = _ValueEntry(value=_to_bytes(value), ttl_seconds=int(time))
        return True

    async def hset(self, key: str, mapping: Dict[str, Any]) -> int:
        existing = self._hashes.setdefault(key, {})
        before = len(existing)
        for field, value in mapping.items():
            existing[str(field)] = _to_bytes(value)
        return len(existing) - before

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        values = self._hashes.get(key) or {}
        return {field.encode(): value for field, value in values.items()}

    async def expire(self, _key: str, _ttl_seconds: int) -> int:
        # TTL is not modeled in the fake; treat as success.
        return 1

    async def lpush(self, key: str, value: Any) -> int:
        lst = self._lists.setdefault(key, [])
        lst.insert(0, _to_bytes(value))
        return len(lst)

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        lst = self._lists.get(key) or []
        stop_inclusive = stop + 1 if stop >= 0 else None
        self._lists[key] = lst[start:stop_inclusive]
        return True

    async def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        lst = self._lists.get(key) or []
        stop_inclusive = stop + 1 if stop >= 0 else None
        return list(lst[start:stop_inclusive])

