from __future__ import annotations

from typing import Any, Dict
from uuid import uuid4

from redis.exceptions import RedisError

from shared.errors.infra_errors import RegistryUnavailableError
from shared.services.storage.redis_service import RedisService

_TEMP_OBJECT_SET_KEY_PREFIX = "foundry:temp-object-set"


class FoundryTemporaryObjectSetStore:
    def __init__(
        self,
        *,
        redis_service: RedisService,
        ttl_seconds: int = 3600,
        key_prefix: str = _TEMP_OBJECT_SET_KEY_PREFIX,
    ) -> None:
        self._redis_service = redis_service
        self._ttl_seconds = max(1, int(ttl_seconds))
        self._key_prefix = str(key_prefix).strip() or _TEMP_OBJECT_SET_KEY_PREFIX

    @staticmethod
    def build_rid() -> str:
        return f"ri.object-set.main.versioned-object-set.{uuid4()}"

    def _key(self, rid: str) -> str:
        return f"{self._key_prefix}:{rid}"

    def _wrap_redis_error(self, exc: RedisError, *, operation: str) -> RegistryUnavailableError:
        return RegistryUnavailableError(
            "Foundry temporary object-set store unavailable",
            registry="foundry_temporary_object_sets",
            operation=operation,
            cause=exc,
        )

    async def create(self, object_set: Dict[str, Any]) -> str:
        rid = self.build_rid()
        try:
            await self._redis_service.set_json(self._key(rid), dict(object_set), ttl=self._ttl_seconds)
        except RedisError as exc:
            raise self._wrap_redis_error(exc, operation="create") from exc
        return rid

    async def get(self, rid: str) -> Dict[str, Any]:
        normalized_rid = str(rid or "").strip()
        if not normalized_rid:
            raise ValueError("objectSetRid is required")
        try:
            object_set = await self._redis_service.get_json(self._key(normalized_rid))
        except RedisError as exc:
            raise self._wrap_redis_error(exc, operation="get") from exc
        if object_set is None:
            raise LookupError(f"ObjectSet not found: {normalized_rid}")
        if not isinstance(object_set, dict):
            raise ValueError("Stored object set payload is invalid")
        return dict(object_set)
