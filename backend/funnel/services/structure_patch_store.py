"""Durable structure-analysis patch store backed by Redis."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from shared.config.settings import get_settings
from shared.models.structure_patch import SheetStructurePatch
from shared.services.storage.redis_service import RedisService, create_redis_service

logger = logging.getLogger(__name__)

_PATCH_KEY_PREFIX = "funnel:structure_patch"
_PATCH_REDIS_LOCK = asyncio.Lock()
_PATCH_REDIS_SERVICE: RedisService | None = None


class StructurePatchStoreUnavailableError(RuntimeError):
    """Raised when the durable patch store is unavailable."""


def _patch_key(sheet_signature: str) -> str:
    return f"{_PATCH_KEY_PREFIX}:{sheet_signature}"


async def initialize_patch_store(redis_service: RedisService | None = None) -> RedisService:
    """Initialize the shared Redis-backed patch store."""
    global _PATCH_REDIS_SERVICE

    if redis_service is not None:
        _PATCH_REDIS_SERVICE = redis_service
        return redis_service

    if _PATCH_REDIS_SERVICE is not None:
        return _PATCH_REDIS_SERVICE

    async with _PATCH_REDIS_LOCK:
        if _PATCH_REDIS_SERVICE is not None:
            return _PATCH_REDIS_SERVICE

        service = create_redis_service(get_settings())
        await service.connect()
        _PATCH_REDIS_SERVICE = service
        return service


async def close_patch_store() -> None:
    """Close the shared Redis-backed patch store if it was initialized."""
    global _PATCH_REDIS_SERVICE

    service = _PATCH_REDIS_SERVICE
    _PATCH_REDIS_SERVICE = None
    if service is None:
        return
    await service.disconnect()


async def _require_patch_store() -> RedisService:
    try:
        return await initialize_patch_store()
    except Exception as exc:
        logger.error("Structure patch store unavailable: %s", exc)
        raise StructurePatchStoreUnavailableError("Structure patch store unavailable") from exc


async def get_patch(sheet_signature: str) -> Optional[SheetStructurePatch]:
    redis_service = await _require_patch_store()
    payload = await redis_service.get_json(_patch_key(sheet_signature))
    if not isinstance(payload, dict):
        return None
    return SheetStructurePatch.model_validate(payload)


async def upsert_patch(patch: SheetStructurePatch) -> SheetStructurePatch:
    redis_service = await _require_patch_store()
    await redis_service.set_json(
        _patch_key(patch.sheet_signature),
        patch.model_dump(mode="json"),
    )
    return patch


async def delete_patch(sheet_signature: str) -> bool:
    redis_service = await _require_patch_store()
    return await redis_service.delete(_patch_key(sheet_signature))
