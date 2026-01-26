from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional


logger = logging.getLogger(__name__)


class PipelineLockError(RuntimeError):
    pass


class PipelineLock:
    def __init__(
        self,
        *,
        redis_client: Any,
        key: str,
        token: str,
        ttl_seconds: int,
        renew_seconds: int,
    ) -> None:
        self._redis = redis_client
        self._key = key
        self._token = token
        self._ttl_seconds = ttl_seconds
        self._renew_seconds = max(0, int(renew_seconds))
        self._renew_task: Optional[asyncio.Task] = None
        self._lost = False

    async def start(self) -> None:
        if self._renew_seconds <= 0:
            return
        if self._renew_task:
            return
        self._renew_task = asyncio.create_task(self._renew_loop())

    def raise_if_lost(self) -> None:
        if self._lost:
            raise PipelineLockError(f"Pipeline lock lost: {self._key}")

    async def release(self) -> None:
        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('DEL', KEYS[1]) else return 0 end"
        )
        try:
            await self._redis.eval(script, 1, self._key, self._token)
        except Exception as exc:
            logger.warning("Failed to release pipeline lock (key=%s): %s", self._key, exc)

    async def _renew_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._renew_seconds)
                ok = await self._extend()
                if not ok:
                    self._lost = True
                    logger.error("Pipeline lock lost during renewal (key=%s)", self._key)
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._lost = True
                logger.error("Pipeline lock renewal failed (key=%s): %s", self._key, exc)
                return

    async def _extend(self) -> bool:
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
        )
        result = await self._redis.eval(script, 1, self._key, self._token, self._ttl_seconds)
        return bool(result)
