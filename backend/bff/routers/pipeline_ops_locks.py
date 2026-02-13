"""Pipeline Builder publish locks.

Extracted from `bff.routers.pipeline_ops`.
"""


import asyncio
import logging
import time
from typing import Any, Optional
from uuid import uuid4

from fastapi import HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.config.settings import get_settings
from shared.services.storage.redis_service import create_redis_service
from shared.utils.path_utils import safe_lakefs_ref

logger = logging.getLogger(__name__)


async def _acquire_pipeline_publish_lock(
    *,
    pipeline_id: str,
    branch: str,
    job_id: str,
) -> Optional[tuple[Any, str, str]]:
    settings = get_settings()
    pipeline_settings = settings.pipeline
    if not pipeline_settings.locks_enabled:
        return None
    redis_service = create_redis_service(settings)
    try:
        await redis_service.connect()
    except Exception as exc:
        if pipeline_settings.locks_required:
            raise
        logger.warning("Pipeline locks disabled (redis unavailable): %s", exc)
        return None
    lock_key = f"pipeline-lock:{pipeline_id}:{safe_lakefs_ref(branch)}"
    ttl_seconds = pipeline_settings.lock_ttl_seconds
    retry_seconds = pipeline_settings.lock_retry_seconds
    timeout_seconds = pipeline_settings.publish_lock_acquire_timeout_seconds
    token = f"{job_id}:{uuid4().hex}"
    start = time.monotonic()
    while True:
        acquired = await redis_service.client.set(lock_key, token, nx=True, ex=ttl_seconds)
        if acquired:
            return redis_service, lock_key, token
        if time.monotonic() - start >= timeout_seconds:
            await redis_service.disconnect()
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "Pipeline branch is busy; retry after the current publish completes",
                code=ErrorCode.CONFLICT,
            )
        await asyncio.sleep(retry_seconds)


async def _release_pipeline_publish_lock(redis_service: Any, lock_key: str, token: str) -> None:
    script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
    try:
        await redis_service.client.eval(script, 1, lock_key, token)
    except Exception as exc:
        logger.warning("Failed to release pipeline publish lock (key=%s): %s", lock_key, exc)
    finally:
        await redis_service.disconnect()
