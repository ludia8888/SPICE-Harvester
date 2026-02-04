"""Pipeline dataset lakeFS helpers.

Small, stable helpers extracted from `bff.routers.pipeline_datasets_ops`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import HTTPException, status

from shared.config.settings import get_settings
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.storage.redis_service import create_redis_service_legacy
from shared.utils.path_utils import safe_lakefs_ref
from shared.utils.s3_uri import parse_s3_uri

logger = logging.getLogger(__name__)


async def _acquire_lakefs_commit_lock(
    *,
    repository: str,
    branch: str,
    job_id: Optional[str] = None,
) -> Optional[tuple[Any, str, str]]:
    pipeline_settings = get_settings().pipeline
    if not pipeline_settings.locks_enabled:
        return None
    redis_service = create_redis_service_legacy()
    try:
        await redis_service.connect()
    except Exception as exc:
        if pipeline_settings.locks_required:
            raise
        logger.warning("lakeFS commit locks disabled (redis unavailable): %s", exc)
        return None
    lock_key = f"lakefs-commit-lock:{repository}:{safe_lakefs_ref(branch)}"
    ttl_seconds = pipeline_settings.lock_ttl_seconds
    retry_seconds = pipeline_settings.lock_retry_seconds
    timeout_seconds = pipeline_settings.publish_lock_acquire_timeout_seconds
    token = f"{job_id or 'commit'}:{uuid4().hex}"
    start = time.monotonic()
    while True:
        acquired = await redis_service.client.set(lock_key, token, nx=True, ex=ttl_seconds)
        if acquired:
            return redis_service, lock_key, token
        if time.monotonic() - start >= timeout_seconds:
            await redis_service.disconnect()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Dataset branch is busy; retry after the current upload completes",
            )
        await asyncio.sleep(retry_seconds)


async def _release_lakefs_commit_lock(redis_service: Any, lock_key: str, token: str) -> None:
    script = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('DEL', KEYS[1]) else return 0 end"
    )
    try:
        await redis_service.client.eval(script, 1, lock_key, token)
    except Exception as exc:
        logger.warning("Failed to release lakeFS commit lock (key=%s): %s", lock_key, exc)
    finally:
        await redis_service.disconnect()


def _lakefs_commit_retry_delay(attempt: int) -> float:
    return min(2.0, 0.5 * max(1, attempt))


async def _resolve_lakefs_commit_from_head(
    *,
    lakefs_client: LakeFSClient,
    lakefs_storage_service: Any,
    repository: str,
    branch: str,
    object_key: str,
    expected_checksum: Optional[str],
    attempts: int = 3,
) -> Optional[str]:
    head_commit_id = await lakefs_client.get_branch_head_commit_id(
        repository=repository,
        branch=branch,
    )
    object_path = f"{head_commit_id}/{object_key}"
    for attempt in range(1, max(1, attempts) + 1):
        try:
            object_meta = await lakefs_storage_service.get_object_metadata(
                bucket=repository,
                key=object_path,
            )
        except FileNotFoundError:
            if attempt < attempts:
                await asyncio.sleep(_lakefs_commit_retry_delay(attempt))
                continue
            return None
        except Exception as exc:
            logger.warning(
                "lakeFS head object metadata lookup failed (repo=%s, key=%s): %s",
                repository,
                object_path,
                exc,
            )
            return None
        if expected_checksum:
            stored_checksum = (object_meta.get("metadata") or {}).get("checksum")
            if not stored_checksum or stored_checksum != expected_checksum:
                if attempt < attempts:
                    await asyncio.sleep(_lakefs_commit_retry_delay(attempt))
                    continue
                return None
        return head_commit_id
    return None


def _resolve_lakefs_raw_repository() -> str:
    repo = str(get_settings().storage.lakefs_raw_repository or "").strip()
    return repo or "raw-datasets"


async def _ensure_lakefs_branch_exists(
    *,
    lakefs_client: LakeFSClient,
    repository: str,
    branch: str,
    source_branch: str = "main",
) -> None:
    resolved_branch = safe_lakefs_ref(branch)
    resolved_source = safe_lakefs_ref(source_branch)
    if resolved_branch == resolved_source:
        return
    try:
        await lakefs_client.create_branch(
            repository=repository,
            name=resolved_branch,
            source=resolved_source,
        )
    except LakeFSConflictError:
        return


def _extract_lakefs_ref_from_artifact_key(artifact_key: str) -> str:
    parsed = parse_s3_uri(artifact_key)
    if not parsed:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_key must be an s3:// URI")
    _, key = parsed
    parts = [part for part in str(key).split("/") if part]
    if len(parts) < 2:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_key must include lakeFS ref prefix")
    return parts[0]


async def _commit_lakefs_with_predicate_fallback(
    *,
    lakefs_client: LakeFSClient,
    lakefs_storage_service: Any,
    repository: str,
    branch: str,
    message: str,
    metadata: Optional[Dict[str, Any]],
    object_key: str,
    expected_checksum: Optional[str] = None,
) -> str:
    lock = None
    if repository == _resolve_lakefs_raw_repository():
        job_id = None
        if metadata:
            job_id = str(metadata.get("ingest_request_id") or metadata.get("dataset_id") or "").strip() or None
        lock = await _acquire_lakefs_commit_lock(repository=repository, branch=branch, job_id=job_id)
    try:
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                return await lakefs_client.commit(
                    repository=repository,
                    branch=branch,
                    message=message,
                    metadata=metadata,
                )
            except LakeFSError as exc:
                message_text = str(exc or "")
                lowered = message_text.lower()
                if "predicate failed" not in lowered and "no changes" not in lowered:
                    raise
                head_commit_id = await _resolve_lakefs_commit_from_head(
                    lakefs_client=lakefs_client,
                    lakefs_storage_service=lakefs_storage_service,
                    repository=repository,
                    branch=branch,
                    object_key=object_key,
                    expected_checksum=expected_checksum,
                )
                if head_commit_id:
                    logger.warning(
                        "lakeFS commit %s; using head commit %s for %s/%s",
                        message_text,
                        head_commit_id,
                        repository,
                        object_key,
                    )
                    return head_commit_id
                if attempt < max_attempts:
                    await asyncio.sleep(_lakefs_commit_retry_delay(attempt))
                    continue
                raise RuntimeError(f"lakeFS commit failed: {exc}") from exc
        raise RuntimeError("lakeFS commit failed after retries")
    finally:
        if lock:
            await _release_lakefs_commit_lock(*lock)
