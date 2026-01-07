from __future__ import annotations

import os
import uuid

import pytest
import redis.asyncio as aioredis

from shared.services.idempotency_service import IdempotencyService


@pytest.mark.asyncio
async def test_idempotency_service_detects_duplicates() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url)

    event_id = uuid.uuid4().hex
    aggregate_id = "agg-123"
    key = f"idempotency:{aggregate_id}:{event_id}"
    service = IdempotencyService(redis_client, ttl_seconds=60)

    try:
        first_duplicate, stored = await service.is_duplicate(
            event_id=event_id,
            event_data={"value": 1},
            aggregate_id=aggregate_id,
        )
        assert first_duplicate is False
        assert stored is None

        second_duplicate, stored = await service.is_duplicate(
            event_id=event_id,
            event_data={"value": 1},
            aggregate_id=aggregate_id,
        )
        assert second_duplicate is True
        assert stored is not None
        assert stored["event_id"] == event_id
        assert stored["aggregate_id"] == aggregate_id
        assert "event_hash" in stored
    finally:
        await redis_client.delete(key)
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_idempotency_service_marks_processed_and_failed() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url)

    event_id = uuid.uuid4().hex
    aggregate_id = "agg-456"
    key = f"idempotency:{aggregate_id}:{event_id}"
    service = IdempotencyService(redis_client, ttl_seconds=60)

    try:
        await service.mark_processed(event_id=event_id, result={"ok": True}, aggregate_id=aggregate_id)
        stored = await redis_client.get(key)
        assert stored is not None

        await service.mark_failed(event_id=event_id, error="boom", aggregate_id=aggregate_id)
        stored_failed = await redis_client.get(key)
        assert stored_failed is not None
    finally:
        await redis_client.delete(key)
        await redis_client.aclose()
