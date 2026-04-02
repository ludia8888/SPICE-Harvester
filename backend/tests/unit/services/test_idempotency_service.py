from __future__ import annotations

import uuid

import pytest

from shared.services.events.idempotency_service import IdempotencyService
from tests.unit.services.fake_async_redis import FakeAsyncRedis


class _WaitAfterResultRedis(FakeAsyncRedis):
    def __init__(self) -> None:
        super().__init__()
        self.result_key_seen_before_delete = False

    async def delete(self, key: str) -> int:
        if key.endswith(":processing"):
            result_key = key[:-len(":processing")] + ":result"
            self.result_key_seen_before_delete = await self.get(result_key) is not None
        return await super().delete(key)


@pytest.mark.asyncio
async def test_idempotency_service_detects_duplicates() -> None:
    redis_client = FakeAsyncRedis()

    event_id = uuid.uuid4().hex
    aggregate_id = "agg-123"
    processing_key = f"idempotency:{aggregate_id}:{event_id}:processing"
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
        await redis_client.delete(processing_key)
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_idempotency_service_marks_processed_and_failed() -> None:
    redis_client = FakeAsyncRedis()

    event_id = uuid.uuid4().hex
    aggregate_id = "agg-456"
    result_key = f"idempotency:{aggregate_id}:{event_id}:result"
    service = IdempotencyService(redis_client, ttl_seconds=60)

    try:
        await service.mark_processed(event_id=event_id, result={"ok": True}, aggregate_id=aggregate_id)
        stored = await redis_client.get(result_key)
        assert stored is not None

        await service.mark_failed(event_id=event_id, error="boom", aggregate_id=aggregate_id)
        stored_failed = await redis_client.get(result_key)
        assert stored_failed is not None
    finally:
        await redis_client.delete(result_key)
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_idempotency_service_retries_after_failed_processing_lock_is_cleared() -> None:
    redis_client = FakeAsyncRedis()
    event_id = uuid.uuid4().hex
    aggregate_id = "agg-789"
    service = IdempotencyService(redis_client, ttl_seconds=60, processing_ttl_seconds=5)

    try:
        is_duplicate, _stored = await service.is_duplicate(
            event_id=event_id,
            event_data={"value": 1},
            aggregate_id=aggregate_id,
        )
        assert is_duplicate is False

        await service.mark_failed(event_id=event_id, error="boom", aggregate_id=aggregate_id, retry_after=1)
        processing = await redis_client.get(f"idempotency:{aggregate_id}:{event_id}:processing")
        assert processing is None

        status = await service.get_processing_status(event_id=event_id, aggregate_id=aggregate_id)
        assert status is not None
        assert status["status"] == "failed"
    finally:
        await redis_client.delete(f"idempotency:{aggregate_id}:{event_id}:processing")
        await redis_client.delete(f"idempotency:{aggregate_id}:{event_id}:result")
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_idempotency_service_sets_result_before_clearing_processing_lock() -> None:
    redis_client = _WaitAfterResultRedis()
    event_id = uuid.uuid4().hex
    aggregate_id = "agg-race"
    service = IdempotencyService(redis_client, ttl_seconds=60)

    try:
        await service.is_duplicate(
            event_id=event_id,
            event_data={"value": 1},
            aggregate_id=aggregate_id,
        )
        await service.mark_processed(event_id=event_id, result={"ok": True}, aggregate_id=aggregate_id)

        assert redis_client.result_key_seen_before_delete is True
    finally:
        await redis_client.aclose()
