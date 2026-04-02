from __future__ import annotations

import uuid

import pytest

from shared.config.search_config import get_instances_index_name
from shared.services.core.consistency_token import ConsistencyToken, ConsistencyTokenService
from tests.unit.services.fake_async_redis import FakeAsyncRedis


@pytest.mark.asyncio
async def test_token_roundtrip() -> None:
    token = ConsistencyToken(
        command_id="command-1234567890",
        timestamp="2024-01-01T00:00:00+00:00",
        sequence_number=10,
        aggregate_id="agg-1",
        version=2,
        projection_lag_ms=123,
    )

    encoded = token.to_string()
    decoded = ConsistencyToken.from_string(encoded)

    assert decoded.command_id == token.command_id[:8]
    assert decoded.sequence_number == token.sequence_number
    assert decoded.aggregate_id == token.aggregate_id
    assert decoded.version == token.version
    assert decoded.db_name == token.db_name
    assert decoded.branch == token.branch
    assert decoded.projection_lag_ms == token.projection_lag_ms


@pytest.mark.asyncio
async def test_consistency_token_service_creates_metadata() -> None:
    service = ConsistencyTokenService(redis_url="redis://localhost:6379")
    await service.connect()
    service.redis_client = FakeAsyncRedis()

    command_id = uuid.uuid4().hex
    abbreviated = command_id[:8]
    projection_key = "projection:avg_lag_ms"

    try:
        await service.redis_client.set(projection_key, "250")
        token = await service.create_token(
            command_id=command_id,
            aggregate_id="agg-1",
            sequence_number=5,
            version=1,
            db_name="demo_db",
            branch="feature/replay",
        )

        assert token.command_id == command_id
        assert token.db_name == "demo_db"
        assert token.branch == "feature/replay"
        assert token.projection_lag_ms == 250

        stored_short = await service.redis_client.get(f"consistency_token:{abbreviated}")
        stored_full = await service.redis_client.get(f"consistency_token:{command_id}")
        assert stored_short is not None
        assert stored_full is not None
    finally:
        if service.redis_client:
            await service.redis_client.delete(projection_key)
            await service.redis_client.delete(f"consistency_token:{abbreviated}")
            await service.redis_client.delete(f"consistency_token:{command_id}")
        await service.disconnect()


class _ESClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def get(self, *, index: str, id: str):  # noqa: A002
        self.calls.append((index, id))
        return {"_source": {"version": 2, "event_sequence": 10}}


@pytest.mark.asyncio
async def test_wait_for_consistency_uses_resolved_instances_index() -> None:
    service = ConsistencyTokenService()
    token = ConsistencyToken(
        command_id="command-1234567890",
        timestamp="2024-01-01T00:00:00+00:00",
        sequence_number=10,
        aggregate_id="agg-1",
        version=2,
        db_name="demo_db",
        branch="feature/replay",
        projection_lag_ms=0,
    )
    es_client = _ESClient()

    success, _ = await service.wait_for_consistency(token, es_client, max_wait_ms=10)

    assert success is True
    assert es_client.calls == [(get_instances_index_name("demo_db", branch="feature/replay"), "agg-1")]


@pytest.mark.asyncio
async def test_wait_for_consistency_requires_index_context_without_db_name() -> None:
    service = ConsistencyTokenService()
    token = ConsistencyToken(
        command_id="command-1234567890",
        timestamp="2024-01-01T00:00:00+00:00",
        sequence_number=10,
        aggregate_id="agg-1",
        version=2,
        projection_lag_ms=0,
    )

    with pytest.raises(ValueError, match="missing db_name"):
        await service.wait_for_consistency(token, _ESClient(), max_wait_ms=10)
