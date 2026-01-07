from __future__ import annotations

import os
import uuid

import pytest

from shared.services.consistency_token import ConsistencyToken, ConsistencyTokenService


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
    assert decoded.projection_lag_ms == token.projection_lag_ms


@pytest.mark.asyncio
async def test_consistency_token_service_creates_metadata() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    service = ConsistencyTokenService(redis_url=redis_url)
    await service.connect()

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
        )

        assert token.command_id == command_id
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
