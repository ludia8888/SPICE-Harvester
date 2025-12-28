"""
Integration tests for the Postgres-backed write-side sequence allocator.

Prereq:
- A reachable Postgres (set POSTGRES_URL). Default matches docker-compose.databases.yml.
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest

from shared.services.aggregate_sequence_allocator import AggregateSequenceAllocator, OptimisticConcurrencyError


def _get_postgres_url_candidates() -> list[str]:
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]
    return [
        "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]


DEFAULT_POSTGRES_URL = _get_postgres_url_candidates()[0]
TEST_SCHEMA = os.getenv("AGG_SEQ_ALLOCATOR_TEST_SCHEMA", "spice_event_registry_seq_test")


async def _make_allocator(*, dsn: str, schema: str) -> AggregateSequenceAllocator:
    last_error = None
    for candidate in ([dsn] if (dsn or "").strip() else _get_postgres_url_candidates()):
        alloc = AggregateSequenceAllocator(dsn=candidate, schema=schema, handler_prefix="write_side_test")
        try:
            await alloc.connect()
            return alloc
        except Exception as e:
            last_error = e
            continue
    msg = (
        "Postgres not available for sequence allocator tests "
        f"(candidates={_get_postgres_url_candidates()!r}): {last_error}"
    )
    if os.getenv("SKIP_POSTGRES_TESTS", "").lower() in ("true", "1", "yes", "on"):
        raise RuntimeError(msg)
    pytest.fail(msg)


async def _truncate(alloc: AggregateSequenceAllocator, schema: str) -> None:
    if not alloc._pool:  # pragma: no cover (defensive)
        return
    async with alloc._pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {schema}.aggregate_versions")


async def _reserve_like_event_store(alloc: AggregateSequenceAllocator, *, aggregate_type: str, aggregate_id: str) -> int:
    handler = alloc.handler_for(aggregate_type)
    seq = await alloc.try_reserve_next_sequence(handler=handler, aggregate_id=aggregate_id)
    if seq is not None:
        return int(seq)
    # Seed is 0 here because this test doesn't depend on S3; seeding behavior is tested separately.
    return await alloc.reserve_next_sequence(handler=handler, aggregate_id=aggregate_id, seed_last_sequence=0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocator_concurrent_reservation_is_unique_and_monotonic():
    alloc1 = await _make_allocator(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA)
    alloc2 = await _make_allocator(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA)
    try:
        await _truncate(alloc1, TEST_SCHEMA)

        aggregate_type = "Instance"
        aggregate_id = f"db:class:{uuid4().hex}"

        # Simulate two independent writers racing to reserve sequences.
        results = await asyncio.gather(
            *[
                _reserve_like_event_store(alloc1 if i % 2 == 0 else alloc2, aggregate_type=aggregate_type, aggregate_id=aggregate_id)
                for i in range(50)
            ]
        )

        assert len(results) == 50
        assert len(set(results)) == 50
        assert min(results) == 1
        assert max(results) == 50
    finally:
        await _truncate(alloc1, TEST_SCHEMA)
        await alloc1.close()
        await alloc2.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocator_seeding_starts_after_existing_stream_max():
    alloc = await _make_allocator(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA)
    try:
        await _truncate(alloc, TEST_SCHEMA)

        handler = alloc.handler_for("Instance")
        aggregate_id = f"db:class:{uuid4().hex}"

        first = await alloc.reserve_next_sequence(handler=handler, aggregate_id=aggregate_id, seed_last_sequence=10)
        assert first == 11

        second = await alloc.try_reserve_next_sequence(handler=handler, aggregate_id=aggregate_id)
        assert second == 12
    finally:
        await _truncate(alloc, TEST_SCHEMA)
        await alloc.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocator_catches_up_when_seed_is_ahead_of_db_state():
    alloc = await _make_allocator(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA)
    try:
        await _truncate(alloc, TEST_SCHEMA)

        handler = alloc.handler_for("Instance")
        aggregate_id = f"db:class:{uuid4().hex}"

        # Start normally (new aggregate).
        assert await alloc.reserve_next_sequence(handler=handler, aggregate_id=aggregate_id, seed_last_sequence=0) == 1
        assert await alloc.try_reserve_next_sequence(handler=handler, aggregate_id=aggregate_id) == 2

        # Now simulate discovering a higher S3 max (e.g., DB restored from older backup).
        caught_up = await alloc.reserve_next_sequence(handler=handler, aggregate_id=aggregate_id, seed_last_sequence=20)
        assert caught_up == 21

        assert await alloc.try_reserve_next_sequence(handler=handler, aggregate_id=aggregate_id) == 22
    finally:
        await _truncate(alloc, TEST_SCHEMA)
        await alloc.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocator_occ_reserves_only_when_expected_matches():
    alloc = await _make_allocator(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA)
    try:
        await _truncate(alloc, TEST_SCHEMA)

        handler = alloc.handler_for("Instance")
        aggregate_id = f"db:class:{uuid4().hex}"

        # No row yet -> fast-path returns None.
        assert (
            await alloc.try_reserve_next_sequence_if_expected(
                handler=handler, aggregate_id=aggregate_id, expected_last_sequence=0
            )
            is None
        )

        # Seed + reserve when expected matches.
        assert (
            await alloc.reserve_next_sequence_if_expected(
                handler=handler,
                aggregate_id=aggregate_id,
                seed_last_sequence=0,
                expected_last_sequence=0,
            )
            == 1
        )

        # Next reservation succeeds when expected matches current last_sequence.
        assert (
            await alloc.try_reserve_next_sequence_if_expected(
                handler=handler, aggregate_id=aggregate_id, expected_last_sequence=1
            )
            == 2
        )

        # Mismatch raises conflict.
        with pytest.raises(OptimisticConcurrencyError):
            await alloc.try_reserve_next_sequence_if_expected(
                handler=handler, aggregate_id=aggregate_id, expected_last_sequence=1
            )
    finally:
        await _truncate(alloc, TEST_SCHEMA)
        await alloc.close()
