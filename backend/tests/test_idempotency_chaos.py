"""
Chaos tests for the idempotency contract.

These tests focus on the "last line of defense":
- Durable idempotency registry (processed_events)
- Lease/heartbeat recovery from worker crashes
- Per-aggregate ordering via sequence_number (stale guard + monotonic advance)

Prereq:
- A reachable Postgres (set POSTGRES_URL). Default matches docker-compose.databases.yml.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import contextmanager
from uuid import uuid4

import pytest
from typing import Optional

from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry


@contextmanager
def _set_env(**updates):
    original = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _get_postgres_url_candidates() -> list[str]:
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]
    return [
        "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]


DEFAULT_POSTGRES_URL = _get_postgres_url_candidates()[0]
TEST_SCHEMA = os.getenv("PROCESSED_EVENT_REGISTRY_TEST_SCHEMA", "spice_event_registry_test")


async def _make_registry(*, dsn: str, schema: str, lease_timeout_seconds: int) -> ProcessedEventRegistry:
    last_error: Optional[Exception] = None
    for candidate in ([dsn] if (dsn or "").strip() else _get_postgres_url_candidates()):
        reg = ProcessedEventRegistry(
            dsn=candidate,
            schema=schema,
            lease_timeout_seconds=lease_timeout_seconds,
        )
        try:
            await reg.connect()
            return reg
        except Exception as e:
            last_error = e
            continue
    msg = (
        "Postgres not available for idempotency chaos tests "
        f"(candidates={_get_postgres_url_candidates()!r}): {last_error}"
    )
    if os.getenv("SKIP_POSTGRES_TESTS", "").lower() in ("true", "1", "yes", "on"):
        pytest.skip(msg)
    pytest.fail(msg)


async def _truncate(reg: ProcessedEventRegistry, schema: str) -> None:
    if not reg._pool:  # pragma: no cover (defensive)
        return
    async with reg._pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {schema}.processed_events, {schema}.aggregate_versions")


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_registry_duplicate_delivery_causes_one_side_effect():
    with _set_env(
        PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=None,
        PROCESSED_EVENT_PG_POOL_MIN="1",
        PROCESSED_EVENT_PG_POOL_MAX="2",
    ):
        reg = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=5)
        try:
            await _truncate(reg, TEST_SCHEMA)

            handler = "chaos:worker"
            event_id = f"evt-{uuid4().hex}"
            aggregate_id = f"agg-{uuid4().hex}"
            seq = 1

            # First delivery -> claim + side-effect + mark_done
            first = await reg.claim(
                handler=handler,
                event_id=event_id,
                aggregate_id=aggregate_id,
                sequence_number=seq,
            )
            assert first.decision == ClaimDecision.CLAIMED
            await reg.mark_done(
                handler=handler,
                event_id=event_id,
                aggregate_id=aggregate_id,
                sequence_number=seq,
            )

            # Redeliveries (publisher dup / kafka redelivery / restart) must be no-op.
            for _ in range(5):
                again = await reg.claim(
                    handler=handler,
                    event_id=event_id,
                    aggregate_id=aggregate_id,
                    sequence_number=seq,
                )
                assert again.decision == ClaimDecision.DUPLICATE_DONE

            async with reg._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    SELECT status, attempt_count
                    FROM {TEST_SCHEMA}.processed_events
                    WHERE handler = $1 AND event_id = $2
                    """,
                    handler,
                    event_id,
                )
                assert row is not None
                assert row["status"] == "done"
                assert int(row["attempt_count"]) == 1
        finally:
            await _truncate(reg, TEST_SCHEMA)
            await reg.close()


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_registry_reclaims_stuck_processing_after_lease_timeout():
    with _set_env(
        PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=None,
        PROCESSED_EVENT_PG_POOL_MIN="1",
        PROCESSED_EVENT_PG_POOL_MAX="2",
    ):
        reg1 = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=1)
        reg2 = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=1)
        try:
            await _truncate(reg1, TEST_SCHEMA)
            handler = "chaos:worker"
            event_id = f"evt-{uuid4().hex}"
            aggregate_id = f"agg-{uuid4().hex}"
            seq = 1

            claimed = await reg1.claim(
                handler=handler,
                event_id=event_id,
                aggregate_id=aggregate_id,
                sequence_number=seq,
            )
            assert claimed.decision == ClaimDecision.CLAIMED

            # Simulate crash: leave status=processing but make heartbeat stale so another worker can reclaim.
            async with reg1._pool.acquire() as conn:
                await conn.execute(
                    f"""
                    UPDATE {TEST_SCHEMA}.processed_events
                    SET heartbeat_at = NOW() - INTERVAL '30 seconds'
                    WHERE handler = $1 AND event_id = $2
                    """,
                    handler,
                    event_id,
                )

            reclaimed = await reg2.claim(
                handler=handler,
                event_id=event_id,
                aggregate_id=aggregate_id,
                sequence_number=seq,
            )
            assert reclaimed.decision == ClaimDecision.CLAIMED
            assert reclaimed.attempt_count == 2

            await reg2.mark_done(
                handler=handler,
                event_id=event_id,
                aggregate_id=aggregate_id,
                sequence_number=seq,
            )

            async with reg1._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    SELECT status, owner, attempt_count
                    FROM {TEST_SCHEMA}.processed_events
                    WHERE handler = $1 AND event_id = $2
                    """,
                    handler,
                    event_id,
                )
                assert row is not None
                assert row["status"] == "done"
                assert row["owner"] == reg2._owner
                assert int(row["attempt_count"]) == 2
        finally:
            await _truncate(reg1, TEST_SCHEMA)
            await reg1.close()
            await reg2.close()


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_registry_concurrent_claim_has_single_winner():
    with _set_env(
        PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=None,
        PROCESSED_EVENT_PG_POOL_MIN="1",
        PROCESSED_EVENT_PG_POOL_MAX="2",
    ):
        reg1 = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=10)
        reg2 = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=10)
        try:
            await _truncate(reg1, TEST_SCHEMA)
            handler = "chaos:worker"
            event_id = f"evt-{uuid4().hex}"
            aggregate_id = f"agg-{uuid4().hex}"
            seq = 1

            r1, r2 = await asyncio.gather(
                reg1.claim(
                    handler=handler,
                    event_id=event_id,
                    aggregate_id=aggregate_id,
                    sequence_number=seq,
                ),
                reg2.claim(
                    handler=handler,
                    event_id=event_id,
                    aggregate_id=aggregate_id,
                    sequence_number=seq,
                ),
            )

            winners = [r for r in (r1, r2) if r.decision == ClaimDecision.CLAIMED]
            assert len(winners) == 1

            losers = [r for r in (r1, r2) if r.decision != ClaimDecision.CLAIMED]
            assert len(losers) == 1
            assert losers[0].decision == ClaimDecision.IN_PROGRESS

            # Cleanup: only the winner (owner) can release the lease.
            if r1.decision == ClaimDecision.CLAIMED:
                await reg1.mark_failed(handler=handler, event_id=event_id, error="cleanup")
            else:
                await reg2.mark_failed(handler=handler, event_id=event_id, error="cleanup")
        finally:
            await _truncate(reg1, TEST_SCHEMA)
            await reg1.close()
            await reg2.close()


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_registry_mark_failed_owner_mismatch_raises():
    with _set_env(
        PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=None,
        PROCESSED_EVENT_PG_POOL_MIN="1",
        PROCESSED_EVENT_PG_POOL_MAX="2",
    ):
        reg1 = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=10)
        reg2 = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=10)
        try:
            await _truncate(reg1, TEST_SCHEMA)

            handler = "chaos:worker"
            event_id = f"evt-{uuid4().hex}"

            claim = await reg1.claim(handler=handler, event_id=event_id)
            assert claim.decision == ClaimDecision.CLAIMED

            with pytest.raises(RuntimeError):
                await reg2.mark_failed(handler=handler, event_id=event_id, error="boom")

            await reg1.mark_failed(handler=handler, event_id=event_id, error="cleanup")
        finally:
            await _truncate(reg1, TEST_SCHEMA)
            await reg1.close()
            await reg2.close()


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_registry_sequence_guard_is_monotonic():
    with _set_env(
        PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=None,
        PROCESSED_EVENT_PG_POOL_MIN="1",
        PROCESSED_EVENT_PG_POOL_MAX="2",
    ):
        reg = await _make_registry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA, lease_timeout_seconds=10)
        try:
            await _truncate(reg, TEST_SCHEMA)

            handler = "chaos:worker"
            aggregate_id = f"agg-{uuid4().hex}"

            # Two events can be in-flight concurrently; the later mark_done must not regress the version.
            e10 = f"evt-{uuid4().hex}"
            e11 = f"evt-{uuid4().hex}"

            c10 = await reg.claim(handler=handler, event_id=e10, aggregate_id=aggregate_id, sequence_number=10)
            c11 = await reg.claim(handler=handler, event_id=e11, aggregate_id=aggregate_id, sequence_number=11)
            assert c10.decision == ClaimDecision.CLAIMED
            assert c11.decision == ClaimDecision.CLAIMED

            # Out-of-order completion: 11 completes before 10.
            await reg.mark_done(handler=handler, event_id=e11, aggregate_id=aggregate_id, sequence_number=11)
            await reg.mark_done(handler=handler, event_id=e10, aggregate_id=aggregate_id, sequence_number=10)

            async with reg._pool.acquire() as conn:
                last_seq = await conn.fetchval(
                    f"""
                    SELECT last_sequence
                    FROM {TEST_SCHEMA}.aggregate_versions
                    WHERE handler = $1 AND aggregate_id = $2
                    """,
                    handler,
                    aggregate_id,
                )
                assert int(last_seq) == 11

            # Now an older event must be rejected as stale (guardrail against out-of-order delivery).
            stale = await reg.claim(
                handler=handler,
                event_id=f"evt-{uuid4().hex}",
                aggregate_id=aggregate_id,
                sequence_number=10,
            )
            assert stale.decision == ClaimDecision.STALE
        finally:
            await _truncate(reg, TEST_SCHEMA)
            await reg.close()


class _FakeRedisService:
    def __init__(self):
        self._status: dict[str, dict] = {}
        self._result: dict[str, dict] = {}

    async def set_command_status(self, command_id: str, status: str, data=None, ttl: int = 86400) -> None:
        self._status[command_id] = {
            "status": status,
            "updated_at": "now",
            "data": data or {},
        }

    async def get_command_status(self, command_id: str):
        return self._status.get(command_id)

    async def set_command_result(self, command_id: str, result: dict, ttl: int = 86400) -> None:
        self._result[command_id] = result

    async def get_command_result(self, command_id: str):
        return self._result.get(command_id)

    async def publish_command_update(self, _command_id: str, _data: dict) -> None:
        return

    async def keys(self, _pattern: str):
        return []


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_command_status_endpoint_exposes_failure_reason():
    from oms.routers.command_status import get_command_status
    from shared.services.command_status_service import CommandStatusService
    from shared.models.commands import CommandStatus
    from uuid import UUID

    redis = _FakeRedisService()
    svc = CommandStatusService(redis)

    command_id = str(uuid4())
    await svc.create_command_status(
        command_id=command_id,
        command_type="TEST_COMMAND",
        aggregate_id="db:class:instance",
        payload={"x": 1},
        user_id="tester",
    )
    await svc.start_processing(command_id=command_id, worker_id="worker-1")
    await svc.fail_command(command_id=command_id, error="boom")

    out = await get_command_status(command_id=command_id, command_status_service=svc)
    assert out.command_id == UUID(command_id)
    assert out.status == CommandStatus.FAILED
    assert out.error == "boom"
    assert isinstance(out.result, dict)
    assert out.result.get("aggregate_id") == "db:class:instance"
    assert out.result.get("history"), "status history should be present for observability"


@pytest.mark.chaos
def test_event_store_rejects_event_id_reuse_with_different_command_payload():
    from oms.services.event_store import EventStore
    from shared.models.event_envelope import EventEnvelope

    store = EventStore()

    base = EventEnvelope(
        event_id="cmd-123",
        event_type="CREATE_INSTANCE_REQUESTED",
        aggregate_type="Instance",
        aggregate_id="db:class:instance",
        data={"payload": {"x": 1}},
        metadata={"kind": "command", "command_id": "cmd-123"},
        sequence_number=1,
    )

    # Kafka routing metadata is allowed to differ (backfilled without changing command semantics).
    same_semantics = base.model_copy(deep=True)
    same_semantics.metadata = {**same_semantics.metadata, "kafka_topic": "instance-commands"}
    store._enforce_idempotency_contract(base, same_semantics, source="test")

    # Different payload with the same event_id must be detected (otherwise retries can silently no-op).
    different = base.model_copy(deep=True)
    different.data = {"payload": {"x": 2}}

    with _set_env(EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE=None):
        with pytest.raises(RuntimeError, match="event_id reuse"):
            store._enforce_idempotency_contract(base, different, source="test")

    with _set_env(EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE="warn"):
        store._enforce_idempotency_contract(base, different, source="test")
