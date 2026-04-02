from __future__ import annotations

from typing import Any

import pytest

from shared.services.registries.processed_event_registry import ClaimDecision, ProcessedEventRegistry


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeConnection:
    def __init__(self, *, lower_inflight: bool = False, lower_inflight_expired: bool = False) -> None:
        self.lower_inflight = lower_inflight
        self.lower_inflight_expired = lower_inflight_expired
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append((sql, args))
        return "OK"

    async def fetchval(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "sequence_number < $3" in sql:
            if self.lower_inflight and not self.lower_inflight_expired:
                return 1
            return None
        if "FROM spice_event_registry.aggregate_versions" in sql:
            return None
        raise AssertionError(f"Unexpected SQL: {sql}")

    async def fetchrow(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "INSERT INTO spice_event_registry.processed_events" in sql:
            return {"attempt_count": 1}
        raise AssertionError(f"Unexpected SQL: {sql}")


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeConnection:
        return self._conn


def _call_index(conn: _FakeConnection, needle: str) -> int:
    for index, (sql, _) in enumerate(conn.calls):
        if needle in sql:
            return index
    raise AssertionError(f"SQL containing {needle!r} was not executed")


@pytest.mark.asyncio
async def test_claim_serializes_aggregate_before_inserting_new_processing_row() -> None:
    conn = _FakeConnection()
    registry = ProcessedEventRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    result = await registry.claim(
        handler="worker",
        event_id="evt-1",
        aggregate_id="agg-1",
        sequence_number=2,
    )

    assert result.decision == ClaimDecision.CLAIMED
    assert _call_index(conn, "pg_advisory_xact_lock") < _call_index(
        conn, "INSERT INTO spice_event_registry.processed_events"
    )


@pytest.mark.asyncio
async def test_claim_returns_in_progress_when_lower_sequence_is_still_processing() -> None:
    conn = _FakeConnection(lower_inflight=True)
    registry = ProcessedEventRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    result = await registry.claim(
        handler="worker",
        event_id="evt-2",
        aggregate_id="agg-1",
        sequence_number=3,
    )

    assert result.decision == ClaimDecision.IN_PROGRESS
    assert not any("INSERT INTO spice_event_registry.processed_events" in sql for sql, _ in conn.calls)


@pytest.mark.asyncio
async def test_claim_ignores_expired_lower_sequence_processing_row() -> None:
    conn = _FakeConnection(lower_inflight=True, lower_inflight_expired=True)
    registry = ProcessedEventRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    result = await registry.claim(
        handler="worker",
        event_id="evt-3",
        aggregate_id="agg-1",
        sequence_number=4,
    )

    assert result.decision == ClaimDecision.CLAIMED
    lower_inflight_call = next(sql for sql, _ in conn.calls if "sequence_number < $3" in sql)
    assert "COALESCE(heartbeat_at, started_at) >= $5" in lower_inflight_call
