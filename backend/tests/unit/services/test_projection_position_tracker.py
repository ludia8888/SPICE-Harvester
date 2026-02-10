"""Tests for ProjectionPositionTracker."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from shared.services.core.projection_position_tracker import ProjectionPositionTracker


class _FakePool:
    """Minimal asyncpg pool fake for position tracker tests."""

    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}
        self._execute_calls: List[Any] = []

    def _key(self, name: str, db: str, branch: str) -> str:
        return f"{name}:{db}:{branch}"

    def acquire(self):
        return _FakeConn(self)


class _FakeConn:
    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def execute(self, query: str, *args):
        self._pool._execute_calls.append((query, args))
        # Simulate UPSERT for update_position
        if "INSERT INTO projection_positions" in query:
            name, db, branch, seq = args[0], args[1], args[2], args[3]
            key = self._pool._key(name, db, branch)
            existing = self._pool._store.get(key, {})
            existing_seq = existing.get("last_sequence", 0)
            self._pool._store[key] = {
                "projection_name": name,
                "db_name": db,
                "branch": branch,
                "last_sequence": max(existing_seq, seq),
                "last_event_id": args[4] if len(args) > 4 else None,
                "last_job_id": args[5] if len(args) > 5 else None,
            }
        elif "DELETE FROM projection_positions" in query:
            name, db, branch = args[0], args[1], args[2]
            key = self._pool._key(name, db, branch)
            self._pool._store.pop(key, None)

    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        if "projection_positions" in query and len(args) >= 3:
            key = self._pool._key(args[0], args[1], args[2])
            return self._pool._store.get(key)
        return None

    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        results = list(self._pool._store.values())
        if args and args[0]:  # db_name filter
            results = [r for r in results if r.get("db_name") == args[0]]
        return results[:100]


@pytest.mark.asyncio
async def test_get_position_empty() -> None:
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    result = await tracker.get_position(
        projection_name="instances",
        db_name="test_db",
    )
    assert result is None


@pytest.mark.asyncio
async def test_update_and_get_position() -> None:
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    ok = await tracker.update_position(
        projection_name="instances",
        db_name="test_db",
        last_sequence=42,
        last_event_id="evt-1",
    )
    assert ok is True

    result = await tracker.get_position(
        projection_name="instances",
        db_name="test_db",
    )
    assert result is not None
    assert result["last_sequence"] == 42
    assert result["last_event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_update_position_monotonic() -> None:
    """Sequence should only increase, never decrease."""
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    await tracker.update_position(
        projection_name="instances",
        db_name="test_db",
        last_sequence=100,
    )
    await tracker.update_position(
        projection_name="instances",
        db_name="test_db",
        last_sequence=50,  # Lower — should be ignored by GREATEST()
    )

    result = await tracker.get_position(
        projection_name="instances",
        db_name="test_db",
    )
    assert result["last_sequence"] == 100  # Should stay at 100


@pytest.mark.asyncio
async def test_reset_position() -> None:
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    await tracker.update_position(
        projection_name="ontologies",
        db_name="test_db",
        last_sequence=99,
    )
    ok = await tracker.reset_position(
        projection_name="ontologies",
        db_name="test_db",
    )
    assert ok is True

    result = await tracker.get_position(
        projection_name="ontologies",
        db_name="test_db",
    )
    assert result is None


@pytest.mark.asyncio
async def test_list_positions() -> None:
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    await tracker.update_position(
        projection_name="instances",
        db_name="test_db",
        last_sequence=10,
    )
    await tracker.update_position(
        projection_name="ontologies",
        db_name="test_db",
        last_sequence=20,
    )

    results = await tracker.list_positions(db_name="test_db")
    assert len(results) == 2


@pytest.mark.asyncio
async def test_compute_lag() -> None:
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    await tracker.update_position(
        projection_name="instances",
        db_name="test_db",
        last_sequence=500,
    )

    lag = await tracker.compute_lag(
        projection_name="instances",
        db_name="test_db",
        current_sequence=750,
    )
    assert lag["lag"] == 250
    assert lag["healthy"] is True


@pytest.mark.asyncio
async def test_compute_lag_unhealthy() -> None:
    pool = _FakePool()
    tracker = ProjectionPositionTracker(pool)

    # No position set — lag from 0
    lag = await tracker.compute_lag(
        projection_name="instances",
        db_name="test_db",
        current_sequence=5000,
    )
    assert lag["lag"] == 5000
    assert lag["healthy"] is False
