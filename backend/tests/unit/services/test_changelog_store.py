"""Tests for ChangelogStore."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.services.registries.changelog_store import ChangelogStore


class _FakePool:
    """Minimal asyncpg pool fake."""

    def __init__(self) -> None:
        self._rows: List[Dict[str, Any]] = []
        self._execute_calls: List[Any] = []

    def acquire(self):
        return _FakeConnection(self)


class _FakeConnection:
    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def execute(self, query: str, *args):
        self._pool._execute_calls.append((query, args))

    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        return self._pool._rows

    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        return self._pool._rows[0] if self._pool._rows else None


@pytest.mark.asyncio
async def test_record_changelog() -> None:
    pool = _FakePool()
    store = ChangelogStore(pool)

    result = await store.record_changelog(
        job_id="job-1",
        db_name="test_db",
        target_class_id="Customer",
        execution_mode="incremental",
        added_count=5,
        modified_count=2,
        deleted_count=1,
        total_instances=100,
    )

    assert result["recorded"] is True
    assert result["job_id"] == "job-1"
    assert result["added_count"] == 5
    assert result["modified_count"] == 2
    assert result["deleted_count"] == 1
    assert len(pool._execute_calls) == 1


@pytest.mark.asyncio
async def test_list_changelogs() -> None:
    pool = _FakePool()
    pool._rows = [
        {"changelog_id": "c1", "job_id": "j1", "added_count": 3},
        {"changelog_id": "c2", "job_id": "j2", "added_count": 0},
    ]
    store = ChangelogStore(pool)

    results = await store.list_changelogs(db_name="test_db")
    assert len(results) == 2
    assert results[0]["changelog_id"] == "c1"


@pytest.mark.asyncio
async def test_list_changelogs_with_class_filter() -> None:
    pool = _FakePool()
    pool._rows = [{"changelog_id": "c1", "target_class_id": "Order"}]
    store = ChangelogStore(pool)

    results = await store.list_changelogs(
        db_name="test_db",
        target_class_id="Order",
    )
    assert len(results) == 1


@pytest.mark.asyncio
async def test_get_changelog() -> None:
    pool = _FakePool()
    pool._rows = [{"changelog_id": "c1", "job_id": "j1", "added_count": 10}]
    store = ChangelogStore(pool)

    result = await store.get_changelog(changelog_id="c1")
    assert result is not None
    assert result["added_count"] == 10


@pytest.mark.asyncio
async def test_get_changelog_not_found() -> None:
    pool = _FakePool()
    pool._rows = []
    store = ChangelogStore(pool)

    result = await store.get_changelog(changelog_id="missing")
    assert result is None


@pytest.mark.asyncio
async def test_record_changelog_failure_handled() -> None:
    """ChangelogStore should not raise on DB failure."""

    class _FailPool:
        def acquire(self):
            return _FailConn()

    class _FailConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, *args):
            raise RuntimeError("DB unavailable")

    store = ChangelogStore(_FailPool())
    result = await store.record_changelog(
        job_id="j1",
        db_name="test_db",
        target_class_id="X",
    )
    assert result["recorded"] is False
