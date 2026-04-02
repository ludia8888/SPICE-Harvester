from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from shared.services.registries.lineage_store import LineageStore


class _FakeConnection:
    def __init__(self, *, resolved_edge_id) -> None:  # noqa: ANN001
        self.resolved_edge_id = resolved_edge_id
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def fetchrow(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        return {"edge_id": self.resolved_edge_id}


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeConnection:
        return self._conn


@pytest.mark.asyncio
async def test_insert_edge_returns_existing_edge_id_when_conflict_reuses_row() -> None:
    requested_edge_id = uuid4()
    existing_edge_id = uuid4()
    conn = _FakeConnection(resolved_edge_id=existing_edge_id)
    store = LineageStore(dsn="postgresql://example.invalid/db")
    store._pool = _FakePool(conn)

    resolved = await store.insert_edge(
        edge_id=requested_edge_id,
        from_node_id="event:evt-1",
        to_node_id="artifact:s3:bucket:path/file.json",
        edge_type="event_stored_in_object_store",
    )

    assert resolved == existing_edge_id
    assert conn.calls
    assert "UNION ALL" in conn.calls[0][0]
