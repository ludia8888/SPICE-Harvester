from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

import pytest

from shared.services.registries.lineage_store import LineageStore


class _AcquireCtx:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeConn":
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001, ANN201
        return False


class _FakeConn:
    def __init__(self, rows: Sequence[dict[str, Any]]) -> None:
        self._rows = list(rows)
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *params: Any) -> Sequence[dict[str, Any]]:
        self.fetch_calls.append((query, params))
        return self._rows


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


@pytest.mark.asyncio
async def test_get_latest_edges_for_projections_returns_latest_writer_context() -> None:
    rows = [
        {
            "projection_name": "orders_projection",
            "from_node_id": "event:evt-1",
            "to_node_id": "artifact:es:demo:main:Order/o1",
            "edge_type": "event_materialized_es_document",
            "occurred_at": datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc),
            "recorded_at": datetime(2026, 2, 15, 12, 0, 1, tzinfo=timezone.utc),
            "run_id": None,
            "metadata": {"producer_run_id": "run-42", "producer_service": "projection-worker"},
        },
        {
            "projection_name": "inventory_projection",
            "from_node_id": "event:evt-2",
            "to_node_id": "artifact:es:demo:main:Inventory/i1",
            "edge_type": "event_materialized_es_document",
            "occurred_at": datetime(2026, 2, 15, 12, 1, 0, tzinfo=timezone.utc),
            "recorded_at": datetime(2026, 2, 15, 12, 1, 1, tzinfo=timezone.utc),
            "run_id": "run-99",
            "metadata": {"producer_service": "projection-worker"},
        },
    ]
    conn = _FakeConn(rows)
    store = LineageStore(dsn="postgresql://unused")
    store._pool = _FakePool(conn)  # type: ignore[assignment]

    result = await store.get_latest_edges_for_projections(
        projection_names=["orders_projection", "inventory_projection"],
        db_name="demo",
        branch="main",
    )

    assert sorted(result.keys()) == ["inventory_projection", "orders_projection"]
    assert result["orders_projection"]["event_id"] == "evt-1"
    assert result["orders_projection"]["run_id"] == "run-42"
    assert result["inventory_projection"]["run_id"] == "run-99"
    assert result["inventory_projection"]["edge_type"] == "event_materialized_es_document"
    assert len(conn.fetch_calls) == 1


@pytest.mark.asyncio
async def test_get_latest_edges_for_projections_returns_empty_for_blank_names() -> None:
    conn = _FakeConn([])
    store = LineageStore(dsn="postgresql://unused")
    store._pool = _FakePool(conn)  # type: ignore[assignment]

    result = await store.get_latest_edges_for_projections(projection_names=["", "   "], db_name="demo")

    assert result == {}
    assert conn.fetch_calls == []


@pytest.mark.asyncio
async def test_get_latest_edges_from_returns_latest_per_parent_node() -> None:
    rows = [
        {
            "from_node_id": "agg:Order:o1",
            "to_node_id": "event:evt-10",
            "edge_type": "aggregate_emitted_event",
            "projection_name": "",
            "occurred_at": datetime(2026, 2, 15, 12, 10, 0, tzinfo=timezone.utc),
            "recorded_at": datetime(2026, 2, 15, 12, 10, 1, tzinfo=timezone.utc),
            "metadata": {"db_name": "demo"},
        },
        {
            "from_node_id": "agg:Inventory:i1",
            "to_node_id": "event:evt-20",
            "edge_type": "aggregate_emitted_event",
            "projection_name": "",
            "occurred_at": datetime(2026, 2, 15, 12, 11, 0, tzinfo=timezone.utc),
            "recorded_at": datetime(2026, 2, 15, 12, 11, 1, tzinfo=timezone.utc),
            "metadata": {"db_name": "demo"},
        },
    ]
    conn = _FakeConn(rows)
    store = LineageStore(dsn="postgresql://unused")
    store._pool = _FakePool(conn)  # type: ignore[assignment]

    result = await store.get_latest_edges_from(
        from_node_ids=["agg:Order:o1", "agg:Inventory:i1"],
        edge_type="aggregate_emitted_event",
        db_name="demo",
        branch="main",
    )

    assert sorted(result.keys()) == ["agg:Inventory:i1", "agg:Order:o1"]
    assert result["agg:Order:o1"]["to_node_id"] == "event:evt-10"
    assert result["agg:Inventory:i1"]["to_node_id"] == "event:evt-20"
    assert len(conn.fetch_calls) == 1
