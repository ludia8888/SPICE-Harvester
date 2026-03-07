from __future__ import annotations

from typing import Any, Optional

import pytest

from shared.services.registries.connector_registry import ConnectorRegistry


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeConnection:
    def __init__(self, *, materialized_state: Optional[dict[str, Any]]) -> None:
        self._materialized_state = dict(materialized_state) if materialized_state is not None else None
        self._state: Optional[dict[str, Any]] = None
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append((sql, args))
        if "INSERT INTO spice_connectors.connector_sync_state" in sql and self._materialized_state is not None:
            self._state = dict(self._materialized_state)
        elif "UPDATE spice_connectors.connector_sync_state" in sql and "SET last_seen_cursor" in sql:
            if self._state is None:
                self._state = {"last_seen_cursor": None, "last_emitted_seq": 0}
            self._state["last_seen_cursor"] = args[2]
            self._state["last_emitted_seq"] = args[3]
        return "OK"

    async def fetchrow(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "SELECT last_seen_cursor, last_emitted_seq" in sql:
            return self._state
        raise AssertionError(f"Unexpected fetchrow SQL: {sql}")


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
async def test_record_poll_result_locks_materialized_sync_state_before_assigning_sequence() -> None:
    conn = _FakeConnection(
        materialized_state={
            "last_seen_cursor": "cursor-7",
            "last_emitted_seq": 7,
        }
    )
    registry = ConnectorRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    envelope = await registry.record_poll_result(
        source_type="google_sheets",
        source_id="sheet-1",
        current_cursor="cursor-8",
    )

    assert envelope is not None
    assert envelope.sequence_number == 8
    assert envelope.data["previous_cursor"] == "cursor-7"
    assert _call_index(conn, "INSERT INTO spice_connectors.connector_sync_state") < _call_index(
        conn,
        "SELECT last_seen_cursor, last_emitted_seq",
    )


@pytest.mark.asyncio
async def test_record_poll_result_suppresses_duplicate_cursor_after_sync_state_materializes() -> None:
    conn = _FakeConnection(
        materialized_state={
            "last_seen_cursor": "cursor-7",
            "last_emitted_seq": 7,
        }
    )
    registry = ConnectorRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)

    envelope = await registry.record_poll_result(
        source_type="google_sheets",
        source_id="sheet-1",
        current_cursor="cursor-7",
    )

    assert envelope is None
    assert not any("INSERT INTO spice_connectors.connector_update_outbox" in sql for sql, _ in conn.calls)
