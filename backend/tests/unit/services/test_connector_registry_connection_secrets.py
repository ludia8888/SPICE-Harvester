from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from shared.services.registries.connector_registry import ConnectorRegistry


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeConnection:
    def __init__(self) -> None:
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

    async def fetchrow(self, sql: str, *args: Any):  # noqa: ANN401
        self.calls.append((sql, args))
        if "INSERT INTO spice_connectors.connector_connection_secrets" in sql:
            return {
                "source_type": args[0],
                "source_id": args[1],
                "secrets_json_enc": args[2],
                "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "updated_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        raise AssertionError(f"Unexpected SQL: {sql}")


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeConnection:
        return self._conn


@pytest.mark.asyncio
async def test_upsert_connection_secrets_does_not_auto_enable_placeholder_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection()
    registry = ConnectorRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FakePool(conn)
    monkeypatch.setattr(
        registry,
        "_encrypt_secrets_payload",
        lambda **kwargs: kwargs["secrets_json"],
    )
    monkeypatch.setattr(
        registry,
        "_decrypt_secrets_payload",
        lambda **kwargs: kwargs["secrets_json_enc"],
    )

    stored = await registry.upsert_connection_secrets(
        source_type="s3",
        source_id="source-1",
        secrets_json={"token": "secret"},
    )

    assert stored == {"token": "secret"}
    assert any(
        "INSERT INTO spice_connectors.connector_sources" in sql and "VALUES ($1, $2, FALSE" in sql
        for sql, _ in conn.calls
    )
