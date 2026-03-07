import json
from datetime import datetime, timezone

import pytest

from shared.services.registries.ontology_key_spec_registry import OntologyKeySpecRegistry


class _FakeConnection:
    def __init__(self) -> None:
        self.fetchrow_args = None
        self.now = datetime.now(timezone.utc)

    async def fetchrow(self, _query: str, db_name: str, branch: str, class_id: str, primary_key: str, title_key: str):
        self.fetchrow_args = {
            "db_name": db_name,
            "branch": branch,
            "class_id": class_id,
            "primary_key": primary_key,
            "title_key": title_key,
        }
        return {
            "db_name": db_name,
            "branch": branch,
            "class_id": class_id,
            "primary_key": json.loads(primary_key),
            "title_key": json.loads(title_key),
            "created_at": self.now,
            "updated_at": self.now,
        }


class _FakeAcquire:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConnection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        _ = exc_type, exc, tb
        return None


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ontology_key_spec_registry_uses_canonical_key_normalization(monkeypatch: pytest.MonkeyPatch):
    registry = OntologyKeySpecRegistry(postgres_url="postgresql://example/test")
    conn = _FakeConnection()
    pool = _FakePool(conn)

    async def _noop_ensure_schema() -> None:
        return None

    async def _fake_ensure_pool():
        return pool

    monkeypatch.setattr(registry, "ensure_schema", _noop_ensure_schema)
    monkeypatch.setattr(registry, "_ensure_pool", _fake_ensure_pool)

    spec = await registry.upsert_key_spec(
        db_name="demo",
        branch="main",
        class_id="Order",
        primary_key={"primaryKeys": ["order_id", "tenant_id", "order_id"]},
        title_key={"titleKey": "order_name, order_display , order_name"},
    )

    assert conn.fetchrow_args is not None
    assert json.loads(conn.fetchrow_args["primary_key"]) == ["order_id", "tenant_id"]
    assert json.loads(conn.fetchrow_args["title_key"]) == ["order_name", "order_display"]
    assert spec.primary_key == ["order_id", "tenant_id"]
    assert spec.title_key == ["order_name", "order_display"]
