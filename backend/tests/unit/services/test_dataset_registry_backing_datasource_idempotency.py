from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.schema_hash import compute_schema_hash_from_payload


class _UniqueViolationError(RuntimeError):
    sqlstate = "23505"


class _FetchrowConnection:
    def __init__(self, row: dict[str, object]) -> None:
        self._row = row
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def __aenter__(self) -> "_FetchrowConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def fetchrow(self, sql: str, *args: object):
        self.calls.append((sql, args))
        return self._row


class _FetchrowPool:
    def __init__(self, conn: _FetchrowConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FetchrowConnection:
        return self._conn


@pytest.mark.asyncio
async def test_get_or_create_backing_datasource_refetches_after_unique_violation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    dataset = SimpleNamespace(
        dataset_id="dataset-1",
        db_name="demo",
        name="orders",
        description="Orders",
        branch="main",
    )
    existing = SimpleNamespace(backing_id="backing-1")
    calls = {"lookup": 0, "create": 0}

    async def _lookup(*, dataset_id: str, branch: str):
        calls["lookup"] += 1
        assert dataset_id == dataset.dataset_id
        assert branch == dataset.branch
        return existing if calls["lookup"] >= 2 else None

    async def _create(**kwargs):  # noqa: ANN003
        calls["create"] += 1
        raise _UniqueViolationError("duplicate key value violates unique constraint")

    monkeypatch.setattr(registry, "get_backing_datasource_by_dataset", _lookup)
    monkeypatch.setattr(registry, "create_backing_datasource", _create)

    resolved = await registry.get_or_create_backing_datasource(dataset=dataset)

    assert resolved is existing
    assert calls == {"lookup": 2, "create": 1}


@pytest.mark.asyncio
async def test_get_or_create_backing_datasource_version_refetches_after_unique_violation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = object()
    existing = SimpleNamespace(
        backing_version_id="backing-version-1",
        backing_id="backing-1",
        dataset_version_id="version-1",
        schema_hash="schema-hash",
        artifact_key="s3://bucket/data.parquet",
    )
    calls = {"lookup": 0, "create": 0}

    async def _lookup(*, backing_id: str, dataset_version_id: str):
        calls["lookup"] += 1
        assert backing_id == "backing-1"
        assert dataset_version_id == "version-1"
        return existing if calls["lookup"] >= 2 else None

    async def _create(**kwargs):  # noqa: ANN003
        calls["create"] += 1
        raise _UniqueViolationError("duplicate key value violates unique constraint")

    async def _resolve(**kwargs):  # noqa: ANN003
        return SimpleNamespace(backing_id="backing-1"), SimpleNamespace(artifact_key="s3://bucket/data.parquet"), "schema-hash"

    monkeypatch.setattr(registry, "_resolve_backing_datasource_version_materialization", _resolve)
    monkeypatch.setattr(registry, "get_backing_datasource_version_for_dataset", _lookup)
    monkeypatch.setattr(registry, "_insert_backing_datasource_version", _create)

    resolved = await registry.get_or_create_backing_datasource_version(
        backing_id="backing-1",
        dataset_version_id="version-1",
        schema_hash="schema-hash",
    )

    assert resolved is existing
    assert calls == {"lookup": 2, "create": 1}


@pytest.mark.asyncio
async def test_create_backing_datasource_version_uses_canonical_schema_hash_over_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = object()
    canonical_sample = {"columns": [{"name": "id", "type": "xsd:string"}]}
    expected_hash = compute_schema_hash_from_payload(canonical_sample)
    captured: dict[str, object] = {}

    async def _get_backing_datasource(*, backing_id: str):
        assert backing_id == "backing-1"
        return SimpleNamespace(backing_id=backing_id, dataset_id="dataset-1")

    async def _get_version(*, version_id: str):
        assert version_id == "version-1"
        return SimpleNamespace(version_id=version_id, dataset_id="dataset-1", sample_json=canonical_sample, artifact_key="s3://bucket/data.parquet")

    async def _get_dataset(*, dataset_id: str):
        assert dataset_id == "dataset-1"
        return SimpleNamespace(schema_json={})

    async def _insert(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return SimpleNamespace(version_id="backing-version-1", schema_hash=kwargs["schema_hash"], artifact_key=kwargs["artifact_key"])

    monkeypatch.setattr(registry, "get_backing_datasource", _get_backing_datasource)
    monkeypatch.setattr(registry, "get_version", _get_version)
    monkeypatch.setattr(registry, "get_dataset", _get_dataset)
    monkeypatch.setattr(registry, "_insert_backing_datasource_version", _insert)

    record = await registry.create_backing_datasource_version(
        backing_id="backing-1",
        dataset_version_id="version-1",
        schema_hash="wrong-hash",
        metadata={"source": "test"},
    )

    assert record.schema_hash == expected_hash
    assert captured["schema_hash"] == expected_hash
    assert captured["artifact_key"] == "s3://bucket/data.parquet"


@pytest.mark.asyncio
async def test_get_or_create_backing_datasource_version_repairs_schema_hash_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = SimpleNamespace(
        version_id="backing-version-1",
        backing_id="backing-1",
        dataset_version_id="version-1",
        schema_hash="stale-hash",
        artifact_key="s3://bucket/old.parquet",
    )
    resolved_version = SimpleNamespace(artifact_key="s3://bucket/new.parquet")
    expected_row = {
        "backing_version_id": existing.version_id,
        "backing_id": existing.backing_id,
        "dataset_version_id": existing.dataset_version_id,
        "schema_hash": "canonical-hash",
        "artifact_key": resolved_version.artifact_key,
        "metadata": {},
        "status": "ACTIVE",
        "created_at": "2025-01-01T00:00:00Z",
    }
    conn = _FetchrowConnection(expected_row)
    registry = DatasetRegistry(dsn="postgresql://example.invalid/db")
    registry._pool = _FetchrowPool(conn)

    async def _resolve(**kwargs):  # noqa: ANN003
        return SimpleNamespace(backing_id="backing-1"), resolved_version, "canonical-hash"

    async def _lookup(*, backing_id: str, dataset_version_id: str):
        assert backing_id == "backing-1"
        assert dataset_version_id == "version-1"
        return existing

    async def _insert(**kwargs):  # noqa: ANN003
        raise AssertionError("insert should not run when existing backing version is found")

    monkeypatch.setattr(registry, "_resolve_backing_datasource_version_materialization", _resolve)
    monkeypatch.setattr(registry, "get_backing_datasource_version_for_dataset", _lookup)
    monkeypatch.setattr(registry, "_insert_backing_datasource_version", _insert)

    repaired = await registry.get_or_create_backing_datasource_version(
        backing_id="backing-1",
        dataset_version_id="version-1",
        schema_hash="wrong-hash",
    )

    assert repaired.schema_hash == "canonical-hash"
    assert repaired.artifact_key == resolved_version.artifact_key
    assert len(conn.calls) == 1
    assert "UPDATE spice_datasets.backing_datasource_versions" in conn.calls[0][0]
