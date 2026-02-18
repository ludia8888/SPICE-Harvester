from __future__ import annotations

import csv
import io
from types import SimpleNamespace
from typing import Any

import pytest

from shared.services.core.connector_ingest_service import ConnectorIngestService


def _parse_saved_csv(data: bytes) -> tuple[list[str], list[list[str]]]:
    rows = list(csv.reader(io.StringIO(data.decode("utf-8"))))
    if not rows:
        return [], []
    return rows[0], rows[1:]


class _Storage:
    def __init__(self) -> None:
        self.saved: list[dict[str, Any]] = []

    async def save_bytes(
        self,
        repository: str,
        key: str,
        payload: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.saved.append(
            {
                "repository": repository,
                "key": key,
                "payload": bytes(payload),
                "content_type": content_type,
            }
        )


class _DatasetRegistry:
    def __init__(self, *, existing_dataset: Any | None = None) -> None:
        self.dataset = existing_dataset
        self.created: list[dict[str, Any]] = []
        self.versions: list[dict[str, Any]] = []

    async def get_dataset_by_source_ref(self, **kwargs: Any):  # noqa: ANN401
        _ = kwargs
        return self.dataset

    async def create_dataset(self, **kwargs: Any):  # noqa: ANN401
        self.created.append(dict(kwargs))
        self.dataset = SimpleNamespace(
            dataset_id="dataset-1",
            db_name=kwargs["db_name"],
            name=kwargs["name"],
            branch=kwargs.get("branch") or "main",
            schema_json=kwargs.get("schema_json") or {},
        )
        return self.dataset

    async def add_version(self, **kwargs: Any):  # noqa: ANN401
        self.versions.append(dict(kwargs))
        return SimpleNamespace(
            version_id="version-1",
            lakefs_commit_id=kwargs["lakefs_commit_id"],
            artifact_key=kwargs["artifact_key"],
            row_count=kwargs["row_count"],
            sample_json=kwargs.get("sample_json") or {},
        )


class _PipelineRegistry:
    def __init__(self, storage: _Storage) -> None:
        self._storage = storage

    async def get_lakefs_storage(self, *, user_id: str | None = None):  # noqa: ANN001
        _ = user_id
        return self._storage


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connector_ingest_service_snapshot_creates_dataset_and_version(monkeypatch: pytest.MonkeyPatch):
    storage = _Storage()
    dataset_registry = _DatasetRegistry()
    pipeline_registry = _PipelineRegistry(storage)
    service = ConnectorIngestService(
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )

    async def _no_branch(*, repository: str, branch: str, source_branch: str = "main") -> None:
        _ = repository, branch, source_branch
        return None

    async def _fixed_commit(**kwargs: Any) -> str:  # noqa: ANN401
        _ = kwargs
        return "commit-1"

    monkeypatch.setattr(service, "_ensure_branch_exists", _no_branch)
    monkeypatch.setattr(service, "_commit", _fixed_commit)

    result = await service.ingest_rows(
        db_name="sales_db",
        source_type="postgresql",
        source_id="orders",
        columns=["id", "name"],
        rows=[{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
        branch="main",
        import_mode="SNAPSHOT",
        source_ref="postgresql:orders",
    )

    assert dataset_registry.created
    assert len(dataset_registry.versions) == 1
    assert result["dataset"]["dataset_id"] == "dataset-1"
    assert result["version"]["lakefs_commit_id"] == "commit-1"
    assert result["version"]["row_count"] == 2

    assert len(storage.saved) == 1
    columns, rows = _parse_saved_csv(storage.saved[0]["payload"])
    assert columns == ["id", "name"]
    assert rows == [["1", "Alice"], ["2", "Bob"]]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connector_ingest_service_append_deduplicates_rows(monkeypatch: pytest.MonkeyPatch):
    existing_dataset = SimpleNamespace(
        dataset_id="dataset-existing",
        db_name="sales_db",
        name="orders",
        branch="main",
        schema_json={},
    )
    storage = _Storage()
    dataset_registry = _DatasetRegistry(existing_dataset=existing_dataset)
    pipeline_registry = _PipelineRegistry(storage)
    service = ConnectorIngestService(
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )

    async def _no_branch(*, repository: str, branch: str, source_branch: str = "main") -> None:
        _ = repository, branch, source_branch
        return None

    async def _fixed_commit(**kwargs: Any) -> str:  # noqa: ANN401
        _ = kwargs
        return "commit-append"

    async def _existing_csv(*, dataset: Any, repository: str):  # noqa: ANN401
        _ = dataset, repository
        return ["id", "name"], [["1", "Alice"], ["2", "Bob"]]

    monkeypatch.setattr(service, "_ensure_branch_exists", _no_branch)
    monkeypatch.setattr(service, "_commit", _fixed_commit)
    monkeypatch.setattr(service, "_load_existing_csv", _existing_csv)

    result = await service.ingest_rows(
        db_name="sales_db",
        source_type="postgresql",
        source_id="orders",
        columns=["id", "name"],
        rows=[{"id": "2", "name": "Bob"}, {"id": "3", "name": "Carol"}],
        import_mode="APPEND",
        source_ref="postgresql:orders",
    )

    assert result["version"]["row_count"] == 3
    assert len(storage.saved) == 1
    columns, rows = _parse_saved_csv(storage.saved[0]["payload"])
    assert columns == ["id", "name"]
    assert rows == [["1", "Alice"], ["2", "Bob"], ["3", "Carol"]]


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "incoming_rows", "expected_rows"),
    [
        (
            "INCREMENTAL",
            [{"id": "2", "name": "Bob"}, {"id": "3", "name": "Carol"}],
            [["1", "Alice"], ["2", "Bob"], ["3", "Carol"]],
        ),
        (
            "CDC",
            [{"id": "2", "name": "Bob"}, {"id": "4", "name": "Dan"}],
            [["1", "Alice"], ["2", "Bob"], ["4", "Dan"]],
        ),
    ],
)
async def test_connector_ingest_service_incremental_and_cdc_use_append_merge(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    incoming_rows: list[dict[str, str]],
    expected_rows: list[list[str]],
):
    existing_dataset = SimpleNamespace(
        dataset_id="dataset-existing",
        db_name="sales_db",
        name="orders",
        branch="main",
        schema_json={},
    )
    storage = _Storage()
    dataset_registry = _DatasetRegistry(existing_dataset=existing_dataset)
    pipeline_registry = _PipelineRegistry(storage)
    service = ConnectorIngestService(
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )

    async def _no_branch(*, repository: str, branch: str, source_branch: str = "main") -> None:
        _ = repository, branch, source_branch
        return None

    async def _fixed_commit(**kwargs: Any) -> str:  # noqa: ANN401
        _ = kwargs
        return f"commit-{mode.lower()}"

    async def _existing_csv(*, dataset: Any, repository: str):  # noqa: ANN401
        _ = dataset, repository
        return ["id", "name"], [["1", "Alice"], ["2", "Bob"]]

    monkeypatch.setattr(service, "_ensure_branch_exists", _no_branch)
    monkeypatch.setattr(service, "_commit", _fixed_commit)
    monkeypatch.setattr(service, "_load_existing_csv", _existing_csv)

    result = await service.ingest_rows(
        db_name="sales_db",
        source_type="postgresql",
        source_id="orders",
        columns=["id", "name"],
        rows=incoming_rows,
        import_mode=mode,
        source_ref="postgresql:orders",
    )

    assert result["version"]["row_count"] == len(expected_rows)
    assert len(storage.saved) == 1
    columns, rows = _parse_saved_csv(storage.saved[0]["payload"])
    assert columns == ["id", "name"]
    assert rows == expected_rows


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connector_ingest_service_update_upserts_by_primary_key(monkeypatch: pytest.MonkeyPatch):
    existing_dataset = SimpleNamespace(
        dataset_id="dataset-existing",
        db_name="sales_db",
        name="orders",
        branch="main",
        schema_json={},
    )
    storage = _Storage()
    dataset_registry = _DatasetRegistry(existing_dataset=existing_dataset)
    pipeline_registry = _PipelineRegistry(storage)
    service = ConnectorIngestService(
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )

    async def _no_branch(*, repository: str, branch: str, source_branch: str = "main") -> None:
        _ = repository, branch, source_branch
        return None

    async def _fixed_commit(**kwargs: Any) -> str:  # noqa: ANN401
        _ = kwargs
        return "commit-update"

    async def _existing_csv(*, dataset: Any, repository: str):  # noqa: ANN401
        _ = dataset, repository
        return ["id", "name"], [["1", "Alice-old"], ["2", "Bob"]]

    monkeypatch.setattr(service, "_ensure_branch_exists", _no_branch)
    monkeypatch.setattr(service, "_commit", _fixed_commit)
    monkeypatch.setattr(service, "_load_existing_csv", _existing_csv)

    result = await service.ingest_rows(
        db_name="sales_db",
        source_type="postgresql",
        source_id="orders",
        columns=["id", "name"],
        rows=[{"id": "1", "name": "Alice-new"}, {"id": "3", "name": "Carol"}],
        import_mode="UPDATE",
        primary_key_column="id",
        source_ref="postgresql:orders",
    )

    assert result["version"]["row_count"] == 3
    assert len(storage.saved) == 1
    columns, rows = _parse_saved_csv(storage.saved[0]["payload"])
    assert columns == ["id", "name"]
    assert rows == [["1", "Alice-new"], ["2", "Bob"], ["3", "Carol"]]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connector_ingest_service_rejects_invalid_import_mode():
    storage = _Storage()
    dataset_registry = _DatasetRegistry()
    pipeline_registry = _PipelineRegistry(storage)
    service = ConnectorIngestService(
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
    )

    with pytest.raises(ValueError, match="import_mode must be one of"):
        await service.ingest_rows(
            db_name="sales_db",
            source_type="postgresql",
            source_id="orders",
            columns=["id"],
            rows=[{"id": "1"}],
            import_mode="UPSERT",
        )
