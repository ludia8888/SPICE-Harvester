from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("pyspark")

from pipeline_worker.main import PipelineWorker


@pytest.mark.asyncio
async def test_list_lakefs_diff_paths_ignores_removed() -> None:
    worker = PipelineWorker()
    worker.lakefs_client = SimpleNamespace(
        list_diff_objects=AsyncMock(
            return_value=[
                {"type": "removed", "path": "datasets/in_ds/part-0000.parquet"},
                {"type": "added", "path": "datasets/in_ds/part-0001.parquet"},
                {"type": "modified", "path": "datasets/in_ds/part-0002.parquet"},
            ]
        )
    )

    paths, diff_ok = await worker._list_lakefs_diff_paths(
        repository="repo",
        ref="commit-2",
        since="commit-1",
        prefix="datasets/in_ds",
        node_id="in1",
    )

    assert diff_ok is True
    assert paths == [
        "datasets/in_ds/part-0001.parquet",
        "datasets/in_ds/part-0002.parquet",
    ]


@pytest.mark.asyncio
async def test_load_input_dataframe_fallback_on_diff_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = PipelineWorker()
    dataset = SimpleNamespace(dataset_id="ds1", name="in_ds", branch="main", source_type="manual")
    version = SimpleNamespace(
        lakefs_commit_id="commit-2",
        artifact_key="s3://repo/commit-2/datasets/in_ds",
        version_id="v1",
    )

    class DummyRegistry:
        async def get_dataset(self, *, dataset_id: str):  # type: ignore[override]
            return dataset

        async def get_latest_version(self, *, dataset_id: str):  # type: ignore[override]
            return version

        async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str):  # type: ignore[override]
            return None

    worker.dataset_registry = DummyRegistry()
    worker.lakefs_client = SimpleNamespace(list_diff_objects=AsyncMock(side_effect=RuntimeError("boom")))

    sentinel = object()

    async def fake_load_artifact_dataframe(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return sentinel

    monkeypatch.setattr(worker, "_load_artifact_dataframe", fake_load_artifact_dataframe)

    result = await worker._load_input_dataframe(
        db_name="db",
        metadata={"datasetId": "ds1", "datasetName": "in_ds"},
        temp_dirs=[],
        branch="main",
        node_id="in1",
        input_snapshots=[],
        previous_commit_id="commit-1",
        use_lakefs_diff=True,
        watermark_column=None,
        watermark_after=None,
    )

    assert result is sentinel


@pytest.mark.asyncio
async def test_load_input_dataframe_removed_only_diff_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = PipelineWorker()
    dataset = SimpleNamespace(dataset_id="ds1", name="in_ds", branch="main", source_type="manual")
    version = SimpleNamespace(
        lakefs_commit_id="commit-2",
        artifact_key="s3://repo/commit-2/datasets/in_ds",
        version_id="v1",
    )

    class DummyRegistry:
        async def get_dataset(self, *, dataset_id: str):  # type: ignore[override]
            return dataset

        async def get_latest_version(self, *, dataset_id: str):  # type: ignore[override]
            return version

        async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str):  # type: ignore[override]
            return None

    worker.dataset_registry = DummyRegistry()
    worker.lakefs_client = SimpleNamespace(
        list_diff_objects=AsyncMock(return_value=[{"type": "removed", "path": "datasets/in_ds/part-0000.parquet"}])
    )

    sentinel = object()
    monkeypatch.setattr(worker, "_empty_dataframe", lambda: sentinel)
    monkeypatch.setattr(worker, "_load_artifact_dataframe", AsyncMock())

    snapshots: list[dict[str, object]] = []
    result = await worker._load_input_dataframe(
        db_name="db",
        metadata={"datasetId": "ds1", "datasetName": "in_ds"},
        temp_dirs=[],
        branch="main",
        node_id="in1",
        input_snapshots=snapshots,
        previous_commit_id="commit-1",
        use_lakefs_diff=True,
        watermark_column=None,
        watermark_after=None,
    )

    assert result is sentinel
    assert snapshots
    assert snapshots[0].get("diff_requested") is True
    assert snapshots[0].get("diff_ok") is True
    assert snapshots[0].get("diff_empty") is True
