from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from pipeline_worker.main import PipelineWorker
from shared.models.pipeline_job import PipelineJob


def test_pipeline_worker_initializes_domain_modules() -> None:
    worker = PipelineWorker()
    assert hasattr(worker, "_run_domain")
    assert hasattr(worker, "_ingest_domain")
    assert hasattr(worker, "_validation_domain")
    assert hasattr(worker, "_output_domain")


@pytest.mark.asyncio
async def test_execute_job_delegates_to_run_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = PipelineWorker()
    captured: dict[str, Any] = {}

    async def _fake_execute(job: PipelineJob) -> None:
        captured["job"] = job

    monkeypatch.setattr(worker._run_domain, "execute_job", _fake_execute)
    job = PipelineJob(
        job_id="job-1",
        pipeline_id="pipe-1",
        db_name="default",
        output_dataset_name="out",
        definition_json={"nodes": [], "edges": []},
    )

    await worker._execute_job(job)
    assert captured["job"] is job


def test_validation_methods_delegate_to_validation_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = PipelineWorker()

    called: dict[str, Any] = {}

    def _fake_validate_execution_prerequisites() -> None:
        called["prereq"] = True

    def _fake_validate_required_subgraph(**kwargs: Any) -> list[str]:
        called["subgraph"] = kwargs
        return ["subgraph-error"]

    def _fake_validate_definition(**kwargs: Any) -> list[str]:
        called["definition"] = kwargs
        return ["definition-error"]

    monkeypatch.setattr(worker._validation_domain, "validate_execution_prerequisites", _fake_validate_execution_prerequisites)
    monkeypatch.setattr(worker._validation_domain, "validate_required_subgraph", _fake_validate_required_subgraph)
    monkeypatch.setattr(worker._validation_domain, "validate_definition", _fake_validate_definition)

    worker._validate_execution_prerequisites()
    errors_subgraph = worker._validate_required_subgraph(
        nodes={"n1": {"type": "output"}},
        incoming={},
        required_node_ids={"n1"},
    )
    errors_definition = worker._validate_definition({"nodes": []}, require_output=False)

    assert called["prereq"] is True
    assert called["subgraph"]["required_node_ids"] == {"n1"}
    assert called["definition"]["require_output"] is False
    assert errors_subgraph == ["subgraph-error"]
    assert errors_definition == ["definition-error"]


def test_ingest_methods_delegate_to_ingest_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = PipelineWorker()
    df_stub = SimpleNamespace(name="df")

    def _fake_apply_watermark_filter(*args: Any, **kwargs: Any) -> Any:
        assert args[0] is df_stub
        return "filtered"

    def _fake_collect_watermark_keys(*args: Any, **kwargs: Any) -> list[str]:
        assert args[0] is df_stub
        return ["k1", "k2"]

    monkeypatch.setattr(worker._ingest_domain, "apply_watermark_filter", _fake_apply_watermark_filter)
    monkeypatch.setattr(worker._ingest_domain, "collect_watermark_keys", _fake_collect_watermark_keys)

    filtered = worker._apply_watermark_filter(
        df_stub,  # type: ignore[arg-type]
        watermark_column="ts",
        watermark_after="2026-01-01T00:00:00Z",
        watermark_keys=["old"],
    )
    keys = worker._collect_watermark_keys(
        df_stub,  # type: ignore[arg-type]
        watermark_column="ts",
        watermark_value="2026-01-01T00:00:00Z",
    )
    assert filtered == "filtered"
    assert keys == ["k1", "k2"]


@pytest.mark.asyncio
async def test_async_domain_methods_delegate(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = PipelineWorker()
    df_stub = SimpleNamespace(name="df")

    async def _fake_apply_input_watermark_and_snapshot(**kwargs: Any) -> Any:
        assert kwargs["df"] is df_stub
        return "watermarked"

    async def _fake_materialize_output_by_kind(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["df"] is df_stub
        return {"artifact_key": "s3://bucket/path"}

    monkeypatch.setattr(
        worker._ingest_domain,
        "apply_input_watermark_and_snapshot",
        _fake_apply_input_watermark_and_snapshot,
    )
    monkeypatch.setattr(
        worker._output_domain,
        "materialize_output_by_kind",
        _fake_materialize_output_by_kind,
    )

    watermarked = await worker._apply_input_watermark_and_snapshot(
        df=df_stub,  # type: ignore[arg-type]
        node_id="n1",
        snapshot={},
        watermark_column="ts",
        watermark_after="2026-01-01T00:00:00Z",
        watermark_keys=[],
        label_scope="test",
        tolerate_max_errors=True,
        always_compute_watermark_max=True,
    )
    output = await worker._materialize_output_by_kind(
        output_kind="dataset",
        output_metadata={},
        df=df_stub,  # type: ignore[arg-type]
        artifact_bucket="bucket",
        prefix="path",
    )

    assert watermarked == "watermarked"
    assert output["artifact_key"] == "s3://bucket/path"
