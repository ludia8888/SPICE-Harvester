from __future__ import annotations

from types import SimpleNamespace

import pytest

import pipeline_scheduler.main as scheduler_main


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_scheduler_main_bootstraps_and_runs(monkeypatch: pytest.MonkeyPatch):
    calls: dict[str, object] = {}

    class _Registry:
        async def initialize(self) -> None:
            calls["registry_initialized"] = True

    class _Queue:
        pass

    class _Scheduler:
        def __init__(
            self,
            registry: _Registry,
            queue: _Queue,
            poll_seconds: int = 30,
            *,
            tracing=None,
            metrics=None,
        ) -> None:
            calls["registry_arg"] = registry
            calls["queue_arg"] = queue
            calls["poll_seconds"] = poll_seconds
            calls["tracing_arg"] = tracing
            calls["metrics_arg"] = metrics

        async def run(self) -> None:
            calls["scheduler_run"] = True

    monkeypatch.setattr(
        scheduler_main,
        "get_settings",
        lambda: SimpleNamespace(
            observability=SimpleNamespace(log_level="INFO"),
            pipeline=SimpleNamespace(scheduler_poll_seconds=17),
        ),
    )
    monkeypatch.setattr(scheduler_main, "configure_logging", lambda level: calls.setdefault("log_level", level))
    monkeypatch.setattr(scheduler_main, "get_tracing_service", lambda name: f"tracing:{name}")
    monkeypatch.setattr(scheduler_main, "get_metrics_collector", lambda name: f"metrics:{name}")
    monkeypatch.setattr(scheduler_main, "PipelineRegistry", _Registry)
    monkeypatch.setattr(scheduler_main, "PipelineJobQueue", _Queue)
    monkeypatch.setattr(scheduler_main, "PipelineScheduler", _Scheduler)

    await scheduler_main.main()

    assert calls["registry_initialized"] is True
    assert calls["scheduler_run"] is True
    assert calls["poll_seconds"] == 17
    assert calls["log_level"] == "INFO"
    assert calls["tracing_arg"] == "tracing:pipeline-scheduler"
    assert calls["metrics_arg"] == "metrics:pipeline-scheduler"
