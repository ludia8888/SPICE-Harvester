from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.services.pipeline_scheduler import PipelineScheduler


class _Registry:
    async def record_run(self, **kwargs):  # noqa: ANN003
        return None


class _Queue:
    def __init__(self) -> None:
        self.published = []

    async def publish(self, job):  # noqa: ANN001
        self.published.append(job)


@pytest.mark.asyncio
async def test_scheduler_emits_ignored_event(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    async def _emit(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return True

    monkeypatch.setattr(
        "shared.services.pipeline_scheduler.emit_pipeline_control_plane_event",
        _emit,
    )

    scheduler = PipelineScheduler(registry=_Registry(), queue=_Queue())
    now = datetime.now(timezone.utc)
    await scheduler._record_scheduler_ignored(
        pipeline_id="pipeline-1",
        now=now,
        reason="dependency_not_satisfied",
        detail="upstream not ready",
    )

    assert captured["event_type"] == "PIPELINE_SCHEDULE_IGNORED"
    assert captured["pipeline_id"] == "pipeline-1"
    assert captured["data"]["reason"] == "dependency_not_satisfied"
