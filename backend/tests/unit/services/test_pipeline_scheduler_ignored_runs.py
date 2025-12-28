from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import pytest

from shared.services.pipeline_scheduler import PipelineScheduler


@dataclass
class _PipelineRecord:
    pipeline_id: str
    db_name: str
    last_build_status: Optional[str] = None
    last_build_at: Optional[datetime] = None


class _Queue:
    def __init__(self) -> None:
        self.published: list[Any] = []

    async def publish(self, job: Any) -> None:
        self.published.append(job)


class _Registry:
    def __init__(self, *, pipelines: list[dict[str, Any]], records: dict[str, _PipelineRecord]) -> None:
        self._pipelines = pipelines
        self._records = records
        self.runs: list[dict[str, Any]] = []
        self.schedule_ticks: list[tuple[str, datetime]] = []

    async def list_scheduled_pipelines(self) -> list[dict[str, Any]]:
        return self._pipelines

    async def get_pipeline(self, *, pipeline_id: str):
        return self._records.get(pipeline_id)

    async def record_run(self, *, pipeline_id: str, job_id: str, mode: str, status: str, output_json: dict[str, Any], finished_at: datetime, **kwargs: Any) -> None:
        self.runs.append(
            {
                "pipeline_id": pipeline_id,
                "job_id": job_id,
                "mode": mode,
                "status": status,
                "output_json": output_json,
                "finished_at": finished_at,
            }
        )

    async def record_schedule_tick(self, *, pipeline_id: str, scheduled_at: datetime) -> None:
        self.schedule_ticks.append((pipeline_id, scheduled_at))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_records_ignored_when_schedule_due_but_dependencies_up_to_date(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    import shared.services.pipeline_scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "_utcnow", lambda: now)

    pipeline_id = "11111111-1111-1111-1111-111111111111"
    dep_id = "22222222-2222-2222-2222-222222222222"

    registry = _Registry(
        pipelines=[
            {
                "pipeline_id": pipeline_id,
                "db_name": "db",
                "pipeline_type": "batch",
                "schedule_interval_seconds": 60,
                "schedule_cron": None,
                "last_scheduled_at": None,
                "last_build_status": "DEPLOYED",
                "last_build_at": now,
                "name": "p",
                "branch": "main",
                "definition_json": {},
                "dependencies": [{"pipeline_id": dep_id, "status": "DEPLOYED"}],
            }
        ],
        records={
            pipeline_id: _PipelineRecord(pipeline_id=pipeline_id, db_name="db", last_build_status="DEPLOYED", last_build_at=now),
            dep_id: _PipelineRecord(pipeline_id=dep_id, db_name="db", last_build_status="DEPLOYED", last_build_at=now),
        },
    )
    queue = _Queue()

    scheduler = PipelineScheduler(registry, queue, poll_seconds=1)
    await scheduler._tick()

    assert queue.published == []
    assert len(registry.runs) == 1
    assert registry.runs[0]["status"] == "IGNORED"
    assert registry.runs[0]["mode"] == "schedule"
    assert registry.runs[0]["output_json"]["reason"] == "up_to_date"
    assert registry.schedule_ticks == [(pipeline_id, now)]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_records_ignored_when_schedule_due_but_dependency_not_satisfied(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    import shared.services.pipeline_scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "_utcnow", lambda: now)

    pipeline_id = "33333333-3333-3333-3333-333333333333"
    dep_id = "44444444-4444-4444-4444-444444444444"

    registry = _Registry(
        pipelines=[
            {
                "pipeline_id": pipeline_id,
                "db_name": "db",
                "pipeline_type": "batch",
                "schedule_interval_seconds": 60,
                "schedule_cron": None,
                "last_scheduled_at": None,
                "last_build_status": "DEPLOYED",
                "last_build_at": now,
                "name": "p",
                "branch": "main",
                "definition_json": {},
                "dependencies": [{"pipeline_id": dep_id, "status": "DEPLOYED"}],
            }
        ],
        records={
            pipeline_id: _PipelineRecord(pipeline_id=pipeline_id, db_name="db", last_build_status="DEPLOYED", last_build_at=now),
            dep_id: _PipelineRecord(pipeline_id=dep_id, db_name="db", last_build_status="FAILED", last_build_at=now),
        },
    )
    queue = _Queue()

    scheduler = PipelineScheduler(registry, queue, poll_seconds=1)
    await scheduler._tick()

    assert queue.published == []
    assert len(registry.runs) == 1
    assert registry.runs[0]["status"] == "IGNORED"
    assert registry.runs[0]["mode"] == "schedule"
    assert registry.runs[0]["output_json"]["reason"] == "dependency_not_satisfied"
    assert registry.schedule_ticks == [(pipeline_id, now)]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_does_not_trigger_dependency_only_when_pipeline_is_newer_than_deps(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    dep_built_at = datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)

    import shared.services.pipeline_scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "_utcnow", lambda: now)

    pipeline_id = "55555555-5555-5555-5555-555555555555"
    dep_id = "66666666-6666-6666-6666-666666666666"

    registry = _Registry(
        pipelines=[
            {
                "pipeline_id": pipeline_id,
                "db_name": "db",
                "pipeline_type": "batch",
                "schedule_interval_seconds": None,
                "schedule_cron": None,
                "last_scheduled_at": None,
                "last_build_status": "DEPLOYED",
                "last_build_at": now,
                "name": "p",
                "branch": "main",
                "definition_json": {},
                "dependencies": [{"pipeline_id": dep_id, "status": "DEPLOYED"}],
            }
        ],
        records={
            pipeline_id: _PipelineRecord(pipeline_id=pipeline_id, db_name="db", last_build_status="DEPLOYED", last_build_at=now),
            dep_id: _PipelineRecord(pipeline_id=dep_id, db_name="db", last_build_status="DEPLOYED", last_build_at=dep_built_at),
        },
    )
    queue = _Queue()

    scheduler = PipelineScheduler(registry, queue, poll_seconds=1)
    await scheduler._tick()

    assert queue.published == []
    assert registry.runs == []
    assert registry.schedule_ticks == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_triggers_interval_schedule_when_due(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    last_run = datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)

    import shared.services.pipeline_scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "_utcnow", lambda: now)

    pipeline_id = "77777777-7777-7777-7777-777777777777"
    registry = _Registry(
        pipelines=[
            {
                "pipeline_id": pipeline_id,
                "db_name": "db",
                "pipeline_type": "batch",
                "schedule_interval_seconds": 60,
                "schedule_cron": None,
                "last_scheduled_at": last_run,
                "last_build_status": "DEPLOYED",
                "last_build_at": last_run,
                "name": "p",
                "branch": "main",
                "output_dataset_name": "pipeline_output",
                "definition_json": {},
            }
        ],
        records={},
    )
    queue = _Queue()

    scheduler = PipelineScheduler(registry, queue, poll_seconds=1)
    await scheduler._tick()

    assert len(queue.published) == 1
    job = queue.published[0]
    assert job.job_id.startswith(f"schedule-{pipeline_id}-")
    assert job.mode == "deploy"
    assert job.schedule_interval_seconds == 60
    assert job.schedule_cron is None
    assert registry.schedule_ticks == [(pipeline_id, now)]
    assert registry.runs == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_triggers_cron_schedule_when_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    last_run = datetime(2025, 1, 1, 11, 59, 0, tzinfo=timezone.utc)

    import shared.services.pipeline_scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "_utcnow", lambda: now)

    pipeline_id = "88888888-8888-8888-8888-888888888888"
    registry = _Registry(
        pipelines=[
            {
                "pipeline_id": pipeline_id,
                "db_name": "db",
                "pipeline_type": "batch",
                "schedule_interval_seconds": None,
                "schedule_cron": "* * * * *",
                "last_scheduled_at": last_run,
                "last_build_status": "DEPLOYED",
                "last_build_at": last_run,
                "name": "p",
                "branch": "main",
                "output_dataset_name": "pipeline_output",
                "definition_json": {},
            }
        ],
        records={},
    )
    queue = _Queue()

    scheduler = PipelineScheduler(registry, queue, poll_seconds=1)
    await scheduler._tick()

    assert len(queue.published) == 1
    job = queue.published[0]
    assert job.job_id.startswith(f"schedule-{pipeline_id}-")
    assert job.mode == "deploy"
    assert job.schedule_interval_seconds is None
    assert job.schedule_cron == "* * * * *"
    assert registry.schedule_ticks == [(pipeline_id, now)]
    assert registry.runs == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_triggers_when_dependency_is_newer_than_pipeline_build(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    pipeline_built_at = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    import shared.services.pipeline_scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "_utcnow", lambda: now)

    pipeline_id = "99999999-9999-9999-9999-999999999999"
    dep_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

    registry = _Registry(
        pipelines=[
            {
                "pipeline_id": pipeline_id,
                "db_name": "db",
                "pipeline_type": "batch",
                "schedule_interval_seconds": None,
                "schedule_cron": None,
                "last_scheduled_at": None,
                "last_build_status": "DEPLOYED",
                "last_build_at": pipeline_built_at,
                "name": "p",
                "branch": "main",
                "definition_json": {},
                "dependencies": [{"pipeline_id": dep_id, "status": "DEPLOYED"}],
            }
        ],
        records={
            pipeline_id: _PipelineRecord(pipeline_id=pipeline_id, db_name="db", last_build_status="DEPLOYED", last_build_at=pipeline_built_at),
            dep_id: _PipelineRecord(pipeline_id=dep_id, db_name="db", last_build_status="DEPLOYED", last_build_at=now),
        },
    )
    queue = _Queue()

    scheduler = PipelineScheduler(registry, queue, poll_seconds=1)
    await scheduler._tick()

    assert len(queue.published) == 1
    job = queue.published[0]
    assert job.job_id.startswith(f"schedule-{pipeline_id}-")
    assert job.mode == "deploy"
    assert registry.schedule_ticks == [(pipeline_id, now)]
    assert registry.runs == []
