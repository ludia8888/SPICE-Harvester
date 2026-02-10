from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from bff.schemas.objectify_requests import RunObjectifyDAGRequest
from bff.services.objectify_dag_service import _ObjectifyDagOrchestrator


@dataclass
class _FakeObjectifyRegistry:
    record: Optional[Any]

    async def get_objectify_job(self, job_id: str) -> Any:  # noqa: ARG002
        return self.record


def _make_orchestrator(record: Any) -> _ObjectifyDagOrchestrator:
    return _ObjectifyDagOrchestrator(
        db_name="demo_db",
        body=RunObjectifyDAGRequest(class_ids=["Order"]),
        dataset_registry=SimpleNamespace(),
        objectify_registry=_FakeObjectifyRegistry(record=record),  # type: ignore[arg-type]
        job_queue=SimpleNamespace(),
        oms_client=SimpleNamespace(),
    )


@pytest.mark.asyncio
async def test_wait_for_objectify_submitted_allows_dataset_primary_completed_without_commands() -> None:
    record = SimpleNamespace(
        status="COMPLETED",
        report={"write_path_mode": "dataset_primary_index", "command_ids": []},
        command_id=None,
        error=None,
    )
    orchestrator = _make_orchestrator(record)

    command_ids = await orchestrator._wait_for_objectify_submitted("job-1", timeout_seconds=1)
    assert command_ids == []


@pytest.mark.asyncio
async def test_wait_for_objectify_submitted_defaults_to_dataset_primary_when_report_missing() -> None:
    record = SimpleNamespace(
        status="COMPLETED",
        report={"command_ids": []},
        command_id=None,
        error=None,
    )
    orchestrator = _make_orchestrator(record)

    command_ids = await orchestrator._wait_for_objectify_submitted("job-1", timeout_seconds=1)
    assert command_ids == []


@pytest.mark.asyncio
async def test_wait_for_objectify_submitted_requires_commands_for_submitted_status() -> None:
    record = SimpleNamespace(
        status="SUBMITTED",
        report={"command_ids": []},
        command_id=None,
        error=None,
    )
    orchestrator = _make_orchestrator(record)

    with pytest.raises(RuntimeError, match="without command_ids"):
        await orchestrator._wait_for_objectify_submitted("job-2", timeout_seconds=1)
