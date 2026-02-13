from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, HTTPException

from bff.schemas.admin_projection_requests import RecomputeProjectionRequest
from bff.services.admin_recompute_projection_service import (
    recompute_projection_task,
    start_recompute_projection,
)


class _TaskManagerStub:
    def __init__(self) -> None:
        self.calls = 0

    async def create_task(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.calls += 1
        self.last_args = args
        self.last_kwargs = kwargs
        return "task-1"


def _request_payload(*, projection: str) -> RecomputeProjectionRequest:
    return RecomputeProjectionRequest(
        db_name="demo_db",
        projection=projection,
        branch="main",
        from_ts=datetime.now(timezone.utc),
        to_ts=None,
        promote=False,
    )


def _http_request_stub() -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(admin_actor="admin-user"),
        client=SimpleNamespace(host="127.0.0.1"),
    )


@pytest.mark.asyncio
async def test_start_recompute_projection_blocks_instances_in_dataset_primary_mode(
) -> None:
    task_manager = _TaskManagerStub()
    with pytest.raises(HTTPException) as raised:
        await start_recompute_projection(
            http_request=_http_request_stub(),  # type: ignore[arg-type]
            request=_request_payload(projection="instances"),
            background_tasks=BackgroundTasks(),
            task_manager=task_manager,  # type: ignore[arg-type]
            redis_service=SimpleNamespace(),
            audit_store=SimpleNamespace(),
            lineage_store=SimpleNamespace(),
            elasticsearch_service=SimpleNamespace(),
        )

    assert raised.value.status_code == 400
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail.get("error") == "REQUEST_VALIDATION_FAILED"
    assert task_manager.calls == 0


@pytest.mark.asyncio
async def test_start_recompute_projection_allows_ontologies_in_dataset_primary_mode(
) -> None:
    task_manager = _TaskManagerStub()
    response = await start_recompute_projection(
        http_request=_http_request_stub(),  # type: ignore[arg-type]
        request=_request_payload(projection="ontologies"),
        background_tasks=BackgroundTasks(),
        task_manager=task_manager,  # type: ignore[arg-type]
        redis_service=SimpleNamespace(),
        audit_store=SimpleNamespace(),
        lineage_store=SimpleNamespace(),
        elasticsearch_service=SimpleNamespace(),
    )

    assert response.status == "accepted"
    assert task_manager.calls == 1


@pytest.mark.asyncio
async def test_recompute_projection_task_blocks_instances_in_dataset_primary_mode(
) -> None:
    with pytest.raises(RuntimeError, match="unsupported in dataset-primary mode"):
        await recompute_projection_task(
            task_id="task-1",
            request=_request_payload(projection="instances"),
            elasticsearch_service=SimpleNamespace(),
            redis_service=SimpleNamespace(),
            audit_store=SimpleNamespace(),
            lineage_store=SimpleNamespace(),
            requested_by=None,
            request_ip=None,
        )
