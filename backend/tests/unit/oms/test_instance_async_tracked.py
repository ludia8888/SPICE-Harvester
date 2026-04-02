from __future__ import annotations

from fastapi import BackgroundTasks
import pytest

from oms.routers import instance_async


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_create_tracked_returns_command_status_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve(*args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        _ = args, kwargs
        return {"commit": "commit-1"}

    monkeypatch.setattr(instance_async, "resolve_ontology_version", _fake_resolve)
    monkeypatch.setattr(
        instance_async,
        "_enforce_ingest_only_if_writeback_enabled",
        lambda **kwargs: None,
    )

    response = await instance_async.bulk_create_instances_with_tracking(
        db_name="demo",
        class_id="Ticket",
        branch="main",
        request=instance_async.BulkInstanceCreateRequest(instances=[{"id": "1"}], metadata={}),
        background_tasks=BackgroundTasks(),
        command_status_service=None,
        event_store=object(),
        user_id="alice",
    )

    assert response["status_url"] == f"/api/v1/instances/demo/async/command/{response['task_id']}/status"
