from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json

import pytest
from fastapi import FastAPI
from starlette.requests import Request

from bff.routers import foundry_connectivity_v2


@dataclass
class _Source:
    source_type: str
    source_id: str
    config_json: dict


class _FakeConnectorRegistry:
    def __init__(self, sources_by_type: dict[str, list[_Source]]) -> None:
        self.sources_by_type = sources_by_type
        self.limits: list[int] = []

    async def list_sources(self, *, source_type: str | None = None, enabled: bool | None = None, limit: int = 500):  # type: ignore[no-untyped-def]
        _ = enabled
        self.limits.append(limit)
        return list(self.sources_by_type.get(source_type or "", []))[:limit]


class _FakeRedis:
    def __init__(self, task_data: dict[str, dict]) -> None:
        self.task_data = task_data

    async def scan_keys(self, pattern: str):  # type: ignore[no-untyped-def]
        _ = pattern
        return list(self.task_data.keys())

    async def get_json(self, key: str):  # type: ignore[no-untyped-def]
        value = self.task_data.get(key)
        return dict(value) if value is not None else None


class _FakeTaskManager:
    def __init__(self, task_data: dict[str, dict]) -> None:
        self.redis = _FakeRedis(task_data)


@pytest.mark.asyncio
async def test_list_connection_owned_sources_expands_beyond_initial_limit() -> None:
    sources = [
        _Source(
            source_type="TABLE_IMPORT",
            source_id=f"src-{idx:04d}",
            config_json={"connection_id": "conn-1"},
        )
        for idx in range(5001)
    ]
    registry = _FakeConnectorRegistry({"TABLE_IMPORT": sources})

    owned = await foundry_connectivity_v2._list_connection_owned_sources(
        connector_registry=registry,
        connection_id="conn-1",
        source_types=("TABLE_IMPORT",),
    )

    assert len(owned) == 5001
    assert registry.limits[:2] == [5000, 10000]


@pytest.mark.asyncio
async def test_list_connections_v2_can_reach_page_after_initial_source_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    app = FastAPI()

    class _RateLimiter:
        async def check_rate_limit(self, request, *, capacity, refill_rate, strategy, tokens):  # type: ignore[no-untyped-def]
            _ = request, capacity, refill_rate, strategy, tokens
            return True, {"remaining": 999, "reset_in": 60}

    app.state.rate_limiter = _RateLimiter()
    request = Request(
        {
            "type": "http",
            "headers": [],
            "app": app,
            "client": ("127.0.0.1", 12345),
            "scheme": "http",
            "server": ("testserver", 80),
            "method": "GET",
            "path": "/v2/connectivity/connections",
            "query_string": b"",
        }
    )
    sources = [
        _Source(
            source_type="CONNECTION",
            source_id=f"conn-{idx:04d}",
            config_json={},
        )
        for idx in range(5001)
    ]
    registry = _FakeConnectorRegistry({"CONNECTION": sources})

    monkeypatch.setattr(foundry_connectivity_v2, "_connection_source_types", lambda: ("CONNECTION",))

    async def _fake_connection_response(*, source, connector_adapter_factory):  # type: ignore[no-untyped-def]
        _ = connector_adapter_factory
        return {"rid": source.source_id}

    monkeypatch.setattr(foundry_connectivity_v2, "_connection_response", _fake_connection_response)

    response = await foundry_connectivity_v2.list_connections_v2(
        request=request,
        preview=True,
        pageSize=10,
        pageToken="5000",
        connector_registry=registry,
        connector_adapter_factory=object(),
    )

    assert response["data"] == [{"rid": "conn-5000"}]
    assert response["nextPageToken"] is None


@pytest.mark.asyncio
async def test_list_connection_export_runs_v2_scans_all_tasks_before_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(timezone.utc)
    task_data = {}
    for idx in range(5002):
        task_data[f"background_task:{idx}"] = {
            "task_id": f"task-{idx}",
            "task_name": f"Task {idx}",
            "task_type": "connectivity_export_run",
            "status": "COMPLETED",
            "priority": "NORMAL",
            "created_at": (now - timedelta(seconds=idx)).isoformat(),
            "started_at": (now - timedelta(seconds=idx)).isoformat(),
            "completed_at": (now - timedelta(seconds=idx - 1)).isoformat(),
            "retry_count": 0,
            "max_retries": 3,
            "metadata": {"connection_id": "conn-1"},
            "child_task_ids": [],
            "result": {"success": True, "data": {}, "warnings": []},
        }

    async def _fake_load_connection_source_or_404(*, connector_registry, connection_rid):  # type: ignore[no-untyped-def]
        _ = connector_registry, connection_rid
        return "conn-1", _Source("CONNECTION", "conn-1", {}), None

    monkeypatch.setattr(
        foundry_connectivity_v2,
        "_load_connection_source_or_404",
        _fake_load_connection_source_or_404,
    )

    response = await foundry_connectivity_v2.list_connection_export_runs_v2(
        connectionRid="ri.foundry.main.connection.conn-1",
        task_manager=_FakeTaskManager(task_data),
        preview=True,
        pageSize=10,
        pageToken="5000",
        connector_registry=object(),
    )

    payload = json.loads(response.body.decode("utf-8"))
    task_ids = [row["taskId"] for row in payload["data"]]
    assert task_ids == ["task-5000", "task-5001"]
    assert payload["nextPageToken"] is None
