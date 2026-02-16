from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

import oms.routers.action_async as action_async
from oms.dependencies import OMSDependencyProvider
from shared.services.registries.action_log_registry import ActionLogRecord, ActionLogStatus


class _FakeEventStore:
    def __init__(self) -> None:
        self.events: List[Any] = []

    async def append_event(self, event: Any) -> None:
        self.events.append(event)


@pytest.fixture
def app_with_router() -> FastAPI:
    app = FastAPI()
    app.include_router(action_async.router)

    async def _fake_db_name(db_name: str) -> str:
        return db_name

    fake_store = _FakeEventStore()

    async def _fake_event_store() -> _FakeEventStore:
        return fake_store

    app.state.fake_event_store = fake_store
    app.dependency_overrides[action_async.ensure_database_exists] = _fake_db_name
    app.dependency_overrides[OMSDependencyProvider.get_event_store] = _fake_event_store
    return app


@pytest.mark.unit
@pytest.mark.asyncio
async def test_submit_batch_registers_dependencies_and_defers_children(
    app_with_router: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: List[Dict[str, Any]] = []

    async def _fake_submit_action_async(**kwargs: Any) -> action_async.ActionSubmitResponse:  # noqa: ANN401
        request = kwargs["request"]
        batch_meta = request.metadata.get("__batch") if isinstance(request.metadata, dict) else {}
        req_id = str(batch_meta.get("request_id") or "unknown")
        action_log_id = (
            "00000000-0000-0000-0000-000000000001"
            if req_id == "root"
            else "00000000-0000-0000-0000-000000000002"
        )
        return action_async.ActionSubmitResponse(
            action_log_id=action_log_id,
            status="PENDING",
            db_name="demo",
            action_type_id="ApproveTicket",
            ontology_commit_id="commit-1",
            base_branch=request.base_branch,
            overlay_branch=request.overlay_branch or "writeback-demo",
            writeback_target={"repo": "ontology-writeback", "branch": "writeback-demo"},
        )

    class _FakeRegistry:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def add_dependency(self, **kwargs: Any) -> None:  # noqa: ANN401
            calls.append(kwargs)

    monkeypatch.setattr(action_async, "submit_action_async", _fake_submit_action_async)
    monkeypatch.setattr(action_async, "ActionLogRegistry", _FakeRegistry)

    transport = ASGITransport(app=app_with_router)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/actions/demo/async/ApproveTicket/submit-batch",
            json={
                "items": [
                    {
                        "request_id": "root",
                        "input": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                        "metadata": {"user_id": "alice", "user_type": "user"},
                    },
                    {
                        "request_id": "child",
                        "input": {"ticket": {"class_id": "Ticket", "instance_id": "t2"}},
                        "metadata": {"user_id": "alice", "user_type": "user"},
                        "depends_on": ["root"],
                    },
                ],
                "base_branch": "main",
            },
        )

    assert response.status_code == 202, response.text
    payload = response.json()
    assert payload["items"][0]["status"] == "PENDING"
    assert payload["items"][1]["status"] == "WAITING_DEPENDENCY"
    assert calls and calls[0]["trigger_on"] == "SUCCEEDED"
    # only root command is emitted immediately
    assert len(app_with_router.state.fake_event_store.events) == 1


def _source_log(action_log_id: str, *, status_value: str) -> ActionLogRecord:
    now = datetime.now(timezone.utc)
    return ActionLogRecord(
        action_log_id=action_log_id,
        db_name="demo",
        action_type_id="ApproveTicket",
        action_type_rid="action_type:ApproveTicket",
        resource_rid=None,
        ontology_commit_id="commit-1",
        input={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
        status=status_value,
        result=None,
        correlation_id="corr-1",
        submitted_by="alice",
        submitted_at=now,
        finished_at=now,
        writeback_target={"repo": "ontology-writeback", "branch": "writeback-demo"},
        writeback_commit_id="commit-undo-source",
        action_applied_event_id="evt-1",
        action_applied_seq=1,
        metadata={"__submit_context": {"base_branch": "main", "overlay_branch": "writeback-demo"}},
        updated_at=now,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_undo_action_creates_pending_undo_command(
    app_with_router: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_action_log_id = "00000000-0000-0000-0000-000000000111"
    created_logs: List[Dict[str, Any]] = []

    class _FakeRegistry:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def get_log(self, *, action_log_id: str) -> ActionLogRecord | None:
            if action_log_id == source_action_log_id:
                return _source_log(action_log_id, status_value=ActionLogStatus.SUCCEEDED.value)
            return None

        async def create_log(self, **kwargs: Any) -> Any:  # noqa: ANN401
            created_logs.append(kwargs)
            return SimpleNamespace()

    class _FakeLakeFSStorage:
        async def load_json(self, *, bucket: str, key: str) -> Dict[str, Any]:
            _ = (bucket, key)
            return {
                "targets": [
                    {
                        "resource_rid": "object_type:Ticket",
                        "instance_id": "t1",
                        "observed_base": {"fields": {"status": "OPEN"}, "links": {}},
                        "applied_changes": {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False},
                    }
                ]
            }

    monkeypatch.setattr(action_async, "ActionLogRegistry", _FakeRegistry)
    monkeypatch.setattr(action_async, "create_lakefs_storage_service", lambda _settings: _FakeLakeFSStorage())

    transport = ASGITransport(app=app_with_router)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            f"/actions/demo/async/logs/{source_action_log_id}/undo",
            json={"metadata": {"user_id": "alice", "user_type": "user"}, "reason": "rollback"},
        )

    assert response.status_code == 202, response.text
    body = response.json()
    UUID(body["action_log_id"])
    assert body["status"] == "PENDING"
    assert created_logs, "undo should create a new action log"
    assert len(app_with_router.state.fake_event_store.events) == 1
