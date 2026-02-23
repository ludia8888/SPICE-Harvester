from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

import oms.routers.action_async as action_async
from oms.dependencies import OMSDependencyProvider


class _FakeEventStore:
    def __init__(self) -> None:
        self.events: List[Any] = []

    async def append_event(self, event: Any) -> None:
        self.events.append(event)


@pytest.fixture
def app_with_router() -> FastAPI:
    app = FastAPI()
    app.include_router(action_async.foundry_router)

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
    response = await action_async.submit_action_batch_async(
        db_name="demo",
        action_type_id="ApproveTicket",
        request=action_async.ActionSubmitBatchRequest(
            items=[
                action_async.ActionSubmitBatchItemRequest(
                    request_id="root",
                    input={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                    metadata={"user_id": "alice", "user_type": "user"},
                ),
                action_async.ActionSubmitBatchItemRequest(
                    request_id="child",
                    input={"ticket": {"class_id": "Ticket", "instance_id": "t2"}},
                    metadata={"user_id": "alice", "user_type": "user"},
                    depends_on=["root"],
                ),
            ],
            base_branch="main",
        ),
        event_store=app_with_router.state.fake_event_store,
    )

    payload = response.model_dump()
    assert payload["items"][0]["status"] == "PENDING"
    assert payload["items"][1]["status"] == "WAITING_DEPENDENCY"
    assert calls and calls[0]["trigger_on"] == "SUCCEEDED"
    # only root command is emitted immediately
    assert len(app_with_router.state.fake_event_store.events) == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_apply_batch_v2_uses_submit_batch_pipeline(
    app_with_router: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: Dict[str, Any] = {}

    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    async def _fake_submit_action_batch_async(**kwargs: Any) -> action_async.ActionSubmitBatchResponse:  # noqa: ANN401
        captured.update(kwargs)
        return action_async.ActionSubmitBatchResponse(
            batch_id="batch-1",
            db_name="demo",
            action_type_id="ApproveTicket",
            items=[],
        )

    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    monkeypatch.setattr(action_async, "submit_action_batch_async", _fake_submit_action_batch_async)

    transport = ASGITransport(app=app_with_router)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/applyBatch?branch=main",
            json={
                "requests": [
                    {"parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}}},
                    {"parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t2"}}},
                ],
                "metadata": {"user_id": "alice", "user_type": "service"},
            },
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload == {}
    submit_request = captured.get("request")
    assert isinstance(submit_request, action_async.ActionSubmitBatchRequest)
    assert submit_request.base_branch == "main"
    assert len(submit_request.items) == 2
    assert submit_request.items[0].input["ticket"]["instance_id"] == "t1"
    assert submit_request.items[0].metadata["user_id"] == "alice"
    assert submit_request.items[0].metadata["user_type"] == "service"
    routing = submit_request.items[0].metadata.get("__compute_routing")
    assert isinstance(routing, dict)
    assert routing["route"] == "writeback"
    assert routing["estimated_count"] == 2
    assert routing["threshold"] == 10_000
    assert routing["selected_backend"] == "index_pruning"
    assert routing["execution_backend"] == "index_pruning"
    assert routing["request_count"] == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_apply_v2_validate_only_returns_validation_payload(
    app_with_router: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: Dict[str, Any] = {}

    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    async def _fake_simulate_action_async(**kwargs: Any) -> Dict[str, Any]:  # noqa: ANN401
        captured.update(kwargs)
        return {"status": "success", "data": {"results": []}}

    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    monkeypatch.setattr(action_async, "simulate_action_async", _fake_simulate_action_async)

    transport = ASGITransport(app=app_with_router)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/apply?branch=main",
            json={
                "options": {"mode": "VALIDATE_ONLY"},
                "parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                "metadata": {"user_id": "alice", "user_type": "service"},
            },
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["validation"]["result"] == "VALID"
    assert payload["parameters"]["ticket"]["result"] == "VALID"
    assert payload["parameters"]["ticket"]["required"] is True
    assert payload["parameters"]["ticket"]["evaluatedConstraints"] == []
    assert payload["validation"]["submissionCriteria"] == []
    assert payload["validation"]["parameters"]["ticket"]["result"] == "VALID"
    sim_request = captured.get("request")
    assert isinstance(sim_request, action_async.ActionSimulateRequest)
    assert sim_request.base_branch == "main"
    assert sim_request.include_effects is False
    assert sim_request.metadata["user_id"] == "alice"
    routing = sim_request.metadata.get("__compute_routing")
    assert isinstance(routing, dict)
    assert routing["route"] == "writeback"
    assert routing["estimated_count"] == 1
    assert routing["threshold"] == 10_000
    assert routing["selected_backend"] == "index_pruning"
    assert routing["execution_backend"] == "index_pruning"
    assert routing["request_count"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_apply_v2_validate_and_execute_returns_validation_payload(
    app_with_router: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: Dict[str, Any] = {}

    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    async def _fake_submit_action_batch_async(**kwargs: Any) -> action_async.ActionSubmitBatchResponse:  # noqa: ANN401
        captured.update(kwargs)
        return action_async.ActionSubmitBatchResponse(
            batch_id="batch-1",
            db_name="demo",
            action_type_id="ApproveTicket",
            items=[],
        )

    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    monkeypatch.setattr(action_async, "submit_action_batch_async", _fake_submit_action_batch_async)

    transport = ASGITransport(app=app_with_router)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/apply?branch=main",
            json={
                "options": {"mode": "VALIDATE_AND_EXECUTE"},
                "parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
                "metadata": {"user_id": "alice", "user_type": "service"},
            },
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["validation"]["result"] == "VALID"
    assert payload["parameters"]["ticket"]["result"] == "VALID"
    assert payload["parameters"]["ticket"]["required"] is True
    assert payload["parameters"]["ticket"]["evaluatedConstraints"] == []
    assert payload["validation"]["submissionCriteria"] == []
    assert payload["validation"]["parameters"]["ticket"]["result"] == "VALID"
    submit_request = captured.get("request")
    assert isinstance(submit_request, action_async.ActionSubmitBatchRequest)
    assert submit_request.base_branch == "main"
    assert len(submit_request.items) == 1
    routing = submit_request.items[0].metadata.get("__compute_routing")
    assert isinstance(routing, dict)
    assert routing["route"] == "writeback"
    assert routing["estimated_count"] == 1
    assert routing["threshold"] == 10_000
    assert routing["selected_backend"] == "index_pruning"
    assert routing["execution_backend"] == "index_pruning"
    assert routing["request_count"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_foundry_apply_batch_v2_routes_to_spark_when_threshold_exceeded(
    app_with_router: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: Dict[str, Any] = {}

    async def _fake_ensure_ontology_database_exists(ontology: str) -> str:
        return ontology

    async def _fake_submit_action_batch_async(**kwargs: Any) -> action_async.ActionSubmitBatchResponse:  # noqa: ANN401
        captured.update(kwargs)
        return action_async.ActionSubmitBatchResponse(
            batch_id="batch-1",
            db_name="demo",
            action_type_id="ApproveTicket",
            items=[],
        )

    monkeypatch.setenv("ONTOLOGY_WRITEBACK_SPARK_THRESHOLD", "1")
    monkeypatch.setattr(action_async, "_ensure_ontology_database_exists", _fake_ensure_ontology_database_exists)
    monkeypatch.setattr(action_async, "submit_action_batch_async", _fake_submit_action_batch_async)

    transport = ASGITransport(app=app_with_router)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v2/ontologies/demo/actions/ApproveTicket/applyBatch?branch=master",
            json={
                "requests": [
                    {"parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t1"}}},
                    {"parameters": {"ticket": {"class_id": "Ticket", "instance_id": "t2"}}},
                ],
                "metadata": {"user_id": "alice", "user_type": "service"},
            },
        )

    assert response.status_code == 200, response.text
    submit_request = captured.get("request")
    assert isinstance(submit_request, action_async.ActionSubmitBatchRequest)
    routing = submit_request.items[0].metadata.get("__compute_routing")
    assert isinstance(routing, dict)
    assert routing["estimated_count"] == 2
    assert routing["threshold"] == 1
    assert routing["selected_backend"] == "spark_on_demand"
    assert routing["execution_backend"] == "spark_on_demand"
    assert routing["spark_job_id"]
