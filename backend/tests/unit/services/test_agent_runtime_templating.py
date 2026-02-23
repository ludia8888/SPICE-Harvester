from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime, AgentRuntimeConfig


class DummyEventStore:
    def __init__(self) -> None:
        self.events = []

    async def append_event(self, envelope) -> None:  # noqa: ANN001
        self.events.append(envelope)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_runtime_resolves_step_output_templates(monkeypatch: pytest.MonkeyPatch) -> None:
    import agent.services.agent_runtime as module

    store = DummyEventStore()
    runtime = AgentRuntime(
        event_store=store,  # type: ignore[arg-type]
        audit_store=None,
        config=AgentRuntimeConfig(
            bff_url="http://bff",
            allowed_services=("bff",),
            max_preview_chars=2000,
            max_payload_bytes=200000,
            timeout_s=1.0,
            service_name="agent",
            bff_token=None,
            command_timeout_s=1.0,
            command_poll_interval_s=0.1,
            command_ws_idle_s=0.1,
            command_ws_enabled=False,
            pipeline_wait_enabled=False,
            block_writes_on_overlay_degraded=False,
            allow_degraded_writes=True,
            auto_retry_enabled=False,
            auto_retry_max_attempts=1,
            auto_retry_base_delay_s=0.0,
            auto_retry_max_delay_s=0.0,
            auto_retry_allow_writes=False,
        ),
    )

    captured: list[tuple[str, str]] = []

    class FakeResponse:
        def __init__(self, payload, *, status_code: int = 200):
            self.status_code = status_code
            self.headers = {"content-type": "application/json"}
            self._payload = payload
            self.text = ""

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, **kwargs):  # noqa: ANN003
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: ANN001
            captured.append((method, url))
            if url.rstrip("/") == "http://bff/api/v2/datasets":
                return FakeResponse(
                    {
                        "rid": "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111",
                    }
                )
            if "http://bff/api/v2/datasets/11111111-1111-1111-1111-111111111111" in url:
                return FakeResponse(
                    {
                        "rid": "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111",
                        "name": "orders",
                    }
                )
            return FakeResponse({"status": "error"}, status_code=404)

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)

    context: dict = {}

    create_call = AgentToolCall(
        step_id="create_dataset",
        tool_id="datasets.create",
        service="bff",
        method="POST",
        path="/api/v2/datasets",
        body={"name": "orders", "parentFolderRid": "ri.foundry.main.folder.demo"},
    )

    create_result = await runtime.execute_tool_call(
        run_id="run-1",
        actor="user-1",
        step_index=0,
        attempt=0,
        tool_call=create_call,
        context=context,
        dry_run=False,
        request_headers={},
        request_id="req-1",
    )

    assert create_result["status"] == "success"
    assert context["step_outputs"]["create_dataset"]["dataset_id"] == "11111111-1111-1111-1111-111111111111"

    get_call = AgentToolCall(
        step_id="get_dataset",
        tool_id="datasets.get",
        service="bff",
        method="GET",
        path="/api/v2/datasets/${steps.create_dataset.dataset_id}",
    )

    get_result = await runtime.execute_tool_call(
        run_id="run-1",
        actor="user-1",
        step_index=1,
        attempt=0,
        tool_call=get_call,
        context=context,
        dry_run=False,
        request_headers={},
        request_id="req-1",
    )

    assert get_result["status"] == "success"
    assert any(
        url == "http://bff/api/v2/datasets/11111111-1111-1111-1111-111111111111" for _, url in captured
    )
