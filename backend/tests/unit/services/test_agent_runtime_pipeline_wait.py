from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime, AgentRuntimeConfig


class DummyEventStore:
    def __init__(self) -> None:
        self.events = []

    async def append_event(self, envelope) -> None:
        self.events.append(envelope)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_runtime_waits_for_pipeline_job_completion(monkeypatch: pytest.MonkeyPatch) -> None:
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
            command_timeout_s=5.0,
            command_poll_interval_s=0.1,
            command_ws_idle_s=0.1,
            command_ws_enabled=False,
            pipeline_wait_enabled=True,
            block_writes_on_overlay_degraded=False,
            allow_degraded_writes=True,
            auto_retry_enabled=False,
            auto_retry_max_attempts=1,
            auto_retry_base_delay_s=0.1,
            auto_retry_max_delay_s=0.1,
            auto_retry_allow_writes=False,
        ),
    )

    captured = {"calls": []}

    class FakeResponse:
        def __init__(self, payload):
            self.status_code = 200
            self.headers = {"content-type": "application/json"}
            self._payload = payload
            self.text = ""

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, **kwargs):
            captured["calls"].append(("client", kwargs.get("timeout")))

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, params=None, json=None, headers=None):
            captured["calls"].append(("request", method, url))
            return FakeResponse(
                {
                    "status": "success",
                    "data": {
                        "pipeline_id": "11111111-1111-1111-1111-111111111111",
                        "job_id": "build-1",
                    },
                }
            )

        async def get(self, url, params=None, headers=None):
            captured["calls"].append(("get", url))
            return FakeResponse(
                {
                    "status": "success",
                    "data": {
                        "runs": [
                            {
                                "run_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                                "pipeline_id": "11111111-1111-1111-1111-111111111111",
                                "job_id": "build-1",
                                "mode": "build",
                                "status": "SUCCESS",
                            }
                        ],
                        "count": 1,
                    },
                }
            )

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)
    monkeypatch.setenv("AGENT_PIPELINE_WAIT_ENABLED", "true")

    tool_call = AgentToolCall(
        tool_id="pipelines.build",
        service="bff",
        method="POST",
        path="/api/v1/pipelines/11111111-1111-1111-1111-111111111111/build",
        query={},
        body={"db_name": "demo"},
        headers={},
        data_scope={},
    )

    result = await runtime.execute_tool_call(
        run_id="run-1",
        actor="user-1",
        step_index=0,
        attempt=0,
        tool_call=tool_call,
        context={},
        dry_run=False,
        request_headers={},
        request_id="req-1",
    )

    assert result["status"] == "success"
    assert result["pipeline_job_id"] == "build-1"
    assert result["pipeline_run_status"] == "SUCCESS"
    assert any(call[0] == "get" for call in captured["calls"])
