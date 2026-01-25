from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime, AgentRuntimeConfig
from uuid import UUID


class DummyEventStore:
    def __init__(self) -> None:
        self.events = []

    async def append_event(self, envelope) -> None:  # noqa: ANN001
        self.events.append(envelope)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_runtime_blocks_missing_required_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    import agent.services.agent_runtime as module

    runtime = AgentRuntime(
        event_store=DummyEventStore(),  # type: ignore[arg-type]
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

    class FakeClient:
        def __init__(self, **kwargs):  # noqa: ANN003
            raise AssertionError("httpx client should not be created when artifacts are missing")

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)

    tool_call = AgentToolCall(
        step_id="needs_artifact",
        tool_id="pipelines.preview",
        service="bff",
        method="POST",
        path="/api/v1/pipelines/abc/preview",
        query={},
        body={"limit": 10},
        headers={},
        data_scope={},
        consumes=["artifact.required"],
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

    assert result["status"] == "failed"
    assert result["http_status"] == 400
    assert result["error_key"] == "artifact_missing"
    assert result["missing_artifacts"] == ["artifact.required"]
    assert UUID(str(result["tool_run_id"]))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_runtime_stores_produced_artifacts_and_resolves_templates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent.services.agent_runtime as module

    runtime = AgentRuntime(
        event_store=DummyEventStore(),  # type: ignore[arg-type]
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

    captured: list[dict[str, object]] = []

    class FakeResponse:
        def __init__(self, payload, *, status_code: int = 200):  # noqa: ANN001
            self.status_code = status_code
            self.headers = {"content-type": "application/json"}
            self._payload = payload
            self.text = ""

        def json(self):  # noqa: ANN001
            return self._payload

    class FakeClient:
        def __init__(self, **kwargs):  # noqa: ANN003
            pass

        async def __aenter__(self):  # noqa: ANN001
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: ANN001
            if url.rstrip("/") == "http://bff/api/v1/pipelines/simulate-definition":
                return FakeResponse(
                    {
                        "status": "success",
                        "data": {
                            "nodes": [{"node_id": "n1", "type": "source"}],
                            "edges": [],
                        },
                    }
                )
            if url.rstrip("/") == "http://bff/api/v1/pipelines":
                captured.append({"method": method, "url": url, "json": json})
                return FakeResponse({"status": "success", "data": {"pipeline_id": "p1"}})
            raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)

    context: dict[str, object] = {}

    step1 = AgentToolCall(
        step_id="suggest_definition",
        tool_id="pipelines.simulate_definition",
        service="bff",
        method="POST",
        path="/api/v1/pipelines/simulate-definition",
        query={},
        body={"definition_json": {"nodes": [], "edges": []}},
        headers={},
        data_scope={},
        produces=["artifact.pipeline_definition"],
    )

    step2 = AgentToolCall(
        step_id="create_pipeline",
        tool_id="pipelines.create",
        service="bff",
        method="POST",
        path="/api/v1/pipelines",
        query={},
        body={"definition_json": "${artifacts.artifact.pipeline_definition}"},
        headers={},
        data_scope={},
        consumes=["artifact.pipeline_definition"],
    )

    result1 = await runtime.execute_tool_call(
        run_id="run-1",
        actor="user-1",
        step_index=0,
        attempt=0,
        tool_call=step1,
        context=context,  # type: ignore[arg-type]
        dry_run=False,
        request_headers={},
        request_id="req-1",
    )
    assert result1["status"] == "success"
    assert UUID(str(result1["tool_run_id"]))

    artifacts = context.get("artifacts")
    assert isinstance(artifacts, dict)
    assert artifacts["artifact.pipeline_definition"] == {
        "nodes": [{"node_id": "n1", "type": "source"}],
        "edges": [],
    }

    result2 = await runtime.execute_tool_call(
        run_id="run-1",
        actor="user-1",
        step_index=1,
        attempt=0,
        tool_call=step2,
        context=context,  # type: ignore[arg-type]
        dry_run=False,
        request_headers={},
        request_id="req-1",
    )
    assert result2["status"] == "success"
    assert UUID(str(result2["tool_run_id"]))

    assert captured == [
        {
            "method": "POST",
            "url": "http://bff/api/v1/pipelines",
            "json": {
                "definition_json": {
                    "nodes": [{"node_id": "n1", "type": "source"}],
                    "edges": [],
                }
            },
        }
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_runtime_compacts_large_tool_payload_instead_of_omitting(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    When a tool response is too large, AgentRuntime should attempt to compact it
    (e.g., drop run_tables) so downstream graph code can still read key fields.
    """
    import agent.services.agent_runtime as module

    runtime = AgentRuntime(
        event_store=DummyEventStore(),  # type: ignore[arg-type]
        audit_store=None,
        config=AgentRuntimeConfig(
            bff_url="http://bff",
            allowed_services=("bff",),
            max_preview_chars=2000,
            max_payload_bytes=1200,  # intentionally small to force compaction
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

    class FakeResponse:
        def __init__(self, payload, *, status_code: int = 200):  # noqa: ANN001
            self.status_code = status_code
            self.headers = {"content-type": "application/json"}
            self._payload = payload
            self.text = ""

        def json(self):  # noqa: ANN001
            return self._payload

    class FakeClient:
        def __init__(self, **kwargs):  # noqa: ANN003
            pass

        async def __aenter__(self):  # noqa: ANN001
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: ANN001
            if url.rstrip("/") != "http://bff/api/v1/pipeline-plans/p1/preview":
                raise AssertionError(f"unexpected url: {url}")
            return FakeResponse(
                {
                    "status": "success",
                    "data": {
                        "preview": {"columns": [{"name": "a"}], "rows": [{"a": "1"}]},
                        "run_tables": {
                            "n1": {
                                "columns": ["a"],
                                "rows": [{"a": "x" * 5000}],
                            }
                        },
                    },
                }
            )

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)

    tool_call = AgentToolCall(
        step_id="preview",
        tool_id="pipeline_plans.preview",
        service="bff",
        method="POST",
        path="/api/v1/pipeline-plans/p1/preview",
        query={},
        body={"include_run_tables": True},
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
    payload = result["payload"]
    assert isinstance(payload, dict)
    assert payload.get("_omitted") is None
    assert isinstance(payload.get("data"), dict)
    assert payload["data"].get("run_tables") is None
    assert payload["data"].get("preview") == {"columns": [{"name": "a"}], "rows": [{"a": "1"}]}
