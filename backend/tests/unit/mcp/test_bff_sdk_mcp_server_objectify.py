from __future__ import annotations

import json
import sys
import types

import pytest


def _decode_mcp_text_result(result: list) -> dict:
    assert result
    first = result[0]
    text = getattr(first, "text", "")
    assert isinstance(text, str)
    return json.loads(text)


def _import_target_with_mcp_stubs(monkeypatch):
    class _Tool:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _ToolsCapability:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _ServerCapabilities:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _TextContent:
        def __init__(self, *, type: str, text: str):
            self.type = type
            self.text = text

    class _Server:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def list_tools(self):
            def _decorator(func):
                return func

            return _decorator

        def call_tool(self):
            def _decorator(func):
                return func

            return _decorator

    async def _stdio_server(*args, **kwargs):
        raise RuntimeError("not used in unit test")

    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    stdio_module = types.ModuleType("mcp.server.stdio")
    types_module = types.ModuleType("mcp.types")

    server_module.Server = _Server
    stdio_module.stdio_server = _stdio_server
    types_module.ServerCapabilities = _ServerCapabilities
    types_module.Tool = _Tool
    types_module.ToolsCapability = _ToolsCapability
    types_module.TextContent = _TextContent

    monkeypatch.setitem(sys.modules, "mcp", mcp_module)
    monkeypatch.setitem(sys.modules, "mcp.server", server_module)
    monkeypatch.setitem(sys.modules, "mcp.server.stdio", stdio_module)
    monkeypatch.setitem(sys.modules, "mcp.types", types_module)
    sys.modules.pop("mcp_servers.bff_sdk_mcp_server", None)

    from mcp_servers import bff_sdk_mcp_server as target

    return target


def test_pipeline_target_rid_normalizes_uuid_rid_and_scheme(monkeypatch):
    target = _import_target_with_mcp_stubs(monkeypatch)

    uuid_value = "29db2af7-84ef-4567-bebb-b457e4696d7a"
    rid = f"ri.foundry.main.pipeline.{uuid_value}"

    assert target._pipeline_target_rid(uuid_value) == rid
    assert target._pipeline_target_rid(rid) == rid
    assert target._pipeline_target_rid(f"pipeline://{uuid_value}") == rid
    assert target._coerce_pipeline_id(rid) == uuid_value
    assert target._coerce_pipeline_id(f"pipeline://{uuid_value}") == uuid_value


@pytest.mark.asyncio
async def test_bff_execute_pipeline_accepts_pipeline_rid(monkeypatch):
    target = _import_target_with_mcp_stubs(monkeypatch)

    captured: dict[str, object] = {}
    pipeline_id = "29db2af7-84ef-4567-bebb-b457e4696d7a"
    pipeline_rid = f"ri.foundry.main.pipeline.{pipeline_id}"

    async def _fake_bff_v2_json(method: str, path: str, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["kwargs"] = kwargs
        return {"status": "accepted", "rid": "ri.foundry.main.build.build-123"}

    monkeypatch.setattr(target, "bff_v2_json", _fake_bff_v2_json)

    result = await target._handle_tool(
        "bff_execute_pipeline",
        {
            "db_name": "qa_db",
            "pipeline_id": pipeline_rid,
            "mode": "build",
            "branch": "main",
        },
    )

    payload = _decode_mcp_text_result(result)
    assert payload["status"] == "accepted"
    assert captured["method"] == "POST"
    assert captured["path"] == "/v2/orchestration/builds/create"
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    json_body = kwargs.get("json_body")
    assert isinstance(json_body, dict)
    assert json_body["target"]["targetRids"] == [pipeline_rid]


@pytest.mark.asyncio
async def test_bff_start_objectify_uses_v1_objectify_fanout(monkeypatch):
    target = _import_target_with_mcp_stubs(monkeypatch)

    calls: list[tuple[str, str, dict]] = []
    pipeline_id = "29db2af7-84ef-4567-bebb-b457e4696d7a"
    pipeline_rid = f"ri.foundry.main.pipeline.{pipeline_id}"

    async def _fake_bff_json(method: str, path: str, **kwargs):
        calls.append((method, path, kwargs))
        if method == "GET" and path == f"/pipelines/{pipeline_id}/datasets":
            return {
                "status": "success",
                "data": {
                    "datasets": [
                        {"dataset_id": "ds-1"},
                        {"id": "ds-2"},
                    ]
                },
            }
        if method == "POST" and path.startswith("/objectify/datasets/"):
            return {"status": "success", "data": {"job_id": f"job-{path.split('/')[3]}"}}
        return {"error": "unexpected path"}

    async def _should_not_call_v2(*args, **kwargs):
        raise AssertionError("bff_start_objectify must not call v2 orchestration builds/create")

    monkeypatch.setattr(target, "bff_json", _fake_bff_json)
    monkeypatch.setattr(target, "bff_v2_json", _should_not_call_v2)

    result = await target._handle_tool(
        "bff_start_objectify",
        {
            "db_name": "qa_db",
            "pipeline_id": pipeline_rid,
            "mapping_spec_id": "f29ea8d3-2f2b-4ef7-8b25-a7dfef316421",
            "allow_partial": True,
            "batch_size": 500,
        },
    )

    payload = _decode_mcp_text_result(result)
    assert payload["status"] == "success"
    assert payload["pipeline_id"] == pipeline_id
    assert payload["targetRid"] == pipeline_rid
    assert payload["dataset_count"] == 2
    assert len(payload["enqueued_jobs"]) == 2
    assert payload["failed_jobs"] == []

    get_calls = [entry for entry in calls if entry[0] == "GET"]
    post_calls = [entry for entry in calls if entry[0] == "POST"]
    assert len(get_calls) == 1
    assert get_calls[0][1] == f"/pipelines/{pipeline_id}/datasets"
    assert {path for _, path, _ in post_calls} == {
        "/objectify/datasets/ds-1/run",
        "/objectify/datasets/ds-2/run",
    }
    for _, _, kwargs in post_calls:
        body = kwargs.get("json_body")
        assert body["mapping_spec_id"] == "f29ea8d3-2f2b-4ef7-8b25-a7dfef316421"
        assert body["allow_partial"] is True
        assert body["batch_size"] == 500
