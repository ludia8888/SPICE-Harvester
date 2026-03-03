from __future__ import annotations

import asyncio

import httpx
import pytest


@pytest.mark.asyncio
async def test_oms_json_uses_grpc_transport(monkeypatch):
    from mcp_servers import pipeline_mcp_http as target

    called: dict[str, object] = {
        "closed": False,
        "http_json_called": False,
        "instances_created": 0,
    }

    async def _should_not_be_called(*args, **kwargs):
        called["http_json_called"] = True
        raise AssertionError("oms_json must not use direct HTTP path")

    class _DummyCompat:
        def __init__(self) -> None:
            called["instances_created"] = int(called["instances_created"]) + 1

        async def request(self, method: str, path: str, **kwargs):
            called["method"] = method
            called["path"] = path
            called["kwargs"] = kwargs
            return httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request(method, f"http://oms.local{path}"),
            )

        async def aclose(self) -> None:
            called["closed"] = True

    monkeypatch.setattr(target, "http_json", _should_not_be_called)
    monkeypatch.setattr(target, "OMSGrpcHttpCompatClient", _DummyCompat)
    monkeypatch.setattr(target, "_OMS_GRPC_COMPAT_CLIENT", None)

    payload = await target.oms_json(
        "POST",
        "/api/v1/database/list",
        params={"branch": "main"},
        json_body={"name": "demo"},
        timeout_seconds=0.1,
    )

    assert payload == {"ok": True}
    assert called["http_json_called"] is False
    assert called["closed"] is False
    assert called["instances_created"] == 1
    assert called["method"] == "POST"
    assert called["path"] == "/api/v1/database/list"

    kwargs = called["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs.get("params") == {"branch": "main"}
    assert kwargs.get("json_body") == {"name": "demo"}
    headers = kwargs.get("headers") or {}
    assert isinstance(headers, dict)
    assert headers.get("Accept") == "application/json"

    payload_2 = await target.oms_json("GET", "/api/v1/database/list", timeout_seconds=0.1)
    assert payload_2 == {"ok": True}
    assert called["instances_created"] == 1
    await target.close_oms_grpc_compat_client()
    assert called["closed"] is True


@pytest.mark.asyncio
async def test_oms_json_timeout_maps_to_504(monkeypatch):
    from mcp_servers import pipeline_mcp_http as target

    class _SlowCompat:
        async def request(self, method: str, path: str, **kwargs):
            await asyncio.sleep(0.2)
            return httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request(method, f"http://oms.local{path}"),
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(target, "OMSGrpcHttpCompatClient", _SlowCompat)
    monkeypatch.setattr(target, "_OMS_GRPC_COMPAT_CLIENT", None)

    payload = await target.oms_json("GET", "/api/v1/database/list", timeout_seconds=0.01)
    assert payload.get("status_code") == 504
    assert "timed out" in str(payload.get("error", "")).lower()
    await target.close_oms_grpc_compat_client()


def test_resolve_db_name_for_bff_call_priority():
    from mcp_servers import pipeline_mcp_http as target

    assert (
        target.resolve_db_name_for_bff_call(
            db_name="explicit-db",
            path="/pipelines",
            params={"db_name": "param-db"},
            json_body={"db_name": "body-db"},
        )
        == "explicit-db"
    )
    assert (
        target.resolve_db_name_for_bff_call(
            db_name="",
            path="/pipelines",
            params={"db_name": "param-db"},
            json_body={"db_name": "body-db"},
        )
        == "param-db"
    )
    assert (
        target.resolve_db_name_for_bff_call(
            db_name="",
            path="/pipelines",
            params=None,
            json_body={"db_name": "body-db"},
        )
        == "body-db"
    )
    assert (
        target.resolve_db_name_for_bff_call(
            db_name="",
            path="/api/v1/databases/path-db/pipelines",
            params=None,
            json_body=None,
        )
        == "path-db"
    )


@pytest.mark.asyncio
async def test_bff_json_includes_project_scope_header(monkeypatch):
    from mcp_servers import pipeline_mcp_http as target

    captured: dict[str, object] = {}

    async def _fake_http_json(method: str, url: str, *, headers: dict[str, str], **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(target, "http_json", _fake_http_json)
    monkeypatch.setattr(target, "_bff_admin_token", lambda: "test-token")
    monkeypatch.setattr(target, "bff_api_base_url", lambda: "http://bff.local/api/v1")

    payload = await target.bff_json(
        "POST",
        "/pipelines",
        db_name="qa_scope_db",
        principal_id="user-1",
        principal_type="user",
        json_body={"db_name": "qa_scope_db"},
    )

    assert payload == {"ok": True}
    assert captured["method"] == "POST"
    assert captured["url"] == "http://bff.local/api/v1/pipelines"
    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers["X-DB-Name"] == "qa_scope_db"
    assert headers["X-Project"] == "qa_scope_db"
    assert headers["X-Project-Scope"] == "qa_scope_db"
    assert headers["X-Principal-Id"] == "user-1"
    assert headers["X-Principal-Type"] == "user"
    assert "Idempotency-Key" in headers
