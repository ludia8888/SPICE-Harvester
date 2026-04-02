from __future__ import annotations

import asyncio
import sys
import types

import pytest


def _import_target_with_mcp_stubs(monkeypatch: pytest.MonkeyPatch):
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


class _FakeTransport:
    def __init__(self, session_id: str):
        self._session_id = session_id
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_streamable_session_registry_reuses_and_closes_session(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _import_target_with_mcp_stubs(monkeypatch)
    registry = target._StreamableSessionRegistry(idle_ttl_seconds=60, max_sessions=10)

    async def _run_server(transport):  # type: ignore[no-untyped-def]
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise

    first = await registry.get_or_create(
        "session-1",
        transport_factory=_FakeTransport,
        server_runner_factory=_run_server,
    )
    second = await registry.get_or_create(
        "session-1",
        transport_factory=_FakeTransport,
        server_runner_factory=_run_server,
    )

    assert first is second
    assert len(registry._sessions) == 1

    await registry.close("session-1")

    assert first.transport.closed is True
    assert "session-1" not in registry._sessions


@pytest.mark.asyncio
async def test_streamable_session_registry_evicts_idle_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _import_target_with_mcp_stubs(monkeypatch)
    registry = target._StreamableSessionRegistry(idle_ttl_seconds=0, max_sessions=10)

    async def _run_server(transport):  # type: ignore[no-untyped-def]
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise

    stale = await registry.get_or_create(
        "stale-session",
        transport_factory=_FakeTransport,
        server_runner_factory=_run_server,
    )
    stale.last_seen = 0.0

    fresh = await registry.get_or_create(
        "fresh-session",
        transport_factory=_FakeTransport,
        server_runner_factory=_run_server,
    )

    await asyncio.sleep(0)

    assert stale.transport.closed is True
    assert "stale-session" not in registry._sessions
    assert registry._sessions["fresh-session"] is fresh

    await registry.shutdown()
