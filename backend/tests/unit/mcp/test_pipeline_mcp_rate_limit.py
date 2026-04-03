from __future__ import annotations

import pytest

from mcp_servers.pipeline_mcp_rate_limit import ToolCallRateLimiter, resolve_tool_call_scope


@pytest.mark.unit
def test_tool_call_rate_limiter_isolates_scopes() -> None:
    limiter = ToolCallRateLimiter(max_calls_per_minute=1, max_calls_per_tool_per_minute=1)

    assert limiter.check_and_record("tool-a", scope="session-1") is None
    assert limiter.check_and_record("tool-a", scope="session-2") is None


@pytest.mark.unit
def test_tool_call_rate_limiter_limits_same_scope() -> None:
    limiter = ToolCallRateLimiter(max_calls_per_minute=2, max_calls_per_tool_per_minute=1)

    assert limiter.check_and_record("tool-a", scope="session-1") is None
    error = limiter.check_and_record("tool-a", scope="session-1")

    assert error is not None
    assert "session-1" in error


@pytest.mark.unit
def test_resolve_tool_call_scope_prefers_known_session_keys() -> None:
    assert resolve_tool_call_scope({"session_id": "abc"}) == "abc"
    assert resolve_tool_call_scope({"client_id": "client-1"}) == "client-1"
    assert resolve_tool_call_scope({"mcp_session_id": "mcp-1"}) == "mcp-1"
    assert resolve_tool_call_scope({"other": "x"}) is None
