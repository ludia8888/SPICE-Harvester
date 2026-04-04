from __future__ import annotations

from types import SimpleNamespace

import pytest

from mcp_servers.pipeline_mcp_rate_limit import ToolCallRateLimiter, resolve_tool_call_scope


pytestmark = [pytest.mark.smoke]


def test_pipeline_mcp_rate_limit_isolates_transport_sessions() -> None:
    limiter = ToolCallRateLimiter(max_calls_per_minute=1, max_calls_per_tool_per_minute=1)
    session_a = SimpleNamespace(
        session=SimpleNamespace(id="mcp-session-a"),
        request=SimpleNamespace(headers={"mcp-session-id": "mcp-session-a"}),
        request_id="request-a",
    )
    session_b = SimpleNamespace(
        session=SimpleNamespace(id="mcp-session-b"),
        request=SimpleNamespace(headers={"mcp-session-id": "mcp-session-b"}),
        request_id="request-b",
    )

    scope_a = resolve_tool_call_scope({}, request_context=session_a)
    scope_b = resolve_tool_call_scope({}, request_context=session_b)

    assert scope_a == "mcp-session-a"
    assert scope_b == "mcp-session-b"
    assert limiter.check_and_record("plan.preview", scope=scope_a) is None
    assert limiter.check_and_record("plan.preview", scope=scope_b) is None
    assert limiter.check_and_record("plan.preview", scope=scope_a) is not None
