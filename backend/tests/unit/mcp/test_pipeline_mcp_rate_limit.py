from __future__ import annotations

from types import SimpleNamespace

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


@pytest.mark.unit
def test_resolve_tool_call_scope_uses_transport_context_before_request_id() -> None:
    request_context = SimpleNamespace(
        session=SimpleNamespace(id="session-from-context"),
        request=SimpleNamespace(headers={"mcp-session-id": "header-session"}),
        request_id="request-1",
    )

    assert resolve_tool_call_scope({}, request_context=request_context) == "session-from-context"


@pytest.mark.unit
def test_resolve_tool_call_scope_falls_back_to_request_id_when_session_is_missing() -> None:
    request_context = SimpleNamespace(
        session=SimpleNamespace(),
        request=SimpleNamespace(headers={}),
        request_id="request-42",
    )

    assert resolve_tool_call_scope({}, request_context=request_context) == "request-42"


@pytest.mark.unit
def test_tool_call_rate_limiter_evicts_idle_scopes() -> None:
    limiter = ToolCallRateLimiter(
        max_calls_per_minute=2,
        max_calls_per_tool_per_minute=2,
        idle_ttl_seconds=120,
    )

    class _FakeTime:
        def __init__(self) -> None:
            self.current = 0.0

        def time(self) -> float:
            return self.current

    fake_time = _FakeTime()
    limiter._time = fake_time  # type: ignore[assignment]

    assert limiter.check_and_record("tool-a", scope="session-1") is None
    fake_time.current = 181.0
    assert limiter.check_and_record("tool-a", scope="session-2") is None

    assert "session-1" not in limiter._scope_last_seen
    assert "session-1" not in limiter._scoped_call_times


@pytest.mark.unit
def test_rate_limit_isolation_does_not_share_missing_explicit_session_arguments() -> None:
    limiter = ToolCallRateLimiter(max_calls_per_minute=1, max_calls_per_tool_per_minute=1)
    request_context_a = SimpleNamespace(session=SimpleNamespace(), request=SimpleNamespace(headers={}), request_id="req-a")
    request_context_b = SimpleNamespace(session=SimpleNamespace(), request=SimpleNamespace(headers={}), request_id="req-b")

    scope_a = resolve_tool_call_scope({}, request_context=request_context_a)
    scope_b = resolve_tool_call_scope({}, request_context=request_context_b)

    assert scope_a == "req-a"
    assert scope_b == "req-b"
    assert limiter.check_and_record("tool-a", scope=scope_a) is None
    assert limiter.check_and_record("tool-a", scope=scope_b) is None
