from __future__ import annotations

import time
from typing import Any, Dict, List, Mapping, Optional


class ToolCallRateLimiter:
    """Rate limit MCP tool calls by logical caller scope."""

    def __init__(
        self,
        *,
        max_calls_per_minute: int = 60,
        max_calls_per_tool_per_minute: int = 30,
        idle_ttl_seconds: int = 300,
    ) -> None:
        self._max_total = max_calls_per_minute
        self._max_per_tool = max_calls_per_tool_per_minute
        self._idle_ttl_seconds = max(60, int(idle_ttl_seconds))
        self._scoped_call_times: Dict[str, List[float]] = {}
        self._scoped_tool_call_times: Dict[str, Dict[str, List[float]]] = {}
        self._scope_last_seen: Dict[str, float] = {}
        self._time = time

    def check_and_record(self, tool_name: str, *, scope: Optional[str] = None) -> Optional[str]:
        now = self._time.time()
        minute_ago = now - 60
        scope_key = str(scope or "").strip() or f"request:{int(now * 1_000_000)}"
        self._evict_idle_scopes(now=now, minute_ago=minute_ago)
        self._scope_last_seen[scope_key] = now

        scoped_calls = [t for t in self._scoped_call_times.get(scope_key, []) if t > minute_ago]
        self._scoped_call_times[scope_key] = scoped_calls

        scoped_tool_calls = self._scoped_tool_call_times.setdefault(scope_key, {})
        tool_calls = [t for t in scoped_tool_calls.get(tool_name, []) if t > minute_ago]
        scoped_tool_calls[tool_name] = tool_calls

        if len(scoped_calls) >= self._max_total:
            return (
                f"RATE LIMIT: Total MCP tool calls exceeded {self._max_total}/minute "
                f"for scope '{scope_key}'. This may indicate an Agent loop. "
                "Wait before retrying or check Agent logic."
            )

        if len(tool_calls) >= self._max_per_tool:
            return (
                f"RATE LIMIT: Tool '{tool_name}' called {self._max_per_tool}+ times/minute "
                f"for scope '{scope_key}'. Consider batching operations or using a different approach."
            )

        scoped_calls.append(now)
        tool_calls.append(now)
        return None

    def _evict_idle_scopes(self, *, now: float, minute_ago: float) -> None:
        idle_cutoff = now - float(self._idle_ttl_seconds)
        scope_keys = set(self._scoped_call_times) | set(self._scoped_tool_call_times) | set(self._scope_last_seen)
        for scope_key in list(scope_keys):
            scoped_calls = [t for t in self._scoped_call_times.get(scope_key, []) if t > minute_ago]
            if scoped_calls:
                self._scoped_call_times[scope_key] = scoped_calls
            else:
                self._scoped_call_times.pop(scope_key, None)

            scoped_tool_calls = self._scoped_tool_call_times.get(scope_key, {})
            filtered_tool_calls = {
                tool_name: [t for t in times if t > minute_ago]
                for tool_name, times in scoped_tool_calls.items()
                if any(t > minute_ago for t in times)
            }
            if filtered_tool_calls:
                self._scoped_tool_call_times[scope_key] = filtered_tool_calls
            else:
                self._scoped_tool_call_times.pop(scope_key, None)

            if (
                self._scope_last_seen.get(scope_key, 0.0) <= idle_cutoff
                and scope_key not in self._scoped_call_times
                and scope_key not in self._scoped_tool_call_times
            ):
                self._scope_last_seen.pop(scope_key, None)


def _coerce_non_empty_token(value: Any) -> Optional[str]:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _value_from_mapping(mapping: Mapping[str, Any], *keys: str) -> Optional[str]:
    lowered = {str(key).strip().lower(): value for key, value in mapping.items()}
    for key in keys:
        value = lowered.get(str(key).strip().lower())
        token = _coerce_non_empty_token(value)
        if token:
            return token
    return None


def _value_from_object(obj: Any, *keys: str) -> Optional[str]:
    for key in keys:
        token = _coerce_non_empty_token(getattr(obj, key, None))
        if token:
            return token
    return None


def _resolve_transport_scope(request_context: Any) -> Optional[str]:
    if request_context is None:
        return None

    session = getattr(request_context, "session", None)
    token = _value_from_object(session, "mcp_session_id", "session_id", "id", "client_id")
    if token:
        return token

    request = getattr(request_context, "request", None)
    headers = getattr(request, "headers", None)
    if isinstance(headers, Mapping):
        token = _value_from_mapping(headers, "mcp-session-id", "x-mcp-session-id")
        if token:
            return token
    elif headers is not None and hasattr(headers, "get"):
        token = _coerce_non_empty_token(headers.get("mcp-session-id") or headers.get("x-mcp-session-id"))
        if token:
            return token

    meta = getattr(request_context, "meta", None)
    if isinstance(meta, Mapping):
        token = _value_from_mapping(meta, "mcp-session-id", "mcp_session_id", "session_id", "client_id")
        if token:
            return token
    elif meta is not None:
        token = _value_from_object(meta, "mcp_session_id", "session_id", "client_id")
        if token:
            return token

    return _coerce_non_empty_token(getattr(request_context, "request_id", None))


def resolve_tool_call_scope(arguments: Optional[dict], *, request_context: Any = None) -> Optional[str]:
    if not isinstance(arguments, dict):
        arguments = {}
    for key in ("session_id", "mcp_session_id", "client_id", "agent_session_id"):
        text = _coerce_non_empty_token(arguments.get(key))
        if text:
            return text
    return _resolve_transport_scope(request_context)
