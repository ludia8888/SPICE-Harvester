from __future__ import annotations

import time
from typing import Dict, List, Optional


class ToolCallRateLimiter:
    """Rate limit MCP tool calls by logical caller scope."""

    def __init__(
        self,
        *,
        max_calls_per_minute: int = 60,
        max_calls_per_tool_per_minute: int = 30,
    ) -> None:
        self._max_total = max_calls_per_minute
        self._max_per_tool = max_calls_per_tool_per_minute
        self._scoped_call_times: Dict[str, List[float]] = {}
        self._scoped_tool_call_times: Dict[str, Dict[str, List[float]]] = {}
        self._time = time

    def check_and_record(self, tool_name: str, *, scope: Optional[str] = None) -> Optional[str]:
        now = self._time.time()
        minute_ago = now - 60
        scope_key = str(scope or "__shared__").strip() or "__shared__"

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


def resolve_tool_call_scope(arguments: Optional[dict]) -> Optional[str]:
    if not isinstance(arguments, dict):
        return None
    for key in ("session_id", "mcp_session_id", "client_id", "agent_session_id"):
        value = arguments.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None
