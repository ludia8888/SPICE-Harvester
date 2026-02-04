from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Mapping

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


def merge_tool_handlers(*maps: Mapping[str, ToolHandler]) -> Dict[str, ToolHandler]:
    merged: Dict[str, ToolHandler] = {}
    duplicates: set[str] = set()

    for mapping in maps:
        for name, handler in mapping.items():
            if name in merged and merged[name] is not handler:
                duplicates.add(name)
            merged[name] = handler

    if duplicates:
        raise ValueError(f"Duplicate MCP tool handlers: {sorted(duplicates)}")

    return merged

