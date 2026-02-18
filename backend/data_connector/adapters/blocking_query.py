from __future__ import annotations

import asyncio
from typing import Any, Callable

DEFAULT_JDBC_THREAD_TIMEOUT_SECONDS = 300


async def run_blocking_query(
    fn: Callable[[], Any],
    *,
    adapter_name: str,
    operation: str,
    timeout_seconds: int = DEFAULT_JDBC_THREAD_TIMEOUT_SECONDS,
) -> Any:
    try:
        return await asyncio.wait_for(asyncio.to_thread(fn), timeout=timeout_seconds)
    except TimeoutError as exc:
        raise RuntimeError(f"{adapter_name} connector timed out during {operation}") from exc
