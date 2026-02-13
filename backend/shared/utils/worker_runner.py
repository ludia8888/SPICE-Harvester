"""Async worker entrypoint runner (Template Method).

Many workers share the same `main()` boilerplate:
- install SIGINT/SIGTERM handlers
- initialize -> run loop -> graceful shutdown

This helper centralizes that control-flow to reduce duplication and drift.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import signal
from contextlib import suppress
from typing import Any, Optional

logger = logging.getLogger(__name__)


async def run_worker_until_stopped(
    worker: Any,
    *,
    task_name: Optional[str] = None,
    running_attr: str = "running",
    shutdown_on_exit: bool = True,
) -> None:
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    stop_task = asyncio.create_task(stop_event.wait(), name="worker.stop_event.wait")
    run_task: Optional[asyncio.Task] = None

    try:
        await worker.initialize()
        run_task = asyncio.create_task(
            worker.run(),
            name=task_name or f"{worker.__class__.__name__}.run",
        )

        done, _ = await asyncio.wait(
            {run_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done and not run_task.done():
            if hasattr(worker, running_attr):
                with suppress(Exception):
                    setattr(worker, running_attr, False)
            run_task.cancel()

        stop_task.cancel()
        with suppress(asyncio.CancelledError):
            await stop_task

        if run_task.cancelled():
            return
        await run_task
    finally:
        stop_task.cancel()
        if run_task and not run_task.done():
            run_task.cancel()
            with suppress(asyncio.CancelledError):
                await run_task
        if shutdown_on_exit and hasattr(worker, "shutdown"):
            await worker.shutdown()


async def run_component_lifecycle(
    component: Any,
    *,
    init_method: str = "initialize",
    run_method: str = "run",
    close_method: str = "close",
) -> None:
    initializer = getattr(component, init_method)
    await initializer()
    primary_exc: Optional[BaseException] = None
    try:
        runner = getattr(component, run_method)
        await runner()
    except BaseException as exc:
        primary_exc = exc
        raise
    finally:
        closer = getattr(component, close_method, None)
        if closer is not None:
            try:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            except Exception as close_exc:
                if primary_exc is not None:
                    logger.warning(
                        "Component close failed after primary lifecycle error: %s",
                        close_exc,
                        exc_info=True,
                    )
                else:
                    raise
