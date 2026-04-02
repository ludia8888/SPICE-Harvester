from __future__ import annotations

import asyncio

import pytest

from shared.foundry.spark_on_demand_dispatcher import (
    SparkOnDemandDispatchTimeoutError,
    SparkOnDemandDispatcher,
)


@pytest.mark.asyncio
async def test_dispatch_executes_job_and_returns_metrics() -> None:
    dispatcher = SparkOnDemandDispatcher(worker_count=1)

    async def _execute(context) -> dict[str, bool]:  # noqa: ANN001
        assert context.route == "search_around"
        assert context.payload["ontology"] == "demo"
        return {"ok": True}

    try:
        result = await dispatcher.dispatch(
            route="search_around",
            payload={"ontology": "demo"},
            execute=_execute,
            timeout_seconds=1.0,
        )

        assert result.route == "search_around"
        assert result.job_id
        assert result.result == {"ok": True}
        assert result.queue_wait_ms >= 0
        assert result.execution_ms >= 0
    finally:
        await dispatcher.shutdown()


@pytest.mark.asyncio
async def test_dispatch_timeout_raises() -> None:
    dispatcher = SparkOnDemandDispatcher(worker_count=1)

    async def _slow(_context) -> str:  # noqa: ANN001
        await asyncio.sleep(0.2)
        return "done"

    try:
        with pytest.raises(SparkOnDemandDispatchTimeoutError):
            await dispatcher.dispatch(
                route="writeback",
                payload={"db_name": "demo"},
                execute=_slow,
                timeout_seconds=0.01,
            )
    finally:
        await dispatcher.shutdown()


@pytest.mark.asyncio
async def test_dispatch_timeout_cancels_running_job() -> None:
    dispatcher = SparkOnDemandDispatcher(worker_count=1)
    cancelled = asyncio.Event()

    async def _slow(_context) -> str:  # noqa: ANN001
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return "done"

    try:
        with pytest.raises(SparkOnDemandDispatchTimeoutError):
            await dispatcher.dispatch(
                route="writeback",
                payload={"db_name": "demo"},
                execute=_slow,
                timeout_seconds=0.01,
            )

        await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    finally:
        await dispatcher.shutdown()


@pytest.mark.asyncio
async def test_dispatch_timeout_skips_queued_job_execution() -> None:
    dispatcher = SparkOnDemandDispatcher(worker_count=1)
    gate = asyncio.Event()
    queued_started = asyncio.Event()

    async def _blocking(_context) -> str:  # noqa: ANN001
        await gate.wait()
        return "first"

    async def _queued(_context) -> str:  # noqa: ANN001
        queued_started.set()
        return "second"

    try:
        first_task = asyncio.create_task(
            dispatcher.dispatch(
                route="search_around",
                payload={"job": "first"},
                execute=_blocking,
                timeout_seconds=1.0,
            )
        )
        await asyncio.sleep(0)

        with pytest.raises(SparkOnDemandDispatchTimeoutError):
            await dispatcher.dispatch(
                route="writeback",
                payload={"job": "second"},
                execute=_queued,
                timeout_seconds=0.01,
            )

        gate.set()
        await first_task
        await asyncio.sleep(0.05)

        assert queued_started.is_set() is False
    finally:
        gate.set()
        await dispatcher.shutdown()
