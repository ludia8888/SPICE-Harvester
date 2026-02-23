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


@pytest.mark.asyncio
async def test_dispatch_timeout_raises() -> None:
    dispatcher = SparkOnDemandDispatcher(worker_count=1)

    async def _slow(_context) -> str:  # noqa: ANN001
        await asyncio.sleep(0.2)
        return "done"

    with pytest.raises(SparkOnDemandDispatchTimeoutError):
        await dispatcher.dispatch(
            route="writeback",
            payload={"db_name": "demo"},
            execute=_slow,
            timeout_seconds=0.01,
        )
