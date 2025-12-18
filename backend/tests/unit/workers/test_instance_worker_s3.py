import asyncio
import time

import pytest

from instance_worker.main import StrictPalantirInstanceWorker


@pytest.mark.unit
@pytest.mark.asyncio
async def test_s3_call_does_not_block_event_loop():
    worker = StrictPalantirInstanceWorker()
    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.05)
            ticks += 1

    def blocking_io():
        time.sleep(0.25)
        return "ok"

    task = asyncio.create_task(ticker())
    result = await worker._s3_call(blocking_io)
    await task

    assert result == "ok"
    assert ticks > 0
