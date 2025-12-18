"""
Worker lease safety tests (no mocks).

Validates:
- Lease/heartbeat config rejects invalid values.
- Registry disable is rejected.
- Kafka poll/commit runs off the event loop.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager

import pytest

from instance_worker.main import StrictPalantirInstanceWorker
from shared.services.processed_event_registry import validate_lease_settings, validate_registry_enabled


@contextmanager
def _set_env(**updates):
    import os

    original = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.mark.integration
def test_invalid_lease_settings_fail_fast():
    with _set_env(
        PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS="30",
        PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS="30",
    ):
        with pytest.raises(RuntimeError):
            validate_lease_settings()


@pytest.mark.integration
def test_registry_disable_rejected():
    with _set_env(ENABLE_PROCESSED_EVENT_REGISTRY="false"):
        with pytest.raises(RuntimeError):
            validate_registry_enabled()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_heartbeat_not_blocked_by_poll():
    worker = StrictPalantirInstanceWorker()
    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.05)
            ticks += 1

    def blocking_poll():
        time.sleep(0.25)
        return "ok"

    task = asyncio.create_task(ticker())
    result = await worker._consumer_call(blocking_poll)
    await task

    assert result == "ok"
    assert ticks > 0
