from __future__ import annotations

import asyncio

import pytest

from ontology_worker.main import OntologyWorker


class _StubTerminus:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    async def database_exists(self, db_name: str) -> bool:
        return self._exists


@pytest.mark.asyncio
async def test_wait_for_database_exists_success() -> None:
    worker = OntologyWorker()
    worker.terminus_service = _StubTerminus(True)

    await worker._wait_for_database_exists(db_name="db", expected=True, timeout_seconds=0.1)


@pytest.mark.asyncio
async def test_wait_for_database_exists_timeout() -> None:
    worker = OntologyWorker()
    worker.terminus_service = _StubTerminus(False)

    with pytest.raises(RuntimeError):
        await worker._wait_for_database_exists(db_name="db", expected=True, timeout_seconds=0.1)


@pytest.mark.asyncio
async def test_consumer_call_executes() -> None:
    worker = OntologyWorker()

    def add(a, b):
        return a + b

    result = await worker._consumer_call(add, 1, 2)
    assert result == 3


@pytest.mark.asyncio
async def test_heartbeat_loop_no_registry() -> None:
    worker = OntologyWorker()
    await asyncio.wait_for(worker._heartbeat_loop(handler="h", event_id="e"), timeout=0.1)
