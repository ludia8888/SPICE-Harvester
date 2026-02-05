from __future__ import annotations

import asyncio

import pytest

from ontology_worker.main import OntologyWorker
from shared.services.kafka.consumer_ops import InlineKafkaConsumerOps


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
async def test_consumer_ops_defaults_to_inline() -> None:
    worker = OntologyWorker()

    class _StubConsumer:
        def poll(self, timeout):
            return ("ok", timeout)

    worker.consumer = _StubConsumer()
    ops = worker._get_consumer_ops()
    assert isinstance(ops, InlineKafkaConsumerOps)
    assert await ops.poll(timeout=0.0) == ("ok", 0.0)


@pytest.mark.asyncio
async def test_heartbeat_loop_no_registry() -> None:
    worker = OntologyWorker()
    await asyncio.wait_for(worker._heartbeat_loop(handler="h", event_id="e"), timeout=0.1)
