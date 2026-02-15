from __future__ import annotations

import asyncio

import pytest

from ontology_worker.main import OntologyWorker
from shared.services.kafka.consumer_ops import InlineKafkaConsumerOps


@pytest.mark.asyncio
async def test_resource_registry_mode_is_default_and_active() -> None:
    worker = OntologyWorker()
    assert worker.ontology_resource_backend == "postgres"
    assert worker._uses_resource_registry_for_ontology is True


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
