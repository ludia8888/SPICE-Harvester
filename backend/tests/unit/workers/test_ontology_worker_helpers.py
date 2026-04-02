from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

import ontology_worker.main as ontology_worker_module
from ontology_worker.main import OntologyWorker
from shared.services.kafka.consumer_ops import InlineKafkaConsumerOps


@pytest.mark.asyncio
async def test_resource_registry_mode_is_default_and_active() -> None:
    worker = OntologyWorker()
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


@pytest.mark.asyncio
async def test_handle_create_database_syncs_registry_after_event_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = OntologyWorker()
    worker.command_status_service = None
    order: list[str] = []

    async def _publish_event(_event) -> None:
        order.append("publish")

    async def _upsert_owner(**kwargs) -> None:  # noqa: ANN003
        _ = kwargs
        order.append("upsert")

    worker.publish_event = AsyncMock(side_effect=_publish_event)
    monkeypatch.setattr(ontology_worker_module, "upsert_database_owner", _upsert_owner)

    await worker.handle_create_database(
        {
            "command_id": "00000000-0000-0000-0000-000000000401",
            "created_by": "tester",
            "payload": {
                "database_name": "demo",
                "description": "desc",
            },
        }
    )

    assert order == ["publish", "upsert"]
