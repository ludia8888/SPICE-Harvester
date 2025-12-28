"""
Search Projection Worker
Consumes instance domain events and indexes them into Elasticsearch/OpenSearch.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from confluent_kafka import Consumer, KafkaError

from shared.config.app_config import AppConfig
from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.elasticsearch_service import create_elasticsearch_service_legacy
from shared.utils.env_utils import parse_bool_env

logger = logging.getLogger(__name__)


class SearchProjectionWorker:
    def __init__(self) -> None:
        self.enabled = bool(parse_bool_env("ENABLE_SEARCH_PROJECTION", False))
        self.topic = (os.getenv("INSTANCE_EVENTS_TOPIC") or AppConfig.INSTANCE_EVENTS_TOPIC).strip()
        self.group_id = (os.getenv("SEARCH_PROJECTION_GROUP") or "search-projection-worker").strip()
        self.index_name = (os.getenv("SEARCH_INDEX") or "objects").strip() or "objects"
        self.consumer: Optional[Consumer] = None
        self.es = None

    async def initialize(self) -> None:
        if not self.enabled:
            logger.info("Search projection disabled; worker will not start.")
            return
        self.es = create_elasticsearch_service_legacy()
        await self.es.connect()
        try:
            exists = await self.es.index_exists(self.index_name)
            if not exists:
                await self.es.create_index(self.index_name)
        except Exception as exc:
            logger.warning("Failed to ensure search index exists: %s", exc)

        self.consumer = Consumer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 300000,
                "session.timeout.ms": 45000,
            }
        )
        self.consumer.subscribe([self.topic])

    async def close(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.es:
            await self.es.disconnect()
            self.es = None

    async def run(self) -> None:
        await self.initialize()
        if not self.enabled:
            return
        logger.info("SearchProjectionWorker started (topic=%s index=%s)", self.topic, self.index_name)
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                payload = msg.value()
                if not payload:
                    continue
                try:
                    envelope = EventEnvelope.model_validate_json(payload)
                    await self._index_event(envelope)
                    self.consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:
                    logger.exception("Failed to index event: %s", exc)
                    self.consumer.commit(message=msg, asynchronous=False)
        finally:
            await self.close()

    async def _index_event(self, envelope: EventEnvelope) -> None:
        if not self.es:
            return
        if envelope.event_type not in {"INSTANCE_CREATED", "INSTANCE_UPDATED", "INSTANCE_MERGED"}:
            return
        aggregate_id = str(envelope.aggregate_id or "")
        if not aggregate_id:
            return
        doc = {
            "event_type": envelope.event_type,
            "aggregate_type": envelope.aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": envelope.occurred_at.isoformat() if envelope.occurred_at else None,
            "data": envelope.data or {},
            "metadata": envelope.metadata or {},
        }
        await self.es.client.index(index=self.index_name, id=aggregate_id, document=doc)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    worker = SearchProjectionWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
