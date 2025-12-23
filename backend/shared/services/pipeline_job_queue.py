"""
Pipeline job queue publisher using Kafka.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

from confluent_kafka import Producer

from shared.config.service_config import ServiceConfig
from shared.models.pipeline_job import PipelineJob

logger = logging.getLogger(__name__)


class PipelineJobQueue:
    def __init__(self) -> None:
        self._producer: Optional[Producer] = None
        self.topic = (os.getenv("PIPELINE_JOBS_TOPIC") or "pipeline-jobs").strip() or "pipeline-jobs"

    def _producer_instance(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                    "client.id": os.getenv("SERVICE_NAME") or "pipeline-job-queue",
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 100,
                    "linger.ms": 10,
                    "compression.type": "snappy",
                }
            )
        return self._producer

    async def publish(self, job: PipelineJob) -> None:
        producer = self._producer_instance()
        payload = job.model_dump(mode="json")
        key = job.pipeline_id.encode("utf-8")
        value = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")

        loop = asyncio.get_running_loop()

        def _send() -> None:
            producer.produce(topic=self.topic, key=key, value=value)
            producer.flush(5)

        await loop.run_in_executor(None, _send)
        logger.info("Published pipeline job %s to %s", job.job_id, self.topic)

