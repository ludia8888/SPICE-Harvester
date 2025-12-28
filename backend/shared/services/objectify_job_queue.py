"""
Objectify job queue publisher using Kafka.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from typing import Optional

from confluent_kafka import Producer

from shared.config.service_config import ServiceConfig
from shared.models.objectify_job import ObjectifyJob

logger = logging.getLogger(__name__)


class ObjectifyJobQueue:
    def __init__(self) -> None:
        self._producer: Optional[Producer] = None
        self.topic = (os.getenv("OBJECTIFY_JOBS_TOPIC") or "objectify-jobs").strip() or "objectify-jobs"
        self.flush_timeout_seconds = float(os.getenv("OBJECTIFY_JOB_QUEUE_FLUSH_TIMEOUT_SECONDS", "5") or "5")

    def _producer_instance(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                    "client.id": os.getenv("SERVICE_NAME") or "objectify-job-queue",
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 100,
                    "linger.ms": 10,
                    "compression.type": "snappy",
                }
            )
        return self._producer

    async def publish(self, job: ObjectifyJob, *, require_delivery: bool = True) -> None:
        producer = self._producer_instance()
        payload = job.model_dump(mode="json")
        key = job.dataset_id.encode("utf-8")
        value = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")

        loop = asyncio.get_running_loop()

        def _send() -> None:
            try:
                if require_delivery:
                    delivery_error: list[Exception] = []
                    delivery_done = threading.Event()

                    def _on_delivery(err, _msg) -> None:
                        if err is not None:
                            delivery_error.append(RuntimeError(str(err)))
                        delivery_done.set()

                    producer.produce(topic=self.topic, key=key, value=value, on_delivery=_on_delivery)
                else:
                    producer.produce(topic=self.topic, key=key, value=value)
            except Exception as exc:
                raise RuntimeError(f"Kafka produce failed for objectify job {job.job_id}: {exc}") from exc

            producer.poll(0.0)

            if require_delivery:
                timeout = max(0.1, self.flush_timeout_seconds)
                remaining = producer.flush(timeout)

                deadline = time.time() + timeout
                while not delivery_done.is_set() and time.time() < deadline:
                    producer.poll(0.1)

                if remaining:
                    raise RuntimeError(
                        f"Kafka flush timed out for objectify job {job.job_id}: {remaining} message(s) not delivered"
                    )
                if not delivery_done.is_set():
                    raise RuntimeError(f"Kafka delivery callback not received for objectify job {job.job_id}")
                if delivery_error:
                    raise delivery_error[0]

        await loop.run_in_executor(None, _send)
        logger.info("Published objectify job %s to %s", job.job_id, self.topic)
