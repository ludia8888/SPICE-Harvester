"""
Kafka producer call strategies (Strategy pattern).

This mirrors `shared.services.kafka.consumer_ops` and exists to eliminate
duplicated per-service boilerplate around:
- dedicated producer executors
- `call_in_executor(...)` wrappers for `produce(...)` and `flush(...)`
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional

from shared.utils.executor_utils import call_in_executor

_LOGGER = logging.getLogger(__name__)


class KafkaProducerOps(ABC):
    """Strategy interface for executing producer operations."""

    @abstractmethod
    async def produce(self, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    async def flush(self, timeout_s: float) -> int:
        raise NotImplementedError

    @abstractmethod
    async def close(self, *, timeout_s: float = 5.0) -> None:
        raise NotImplementedError


@dataclass(slots=True)
class InlineKafkaProducerOps(KafkaProducerOps):
    """Execute producer operations inline on the event-loop thread."""

    producer: Any

    async def produce(self, **kwargs) -> None:
        self.producer.produce(**kwargs)

    async def flush(self, timeout_s: float) -> int:
        return int(self.producer.flush(timeout_s))

    async def close(self, *, timeout_s: float = 5.0) -> None:
        self.producer.flush(timeout_s)


class ExecutorKafkaProducerOps(KafkaProducerOps):
    """
    Execute all producer operations on a dedicated single thread.

    This keeps `confluent_kafka.Producer` usage thread-consistent, while keeping
    blocking calls off the asyncio event loop.
    """

    def __init__(
        self,
        producer: Any,
        *,
        thread_name_prefix: str = "kafka-producer",
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        self._producer = producer
        self._owns_executor = executor is None
        self._executor = executor or ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=str(thread_name_prefix or "kafka-producer"),
        )

    async def produce(self, **kwargs) -> None:
        await call_in_executor(self._executor, self._producer.produce, **kwargs)

    async def flush(self, timeout_s: float) -> int:
        remaining = await call_in_executor(self._executor, self._producer.flush, float(timeout_s))
        return int(remaining or 0)

    async def close(self, *, timeout_s: float = 5.0) -> None:
        try:
            await call_in_executor(self._executor, self._producer.flush, float(timeout_s))
        finally:
            if self._owns_executor:
                try:
                    self._executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    self._executor.shutdown(wait=False)


async def close_kafka_producer(
    *,
    producer: Any = None,
    producer_ops: Optional[KafkaProducerOps] = None,
    timeout_s: float = 5.0,
    warning_logger: Optional[logging.Logger] = None,
    warning_message: str = "Kafka producer flush failed during shutdown: %s",
) -> None:
    timeout = float(timeout_s)
    logger = warning_logger or _LOGGER
    try:
        if producer_ops is not None:
            await producer_ops.close(timeout_s=timeout)
            return

        if producer is None:
            return

        try:
            await asyncio.to_thread(producer.flush, timeout)
        except TypeError:
            await asyncio.to_thread(producer.flush)
    except Exception as exc:
        logger.warning(warning_message, exc, exc_info=True)
