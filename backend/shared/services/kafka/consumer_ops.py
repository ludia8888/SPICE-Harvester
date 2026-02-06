"""
Kafka consumer call strategies (Strategy pattern).

Goal: eliminate duplicated per-worker boilerplate such as:
- `_consumer_executor` + `_consumer_call`
- `_poll_message` / `_commit` / `_seek` wrappers

We intentionally keep the interface narrow (poll/commit/seek/close). Partition
pause/resume is included so partitioned workers can keep all consumer calls
thread-consistent when using an executor strategy.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar

from confluent_kafka import TopicPartition

from shared.utils.executor_utils import call_in_executor

T = TypeVar("T")


class KafkaConsumerOps(ABC):
    """Strategy interface for executing consumer operations."""

    @abstractmethod
    async def poll(self, *, timeout: float) -> Any:  # noqa: ANN401
        raise NotImplementedError

    @abstractmethod
    async def commit_sync(self, msg: Any) -> None:  # noqa: ANN401
        raise NotImplementedError

    @abstractmethod
    async def seek(self, tp: TopicPartition) -> None:
        raise NotImplementedError

    @abstractmethod
    async def pause(self, partitions: list[TopicPartition]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def resume(self, partitions: list[TopicPartition]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Execute an arbitrary call in the consumer's execution context.

        The provided `fn` is invoked as: `fn(consumer, *args, **kwargs)`.
        """
        raise NotImplementedError


@dataclass(slots=True)
class InlineKafkaConsumerOps(KafkaConsumerOps):
    """Execute consumer operations inline on the event-loop thread."""

    consumer: Any

    async def poll(self, *, timeout: float) -> Any:  # noqa: ANN401
        return self.consumer.poll(timeout)

    async def commit_sync(self, msg: Any) -> None:  # noqa: ANN401
        self.consumer.commit_sync(msg)

    async def seek(self, tp: TopicPartition) -> None:
        self.consumer.seek(tp)

    async def pause(self, partitions: list[TopicPartition]) -> None:
        self.consumer.pause(partitions)

    async def resume(self, partitions: list[TopicPartition]) -> None:
        self.consumer.resume(partitions)

    async def close(self) -> None:
        self.consumer.close()

    async def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        return fn(self.consumer, *args, **kwargs)


class ExecutorKafkaConsumerOps(KafkaConsumerOps):
    """
    Execute all consumer operations on a dedicated single thread.

    This keeps `confluent_kafka.Consumer` usage thread-consistent, while keeping
    blocking calls off the asyncio event loop.
    """

    def __init__(
        self,
        consumer: Any,
        *,
        thread_name_prefix: str = "kafka-consumer",
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        self._consumer = consumer
        self._owns_executor = executor is None
        self._executor = executor or ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=str(thread_name_prefix or "kafka-consumer"),
        )

    async def poll(self, *, timeout: float) -> Any:  # noqa: ANN401
        return await call_in_executor(self._executor, self._consumer.poll, timeout)

    async def commit_sync(self, msg: Any) -> None:  # noqa: ANN401
        await call_in_executor(self._executor, self._consumer.commit_sync, msg)

    async def seek(self, tp: TopicPartition) -> None:
        await call_in_executor(self._executor, self._consumer.seek, tp)

    async def pause(self, partitions: list[TopicPartition]) -> None:
        await call_in_executor(self._executor, self._consumer.pause, partitions)

    async def resume(self, partitions: list[TopicPartition]) -> None:
        await call_in_executor(self._executor, self._consumer.resume, partitions)

    async def close(self) -> None:
        try:
            await call_in_executor(self._executor, self._consumer.close)
        finally:
            if self._owns_executor:
                try:
                    self._executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    self._executor.shutdown(wait=False)

    async def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        return await call_in_executor(self._executor, fn, self._consumer, *args, **kwargs)
