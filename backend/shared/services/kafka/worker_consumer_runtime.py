"""
Kafka worker consumer runtime helpers (Facade over Strategy).

We already model "how to call the Kafka consumer" as a Strategy via
`KafkaConsumerOps` (inline vs single-thread executor). This module builds on
that by providing a small Facade that centralizes the remaining per-worker
boilerplate:
- poll wrapper (supports injectable poller for tests)
- commit/seek wrappers with revoked-partition guard
- pause/resume wrappers with consistent error logging

This keeps worker Template Method implementations focused on business logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from confluent_kafka import TopicPartition

from shared.services.kafka.consumer_ops import KafkaConsumerOps

logger = logging.getLogger(__name__)

PartitionKey = tuple[str, int]
Poller = Callable[[float], Awaitable[Any]]


@dataclass(slots=True)
class WorkerConsumerRuntime:
    ops: KafkaConsumerOps
    loop_label: Callable[[], str]
    revoked_partitions: set[PartitionKey]
    uses_commit_state: Callable[[], bool]
    commit_state_by_partition: dict[PartitionKey, bool]

    async def poll_message(self, *, timeout: float, poller: Optional[Poller] = None) -> Any:  # noqa: ANN401
        if poller is not None:
            return await poller(timeout)
        return await self.ops.poll(timeout=timeout)

    async def commit(self, msg: Any) -> None:  # noqa: ANN401
        key: PartitionKey = (str(msg.topic()), int(msg.partition()))
        if key in self.revoked_partitions:
            logger.info(
                "Skipping %s commit; partition revoked (topic=%s partition=%s offset=%s)",
                self.loop_label(),
                str(msg.topic()),
                int(msg.partition()),
                int(msg.offset()),
            )
            return
        if self.uses_commit_state():
            self.commit_state_by_partition[key] = True
        await self.ops.commit_sync(msg)

    async def seek(self, *, topic: str, partition: int, offset: int) -> None:
        key: PartitionKey = (str(topic), int(partition))
        if key in self.revoked_partitions:
            logger.info(
                "Skipping %s seek; partition revoked (topic=%s partition=%s offset=%s)",
                self.loop_label(),
                str(topic),
                int(partition),
                int(offset),
            )
            return
        if self.uses_commit_state():
            self.commit_state_by_partition[key] = False
        await self.ops.seek(TopicPartition(topic, partition, offset))

    async def pause_partition(self, *, topic: str, partition: int) -> None:
        try:
            await self.ops.pause([TopicPartition(topic, partition)])
        except Exception as exc:
            logger.warning(
                "Failed to pause %s partition (topic=%s partition=%s): %s",
                self.loop_label(),
                topic,
                partition,
                exc,
            )

    async def resume_partition(self, *, topic: str, partition: int) -> None:
        try:
            await self.ops.resume([TopicPartition(topic, partition)])
        except Exception as exc:
            logger.warning(
                "Failed to resume %s partition (topic=%s partition=%s): %s",
                self.loop_label(),
                topic,
                partition,
                exc,
            )

