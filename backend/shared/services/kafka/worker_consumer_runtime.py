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

    async def commit(self, msg: Any) -> bool:  # noqa: ANN401
        topic = str(msg.topic())
        partition = int(msg.partition())
        offset = int(msg.offset())
        key: PartitionKey = (topic, partition)
        revoked = self.revoked_partitions

        def _commit_if_not_revoked(consumer: Any) -> bool:  # noqa: ANN401
            if key in revoked:
                return False
            consumer.commit_sync(msg)
            return True

        did_commit = await self.ops.call(_commit_if_not_revoked)
        if not did_commit:
            logger.info(
                "Skipping %s commit; partition revoked (topic=%s partition=%s offset=%s)",
                self.loop_label(),
                topic,
                partition,
                offset,
            )
            return False

        if self.uses_commit_state():
            self.commit_state_by_partition[key] = True
        return True

    async def seek(self, *, topic: str, partition: int, offset: int) -> bool:
        key: PartitionKey = (str(topic), int(partition))
        revoked = self.revoked_partitions

        def _seek_if_not_revoked(consumer: Any) -> bool:  # noqa: ANN401
            if key in revoked:
                return False
            consumer.seek(TopicPartition(topic, partition, offset))
            return True

        did_seek = await self.ops.call(_seek_if_not_revoked)
        if not did_seek:
            logger.info(
                "Skipping %s seek; partition revoked (topic=%s partition=%s offset=%s)",
                self.loop_label(),
                str(topic),
                int(partition),
                int(offset),
            )
            return False

        if self.uses_commit_state():
            self.commit_state_by_partition[key] = False
        return True

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
