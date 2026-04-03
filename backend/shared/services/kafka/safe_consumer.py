"""
Safe Kafka Consumer with Strong Consistency Guarantees.

This module provides a hardened Kafka consumer that enforces:
- read_committed isolation level (Critical Gap 1)
- Rebalance callbacks for graceful partition handoff (Critical Gap 2)
- Proper offset management for exactly-once semantics

CRITICAL: All workers MUST use this consumer instead of raw confluent_kafka.Consumer
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Consumer, TopicPartition

from shared.config.settings import get_settings
from shared.observability.tracing import trace_kafka_operation

logger = logging.getLogger(__name__)


class ConsumerState(str, Enum):
    """Consumer lifecycle states."""
    CREATED = "created"
    RUNNING = "running"
    REBALANCING = "rebalancing"
    PAUSED = "paused"
    CLOSED = "closed"


@dataclass
class PartitionState:
    """Track state for each assigned partition."""
    topic: str
    partition: int
    current_offset: Optional[int] = None
    pending_offset: Optional[int] = None  # Offset of message being processed
    last_committed: Optional[int] = None
    processing: bool = False
    assigned_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class RebalanceHandler:
    """
    Handle consumer group rebalancing events.

    Critical for avoiding message loss during partition reassignment.
    """

    def __init__(
        self,
        consumer: "SafeKafkaConsumer",
        on_revoke_callback: Optional[Callable[[List[TopicPartition]], None]] = None,
        on_assign_callback: Optional[Callable[[List[TopicPartition]], None]] = None,
    ):
        self.consumer = consumer
        self.on_revoke_callback = on_revoke_callback
        self.on_assign_callback = on_assign_callback
        self._lock = threading.Lock()

    def on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        """
        Called before partitions are revoked.

        CRITICAL: Must commit any pending offsets and stop processing.
        """
        with self._lock:
            logger.info(
                "Partitions being revoked: %s",
                [(p.topic, p.partition) for p in partitions],
            )

            # Mark consumer as rebalancing
            self.consumer._state = ConsumerState.REBALANCING

            # Commit any pending offsets before revoke
            try:
                pending_commits = []
                for tp in partitions:
                    key = (tp.topic, tp.partition)
                    state = self.consumer._partition_states.get(key)
                    if state and state.pending_offset is not None:
                        # Don't commit if still processing - let the new owner handle it
                        if not state.processing:
                            pending_commits.append(
                                TopicPartition(tp.topic, tp.partition, state.pending_offset + 1)
                            )
                        else:
                            logger.warning(
                                "Partition %s:%d revoked while processing offset %d",
                                tp.topic, tp.partition, state.pending_offset,
                            )

                if pending_commits:
                    consumer.commit(offsets=pending_commits, asynchronous=False)
                    logger.info("Committed %d pending offsets before revoke", len(pending_commits))

            except Exception as exc:
                logger.error("Failed to commit before revoke: %s", exc)

            # Clear partition states for revoked partitions
            for tp in partitions:
                key = (tp.topic, tp.partition)
                self.consumer._partition_states.pop(key, None)

            # Call user callback if provided
            if self.on_revoke_callback:
                try:
                    self.on_revoke_callback(partitions)
                except Exception as exc:
                    logger.error("User on_revoke callback failed: %s", exc)

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        """
        Called after partitions are assigned.

        Initialize tracking state for new partitions.
        """
        with self._lock:
            logger.info(
                "Partitions assigned: %s",
                [(p.topic, p.partition, p.offset) for p in partitions],
            )

            # Initialize partition states
            for tp in partitions:
                key = (tp.topic, tp.partition)
                self.consumer._partition_states[key] = PartitionState(
                    topic=tp.topic,
                    partition=tp.partition,
                    current_offset=tp.offset if tp.offset >= 0 else None,
                )

            # Mark consumer as running
            self.consumer._state = ConsumerState.RUNNING

            # Call user callback if provided
            if self.on_assign_callback:
                try:
                    self.on_assign_callback(partitions)
                except Exception as exc:
                    logger.error("User on_assign callback failed: %s", exc)


class SafeKafkaConsumer:
    """
    Production-hardened Kafka consumer with strong consistency guarantees.

    Features:
    - Enforced read_committed isolation level
    - Rebalance-safe offset management
    - Graceful shutdown with pending commit flush
    - Partition state tracking

    Usage:
        consumer = SafeKafkaConsumer(
            group_id="my-worker-group",
            topics=["my-topic"],
            service_name="my-worker",
        )

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            process(msg)
            consumer.commit_sync(msg)
    """

    # Critical settings that MUST be enforced
    ENFORCED_SETTINGS = {
        "isolation.level": "read_committed",  # Critical Gap 1
        "enable.auto.commit": False,  # Manual commit only
    }

    def __init__(
        self,
        group_id: str,
        topics: List[str],
        service_name: str,
        *,
        extra_config: Optional[Dict[str, Any]] = None,
        on_revoke: Optional[Callable[[List[TopicPartition]], None]] = None,
        on_assign: Optional[Callable[[List[TopicPartition]], None]] = None,
        subscribe: bool = True,
        session_timeout_ms: int = 45000,
        max_poll_interval_ms: int = 300000,
        heartbeat_interval_ms: int = 3000,
    ):
        """
        Create a safe Kafka consumer.

        Args:
            group_id: Consumer group ID
            topics: List of topics to subscribe
            service_name: Service name for logging/metrics
            extra_config: Additional Kafka config (cannot override ENFORCED_SETTINGS)
            on_revoke: Callback when partitions are revoked
            on_assign: Callback when partitions are assigned
            session_timeout_ms: Session timeout (default 45s)
            max_poll_interval_ms: Max poll interval (default 5min)
            heartbeat_interval_ms: Heartbeat interval (default 3s)
        """
        settings = get_settings()

        # Build base config
        config: Dict[str, Any] = {
            "bootstrap.servers": settings.database.kafka_servers,
            "group.id": group_id,
            "client.id": f"{service_name}-consumer",
            "auto.offset.reset": "earliest",
            "session.timeout.ms": session_timeout_ms,
            "max.poll.interval.ms": max_poll_interval_ms,
            "heartbeat.interval.ms": heartbeat_interval_ms,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,
            "check.crcs": True,
        }

        # Apply extra config (if provided)
        if extra_config:
            for key, value in extra_config.items():
                if key in self.ENFORCED_SETTINGS:
                    logger.warning(
                        "Ignoring attempt to override enforced setting %s=%s (required: %s)",
                        key, value, self.ENFORCED_SETTINGS[key],
                    )
                else:
                    config[key] = value

        # Apply ENFORCED settings (cannot be overridden)
        config.update(self.ENFORCED_SETTINGS)

        # Log the critical settings for audit
        logger.info(
            "Creating SafeKafkaConsumer: group=%s, topics=%s, isolation.level=%s",
            group_id, topics, config["isolation.level"],
        )

        self._group_id = group_id
        self._topics = topics
        self._service_name = service_name
        self._state = ConsumerState.CREATED
        self._partition_states: Dict[tuple, PartitionState] = {}
        self._pending_commits: List[TopicPartition] = []
        self._lock = threading.Lock()

        # Create consumer with rebalance handler
        self._rebalance_handler = RebalanceHandler(
            consumer=self,
            on_revoke_callback=on_revoke,
            on_assign_callback=on_assign,
        )

        self._consumer = Consumer(config)
        if subscribe:
            self._consumer.subscribe(
                topics,
                on_revoke=self._rebalance_handler.on_revoke,
                on_assign=self._rebalance_handler.on_assign,
            )

        self._state = ConsumerState.RUNNING

    @property
    def state(self) -> ConsumerState:
        """Current consumer state."""
        return self._state

    @property
    def is_rebalancing(self) -> bool:
        """Check if consumer is currently rebalancing."""
        return self._state == ConsumerState.REBALANCING

    @trace_kafka_operation("kafka.safe_poll")
    def poll(self, timeout: float = 1.0) -> Optional[Any]:
        """
        Poll for a message with rebalance awareness.

        Returns None during rebalancing to prevent processing.
        """
        if self._state == ConsumerState.REBALANCING:
            # During rebalance, just poll to handle callbacks but don't process
            self._consumer.poll(0.1)
            return None

        if self._state != ConsumerState.RUNNING:
            return None

        msg = self._consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            return msg  # Let caller handle error

        # Track partition state
        with self._lock:
            key = (msg.topic(), msg.partition())
            state = self._partition_states.get(key)
            if state:
                state.pending_offset = msg.offset()
                state.processing = True

        return msg

    def wait_for_assignment(self, *, timeout_seconds: float = 10.0) -> bool:
        """
        Block until partitions are assigned (or timeout).

        NOTE: Creating a confluent_kafka.Consumer does not immediately join the consumer group. Partition
        assignment happens only after polling. Tests and workers that rely on `auto.offset.reset=latest`
        should call this before producing signals they want to observe.
        """
        deadline = time.monotonic() + float(timeout_seconds)
        last_poll_signature: Optional[tuple[type[BaseException], str]] = None
        consecutive_poll_failures = 0
        while time.monotonic() < deadline:
            if self._state == ConsumerState.CLOSED:
                raise RuntimeError("Kafka consumer closed while waiting for partition assignment")
            try:
                self._consumer.poll(0.1)
            except Exception as exc:
                signature = (type(exc), str(exc))
                if signature == last_poll_signature:
                    consecutive_poll_failures += 1
                else:
                    last_poll_signature = signature
                    consecutive_poll_failures = 1
                logger.warning(
                    "Kafka poll failed while waiting for partition assignment (attempt=%d): %s",
                    consecutive_poll_failures,
                    exc,
                    exc_info=True,
                )
                if consecutive_poll_failures >= 3:
                    raise RuntimeError(
                        "Kafka consumer poll failed repeatedly while waiting for partition assignment"
                    ) from exc
                time.sleep(0.1)
                continue
            try:
                if self._consumer.assignment():
                    return True
            except Exception as exc:
                logger.warning(
                    "Failed to read consumer assignment while waiting for partition assignment: %s",
                    exc,
                    exc_info=True,
                )
            time.sleep(0.1)
        return False

    def mark_processed(self, msg: Any) -> None:
        """
        Mark a message as successfully processed.

        Call this AFTER processing is complete but BEFORE commit.
        """
        with self._lock:
            key = (msg.topic(), msg.partition())
            state = self._partition_states.get(key)
            if state:
                state.processing = False
                state.current_offset = msg.offset()
                # Queue for commit
                self._pending_commits.append(
                    TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                )

    @trace_kafka_operation("kafka.safe_commit")
    def commit(
        self,
        message: Optional[Any] = None,
        offsets: Optional[List[TopicPartition]] = None,
        asynchronous: bool = False,
        *,
        msg: Optional[Any] = None,
    ) -> None:
        """
        Commit offsets.

        Args:
            message: Specific message to commit (if None, commits all pending)
            offsets: Explicit offsets to commit (TopicPartition list)
            asynchronous: Whether to commit asynchronously
            msg: Backwards-compatible alias for `message`
        """
        if msg is not None:
            if message is not None:
                raise TypeError("Provide only one of 'message' or 'msg'")
            message = msg
        if message is not None and offsets is not None:
            raise TypeError("Provide only one of 'message' or 'offsets'")

        if self._state == ConsumerState.REBALANCING:
            logger.warning("Skipping commit during rebalance")
            return

        with self._lock:
            if message is not None:
                # Treat explicit commit as successful processing for partition-state bookkeeping.
                key = (message.topic(), message.partition())
                state = self._partition_states.get(key)
                if state:
                    state.processing = False
                    state.current_offset = message.offset()
                    state.pending_offset = message.offset()

                self._consumer.commit(message=message, asynchronous=asynchronous)

                if state:
                    state.last_committed = message.offset()
                return

            if offsets is not None:
                self._consumer.commit(offsets=offsets, asynchronous=asynchronous)
                for tp in offsets:
                    key = (tp.topic, tp.partition)
                    state = self._partition_states.get(key)
                    if state:
                        state.last_committed = tp.offset - 1
                        state.processing = False
                        state.current_offset = tp.offset - 1
                        state.pending_offset = tp.offset - 1
                return

            if self._pending_commits:
                self._consumer.commit(offsets=self._pending_commits, asynchronous=asynchronous)
                # Update last committed for all
                for tp in self._pending_commits:
                    key = (tp.topic, tp.partition)
                    state = self._partition_states.get(key)
                    if state:
                        state.last_committed = tp.offset - 1
                        state.processing = False
                        state.current_offset = tp.offset - 1
                        state.pending_offset = tp.offset - 1
                self._pending_commits.clear()

    @trace_kafka_operation("kafka.safe_commit_sync")
    def commit_sync(self, msg: Any) -> None:
        """Synchronously commit a specific message offset."""
        self.commit(message=msg, asynchronous=False)

    def seek(self, partition: TopicPartition) -> None:
        """
        Seek to a specific offset for a partition.

        NOTE: This is used by workers to retry the current message (poison handling / backoff).
        """
        with self._lock:
            key = (partition.topic, partition.partition)
            state = self._partition_states.get(key)
            if state:
                # We are explicitly rewinding/positioning; clear any in-flight bookkeeping.
                state.processing = False
                state.pending_offset = None
                # Best-effort: reflect the next message that will be delivered after seek.
                if partition.offset is not None and partition.offset >= 0:
                    state.current_offset = partition.offset - 1
        self._consumer.seek(partition)

    @trace_kafka_operation("kafka.safe_close")
    def close(self, timeout: float = 10.0) -> None:
        """
        Gracefully close the consumer.

        Commits any pending offsets before closing.
        """
        logger.info("Closing SafeKafkaConsumer for group %s", self._group_id)
        self._state = ConsumerState.CLOSED

        try:
            # Commit any pending offsets
            with self._lock:
                if self._pending_commits:
                    try:
                        self._consumer.commit(offsets=self._pending_commits, asynchronous=False)
                        logger.info("Committed %d pending offsets on close", len(self._pending_commits))
                    except Exception as exc:
                        logger.error("Failed to commit on close: %s", exc)
                    self._pending_commits.clear()
        finally:
            self._consumer.close()

    def __enter__(self) -> "SafeKafkaConsumer":
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.close()

    # Delegate common methods to underlying consumer
    def list_topics(self, topic: Optional[str] = None, timeout: float = -1) -> Any:
        """List available topics."""
        return self._consumer.list_topics(topic, timeout)

    def get_watermark_offsets(self, partition: TopicPartition, timeout: float = -1) -> tuple[int, int]:
        """Return (low, high) offsets for a partition."""
        return self._consumer.get_watermark_offsets(partition, timeout)

    def committed(self, partitions: List[TopicPartition], timeout: float = -1) -> List[TopicPartition]:
        """Return committed offsets for partitions in this consumer group."""
        return self._consumer.committed(partitions, timeout)

    def assignment(self) -> List[TopicPartition]:
        """Get current partition assignment."""
        return self._consumer.assignment()

    def position(self, partitions: List[TopicPartition]) -> List[TopicPartition]:
        """Get current position for partitions."""
        return self._consumer.position(partitions)

    def pause(self, partitions: List[TopicPartition]) -> None:
        """Pause fetching from the provided partitions (backpressure)."""
        if self._state in {ConsumerState.CLOSED}:
            return
        self._consumer.pause(partitions)

    def resume(self, partitions: List[TopicPartition]) -> None:
        """Resume fetching from the provided partitions (backpressure)."""
        if self._state in {ConsumerState.CLOSED}:
            return
        self._consumer.resume(partitions)


def create_safe_consumer(
    group_id: str,
    topics: List[str],
    service_name: str,
    **kwargs,
) -> SafeKafkaConsumer:
    """
    Factory function to create a SafeKafkaConsumer.

    This is the ONLY approved way to create Kafka consumers in production.

    Args:
        group_id: Consumer group ID
        topics: Topics to subscribe
        service_name: Service name
        **kwargs: Additional arguments passed to SafeKafkaConsumer

    Returns:
        Configured SafeKafkaConsumer instance
    """
    return SafeKafkaConsumer(
        group_id=group_id,
        topics=topics,
        service_name=service_name,
        **kwargs,
    )


# Validation at import time
def validate_consumer_config(config: Dict[str, Any]) -> None:
    """
    Validate that a consumer config meets safety requirements.

    Raises:
        ValueError: If config violates safety requirements
    """
    isolation = config.get("isolation.level", "read_uncommitted")
    if isolation != "read_committed":
        raise ValueError(
            f"CRITICAL: isolation.level must be 'read_committed', got '{isolation}'. "
            "Use SafeKafkaConsumer instead of raw Consumer."
        )

    auto_commit = config.get("enable.auto.commit", True)
    if auto_commit:
        raise ValueError(
            "CRITICAL: enable.auto.commit must be False for exactly-once semantics. "
            "Use SafeKafkaConsumer instead of raw Consumer."
        )
