"""
Kafka worker runtime helpers (Template Method + Strategy).

This module centralizes the common per-message flow used by Kafka workers that
require:
- SafeKafkaConsumer (commit/seek)
- ProcessedEventRegistry (claim/mark_done/mark_failed)
- Heartbeat loop to extend leases while processing
- Retry/backoff + DLQ publishing

Concrete workers implement small Strategy hooks to parse payloads, extract the
ProcessedEventRegistry key, classify retryability, and publish DLQ messages.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Deque, Dict, Generic, Optional, TypeVar

from confluent_kafka import KafkaError, TopicPartition

from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import attach_context_from_kafka
from shared.services.kafka.consumer_ops import InlineKafkaConsumerOps, KafkaConsumerOps
from shared.services.kafka.worker_consumer_runtime import WorkerConsumerRuntime
from shared.services.registries.processed_event_heartbeat import run_processed_event_heartbeat_loop
from shared.services.registries.processed_event_registry import (
    ClaimDecision,
    ClaimResult,
    ProcessedEventRegistry,
)

logger = logging.getLogger(__name__)

PayloadT = TypeVar("PayloadT")
ResultT = TypeVar("ResultT")
PartitionKey = tuple[str, int]


@dataclass(frozen=True)
class RegistryKey:
    event_id: str
    aggregate_id: Optional[str] = None
    sequence_number: Optional[int] = None


@dataclass(frozen=True)
class HeartbeatOptions:
    interval_seconds: Optional[int] = None
    stop_when_false: bool = False
    continue_on_exception: bool = True
    warning_message: Optional[str] = None


class ProcessedEventKafkaWorker(Generic[PayloadT, ResultT], ABC):
    """
    Template Method for Kafka workers guarded by ProcessedEventRegistry.

    This class handles the shared control-flow (claim/heartbeat/retry/DLQ) while
    delegating variable parts (payload parsing, side-effects, DLQ shape) to
    Strategy hooks implemented by each worker.
    """

    # --- Required wiring (provided by concrete worker instances) ---
    consumer: Any  # SafeKafkaConsumer
    consumer_ops: Optional[KafkaConsumerOps]
    processed: Optional[ProcessedEventRegistry]
    handler: str
    max_retries: int
    backoff_base: int
    backoff_max: int

    # Optional observability hooks used by many workers.
    tracing: Any  # get_tracing_service(...)
    metrics: Any  # get_metrics_collector(...)

    # --- Strategy hooks ---
    @abstractmethod
    def _parse_payload(self, payload: Any) -> PayloadT:
        """Parse msg.value() into a domain payload (may raise)."""

    @abstractmethod
    def _registry_key(self, payload: PayloadT) -> RegistryKey:
        """Extract the ProcessedEventRegistry key for this payload."""

    @abstractmethod
    async def _process_payload(self, payload: PayloadT) -> ResultT:
        """Perform the worker side-effect for a claimed payload."""

    @abstractmethod
    async def _send_to_dlq(
        self,
        *,
        msg: Any,
        payload: PayloadT,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        """Publish a DLQ record for a terminal failure."""

    # --- Optional overrides ---
    def _service_name(self) -> Optional[str]:
        return getattr(self, "service_name", None) or None

    def _fallback_metadata(self, payload: PayloadT) -> Optional[Dict[str, Any]]:
        return None

    def _span_name(self, *, payload: PayloadT) -> str:
        name = self._service_name() or self.__class__.__name__
        return f"{name}.process_message"

    def _span_attributes(self, *, msg: Any, payload: PayloadT, registry_key: RegistryKey) -> Dict[str, Any]:
        return {
            "messaging.system": "kafka",
            "messaging.destination": msg.topic(),
            "messaging.destination_kind": "topic",
            "messaging.kafka.partition": msg.partition(),
            "messaging.kafka.offset": msg.offset(),
            "event.id": registry_key.event_id,
            "event.aggregate_id": registry_key.aggregate_id,
        }

    def _metric_event_name(self, *, payload: PayloadT) -> Optional[str]:
        return None

    def _registry_handler(self, *, msg: Any, payload: PayloadT) -> str:
        return str(getattr(self, "handler", "") or "")

    def _is_retryable_error(self, exc: Exception, *, payload: PayloadT) -> bool:
        # Safe default: parsing/validation errors should not be retried forever.
        return not isinstance(exc, (ValueError, TypeError))

    def _in_progress_sleep_seconds(self, *, claim: ClaimResult, payload: PayloadT) -> float:
        return 2.0

    def _should_seek_on_in_progress(self, *, claim: ClaimResult, payload: PayloadT) -> bool:
        return True

    def _should_seek_on_retry(self, *, attempt_count: int, payload: PayloadT) -> bool:
        return True

    def _should_mark_done_after_dlq(self, *, payload: PayloadT, error: str) -> bool:
        return False

    def _cancel_inflight_on_revoke(self) -> bool:
        """
        Whether to cancel in-flight partition tasks on Kafka rebalance revoke.

        Default: True (conservative) to avoid duplicate work across consumers.
        Workers with strong idempotency guarantees and long-running jobs may
        override this to False so in-flight work can finish while the new owner
        observes the ProcessedEventRegistry lease.
        """

        return True

    def _backoff_seconds(self, *, attempt_count: int, payload: PayloadT) -> int:
        return min(self.backoff_max, int(self.backoff_base * (2 ** max(0, attempt_count - 1))))

    def _max_retries_for_error(self, exc: Exception, *, payload: PayloadT, error: str, retryable: bool) -> int:
        return int(self.max_retries)

    def _backoff_seconds_for_error(
        self,
        exc: Exception,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> int:
        return self._backoff_seconds(attempt_count=attempt_count, payload=payload)

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:
        logger.exception("Invalid Kafka payload; skipping: %s", error)

    async def _on_success(self, *, payload: PayloadT, result: ResultT, duration_s: float) -> None:
        metrics = getattr(self, "metrics", None)
        name = self._metric_event_name(payload=payload)
        if not metrics or not name:
            return
        try:
            metrics.record_event(name, action="processed", duration=duration_s)
        except Exception:
            pass

    async def _on_retry_scheduled(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        return

    async def _on_terminal_failure(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        return

    async def _mark_retryable_failure(
        self,
        *,
        payload: PayloadT,
        registry_key: RegistryKey,
        handler: str,
        error: str,
    ) -> None:
        await self.processed.mark_failed(
            handler=handler,
            event_id=str(registry_key.event_id),
            error=error,
        )

    async def _commit(self, msg: Any) -> None:
        if not getattr(self, "consumer", None):
            return
        if not hasattr(self, "_revoked_partitions"):
            self._init_partition_state(reset=False)
        await self._get_consumer_runtime().commit(msg)

    async def _seek(self, *, topic: str, partition: int, offset: int) -> None:
        if not getattr(self, "consumer", None):
            return
        if not hasattr(self, "_revoked_partitions"):
            self._init_partition_state(reset=False)
        await self._get_consumer_runtime().seek(topic=topic, partition=partition, offset=offset)

    def _heartbeat_options(self) -> HeartbeatOptions:
        return HeartbeatOptions()

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        options = self._heartbeat_options()
        await run_processed_event_heartbeat_loop(
            self.processed,
            handler=handler,
            event_id=event_id,
            interval_seconds=options.interval_seconds,
            stop_when_false=options.stop_when_false,
            continue_on_exception=options.continue_on_exception,
            logger=logger,
            warning_message=options.warning_message,
        )

    async def _poll_message(self, *, timeout: float) -> Any:
        poller = getattr(self, "_poll", None)
        if not getattr(self, "consumer", None):
            return None
        return await self._get_consumer_runtime().poll_message(
            timeout=timeout,
            poller=poller if callable(poller) else None,
        )

    def _get_consumer_ops(self) -> KafkaConsumerOps:
        ops = getattr(self, "consumer_ops", None)
        if ops is not None:
            return ops
        consumer = getattr(self, "consumer", None)
        if consumer is None:
            raise RuntimeError("consumer not initialized")
        ops = InlineKafkaConsumerOps(consumer)
        setattr(self, "consumer_ops", ops)
        return ops

    def _get_consumer_runtime(self) -> WorkerConsumerRuntime:
        if not hasattr(self, "_event_loop"):
            with suppress(RuntimeError):
                setattr(self, "_event_loop", asyncio.get_running_loop())
        if not hasattr(self, "_revoked_partitions"):
            self._init_partition_state(reset=False)
        return WorkerConsumerRuntime(
            ops=self._get_consumer_ops(),
            loop_label=self._loop_label,
            revoked_partitions=self._revoked_partitions,
            uses_commit_state=self._uses_commit_state,
            commit_state_by_partition=self._commit_state_by_partition,
        )

    async def _close_consumer_runtime(self) -> None:
        ops = getattr(self, "consumer_ops", None)
        consumer = getattr(self, "consumer", None)

        if ops is not None:
            try:
                await ops.close()
            except Exception as exc:
                logger.warning(
                    "Kafka consumer close failed during %s shutdown: %s",
                    self._loop_label(),
                    exc,
                    exc_info=True,
                )
            finally:
                setattr(self, "consumer_ops", None)
                setattr(self, "consumer", None)
            return

        if consumer is not None:
            try:
                consumer.close()
            except Exception as exc:
                logger.warning(
                    "Kafka consumer close failed during %s shutdown: %s",
                    self._loop_label(),
                    exc,
                    exc_info=True,
                )
            finally:
                setattr(self, "consumer", None)

    def _is_partition_eof(self, msg: Any) -> bool:
        try:
            error = msg.error()
        except Exception:
            return False
        if not error:
            return False
        try:
            return error.code() == KafkaError._PARTITION_EOF
        except Exception:
            return False

    def _loop_label(self) -> str:
        return (self._service_name() or self.__class__.__name__).strip() or self.__class__.__name__

    async def _on_poll_exception(self, exc: Exception) -> None:
        logger.warning("Kafka poll failed (will retry): %s", exc, exc_info=True)

    async def _on_kafka_message_error(self, msg: Any) -> None:
        try:
            logger.error("Kafka error: %s", msg.error())
        except Exception:
            logger.error("Kafka error: %s", msg)

    async def _on_unexpected_message_error(self, exc: Exception, msg: Any) -> None:
        logger.error("Unexpected %s error: %s", self._loop_label(), exc, exc_info=True)

    async def _seek_on_unexpected_error(self, msg: Any) -> None:
        with suppress(Exception):
            await self._seek(
                topic=str(msg.topic()),
                partition=int(msg.partition()),
                offset=int(msg.offset()),
            )

    def _init_partition_state(self, *, reset: bool = True) -> None:
        if not hasattr(self, "_event_loop"):
            with suppress(RuntimeError):
                setattr(self, "_event_loop", asyncio.get_running_loop())
        if reset or not hasattr(self, "_revoked_partitions"):
            self._revoked_partitions: set[PartitionKey] = set()
        if reset or not hasattr(self, "_inflight_by_partition"):
            self._inflight_by_partition: Dict[PartitionKey, asyncio.Task] = {}
        if reset or not hasattr(self, "_pending_by_partition"):
            self._pending_by_partition: Dict[PartitionKey, Deque[Any]] = {}
        if reset or not hasattr(self, "_commit_state_by_partition"):
            self._commit_state_by_partition: Dict[PartitionKey, bool] = {}
        if reset or not hasattr(self, "_rebalance_in_progress"):
            self._rebalance_in_progress = False

    def _buffer_messages(self) -> bool:
        return False

    def _pending_log_thresholds(self) -> set[int]:
        return set()

    def _uses_commit_state(self) -> bool:
        return False

    def _busy_partition_sleep_seconds(self) -> float:
        return 0.1

    def _partition_task_name(self, msg: Any) -> str:
        label = self._loop_label()
        return f"{label}:{msg.topic()}:{msg.partition()}:{msg.offset()}"

    def _partition_key(self, msg: Any) -> PartitionKey:
        return (str(msg.topic()), int(msg.partition()))

    def _log_buffered_message(self, *, msg: Any, pending_count: int) -> None:
        logger.info(
            "Buffered %s message while partition busy (topic=%s partition=%s pending=%s offset=%s)",
            self._loop_label(),
            msg.topic(),
            msg.partition(),
            pending_count,
            msg.offset(),
        )

    async def _pause_partition(self, *, topic: str, partition: int) -> None:
        if not getattr(self, "consumer", None):
            return
        await self._get_consumer_runtime().pause_partition(topic=topic, partition=partition)

    async def _resume_partition(self, *, topic: str, partition: int) -> None:
        if not getattr(self, "consumer", None):
            return
        await self._get_consumer_runtime().resume_partition(topic=topic, partition=partition)

    def _log_background_task_exception(self, task: asyncio.Task) -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Background task introspection failed: %s", exc)
            return
        if exc:
            logger.error("Background task crashed: %s", exc, exc_info=True)

    def _run_on_event_loop_thread(self, fn) -> None:  # noqa: ANN001
        loop = getattr(self, "_event_loop", None)
        if loop is None:
            # If we have no captured loop, only execute inline when we are already
            # on the asyncio event-loop thread.
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                logger.warning("Event loop not captured; skipping thread-unsafe rebalance side-effects")
                return
            fn()
            return
        try:
            loop.call_soon_threadsafe(fn)
        except RuntimeError:
            logger.warning("Event loop unavailable; skipping thread-unsafe rebalance side-effects")

    def _handle_partitions_revoked(self, partitions: list, *, clear_pending: bool = True) -> None:
        revoked = {(p.topic, int(p.partition)) for p in partitions}
        self._revoked_partitions |= revoked

        def _apply_revocation() -> None:
            for key in revoked:
                task = self._inflight_by_partition.get(key)
                if task and self._cancel_inflight_on_revoke():
                    task.cancel()
                if clear_pending:
                    self._pending_by_partition.pop(key, None)

        # Rebalance callbacks can run on the Kafka consumer thread; cancelling asyncio tasks must happen
        # on the event-loop thread.
        self._run_on_event_loop_thread(_apply_revocation)

    def _handle_partitions_assigned(self, partitions: list, *, resume: bool = True) -> None:
        for p in partitions:
            self._revoked_partitions.discard((p.topic, int(p.partition)))
        if resume and getattr(self, "consumer", None) and partitions:
            try:
                self.consumer.resume([TopicPartition(p.topic, p.partition) for p in partitions])
            except Exception as exc:
                logger.warning("Failed to resume %s partitions after assign: %s", self._loop_label(), exc)

    def _on_partitions_revoked(self, partitions: list) -> None:
        """
        Default SafeKafkaConsumer rebalance revoke callback.

        Concrete workers can pass this as `on_revoke=self._on_partitions_revoked`
        without duplicating boilerplate. Override in workers that need custom
        behavior.
        """
        self._init_partition_state(reset=False)
        self._rebalance_in_progress = True
        logger.info(
            "%s partitions revoked: %s",
            self._loop_label(),
            [(p.topic, int(p.partition)) for p in partitions],
        )
        self._handle_partitions_revoked(partitions, clear_pending=True)

    def _on_partitions_assigned(self, partitions: list) -> None:
        """
        Default SafeKafkaConsumer rebalance assign callback.

        Override in workers that need custom resume semantics.
        """
        self._init_partition_state(reset=False)
        self._rebalance_in_progress = False
        logger.info(
            "%s partitions assigned: %s",
            self._loop_label(),
            [(p.topic, int(p.partition)) for p in partitions],
        )
        self._handle_partitions_assigned(partitions, resume=True)

    async def _handle_busy_partition_message(self, msg: Any) -> None:
        topic = str(msg.topic())
        partition = int(msg.partition())
        logger.warning(
            "Received %s message while partition busy; rewinding (topic=%s partition=%s offset=%s)",
            self._loop_label(),
            topic,
            partition,
            msg.offset(),
        )
        try:
            await self._seek(topic=topic, partition=partition, offset=int(msg.offset()))
            await self._pause_partition(topic=topic, partition=partition)
        except Exception as exc:
            logger.warning("Failed to rewind/pause busy partition: %s", exc)
        await asyncio.sleep(self._busy_partition_sleep_seconds())

    async def _start_partition_task(self, msg: Any) -> None:
        if not getattr(self, "consumer", None):
            return
        topic = str(msg.topic())
        partition = int(msg.partition())
        key = (topic, partition)

        await self._pause_partition(topic=topic, partition=partition)
        task = asyncio.create_task(
            self._handle_partition_message(msg),
            name=self._partition_task_name(msg),
        )
        self._inflight_by_partition[key] = task
        task.add_done_callback(self._log_background_task_exception)

    async def _handle_partition_message(self, msg: Any) -> None:
        if not getattr(self, "consumer", None):
            return
        key = self._partition_key(msg)
        if self._uses_commit_state():
            self._commit_state_by_partition[key] = False
        try:
            await self.handle_message(msg)
        finally:
            committed = True
            if self._uses_commit_state():
                committed = bool(self._commit_state_by_partition.pop(key, False))
            current = self._inflight_by_partition.get(key)
            if current is asyncio.current_task():
                self._inflight_by_partition.pop(key, None)
            if not getattr(self, "consumer", None):
                return
            if key in self._revoked_partitions:
                self._pending_by_partition.pop(key, None)
                return

            if self._buffer_messages():
                if not committed:
                    self._pending_by_partition.pop(key, None)
                    await self._resume_partition(topic=key[0], partition=key[1])
                    return
                pending = self._pending_by_partition.get(key)
                if pending and len(pending) > 0:
                    next_msg = pending.popleft()
                    if len(pending) == 0:
                        self._pending_by_partition.pop(key, None)
                    await self._start_partition_task(next_msg)
                    return

            await self._resume_partition(topic=key[0], partition=key[1])

    async def _cancel_inflight_tasks(self) -> None:
        inflight = list(self._inflight_by_partition.values())
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)

    async def _poll_for_message(
        self,
        *,
        poll_timeout: float,
        idle_sleep: Optional[float],
        missing_consumer_sleep: float,
        poll_exception_sleep: float,
    ) -> Any:  # noqa: ANN401
        """
        Shared polling template for worker loops.

        Returns a *valid* Kafka message (no errors) or None when the caller
        should continue the loop. This centralizes common control-flow and
        reduces drift between run-loop implementations.
        """

        if not getattr(self, "consumer", None):
            if missing_consumer_sleep:
                await asyncio.sleep(missing_consumer_sleep)
            return None

        try:
            msg = await self._poll_message(timeout=poll_timeout)
        except Exception as exc:
            await self._on_poll_exception(exc)
            if poll_exception_sleep:
                await asyncio.sleep(poll_exception_sleep)
            return None

        if msg is None:
            if idle_sleep is not None:
                await asyncio.sleep(idle_sleep)
            return None

        if msg.error():
            if self._is_partition_eof(msg):
                return None
            await self._on_kafka_message_error(msg)
            return None

        return msg

    async def run_loop(
        self,
        *,
        poll_timeout: float = 1.0,
        idle_sleep: Optional[float] = None,
        missing_consumer_sleep: float = 0.2,
        poll_exception_sleep: float = 1.0,
        unexpected_error_sleep: float = 2.0,
        post_seek_sleep: float = 1.0,
        seek_on_error: bool = True,
        catch_exceptions: bool = True,
    ) -> None:
        self._init_partition_state(reset=False)
        while self.running:
            msg = await self._poll_for_message(
                poll_timeout=poll_timeout,
                idle_sleep=idle_sleep,
                missing_consumer_sleep=missing_consumer_sleep,
                poll_exception_sleep=poll_exception_sleep,
            )
            if msg is None:
                continue

            if not catch_exceptions:
                await self.handle_message(msg)
                continue

            try:
                await self.handle_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._on_unexpected_message_error(exc, msg)
                if unexpected_error_sleep:
                    await asyncio.sleep(unexpected_error_sleep)
                if seek_on_error:
                    await self._seek_on_unexpected_error(msg)
                    if post_seek_sleep:
                        await asyncio.sleep(post_seek_sleep)

    async def run_partitioned_loop(
        self,
        *,
        poll_timeout: float = 0.25,
        idle_sleep: Optional[float] = 0.0,
        missing_consumer_sleep: float = 0.2,
        poll_exception_sleep: float = 1.0,
    ) -> None:
        self._init_partition_state(reset=False)

        while self.running:
            msg = await self._poll_for_message(
                poll_timeout=poll_timeout,
                idle_sleep=idle_sleep,
                missing_consumer_sleep=missing_consumer_sleep,
                poll_exception_sleep=poll_exception_sleep,
            )
            if msg is None:
                continue

            key = self._partition_key(msg)
            if key in self._inflight_by_partition:
                if self._buffer_messages():
                    pending = self._pending_by_partition.setdefault(key, deque())
                    pending.append(msg)
                    thresholds = self._pending_log_thresholds()
                    if thresholds and len(pending) in thresholds:
                        self._log_buffered_message(msg=msg, pending_count=len(pending))
                else:
                    await self._handle_busy_partition_message(msg)
                continue

            pending = self._pending_by_partition.get(key)
            if self._buffer_messages() and pending and len(pending) > 0:
                pending.append(msg)
                msg_to_process = pending.popleft()
                if len(pending) == 0:
                    self._pending_by_partition.pop(key, None)
            else:
                msg_to_process = msg

            await self._start_partition_task(msg_to_process)

    async def handle_message(self, msg: Any) -> None:
        """
        Handle a single Kafka message (msg.value()).

        This method owns:
        - claim/heartbeat lifecycle
        - commit/seek decisions
        - retry/backoff + DLQ threshold
        """
        if not getattr(self, "consumer", None):
            raise RuntimeError("consumer not initialized")
        if not getattr(self, "processed", None):
            raise RuntimeError("ProcessedEventRegistry not initialized")
        if not getattr(self, "handler", None):
            raise RuntimeError("handler not configured")

        topic = str(msg.topic())
        partition = int(msg.partition())
        offset = int(msg.offset())

        kafka_headers = None
        try:
            kafka_headers = msg.headers()
        except Exception:
            kafka_headers = None

        payload_raw = msg.value()
        raw_text: Optional[str] = None
        if isinstance(payload_raw, (bytes, bytearray)):
            raw_text = payload_raw.decode("utf-8", errors="replace")

        if not payload_raw:
            await self._commit(msg)
            return

        try:
            payload = self._parse_payload(payload_raw)
        except Exception as exc:
            await self._on_parse_error(msg=msg, raw_payload=raw_text, error=exc)
            await self._commit(msg)
            return

        registry_key = self._registry_key(payload)

        fallback_metadata = self._fallback_metadata(payload)
        service_name = self._service_name()

        tracing = getattr(self, "tracing", None)
        span_name = self._span_name(payload=payload)
        span_attrs = self._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        start = time.monotonic()

        def _span_ctx():
            if tracing is None:
                return None
            span = getattr(tracing, "span", None)
            if not callable(span):
                return None
            return span(span_name, attributes=span_attrs)

        with attach_context_from_kafka(
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata if isinstance(fallback_metadata, dict) else None,
            service_name=service_name,
        ):
            span_cm = _span_ctx()
            if span_cm is None:
                return await self._handle_claimed(
                    msg=msg,
                    payload=payload,
                    registry_key=registry_key,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    raw_text=raw_text,
                    start=start,
                )

            with span_cm:
                return await self._handle_claimed(
                    msg=msg,
                    payload=payload,
                    registry_key=registry_key,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    raw_text=raw_text,
                    start=start,
                )

    async def _handle_claimed(
        self,
        *,
        msg: Any,
        payload: PayloadT,
        registry_key: RegistryKey,
        topic: str,
        partition: int,
        offset: int,
        raw_text: Optional[str],
        start: float,
    ) -> None:
        handler = str(self._registry_handler(msg=msg, payload=payload) or "").strip()
        if not handler:
            raise RuntimeError("registry handler not configured")

        claim = await self.processed.claim(
            handler=handler,
            event_id=str(registry_key.event_id),
            aggregate_id=str(registry_key.aggregate_id) if registry_key.aggregate_id else None,
            sequence_number=int(registry_key.sequence_number) if registry_key.sequence_number is not None else None,
        )

        if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
            await self._commit(msg)
            return

        if claim.decision == ClaimDecision.IN_PROGRESS:
            await asyncio.sleep(self._in_progress_sleep_seconds(claim=claim, payload=payload))
            if self._should_seek_on_in_progress(claim=claim, payload=payload):
                await self._seek(topic=topic, partition=partition, offset=offset)
            return

        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(handler=handler, event_id=str(registry_key.event_id))
        )

        try:
            result = await self._process_payload(payload)
            await self.processed.mark_done(
                handler=handler,
                event_id=str(registry_key.event_id),
                aggregate_id=str(registry_key.aggregate_id) if registry_key.aggregate_id else None,
                sequence_number=int(registry_key.sequence_number) if registry_key.sequence_number is not None else None,
            )
            await self._commit(msg)
            await self._on_success(payload=payload, result=result, duration_s=time.monotonic() - start)
            return
        except asyncio.CancelledError:
            try:
                await self.processed.mark_failed(
                    handler=handler,
                    event_id=str(registry_key.event_id),
                    error="cancelled",
                )
            except Exception as mark_err:
                logger.warning("Failed to mark cancelled event failed: %s", mark_err, exc_info=True)
            raise
        except Exception as exc:
            err = str(exc)
            retryable = self._is_retryable_error(exc, payload=payload)
            attempt_count = int(getattr(claim, "attempt_count", 1) or 1)
            max_retries = int(self._max_retries_for_error(exc, payload=payload, error=err, retryable=retryable))
            max_retries = max(1, max_retries)

            if not retryable:
                attempt_count = max_retries

            if attempt_count >= max_retries:
                try:
                    await self.processed.mark_failed(
                        handler=handler,
                        event_id=str(registry_key.event_id),
                        error=err,
                    )
                except Exception as mark_err:
                    logger.warning("Failed to mark event failed: %s", mark_err, exc_info=True)
                await self._on_terminal_failure(
                    payload=payload,
                    error=err,
                    attempt_count=attempt_count,
                    retryable=retryable,
                )
                await self._send_to_dlq(
                    msg=msg,
                    payload=payload,
                    raw_payload=raw_text,
                    error=err,
                    attempt_count=attempt_count,
                )
                if self._should_mark_done_after_dlq(payload=payload, error=err):
                    try:
                        await self.processed.mark_done(
                            handler=handler,
                            event_id=str(registry_key.event_id),
                            aggregate_id=str(registry_key.aggregate_id) if registry_key.aggregate_id else None,
                            sequence_number=int(registry_key.sequence_number)
                            if registry_key.sequence_number is not None
                            else None,
                        )
                    except Exception as mark_done_err:
                        logger.warning(
                            "Failed to mark event done after DLQ (event_id=%s): %s",
                            registry_key.event_id,
                            mark_done_err,
                            exc_info=True,
                        )
                await self._commit(msg)
                return

            try:
                await self._mark_retryable_failure(payload=payload, registry_key=registry_key, handler=handler, error=err)
            except Exception as mark_err:
                logger.warning("Failed to mark event retrying/failed: %s", mark_err, exc_info=True)

            backoff_s = int(
                self._backoff_seconds_for_error(
                    exc,
                    payload=payload,
                    error=err,
                    attempt_count=attempt_count,
                    retryable=retryable,
                )
            )
            backoff_s = max(0, backoff_s)
            await self._on_retry_scheduled(
                payload=payload,
                error=err,
                attempt_count=attempt_count,
                backoff_s=backoff_s,
                retryable=retryable,
            )
            await asyncio.sleep(backoff_s)
            if self._should_seek_on_retry(attempt_count=attempt_count, payload=payload):
                await self._seek(topic=topic, partition=partition, offset=offset)
            return
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass


class StrictHeartbeatKafkaWorker(ProcessedEventKafkaWorker[PayloadT, ResultT], ABC):
    """
    Common specialization that fails fast when ProcessedEventRegistry heartbeats fail.

    Many workers want the runtime to stop extending leases if the registry becomes
    unavailable (fail-closed) rather than silently continuing work without durable
    idempotency guarantees.
    """

    def _heartbeat_options(self) -> HeartbeatOptions:  # type: ignore[override]
        return HeartbeatOptions(
            stop_when_false=True,
            continue_on_exception=False,
        )


class EventEnvelopeKafkaWorker(ProcessedEventKafkaWorker[EventEnvelope, ResultT], ABC):
    """
    Specialization of ProcessedEventKafkaWorker for EventEnvelope payloads.

    Provides a default payload parser, registry key extraction, and common span
    attributes so envelope-based workers don't duplicate boilerplate.
    """

    expected_envelope_kind: Optional[str] = None

    def _parse_payload(self, payload: Any) -> EventEnvelope:  # type: ignore[override]
        try:
            envelope = EventEnvelope.model_validate_json(payload)
        except Exception as exc:
            raise ValueError(f"Invalid EventEnvelope JSON: {exc}") from exc

        expected_kind = (getattr(self, "expected_envelope_kind", None) or "").strip()
        if expected_kind:
            kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
            if kind != expected_kind:
                raise ValueError(f"Unexpected envelope kind (expected={expected_kind} got={kind})")

        return envelope

    def _registry_key(self, payload: EventEnvelope) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(
            event_id=str(payload.event_id),
            aggregate_id=str(payload.aggregate_id or ""),
            sequence_number=int(payload.sequence_number) if payload.sequence_number is not None else None,
        )

    def _fallback_metadata(self, payload: EventEnvelope) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        return payload.metadata if isinstance(payload.metadata, dict) else None

    def _span_attributes(self, *, msg: Any, payload: EventEnvelope, registry_key: RegistryKey) -> Dict[str, Any]:  # type: ignore[override]
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        attrs.update(
            {
                "event.type": str(payload.event_type),
                "event.aggregate_type": str(payload.aggregate_type),
                "event.aggregate_id": str(payload.aggregate_id or ""),
                "event.sequence_number": payload.sequence_number,
            }
        )
        return attrs

    def _metric_event_name(self, *, payload: EventEnvelope) -> Optional[str]:  # type: ignore[override]
        event_type = str(payload.event_type or "").strip()
        return event_type or None


class StrictHeartbeatEventEnvelopeKafkaWorker(EventEnvelopeKafkaWorker[ResultT], ABC):
    """EventEnvelopeKafkaWorker with the common strict heartbeat policy."""

    def _heartbeat_options(self) -> HeartbeatOptions:  # type: ignore[override]
        return HeartbeatOptions(
            stop_when_false=True,
            continue_on_exception=False,
        )
