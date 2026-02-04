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
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, TypeVar

from confluent_kafka import KafkaError, TopicPartition

from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import attach_context_from_kafka
from shared.services.registries.processed_event_heartbeat import run_processed_event_heartbeat_loop
from shared.services.registries.processed_event_registry import (
    ClaimDecision,
    ClaimResult,
    ProcessedEventRegistry,
)

logger = logging.getLogger(__name__)

PayloadT = TypeVar("PayloadT")
ResultT = TypeVar("ResultT")


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
        self.consumer.commit_sync(msg)

    async def _seek(self, *, topic: str, partition: int, offset: int) -> None:
        if not getattr(self, "consumer", None):
            return
        self.consumer.seek(TopicPartition(topic, partition, offset))

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
        if callable(poller):
            return await poller(timeout)
        if not getattr(self, "consumer", None):
            return None
        return await asyncio.to_thread(self.consumer.poll, timeout)

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
        while self.running:
            if not getattr(self, "consumer", None):
                if missing_consumer_sleep:
                    await asyncio.sleep(missing_consumer_sleep)
                continue

            try:
                msg = await self._poll_message(timeout=poll_timeout)
            except Exception as exc:
                await self._on_poll_exception(exc)
                if poll_exception_sleep:
                    await asyncio.sleep(poll_exception_sleep)
                continue

            if msg is None:
                if idle_sleep is not None:
                    await asyncio.sleep(idle_sleep)
                continue

            if msg.error():
                if self._is_partition_eof(msg):
                    continue
                await self._on_kafka_message_error(msg)
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
