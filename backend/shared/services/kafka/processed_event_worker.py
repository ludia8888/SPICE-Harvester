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
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Deque, Dict, Generic, Optional, TypeVar, cast

from confluent_kafka import KafkaError, TopicPartition
from pydantic import BaseModel

from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import attach_context_from_kafka
from shared.observability.tracing import trace_kafka_operation
from shared.services.kafka.consumer_ops import ExecutorKafkaConsumerOps, InlineKafkaConsumerOps, KafkaConsumerOps
from shared.services.kafka.dlq_publisher import (
    DlqPublishSpec,
    EnvelopeDlqSpec,
    build_envelope_dlq_event,
    publish_envelope_dlq,
    publish_standard_dlq,
)
from shared.services.kafka.safe_consumer import create_safe_consumer
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


@dataclass(frozen=True)
class ParseErrorContext:
    stage: str
    payload_text: Optional[str]
    payload_obj: Optional[Dict[str, Any]]
    fallback_metadata: Optional[Dict[str, Any]]
    cause: Exception


@dataclass(frozen=True)
class FailureLogContext:
    message: str
    args: tuple[Any, ...]


@dataclass
class CommandEnvelopePayload:
    command: Dict[str, Any]
    envelope_metadata: Optional[Dict[str, Any]] = None
    envelope: Optional[EventEnvelope] = None
    raw_text: Optional[str] = None
    stage: str = "process_command"


class CommandParseError(ValueError):
    def __init__(
        self,
        *,
        stage: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        fallback_metadata: Optional[Dict[str, Any]],
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.stage = str(stage)
        self.payload_text = payload_text
        self.payload_obj = payload_obj
        self.fallback_metadata = fallback_metadata
        self.cause = cause


@dataclass(frozen=True)
class WorkerRuntimeConfig:
    service_name: str
    handler: str
    kafka_servers: str
    dlq_topic: str
    dlq_flush_timeout_seconds: float
    max_retry_attempts: int
    backoff_base: int = 1
    backoff_max: int = 60


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
    parse_error_enable_dlq: bool = False
    parse_error_publish_failure_message: str = "Failed to publish invalid payload to DLQ: %s"
    parse_error_invalid_payload_message: str = "Invalid payload; skipping: %s"
    parse_error_raise_on_publish_failure: bool = True

    def _bootstrap_worker_runtime(
        self,
        *,
        config: WorkerRuntimeConfig,
        tracing: Any = None,
        metrics: Any = None,
    ) -> None:
        self.running = False
        self.service_name = str(config.service_name or "").strip() or self.__class__.__name__
        self.handler = str(config.handler or "").strip() or self.service_name
        self.kafka_servers = str(config.kafka_servers or "").strip()
        self.consumer = None
        self.consumer_ops = None
        self.dlq_producer = None
        self.dlq_topic = str(config.dlq_topic or "").strip()
        self.dlq_flush_timeout_seconds = float(config.dlq_flush_timeout_seconds)
        self.max_retry_attempts = int(config.max_retry_attempts)
        self.max_retries = int(self.max_retry_attempts)
        self.backoff_base = int(config.backoff_base)
        self.backoff_max = int(config.backoff_max)
        if tracing is not None:
            self.tracing = tracing
        if metrics is not None:
            self.metrics = metrics

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

    async def _send_to_dlq(
        self,
        *,
        msg: Any,
        payload: Optional[PayloadT] = None,
        raw_payload: Optional[str] = None,
        error: str,
        attempt_count: int,
        stage: str = "process_command",
        payload_text: Optional[str] = None,
        payload_obj: Optional[Dict[str, Any]] = None,
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Publish a DLQ record for a terminal failure."""
        publisher = getattr(self, "_publish_to_dlq", None)
        if not callable(publisher):
            raise RuntimeError("DLQ publisher is not configured for command payload")
        await self._send_command_payload_dlq_record(
            msg=msg,
            error=error,
            attempt_count=int(attempt_count),
            payload=payload,
            raw_payload=raw_payload,
            stage=str(stage or "process_command"),
            default_stage="process_command",
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            publisher=publisher,
        )

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

    async def _send_parse_error_via_publish_to_dlq(
        self,
        *,
        msg: Any,
        stage: str,
        cause_text: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        kafka_headers: Optional[Any],
        fallback_metadata: Optional[Dict[str, Any]],
    ) -> None:
        publisher = getattr(self, "_publish_to_dlq", None)
        if not callable(publisher):
            raise RuntimeError("parse error DLQ publishing is not configured for this worker")
        await publisher(
            msg=msg,
            stage=stage,
            error=cause_text,
            attempt_count=1,
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
        )

    async def _before_parse_error_handling(
        self,
        *,
        msg: Any,
        raw_payload: Optional[str],
        error: Exception,
        context: ParseErrorContext,
    ) -> None:
        return

    def _resolve_parse_error_fallback_metadata(
        self,
        *,
        raw_payload: Optional[str],
        context: ParseErrorContext,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(context.fallback_metadata, dict):
            return context.fallback_metadata
        metadata = self._fallback_metadata_from_raw_payload(raw_payload)
        if metadata is not None:
            return metadata
        return self._fallback_metadata_from_raw_payload(context.payload_text)

    async def _send_parse_error_to_dlq(
        self,
        *,
        msg: Any,
        raw_payload: Optional[str],
        context: ParseErrorContext,
        kafka_headers: Optional[Any],
        fallback_metadata: Optional[Dict[str, Any]],
    ) -> None:
        _ = raw_payload
        await self._send_parse_error_via_publish_to_dlq(
            msg=msg,
            stage=context.stage,
            cause_text=str(context.cause),
            payload_text=context.payload_text,
            payload_obj=context.payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
        )

    async def _after_parse_error_dlq_success(
        self,
        *,
        msg: Any,
        raw_payload: Optional[str],
        error: Exception,
        context: ParseErrorContext,
    ) -> None:
        return

    async def _after_parse_error_dlq_failure(
        self,
        *,
        msg: Any,
        raw_payload: Optional[str],
        error: Exception,
        context: ParseErrorContext,
        dlq_error: Exception,
    ) -> None:
        return

    async def _run_parse_error_hook(
        self,
        *,
        hook: Callable[[], Awaitable[None]],
        warning_message: str,
        logger_instance: Optional[logging.Logger] = None,
    ) -> None:
        log = logger_instance or logger
        try:
            await hook()
        except Exception as exc:
            log.warning(warning_message, exc, exc_info=True)

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:
        context = self._parse_error_context(raw_payload=raw_payload, error=error)
        invalid_payload_message = str(
            getattr(self, "parse_error_invalid_payload_message", self.parse_error_invalid_payload_message)
        )
        await self._run_parse_error_hook(
            hook=lambda: self._before_parse_error_handling(
                msg=msg,
                raw_payload=raw_payload,
                error=error,
                context=context,
            ),
            warning_message="Parse error pre-handle hook failed: %s",
            logger_instance=logger,
        )
        if not bool(getattr(self, "parse_error_enable_dlq", False)):
            logger.exception(invalid_payload_message, context.cause)
            return

        await self._publish_parse_error_to_dlq(
            msg=msg,
            raw_payload=raw_payload,
            error=error,
            publish_failure_message=str(
                getattr(self, "parse_error_publish_failure_message", self.parse_error_publish_failure_message)
            ),
            invalid_payload_message=invalid_payload_message,
            raise_on_publish_failure=bool(
                getattr(self, "parse_error_raise_on_publish_failure", self.parse_error_raise_on_publish_failure)
            ),
            logger_instance=logger,
            context=context,
        )

    def _parse_error_context(self, *, raw_payload: Optional[str], error: Exception) -> ParseErrorContext:
        stage = str(getattr(error, "stage", "parse") or "parse")
        payload_text = getattr(error, "payload_text", None)
        if payload_text is None:
            payload_text = raw_payload
        payload_obj = getattr(error, "payload_obj", None)
        payload_obj_final = payload_obj if isinstance(payload_obj, dict) else None
        fallback_metadata = getattr(error, "fallback_metadata", None)
        fallback_metadata_final = fallback_metadata if isinstance(fallback_metadata, dict) else None
        cause = getattr(error, "cause", error)
        cause_final = cause if isinstance(cause, Exception) else Exception(str(cause))
        return ParseErrorContext(
            stage=stage,
            payload_text=payload_text,
            payload_obj=payload_obj_final,
            fallback_metadata=fallback_metadata_final,
            cause=cause_final,
        )

    def _safe_message_headers(self, msg: Any) -> Optional[Any]:
        try:
            return msg.headers()
        except Exception as exc:
            logger.warning("Failed to read Kafka message headers: %s", exc, exc_info=True)
        return None

    def _fallback_metadata_from_raw_payload(self, raw_payload: Optional[str]) -> Optional[Dict[str, Any]]:
        if not raw_payload:
            return None
        try:
            payload_obj = json.loads(raw_payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload_obj, dict):
            return None
        metadata = payload_obj.get("metadata")
        if not isinstance(metadata, dict):
            return None
        return dict(metadata)

    async def _publish_parse_error_to_dlq(
        self,
        *,
        msg: Any,
        raw_payload: Optional[str],
        error: Exception,
        dlq_sender: Optional[Callable[
            [str, str, Optional[str], Optional[Dict[str, Any]], Optional[Any], Optional[Dict[str, Any]]],
            Awaitable[None],
        ]] = None,
        publish_failure_message: str,
        invalid_payload_message: str,
        raise_on_publish_failure: bool = True,
        logger_instance: Optional[logging.Logger] = None,
        context: Optional[ParseErrorContext] = None,
    ) -> bool:
        log = logger_instance or logger
        context = context or self._parse_error_context(raw_payload=raw_payload, error=error)
        kafka_headers = self._safe_message_headers(msg)
        fallback_metadata = self._resolve_parse_error_fallback_metadata(
            raw_payload=raw_payload,
            context=context,
        )

        if dlq_sender is None:
            async def _default_sender(
                stage: str,
                cause_text: str,
                payload_text: Optional[str],
                payload_obj: Optional[Dict[str, Any]],
                kafka_headers_arg: Optional[Any],
                fallback_metadata_arg: Optional[Dict[str, Any]],
            ) -> None:
                _ = stage, cause_text, payload_text, payload_obj
                await self._send_parse_error_to_dlq(
                    msg=msg,
                    raw_payload=raw_payload,
                    context=context,
                    kafka_headers=kafka_headers_arg,
                    fallback_metadata=fallback_metadata_arg,
                )

            dlq_sender = _default_sender

        sent = False
        try:
            await dlq_sender(
                context.stage,
                str(context.cause),
                context.payload_text,
                context.payload_obj,
                kafka_headers,
                fallback_metadata,
            )
        except Exception as dlq_err:
            await self._run_parse_error_hook(
                hook=lambda: self._after_parse_error_dlq_failure(
                    msg=msg,
                    raw_payload=raw_payload,
                    error=error,
                    context=context,
                    dlq_error=dlq_err,
                ),
                warning_message="Parse error DLQ failure hook failed: %s",
                logger_instance=log,
            )
            log.error(publish_failure_message, dlq_err, exc_info=True)
            if raise_on_publish_failure:
                raise
        else:
            sent = True
            await self._run_parse_error_hook(
                hook=lambda: self._after_parse_error_dlq_success(
                    msg=msg,
                    raw_payload=raw_payload,
                    error=error,
                    context=context,
                ),
                warning_message="Parse error DLQ success hook failed: %s",
                logger_instance=log,
            )
        log.exception(invalid_payload_message, context.cause)
        return sent

    def _normalize_dlq_publish_inputs(
        self,
        *,
        msg: Any,
        stage: str,
        default_stage: str,
        raw_payload: Optional[str],
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        kafka_headers: Optional[Any],
        fallback_metadata: Optional[Dict[str, Any]],
        inferred_payload_obj: Optional[Dict[str, Any]] = None,
        inferred_metadata: Optional[Dict[str, Any]] = None,
        inferred_stage: Optional[str] = None,
    ) -> tuple[str, Optional[str], Optional[Dict[str, Any]], Optional[Any], Optional[Dict[str, Any]]]:
        if kafka_headers is None:
            try:
                kafka_headers = msg.headers()
            except Exception as exc:
                logger.warning(
                    "Failed to read Kafka headers during DLQ normalization: %s",
                    exc,
                    exc_info=True,
                )

        stage_final = str(stage or "").strip() or str(default_stage or "").strip() or "process_command"
        inferred_stage_final = str(inferred_stage or "").strip()
        if inferred_stage_final and stage_final == (str(default_stage or "").strip() or "process_command"):
            stage_final = inferred_stage_final

        payload_text_final = payload_text if payload_text is not None else raw_payload
        payload_obj_final = payload_obj if payload_obj is not None else inferred_payload_obj
        metadata_final = fallback_metadata if fallback_metadata is not None else inferred_metadata

        return stage_final, payload_text_final, payload_obj_final, kafka_headers, metadata_final

    async def _publish_standard_dlq_record(
        self,
        *,
        producer: Optional[Any],
        msg: Any,
        worker: str,
        dlq_spec: DlqPublishSpec,
        error: str,
        attempt_count: Optional[int],
        stage: Optional[str],
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        extra: Optional[Dict[str, Any]] = None,
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
        tracing: Optional[Any] = None,
        metrics: Optional[Any] = None,
        lock: Optional[asyncio.Lock] = None,
        raise_on_missing_producer: bool = True,
        missing_producer_message: str = "DLQ producer not configured",
    ) -> bool:
        if not producer:
            if raise_on_missing_producer:
                raise RuntimeError(str(missing_producer_message or "DLQ producer not configured"))
            logger.error("%s; dropping DLQ payload: %s", missing_producer_message, error)
            return False

        await publish_standard_dlq(
            producer=producer,
            msg=msg,
            worker=str(worker or "").strip() or worker,
            dlq_spec=dlq_spec,
            error=error,
            attempt_count=int(attempt_count) if attempt_count is not None else None,
            stage=stage,
            payload_text=payload_text,
            payload_obj=payload_obj,
            extra=extra,
            tracing=tracing,
            metrics=metrics,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata if isinstance(fallback_metadata, dict) else None,
            lock=lock,
        )
        return True

    async def _send_standard_dlq_record(
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        stage: str,
        default_stage: str,
        raw_payload: Optional[str],
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        kafka_headers: Optional[Any],
        fallback_metadata: Optional[Dict[str, Any]],
        publisher: Callable[..., Awaitable[None]],
        inferred_payload_obj: Optional[Dict[str, Any]] = None,
        inferred_metadata: Optional[Dict[str, Any]] = None,
        inferred_stage: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        stage_final, payload_text_final, payload_obj_final, kafka_headers_final, fallback_metadata_final = (
            self._normalize_dlq_publish_inputs(
                msg=msg,
                stage=stage,
                default_stage=default_stage,
                raw_payload=raw_payload,
                payload_text=payload_text,
                payload_obj=payload_obj,
                kafka_headers=kafka_headers,
                fallback_metadata=fallback_metadata,
                inferred_payload_obj=inferred_payload_obj,
                inferred_metadata=inferred_metadata,
                inferred_stage=inferred_stage,
            )
        )

        await publisher(
            msg=msg,
            stage=str(stage_final),
            error=error,
            attempt_count=int(attempt_count),
            payload_text=payload_text_final,
            payload_obj=payload_obj_final,
            kafka_headers=kafka_headers_final,
            fallback_metadata=fallback_metadata_final if isinstance(fallback_metadata_final, dict) else None,
            extra=extra if isinstance(extra, dict) else None,
        )

    async def _send_command_payload_dlq_record(
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        payload: Optional[Any],
        raw_payload: Optional[str],
        stage: str,
        default_stage: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        kafka_headers: Optional[Any],
        fallback_metadata: Optional[Dict[str, Any]],
        publisher: Callable[..., Awaitable[None]],
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        inferred_payload_obj: Optional[Dict[str, Any]] = None
        inferred_metadata: Optional[Dict[str, Any]] = None
        if payload is not None:
            envelope_obj = getattr(payload, "envelope", None)
            command_obj = getattr(payload, "command", None)
            metadata_obj = getattr(payload, "envelope_metadata", None)
            if envelope_obj is not None and hasattr(envelope_obj, "model_dump"):
                envelope_dump = envelope_obj.model_dump(mode="json")
                if isinstance(envelope_dump, dict):
                    inferred_payload_obj = envelope_dump
            if isinstance(command_obj, dict):
                inferred_payload_obj = inferred_payload_obj or command_obj
            if isinstance(metadata_obj, dict):
                inferred_metadata = metadata_obj

        await self._send_standard_dlq_record(
            msg=msg,
            error=error,
            attempt_count=int(attempt_count),
            stage=stage,
            default_stage=default_stage,
            raw_payload=raw_payload,
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            publisher=publisher,
            inferred_payload_obj=inferred_payload_obj,
            inferred_metadata=inferred_metadata,
            extra=extra,
        )

    async def _send_default_command_payload_dlq(
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        payload: Optional[Any],
        raw_payload: Optional[str],
        publisher: Optional[Callable[..., Awaitable[None]]] = None,
    ) -> None:
        publisher_fn = publisher or getattr(self, "_publish_to_dlq", None)
        if not callable(publisher_fn):
            raise RuntimeError("DLQ publisher is not configured for command payload")
        await self._send_command_payload_dlq_record(
            msg=msg,
            error=error,
            attempt_count=int(attempt_count),
            payload=payload,
            raw_payload=raw_payload,
            stage="process_command",
            default_stage="process_command",
            payload_text=None,
            payload_obj=None,
            kafka_headers=None,
            fallback_metadata=None,
            publisher=publisher_fn,
        )

    async def _on_success(self, *, payload: PayloadT, result: ResultT, duration_s: float) -> None:
        metrics = getattr(self, "metrics", None)
        name = self._metric_event_name(payload=payload)
        if not metrics or not name:
            return
        try:
            metrics.record_event(name, action="processed", duration=duration_s)
        except Exception as exc:
            logger.warning("Failed to record processed-event metric %s: %s", name, exc, exc_info=True)

    async def _on_retry_scheduled(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        await self._after_retry_scheduled(
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            backoff_s=backoff_s,
            retryable=retryable,
        )
        log_context = self._retry_log_context(
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            backoff_s=backoff_s,
            retryable=retryable,
        )
        if log_context is not None:
            logger.warning(log_context.message, *log_context.args)

    async def _on_terminal_failure(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        await self._after_terminal_failure(
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            retryable=retryable,
        )
        log_context = self._terminal_failure_log_context(
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            retryable=retryable,
        )
        if log_context is not None:
            logger.error(log_context.message, *log_context.args)

    async def _after_retry_scheduled(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        return

    async def _after_terminal_failure(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        return

    def _retry_log_context(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> FailureLogContext | None:
        return None

    def _terminal_failure_log_context(
        self,
        *,
        payload: PayloadT,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> FailureLogContext | None:
        return None

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

    def _initialize_safe_consumer_runtime(
        self,
        *,
        group_id: str,
        topics: list[str],
        service_name: str,
        thread_name_prefix: str,
        max_poll_interval_ms: Optional[int] = None,
        session_timeout_ms: Optional[int] = None,
        reset_partition_state: bool = False,
    ) -> None:
        create_kwargs: Dict[str, Any] = {
            "group_id": str(group_id or "").strip(),
            "topics": list(topics),
            "service_name": str(service_name or "").strip() or self._loop_label(),
            "on_revoke": self._on_partitions_revoked,
            "on_assign": self._on_partitions_assigned,
        }
        if max_poll_interval_ms is not None:
            create_kwargs["max_poll_interval_ms"] = int(max_poll_interval_ms)
        if session_timeout_ms is not None:
            create_kwargs["session_timeout_ms"] = int(session_timeout_ms)

        self.consumer = create_safe_consumer(**create_kwargs)
        self.consumer_ops = ExecutorKafkaConsumerOps(
            self.consumer,
            thread_name_prefix=str(thread_name_prefix or "worker-kafka"),
        )
        self._rebalance_in_progress = False
        if reset_partition_state:
            self._init_partition_state(reset=True)

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
        except (AttributeError, TypeError, RuntimeError):
            return False
        if not error:
            return False
        try:
            return error.code() == KafkaError._PARTITION_EOF
        except (AttributeError, TypeError, RuntimeError):
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
        try:
            await self._seek(
                topic=str(msg.topic()),
                partition=int(msg.partition()),
                offset=int(msg.offset()),
            )
        except Exception as exc:
            logger.warning("Failed to seek after unexpected message error: %s", exc, exc_info=True)

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
        primary_exc: Optional[BaseException] = None
        try:
            await self.handle_message(msg)
        except BaseException as exc:
            primary_exc = exc
            raise
        finally:
            committed = True
            if self._uses_commit_state():
                committed = bool(self._commit_state_by_partition.pop(key, False))
            current = self._inflight_by_partition.get(key)
            if current is asyncio.current_task():
                self._inflight_by_partition.pop(key, None)
            should_resume_partition = False
            next_msg: Optional[Any] = None

            if not getattr(self, "consumer", None):
                should_resume_partition = False
            elif key in self._revoked_partitions:
                self._pending_by_partition.pop(key, None)
            elif self._buffer_messages():
                if not committed:
                    self._pending_by_partition.pop(key, None)
                    should_resume_partition = True
                else:
                    pending = self._pending_by_partition.get(key)
                    if pending and len(pending) > 0:
                        next_msg = pending.popleft()
                        if len(pending) == 0:
                            self._pending_by_partition.pop(key, None)
                    else:
                        should_resume_partition = True
            else:
                should_resume_partition = True

            try:
                if next_msg is not None:
                    await self._start_partition_task(next_msg)
                elif should_resume_partition:
                    await self._resume_partition(topic=key[0], partition=key[1])
            except Exception as cleanup_exc:
                if primary_exc is not None:
                    logger.warning(
                        "%s partition cleanup failed after primary error (topic=%s partition=%s): %s",
                        self._loop_label(),
                        key[0],
                        key[1],
                        cleanup_exc,
                        exc_info=True,
                    )
                else:
                    raise

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
            logger.warning("Kafka poll_message failed: %s", exc, exc_info=True)
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

    @trace_kafka_operation("kafka.run_loop")
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
                logger.warning("Kafka handle_message failed: %s", exc, exc_info=True)
                await self._on_unexpected_message_error(exc, msg)
                if unexpected_error_sleep:
                    await asyncio.sleep(unexpected_error_sleep)
                if seek_on_error:
                    await self._seek_on_unexpected_error(msg)
                    if post_seek_sleep:
                        await asyncio.sleep(post_seek_sleep)

    @trace_kafka_operation("kafka.run_partitioned_loop")
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
        except (AttributeError, TypeError, RuntimeError):
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
            logger.warning("Failed to parse Kafka payload, routing parse error handling: %s", exc, exc_info=True)
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
                except Exception as heartbeat_exc:
                    logger.warning(
                        "Heartbeat task join failed after cancellation (event_id=%s): %s",
                        registry_key.event_id,
                        heartbeat_exc,
                        exc_info=True,
                    )


class StrictHeartbeatPolicyMixin:
    """
    Shared strict heartbeat policy for ProcessedEventRegistry-backed workers.

    When heartbeat updates fail, workers fail closed so lease ownership does not
    silently continue without durable idempotency guarantees.
    """

    def _heartbeat_options(self) -> HeartbeatOptions:
        return HeartbeatOptions(
            stop_when_false=True,
            continue_on_exception=False,
        )


class StrictHeartbeatKafkaWorker(
    StrictHeartbeatPolicyMixin,
    ProcessedEventKafkaWorker[PayloadT, ResultT],
    ABC,
):
    """ProcessedEventKafkaWorker with strict heartbeat behavior."""


CommandPayloadT = TypeVar("CommandPayloadT", bound=CommandEnvelopePayload)


class CommandEnvelopeKafkaWorker(ProcessedEventKafkaWorker[CommandPayloadT, ResultT], ABC):
    """
    Specialization of ProcessedEventKafkaWorker for command envelopes.

    Many workers consume the same transport contract:
    Kafka bytes -> UTF-8 JSON -> EventEnvelope(kind=command) -> command dict.
    This class keeps that transport boilerplate in one place so concrete workers
    only own their domain behavior.
    """

    expected_envelope_kind: Optional[str] = "command"
    command_payload_stage: str = "process_command"
    command_payload_cls: type[CommandEnvelopePayload] = CommandEnvelopePayload
    command_parse_error_cls: type[CommandParseError] = CommandParseError

    def _command_dlq_extra(self, payload: CommandPayloadT) -> Optional[Dict[str, Any]]:
        _ = payload
        return None

    def _raise_command_parse_error(
        self,
        *,
        stage: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        fallback_metadata: Optional[Dict[str, Any]],
        cause: Exception,
    ) -> None:
        raise self.command_parse_error_cls(
            stage=stage,
            payload_text=payload_text,
            payload_obj=payload_obj,
            fallback_metadata=fallback_metadata,
            cause=cause,
        )

    def _command_from_envelope(self, envelope: EventEnvelope) -> Dict[str, Any]:
        command = dict(envelope.data or {})
        if command.get("aggregate_id") and command.get("aggregate_id") != envelope.aggregate_id:
            raise ValueError(
                f"aggregate_id mismatch: command.aggregate_id={command.get('aggregate_id')} "
                f"envelope.aggregate_id={envelope.aggregate_id}"
            )
        command["aggregate_id"] = envelope.aggregate_id
        command.setdefault("event_id", envelope.event_id)
        command.setdefault("sequence_number", envelope.sequence_number)
        return command

    def _build_command_payload(
        self,
        *,
        envelope: EventEnvelope,
        command: Dict[str, Any],
        raw_text: str,
        fallback_metadata: Optional[Dict[str, Any]],
    ) -> CommandPayloadT:
        payload_cls = getattr(self, "command_payload_cls", CommandEnvelopePayload)
        stage = str(getattr(self, "command_payload_stage", "process_command") or "process_command")
        return cast(
            CommandPayloadT,
            payload_cls(
                command=command,
                envelope_metadata=fallback_metadata,
                envelope=envelope,
                raw_text=raw_text,
                stage=stage,
            ),
        )

    def _parse_payload(self, payload: Any) -> CommandPayloadT:  # type: ignore[override]
        if not isinstance(payload, (bytes, bytearray)):
            self._raise_command_parse_error(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=TypeError("Kafka payload must be bytes"),
            )

        try:
            raw_text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            self._raise_command_parse_error(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=exc,
            )

        raw_message: Any
        try:
            raw_message = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            self._raise_command_parse_error(
                stage="parse_json",
                payload_text=raw_text,
                payload_obj=None,
                fallback_metadata=None,
                cause=exc,
            )

        fallback_metadata_obj = raw_message.get("metadata") if isinstance(raw_message, dict) else None
        fallback_metadata = dict(fallback_metadata_obj) if isinstance(fallback_metadata_obj, dict) else None

        try:
            envelope = EventEnvelope.model_validate(raw_message if isinstance(raw_message, dict) else {})
        except Exception as exc:
            payload_obj = raw_message if isinstance(raw_message, dict) else None
            self._raise_command_parse_error(
                stage="parse_envelope",
                payload_text=raw_text,
                payload_obj=payload_obj,
                fallback_metadata=fallback_metadata,
                cause=exc,
            )

        expected_kind = str(getattr(self, "expected_envelope_kind", "") or "").strip()
        metadata = envelope.metadata if isinstance(envelope.metadata, dict) else fallback_metadata
        metadata_final = dict(metadata) if isinstance(metadata, dict) else None
        if expected_kind:
            kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
            if kind != expected_kind:
                self._raise_command_parse_error(
                    stage="validate_envelope",
                    payload_text=raw_text,
                    payload_obj=envelope.model_dump(mode="json"),
                    fallback_metadata=metadata_final,
                    cause=ValueError(f"unexpected_envelope_kind:{kind}"),
                )

        try:
            command = self._command_from_envelope(envelope)
        except Exception as exc:
            self._raise_command_parse_error(
                stage="validate_envelope",
                payload_text=raw_text,
                payload_obj=envelope.model_dump(mode="json"),
                fallback_metadata=metadata_final,
                cause=exc,
            )

        return self._build_command_payload(
            envelope=envelope,
            command=command,
            raw_text=raw_text,
            fallback_metadata=metadata_final,
        )

    def _fallback_metadata(self, payload: CommandPayloadT) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        metadata = getattr(payload, "envelope_metadata", None)
        return metadata if isinstance(metadata, dict) else None

    def _registry_key(self, payload: CommandPayloadT) -> RegistryKey:  # type: ignore[override]
        command = payload.command if isinstance(payload.command, dict) else {}

        registry_event_id = command.get("event_id") or command.get("command_id")
        registry_event_id = str(registry_event_id or "").strip()
        if not registry_event_id:
            raise ValueError("event_id is required")

        aggregate_id = command.get("aggregate_id")
        aggregate_id = str(aggregate_id).strip() if aggregate_id is not None else None
        if not aggregate_id:
            aggregate_id = None

        sequence_number = command.get("sequence_number")
        try:
            seq_int = int(sequence_number) if sequence_number is not None else None
        except (TypeError, ValueError):
            seq_int = None

        return RegistryKey(event_id=registry_event_id, aggregate_id=aggregate_id, sequence_number=seq_int)

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: Optional[CommandPayloadT] = None,
        raw_payload: Optional[str] = None,
        error: str,
        attempt_count: int,
        stage: Optional[str] = None,
        payload_text: Optional[str] = None,
        payload_obj: Optional[Dict[str, Any]] = None,
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        publisher = getattr(self, "_publish_to_dlq", None)
        if not callable(publisher):
            raise RuntimeError("DLQ publisher is not configured for command payload")
        default_stage = str(getattr(self, "command_payload_stage", "process_command") or "process_command")
        await self._send_command_payload_dlq_record(
            msg=msg,
            error=error,
            attempt_count=int(attempt_count),
            payload=payload,
            raw_payload=raw_payload,
            stage=str(stage or default_stage),
            default_stage=default_stage,
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            publisher=publisher,
            extra=self._command_dlq_extra(payload) if payload is not None else None,
        )


class StrictCommandEnvelopeKafkaWorker(
    StrictHeartbeatPolicyMixin,
    CommandEnvelopeKafkaWorker[CommandPayloadT, ResultT],
    ABC,
):
    """CommandEnvelopeKafkaWorker with strict heartbeat behavior."""


ModelPayloadT = TypeVar("ModelPayloadT", bound=BaseModel)


class JsonModelKafkaWorker(ProcessedEventKafkaWorker[ModelPayloadT, ResultT], ABC):
    """
    Specialization of ProcessedEventKafkaWorker for plain JSON model payloads.

    This is useful for workers like pipeline/objectify that do not consume
    EventEnvelope, but still share the same transport contract:
    Kafka bytes -> UTF-8 JSON -> validated Pydantic model.
    """

    json_model_cls: type[BaseModel] = BaseModel
    json_model_parse_error_cls: type[CommandParseError] = CommandParseError
    registry_event_id_field: str = "job_id"
    registry_aggregate_id_field: Optional[str] = None
    registry_sequence_field: Optional[str] = None
    json_model_metadata_fields: tuple[str, ...] = ()
    json_model_span_name: Optional[str] = None
    json_model_metric_event_name: Optional[str] = None
    json_model_span_attribute_fields: tuple[tuple[str, str], ...] = ()
    json_model_dlq_default_stage: str = "process_message"

    def _json_model_dlq_extra(self, payload: ModelPayloadT) -> Optional[Dict[str, Any]]:
        _ = payload
        return None

    def _raise_json_model_parse_error(
        self,
        *,
        stage: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        cause: Exception,
    ) -> None:
        raise self.json_model_parse_error_cls(
            stage=stage,
            payload_text=payload_text,
            payload_obj=payload_obj,
            fallback_metadata=None,
            cause=cause,
        )

    def _parse_payload(self, payload: Any) -> ModelPayloadT:  # type: ignore[override]
        if not isinstance(payload, (bytes, bytearray)):
            self._raise_json_model_parse_error(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                cause=TypeError("Kafka payload must be bytes"),
            )

        try:
            raw_text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            self._raise_json_model_parse_error(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                cause=exc,
            )

        raw_obj: Any
        try:
            raw_obj = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            self._raise_json_model_parse_error(
                stage="json",
                payload_text=raw_text,
                payload_obj=None,
                cause=exc,
            )

        try:
            return cast(ModelPayloadT, self.json_model_cls.model_validate(raw_obj))
        except Exception as exc:
            self._raise_json_model_parse_error(
                stage="validate",
                payload_text=raw_text,
                payload_obj=raw_obj if isinstance(raw_obj, dict) else None,
                cause=exc,
            )

    def _registry_key(self, payload: ModelPayloadT) -> RegistryKey:  # type: ignore[override]
        event_id_value = getattr(payload, str(self.registry_event_id_field or "").strip() or "job_id", None)
        event_id = str(event_id_value or "").strip()
        if not event_id:
            raise ValueError("event_id is required")

        aggregate_field = str(getattr(self, "registry_aggregate_id_field", None) or "").strip()
        aggregate_value = getattr(payload, aggregate_field, None) if aggregate_field else None
        aggregate_id = str(aggregate_value).strip() if aggregate_value is not None else None
        if not aggregate_id:
            aggregate_id = None

        sequence_field = str(getattr(self, "registry_sequence_field", None) or "").strip()
        sequence_value = getattr(payload, sequence_field, None) if sequence_field else None
        try:
            sequence_number = int(sequence_value) if sequence_value is not None else None
        except (TypeError, ValueError):
            sequence_number = None

        return RegistryKey(
            event_id=event_id,
            aggregate_id=aggregate_id,
            sequence_number=sequence_number,
        )

    def _fallback_metadata(self, payload: ModelPayloadT) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        fields = tuple(getattr(self, "json_model_metadata_fields", ()) or ())
        if not fields:
            return None
        metadata: Dict[str, Any] = {}
        for field_name in fields:
            key = str(field_name or "").strip()
            if not key:
                continue
            metadata[key] = getattr(payload, key, None)
        return metadata

    def _span_name(self, *, payload: ModelPayloadT) -> str:
        _ = payload
        configured = str(getattr(self, "json_model_span_name", "") or "").strip()
        if configured:
            return configured
        name = self._service_name() or self.__class__.__name__
        return f"{name}.process_message"

    def _span_attributes(self, *, msg: Any, payload: ModelPayloadT, registry_key: RegistryKey) -> Dict[str, Any]:
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        mappings = tuple(getattr(self, "json_model_span_attribute_fields", ()) or ())
        for payload_field, attr_name in mappings:
            payload_key = str(payload_field or "").strip()
            span_key = str(attr_name or "").strip()
            if not payload_key or not span_key:
                continue
            attrs[span_key] = getattr(payload, payload_key, None)
        return attrs

    def _metric_event_name(self, *, payload: ModelPayloadT) -> Optional[str]:
        _ = payload
        configured = str(getattr(self, "json_model_metric_event_name", "") or "").strip()
        return configured or None

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: Optional[ModelPayloadT] = None,
        raw_payload: Optional[str] = None,
        error: str,
        attempt_count: int,
        stage: Optional[str] = None,
        payload_text: Optional[str] = None,
        payload_obj: Optional[Dict[str, Any]] = None,
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        publisher = getattr(self, "_publish_to_dlq", None)
        if not callable(publisher):
            raise RuntimeError("DLQ publisher is not configured for JSON model payload")
        default_stage = str(getattr(self, "json_model_dlq_default_stage", "process_message") or "process_message")
        await self._send_standard_dlq_record(
            msg=msg,
            error=error,
            attempt_count=int(attempt_count),
            stage=str(stage or default_stage),
            default_stage=default_stage,
            raw_payload=raw_payload,
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            publisher=publisher,
            inferred_metadata=self._fallback_metadata(payload) if payload is not None else None,
            extra=self._json_model_dlq_extra(payload) if payload is not None else None,
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

    def _envelope_dlq_spec(self) -> EnvelopeDlqSpec:
        spec = getattr(self, "_dlq_spec", None)
        if not isinstance(spec, EnvelopeDlqSpec):
            raise RuntimeError("Envelope DLQ spec is not configured for envelope payload")
        return spec

    def _envelope_dlq_producer_ops(self) -> Optional[Any]:
        return getattr(self, "dlq_producer_ops", None)

    def _envelope_dlq_key_fallback(self, payload: EventEnvelope) -> Optional[str]:
        _ = payload
        return None

    def _envelope_dlq_missing_producer_message(self) -> str:
        return "DLQ producer not configured; dropping DLQ payload: %s"

    async def _publish_envelope_failure_to_dlq(
        self,
        *,
        msg: Any,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        producer_ops: Optional[Any],
        spec: EnvelopeDlqSpec,
        key: Optional[bytes] = None,
        missing_producer_message: str = "DLQ producer not configured; dropping DLQ payload: %s",
    ) -> None:
        if not producer_ops:
            logger.error(missing_producer_message, error)
            return

        dlq_env = build_envelope_dlq_event(
            envelope=payload,
            spec=spec,
            error=error,
            attempt_count=attempt_count,
        )
        key_bytes = key
        if key_bytes is None:
            key_text = str(dlq_env.aggregate_id or dlq_env.event_id or self._loop_label())
            key_bytes = key_text.encode("utf-8")

        await publish_envelope_dlq(
            producer_ops=producer_ops,
            spec=spec,
            envelope=dlq_env,
            tracing=getattr(self, "tracing", None),
            metrics=getattr(self, "metrics", None),
            key=key_bytes,
        )

    async def _send_envelope_failure_to_dlq(
        self,
        *,
        msg: Any,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        producer_ops: Optional[Any],
        spec: EnvelopeDlqSpec,
        key_fallback: Optional[str] = None,
        missing_producer_message: str = "DLQ producer not configured; dropping DLQ payload: %s",
    ) -> None:
        key_text = str(payload.aggregate_id or payload.event_id or key_fallback or self._loop_label())
        key = key_text.encode("utf-8")
        await self._publish_envelope_failure_to_dlq(
            msg=msg,
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            producer_ops=producer_ops,
            spec=spec,
            key=key,
            missing_producer_message=missing_producer_message,
        )

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: EventEnvelope,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
        **_kwargs: Any,
    ) -> None:
        _ = raw_payload
        await self._send_envelope_failure_to_dlq(
            msg=msg,
            payload=payload,
            error=error,
            attempt_count=int(attempt_count),
            producer_ops=self._envelope_dlq_producer_ops(),
            spec=self._envelope_dlq_spec(),
            key_fallback=self._envelope_dlq_key_fallback(payload),
            missing_producer_message=self._envelope_dlq_missing_producer_message(),
        )


class StrictHeartbeatEventEnvelopeKafkaWorker(
    StrictHeartbeatPolicyMixin,
    EventEnvelopeKafkaWorker[ResultT],
    ABC,
):
    """EventEnvelopeKafkaWorker with strict heartbeat behavior."""
