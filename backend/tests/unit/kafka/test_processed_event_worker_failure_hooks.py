from __future__ import annotations

import logging
from typing import Any

import pytest

from shared.services.kafka.processed_event_worker import (
    CommandParseError,
    FailureLogContext,
    ProcessedEventKafkaWorker,
    RegistryKey,
)


class _FailureHookWorker(ProcessedEventKafkaWorker[dict[str, Any], None]):
    def __init__(self) -> None:
        self.consumer = None
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub_handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10
        self.hook_calls: list[tuple[str, str]] = []

    def _parse_payload(self, payload: Any) -> dict[str, Any]:  # type: ignore[override]
        return dict(payload or {})

    def _registry_key(self, payload: dict[str, Any]) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(event_id=str(payload.get("event_id") or "evt"))

    async def _process_payload(self, payload: dict[str, Any]) -> None:  # type: ignore[override]
        return None

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: dict[str, Any],
        raw_payload: str | None,
        error: str,
        attempt_count: int,
    ) -> None:
        return None

    async def _after_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: dict[str, Any],
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        _ = payload, attempt_count, backoff_s, retryable
        self.hook_calls.append(("retry", error))

    async def _after_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: dict[str, Any],
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        _ = payload, attempt_count, retryable
        self.hook_calls.append(("terminal", error))

    def _retry_log_context(  # type: ignore[override]
        self,
        *,
        payload: dict[str, Any],
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> FailureLogContext:
        _ = payload, retryable
        return FailureLogContext(
            message="retry hook fired (attempt=%s backoff=%s error=%s)",
            args=(attempt_count, backoff_s, error),
        )

    def _terminal_failure_log_context(  # type: ignore[override]
        self,
        *,
        payload: dict[str, Any],
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> FailureLogContext:
        _ = payload, retryable
        return FailureLogContext(
            message="terminal hook fired (attempt=%s error=%s)",
            args=(attempt_count, error),
        )


class _MsgWithHeaders:
    def headers(self):
        return [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]


class _ParseErrorHookWorker(_FailureHookWorker):
    parse_error_enable_dlq = True
    parse_error_raise_on_publish_failure = False

    def __init__(self) -> None:
        super().__init__()
        self.parse_calls: list[tuple[str, str, Any]] = []
        self.fail_send = False

    async def _before_parse_error_handling(  # type: ignore[override]
        self,
        *,
        msg: Any,
        raw_payload: str | None,
        error: Exception,
        context,
    ) -> None:
        _ = msg, raw_payload, error
        self.parse_calls.append(("before", context.stage, context.payload_obj))

    async def _send_parse_error_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        raw_payload: str | None,
        context,
        kafka_headers: Any,
        fallback_metadata: dict[str, Any] | None,
    ) -> None:
        _ = msg, raw_payload
        if self.fail_send:
            raise RuntimeError("dlq failed")
        self.parse_calls.append(("send", context.stage, fallback_metadata))
        assert kafka_headers == [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]

    async def _after_parse_error_dlq_success(  # type: ignore[override]
        self,
        *,
        msg: Any,
        raw_payload: str | None,
        error: Exception,
        context,
    ) -> None:
        _ = msg, raw_payload, error
        self.parse_calls.append(("success", context.stage, context.payload_obj))

    async def _after_parse_error_dlq_failure(  # type: ignore[override]
        self,
        *,
        msg: Any,
        raw_payload: str | None,
        error: Exception,
        context,
        dlq_error: Exception,
    ) -> None:
        _ = msg, raw_payload, error
        self.parse_calls.append(("failure", context.stage, str(dlq_error)))


@pytest.mark.asyncio
async def test_processed_event_worker_retry_hook_runs_side_effects_then_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = _FailureHookWorker()

    with caplog.at_level(logging.WARNING, logger="shared.services.kafka.processed_event_worker"):
        await worker._on_retry_scheduled(
            payload={"event_id": "evt-1"},
            error="boom",
            attempt_count=2,
            backoff_s=5,
            retryable=True,
        )

    assert worker.hook_calls == [("retry", "boom")]
    assert any("retry hook fired" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_processed_event_worker_terminal_hook_runs_side_effects_then_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = _FailureHookWorker()

    with caplog.at_level(logging.ERROR, logger="shared.services.kafka.processed_event_worker"):
        await worker._on_terminal_failure(
            payload={"event_id": "evt-1"},
            error="boom",
            attempt_count=3,
            retryable=False,
        )

    assert worker.hook_calls == [("terminal", "boom")]
    assert any("terminal hook fired" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_processed_event_worker_parse_error_hooks_wrap_send_flow() -> None:
    worker = _ParseErrorHookWorker()
    error = CommandParseError(
        stage="validate",
        payload_text='{"job_id":"job-1"}',
        payload_obj={"job_id": "job-1"},
        fallback_metadata=None,
        cause=ValueError("bad payload"),
    )

    await worker._on_parse_error(
        msg=_MsgWithHeaders(),
        raw_payload='{"metadata":{"kind":"domain"},"job_id":"job-1"}',
        error=error,
    )

    assert worker.parse_calls == [
        ("before", "validate", {"job_id": "job-1"}),
        ("send", "validate", {"kind": "domain"}),
        ("success", "validate", {"job_id": "job-1"}),
    ]


@pytest.mark.asyncio
async def test_processed_event_worker_parse_error_failure_hook_runs_before_swallowing_dlq_error() -> None:
    worker = _ParseErrorHookWorker()
    worker.fail_send = True
    error = CommandParseError(
        stage="validate",
        payload_text='{"job_id":"job-1"}',
        payload_obj={"job_id": "job-1"},
        fallback_metadata=None,
        cause=ValueError("bad payload"),
    )

    await worker._on_parse_error(
        msg=_MsgWithHeaders(),
        raw_payload='{"metadata":{"kind":"domain"},"job_id":"job-1"}',
        error=error,
    )

    assert worker.parse_calls == [
        ("before", "validate", {"job_id": "job-1"}),
        ("failure", "validate", "dlq failed"),
    ]
