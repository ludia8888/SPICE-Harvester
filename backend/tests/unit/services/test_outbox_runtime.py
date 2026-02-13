from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from shared.services.events.outbox_runtime import (
    build_outbox_worker_id,
    flush_outbox_until_empty,
    maybe_purge_with_interval,
    run_outbox_poll_loop,
)


def test_build_outbox_worker_id_prefers_configured_value() -> None:
    assert (
        build_outbox_worker_id(
            configured_worker_id="  worker-1  ",
            service_name="ignored",
            hostname="ignored",
            default_service_name="fallback",
        )
        == "worker-1"
    )


def test_build_outbox_worker_id_uses_service_and_hostname() -> None:
    worker_id = build_outbox_worker_id(
        configured_worker_id=None,
        service_name="svc-a",
        hostname="host-a",
        default_service_name="fallback",
    )
    assert worker_id.startswith("svc-a:host-a:")


@pytest.mark.asyncio
async def test_maybe_purge_with_interval_executes_and_logs() -> None:
    calls: list[tuple[int, int]] = []
    infos: list[tuple[str, int]] = []
    warnings: list[tuple[str, str]] = []

    async def _purge_call(*, retention_days: int, limit: int) -> int:
        calls.append((retention_days, limit))
        return 2

    updated = await maybe_purge_with_interval(
        retention_days=7,
        purge_interval_seconds=10,
        purge_limit=99,
        last_purge=datetime.now(timezone.utc) - timedelta(seconds=20),
        purge_call=_purge_call,
        info_logger=lambda msg, value: infos.append((msg, value)),
        warning_logger=lambda msg, exc: warnings.append((msg, str(exc))),
        success_message="deleted %s rows",
        failure_message="failed: %s",
    )

    assert calls == [(7, 99)]
    assert infos == [("deleted %s rows", 2)]
    assert warnings == []
    assert updated.tzinfo is not None


@pytest.mark.asyncio
async def test_run_outbox_poll_loop_runs_flush_purge_and_close() -> None:
    stop_event = asyncio.Event()
    warnings: list[tuple[str, str]] = []

    class _Publisher:
        def __init__(self) -> None:
            self.flush_count = 0
            self.purge_count = 0
            self.closed = False

        async def flush_once(self) -> None:
            self.flush_count += 1
            stop_event.set()

        async def maybe_purge(self) -> None:
            self.purge_count += 1

        async def close(self) -> None:
            self.closed = True

    publisher = _Publisher()
    await run_outbox_poll_loop(
        publisher=publisher,
        poll_interval_seconds=1,
        stop_event=stop_event,
        warning_logger=lambda msg, exc: warnings.append((msg, str(exc))),
        failure_message="loop failed: %s",
    )

    assert publisher.flush_count == 1
    assert publisher.purge_count == 1
    assert publisher.closed is True
    assert warnings == []


@pytest.mark.asyncio
async def test_flush_outbox_until_empty_stops_and_closes() -> None:
    class _Publisher:
        def __init__(self) -> None:
            self.values = [2, 1, 0]
            self.closed = False

        async def flush_once(self) -> int:
            return self.values.pop(0)

        async def close(self) -> None:
            self.closed = True

    publisher = _Publisher()
    await flush_outbox_until_empty(
        publisher=publisher,
        is_empty=lambda processed: processed == 0,
    )

    assert publisher.values == []
    assert publisher.closed is True


@pytest.mark.asyncio
async def test_flush_outbox_until_empty_propagates_primary_error_without_close() -> None:
    class _Publisher:
        async def flush_once(self) -> int:
            raise RuntimeError("flush-failed")

    with pytest.raises(RuntimeError, match="flush-failed"):
        await flush_outbox_until_empty(
            publisher=_Publisher(),
            is_empty=lambda processed: processed == 0,
        )


@pytest.mark.asyncio
async def test_flush_outbox_until_empty_keeps_primary_error_when_close_also_fails(caplog: pytest.LogCaptureFixture) -> None:
    class _Publisher:
        async def flush_once(self) -> int:
            raise RuntimeError("flush-failed")

        async def close(self) -> None:
            raise RuntimeError("close-failed")

    with caplog.at_level(logging.WARNING, logger="shared.services.events.outbox_runtime"):
        with pytest.raises(RuntimeError, match="flush-failed"):
            await flush_outbox_until_empty(
                publisher=_Publisher(),
                is_empty=lambda processed: processed == 0,
            )

    assert any("close failed after primary error" in rec.message.lower() for rec in caplog.records)


@pytest.mark.asyncio
async def test_flush_outbox_until_empty_raises_close_error_when_no_primary_error() -> None:
    class _Publisher:
        async def flush_once(self) -> int:
            return 0

        async def close(self) -> None:
            raise RuntimeError("close-failed")

    with pytest.raises(RuntimeError, match="close-failed"):
        await flush_outbox_until_empty(
            publisher=_Publisher(),
            is_empty=lambda processed: processed == 0,
        )


@pytest.mark.asyncio
async def test_run_outbox_poll_loop_raises_close_error_when_no_primary_error() -> None:
    stop_event = asyncio.Event()

    class _Publisher:
        async def flush_once(self) -> int:
            stop_event.set()
            return 1

        async def maybe_purge(self) -> None:
            return None

        async def close(self) -> None:
            raise RuntimeError("close-failed")

    with pytest.raises(RuntimeError, match="close-failed"):
        await run_outbox_poll_loop(
            publisher=_Publisher(),
            poll_interval_seconds=1,
            stop_event=stop_event,
            warning_logger=lambda _msg, _exc: None,
            failure_message="loop failed: %s",
        )
