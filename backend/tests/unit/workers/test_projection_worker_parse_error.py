from __future__ import annotations

from unittest.mock import ANY, AsyncMock

import pytest

from projection_worker.main import ProjectionWorker


class _MsgWithHeaders:
    def headers(self):
        return [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]


@pytest.mark.asyncio
async def test_projection_parse_error_uses_shared_flow_and_recovers_metadata() -> None:
    worker = object.__new__(ProjectionWorker)
    worker._publish_to_dlq = AsyncMock()

    await worker._on_parse_error(
        msg=_MsgWithHeaders(),
        raw_payload='{"event_id":"evt-1","metadata":{"kind":"domain","trace_id":"abc"}}',
        error=ValueError("bad envelope"),
    )

    worker._publish_to_dlq.assert_awaited_once_with(
        msg=ANY,
        error="bad envelope",
        attempt_count=1,
        payload_text='{"event_id":"evt-1","metadata":{"kind":"domain","trace_id":"abc"}}',
        kafka_headers=[("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")],
        fallback_metadata={"kind": "domain", "trace_id": "abc"},
    )
