from __future__ import annotations

import asyncio
import json

import pytest
from unittest.mock import AsyncMock

from shared.services.agent.agent_retention_worker import run_agent_session_retention_worker


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_retention_worker_calls_apply_retention_once() -> None:
    stop_event = asyncio.Event()

    session_registry = AsyncMock()
    session_registry.list_expired_file_uploads = AsyncMock(return_value=[{"bucket": "b", "key": "k"}])
    storage_service = AsyncMock()
    storage_service.delete_object = AsyncMock(return_value=True)

    async def _apply_retention(**_kwargs):  # noqa: ANN001
        stop_event.set()

    session_registry.apply_retention = AsyncMock(side_effect=_apply_retention)

    task = asyncio.create_task(
        run_agent_session_retention_worker(
            session_registry=session_registry,
            poll_interval_seconds=3600,
            retention_days=1,
            stop_event=stop_event,
            action="redact",
            storage_service=storage_service,
        )
    )

    await asyncio.wait_for(stop_event.wait(), timeout=1.0)
    await asyncio.wait_for(task, timeout=1.0)
    session_registry.apply_retention.assert_awaited()
    storage_service.delete_object.assert_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_retention_worker_supports_policy_per_object_type() -> None:
    stop_event = asyncio.Event()

    session_registry = AsyncMock()
    session_registry.list_expired_file_uploads = AsyncMock(return_value=[{"bucket": "b", "key": "k"}])
    storage_service = AsyncMock()
    storage_service.delete_object = AsyncMock(return_value=True)

    calls: list[dict] = []

    async def _apply_retention(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        if len(calls) >= 2:
            stop_event.set()
        return {}

    session_registry.apply_retention = AsyncMock(side_effect=_apply_retention)

    policy_json = json.dumps(
        {
            "messages": {"days": 1, "action": "redact"},
            "context_items:file_upload": {"days": 2, "action": "delete"},
        }
    )

    task = asyncio.create_task(
        run_agent_session_retention_worker(
            session_registry=session_registry,
            poll_interval_seconds=3600,
            retention_days=0,
            stop_event=stop_event,
            action="redact",
            storage_service=storage_service,
            retention_policy_json=policy_json,
        )
    )

    await asyncio.wait_for(stop_event.wait(), timeout=1.0)
    await asyncio.wait_for(task, timeout=1.0)

    assert any(bool(call.get("include_messages")) for call in calls)
    assert any(call.get("context_item_type") == "file_upload" for call in calls)
    storage_service.delete_object.assert_awaited()
