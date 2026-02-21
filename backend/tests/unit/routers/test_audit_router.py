from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi import HTTPException

from bff.routers.audit import list_audit_logs


class _RecordingAuditStore:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] | None = None

    async def list_logs(self, **kwargs):  # noqa: ANN003
        self.last_kwargs = kwargs
        return []


class _ValueErrorAuditStore:
    async def list_logs(self, **_kwargs):  # noqa: ANN003
        raise ValueError("dictionary update sequence element #0 has length 1; 2 is required")


@pytest.mark.asyncio
async def test_list_audit_logs_normalizes_status_filter() -> None:
    store = _RecordingAuditStore()

    payload = await list_audit_logs(
        partition_key="db:test",
        status_filter="SUCCESS",
        since=None,
        until=None,
        limit=10,
        offset=0,
        audit_store=store,
    )

    assert payload["status"] == "success"
    assert isinstance(store.last_kwargs, dict)
    assert store.last_kwargs["status"] == "success"


@pytest.mark.asyncio
async def test_list_audit_logs_rejects_invalid_status_filter() -> None:
    store = _RecordingAuditStore()

    with pytest.raises(HTTPException) as raised:
        await list_audit_logs(
            status_filter="unknown",
            since=None,
            until=None,
            audit_store=store,
        )

    assert raised.value.status_code == 400
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail["code"] == "REQUEST_VALIDATION_FAILED"


@pytest.mark.asyncio
async def test_list_audit_logs_rejects_invalid_time_window() -> None:
    store = _RecordingAuditStore()

    with pytest.raises(HTTPException) as raised:
        await list_audit_logs(
            since=datetime(2026, 2, 1, tzinfo=UTC),
            until=datetime(2026, 1, 1, tzinfo=UTC),
            audit_store=store,
        )

    assert raised.value.status_code == 400
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail["code"] == "REQUEST_VALIDATION_FAILED"


@pytest.mark.asyncio
async def test_list_audit_logs_treats_store_value_error_as_internal_error() -> None:
    with pytest.raises(HTTPException) as raised:
        await list_audit_logs(
            partition_key="db:test",
            status_filter=None,
            since=None,
            until=None,
            audit_store=_ValueErrorAuditStore(),
        )

    assert raised.value.status_code == 500
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail["code"] == "INTERNAL_ERROR"
