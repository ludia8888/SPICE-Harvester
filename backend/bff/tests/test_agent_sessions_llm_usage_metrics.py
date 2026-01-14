from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.routers.agent_sessions import get_llm_usage_metrics
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import AgentSessionLLMUsageAggregateRecord


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


def _principal(*, user_id: str = "user-1", tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, verified=True)


class _FakeSessions:
    def __init__(self, *, rows: list[AgentSessionLLMUsageAggregateRecord]) -> None:
        self._rows = list(rows)
        self.calls: list[dict] = []

    async def aggregate_llm_usage(self, **kwargs):  # noqa: ANN003
        self.calls.append(dict(kwargs))
        return list(self._rows)


@pytest.mark.asyncio
async def test_llm_usage_metrics_returns_totals_and_rows() -> None:
    now = datetime.now(timezone.utc)
    rows = [
        AgentSessionLLMUsageAggregateRecord(
            tenant_id="tenant-1",
            user_id=None,
            model_id="gpt-4.1-mini",
            calls=2,
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
            cost_estimate=0.123,
            first_at=now,
            last_at=now,
        )
    ]
    sessions = _FakeSessions(rows=rows)
    req = _Request(principal=_principal())

    resp = await get_llm_usage_metrics(
        request=req,
        sessions=sessions,
        scope="tenant",
        group_by="model",
        start_time=None,
        end_time=None,
        limit=200,
        offset=0,
    )
    assert resp.status == "success"
    assert resp.data["totals"]["calls"] == 2
    assert resp.data["totals"]["total_tokens"] == 15
    assert resp.data["rows"][0]["model_id"] == "gpt-4.1-mini"
    assert sessions.calls
    assert sessions.calls[0]["tenant_id"] == "tenant-1"


@pytest.mark.asyncio
async def test_llm_usage_metrics_rejects_invalid_scope() -> None:
    now = datetime.now(timezone.utc)
    sessions = _FakeSessions(
        rows=[
            AgentSessionLLMUsageAggregateRecord(
                tenant_id="tenant-1",
                user_id=None,
                model_id=None,
                calls=0,
                prompt_tokens=0,
                completion_tokens=0,
                total_tokens=0,
                cost_estimate=0.0,
                first_at=now,
                last_at=now,
            )
        ]
    )
    req = _Request(principal=_principal())

    with pytest.raises(HTTPException) as exc:
        await get_llm_usage_metrics(
            request=req,
            sessions=sessions,
            scope="wat",
            group_by="model",
            start_time=None,
            end_time=None,
            limit=200,
            offset=0,
        )
    assert exc.value.status_code == 400

