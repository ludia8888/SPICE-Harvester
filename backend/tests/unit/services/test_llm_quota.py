from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.services.llm_quota import LLMQuotaExceededError, enforce_llm_quota


class _Client:
    def __init__(self, responses):  # noqa: ANN001
        self._responses = list(responses)
        self.calls: list[tuple] = []

    async def eval(self, *args):  # noqa: ANN001
        self.calls.append(args)
        if not self._responses:
            return [1, 1, 1]
        return self._responses.pop(0)


class _Redis:
    def __init__(self, responses):  # noqa: ANN001
        self.client = _Client(responses)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_llm_quota_is_noop_without_policy() -> None:
    await enforce_llm_quota(
        redis_service=None,
        tenant_id="t",
        user_id="u",
        model_id="m",
        system_prompt="sys",
        user_prompt="user",
        data_policies={},
        now=datetime.now(timezone.utc),
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_llm_quota_raises_when_denied() -> None:
    redis_service = _Redis(responses=[[0, 1, 100]])
    with pytest.raises(LLMQuotaExceededError):
        await enforce_llm_quota(
            redis_service=redis_service,  # type: ignore[arg-type]
            tenant_id="tenant-1",
            user_id="user-1",
            model_id="gpt-test",
            system_prompt="sys",
            user_prompt="hello",
            data_policies={"llm": {"quota": {"window_seconds": 60, "max_calls": 1, "max_total_tokens": 10}}},
            now=datetime.now(timezone.utc),
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_llm_quota_consumes_for_tenant_and_user() -> None:
    redis_service = _Redis(responses=[[1, 1, 3], [1, 1, 3]])
    await enforce_llm_quota(
        redis_service=redis_service,  # type: ignore[arg-type]
        tenant_id="tenant-1",
        user_id="user-1",
        model_id="gpt-test",
        system_prompt="sys",
        user_prompt="hello",
        data_policies={"llm": {"quota": {"window_seconds": 60, "max_calls": 10, "max_total_tokens": 1000}}},
        now=datetime.now(timezone.utc),
    )
    assert len(redis_service.client.calls) == 2

