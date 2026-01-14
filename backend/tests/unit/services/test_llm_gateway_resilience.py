from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from shared.services.llm_gateway import LLMGateway, LLMHTTPStatusError, LLMUnavailableError
from shared.utils.llm_safety import sha256_hex, stable_json_dumps


class _Out(BaseModel):
    ok: bool


@pytest.mark.unit
@pytest.mark.asyncio
async def test_llm_gateway_retries_on_http_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "openai_compat")
    monkeypatch.setenv("LLM_BASE_URL", "http://example.invalid")
    monkeypatch.setenv("LLM_MODEL", "gpt-test")

    gateway = LLMGateway()
    gateway.retry_max_attempts = 2
    gateway.retry_base_delay_s = 0.0
    gateway.retry_max_delay_s = 0.0
    gateway.circuit_failure_threshold = 5

    calls: dict[str, int] = {"n": 0}

    async def _stub_call(**_kwargs):  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] == 1:
            raise LLMHTTPStatusError(status_code=500, body_preview="boom")
        return {"ok": True}, {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}

    monkeypatch.setattr(gateway, "_call_openai_compat_json", _stub_call)

    out, meta = await gateway.complete_json(
        task="TEST",
        system_prompt="system",
        user_prompt="user",
        response_model=_Out,
        audit_partition_key=None,
    )
    assert out.ok is True
    assert meta.cache_hit is False
    assert calls["n"] == 2
    assert meta.prompt_tokens == 1
    assert meta.completion_tokens == 2
    assert meta.total_tokens == 3

    circuit = gateway._circuit.get("openai_compat:gpt-test")  # type: ignore[attr-defined]
    assert circuit is not None
    assert float(circuit.get("failures") or 0.0) == 0.0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_llm_gateway_circuit_breaker_opens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "openai_compat")
    monkeypatch.setenv("LLM_BASE_URL", "http://example.invalid")
    monkeypatch.setenv("LLM_MODEL", "gpt-test")

    gateway = LLMGateway()
    gateway.retry_max_attempts = 1
    gateway.circuit_failure_threshold = 2
    gateway.circuit_open_seconds = 60.0

    calls: dict[str, int] = {"n": 0}

    async def _stub_call(**_kwargs):  # noqa: ANN001
        calls["n"] += 1
        raise LLMHTTPStatusError(status_code=503, body_preview="unavailable")

    monkeypatch.setattr(gateway, "_call_openai_compat_json", _stub_call)

    with pytest.raises(LLMHTTPStatusError):
        await gateway.complete_json(
            task="TEST",
            system_prompt="system",
            user_prompt="user",
            response_model=_Out,
            audit_partition_key=None,
        )
    with pytest.raises(LLMHTTPStatusError):
        await gateway.complete_json(
            task="TEST",
            system_prompt="system",
            user_prompt="user",
            response_model=_Out,
            audit_partition_key=None,
        )

    with pytest.raises(LLMUnavailableError):
        await gateway.complete_json(
            task="TEST",
            system_prompt="system",
            user_prompt="user",
            response_model=_Out,
            audit_partition_key=None,
        )
    assert calls["n"] == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_llm_gateway_cache_key_is_partition_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "mock")
    monkeypatch.setenv("LLM_MODEL", "mock")
    monkeypatch.setenv("LLM_MOCK_JSON_TEST", json.dumps({"ok": True}))

    class _StubRedis:
        def __init__(self) -> None:
            self.keys: list[str] = []

        async def get_json(self, _key: str):  # noqa: ANN001
            return None

        async def set_json(self, key: str, _value, ttl: int):  # noqa: ANN001
            self.keys.append(key)
            assert ttl > 0

    redis = _StubRedis()
    gateway = LLMGateway()

    async def _call(partition_key: str) -> None:
        await gateway.complete_json(
            task="TEST",
            system_prompt="system",
            user_prompt="user",
            response_model=_Out,
            redis_service=redis,  # type: ignore[arg-type]
            audit_partition_key=partition_key,
        )

    await _call("agent_session:11111111-1111-1111-1111-111111111111")
    await _call("agent_session:22222222-2222-2222-2222-222222222222")
    assert len(redis.keys) == 2
    assert redis.keys[0] != redis.keys[1]

    prompt_obj = {"system": "system", "user": "user", "task": "TEST", "model": "mock"}
    prompt_hash = sha256_hex(stable_json_dumps(prompt_obj))
    assert prompt_hash in redis.keys[0]

