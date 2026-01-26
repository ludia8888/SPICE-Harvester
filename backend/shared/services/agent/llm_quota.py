from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from shared.services.storage.redis_service import RedisService


@dataclass(frozen=True)
class LLMQuotaSpec:
    window_seconds: int
    max_calls: int
    max_total_tokens: int


class LLMQuotaExceededError(RuntimeError):
    def __init__(self, message: str, *, spec: LLMQuotaSpec, used_calls: int, used_tokens: int) -> None:
        super().__init__(message)
        self.spec = spec
        self.used_calls = int(used_calls)
        self.used_tokens = int(used_tokens)


def approx_token_count(payload: Any) -> int:
    if payload is None:
        return 0
    text = str(payload)
    text = text.strip()
    if not text:
        return 0
    # Rough heuristic: ~4 chars/token for English-ish text; safe for budgeting.
    return max(1, int((len(text) + 3) / 4))


def _sanitize_key_part(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "unknown"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw)[:120] or "unknown"


def _extract_quota_spec(*, data_policies: Any, model_id: str) -> Optional[LLMQuotaSpec]:
    if not isinstance(data_policies, dict):
        return None
    llm_policy = data_policies.get("llm")
    if not isinstance(llm_policy, dict):
        return None

    raw_quota = llm_policy.get("quota") or llm_policy.get("quotas") or llm_policy.get("rate_limit") or llm_policy.get("rate_limits")
    if not isinstance(raw_quota, dict):
        return None

    spec_payload: dict[str, Any]
    if isinstance(raw_quota.get("default"), dict):
        spec_payload = dict(raw_quota.get("default") or {})
    else:
        spec_payload = dict(raw_quota)

    model_overrides = raw_quota.get("models") or raw_quota.get("per_model") or raw_quota.get("model_overrides")
    if isinstance(model_overrides, dict):
        override = model_overrides.get(model_id) or model_overrides.get(str(model_id).lower())
        if isinstance(override, dict):
            spec_payload.update(dict(override))

    window_seconds = int(spec_payload.get("window_seconds") or spec_payload.get("windowSeconds") or 0) or 0
    max_calls = int(spec_payload.get("max_calls") or spec_payload.get("maxCalls") or 0) or 0
    max_tokens = int(
        spec_payload.get("max_total_tokens")
        or spec_payload.get("maxTotalTokens")
        or spec_payload.get("max_tokens")
        or spec_payload.get("maxTokens")
        or 0
    )

    if window_seconds <= 0:
        return None
    if max_calls <= 0 and max_tokens <= 0:
        return None

    return LLMQuotaSpec(window_seconds=window_seconds, max_calls=max_calls, max_total_tokens=max_tokens)


async def enforce_llm_quota(
    *,
    redis_service: Optional[RedisService],
    tenant_id: Optional[str],
    user_id: Optional[str],
    model_id: str,
    system_prompt: Any,
    user_prompt: Any,
    data_policies: Any,
    now: Optional[datetime] = None,
) -> None:
    """
    Best-effort quota enforcement (NFR-004) using Redis counters.

    This is intentionally lightweight and approximate:
    - consumes 1 call per request
    - consumes estimated prompt tokens (system+user)
    - enforces per-tenant+model and per-user+model limits
    """
    if redis_service is None:
        return
    tenant = str(tenant_id or "").strip()
    user = str(user_id or "").strip()
    model = str(model_id or "").strip()
    if not tenant or not user or not model:
        return

    spec = _extract_quota_spec(data_policies=data_policies, model_id=model)
    if spec is None:
        return

    now = now or datetime.now(timezone.utc)
    bucket = int(now.timestamp() // max(1, int(spec.window_seconds)))

    safe_tenant = _sanitize_key_part(tenant)
    safe_user = _sanitize_key_part(user)
    safe_model = _sanitize_key_part(model)

    estimated_tokens = approx_token_count(system_prompt) + approx_token_count(user_prompt)

    async def _consume(prefix: str) -> None:
        calls_key = f"{prefix}:calls"
        tokens_key = f"{prefix}:tokens"
        ttl_seconds = int(spec.window_seconds * 2)

        lua = """
        local calls_key = KEYS[1]
        local tokens_key = KEYS[2]
        local max_calls = tonumber(ARGV[1])
        local max_tokens = tonumber(ARGV[2])
        local ttl = tonumber(ARGV[3])
        local add_tokens = tonumber(ARGV[4])

        local current_calls = tonumber(redis.call('GET', calls_key) or '0')
        local current_tokens = tonumber(redis.call('GET', tokens_key) or '0')

        if max_calls > 0 and (current_calls + 1) > max_calls then
            return {0, current_calls, current_tokens}
        end
        if max_tokens > 0 and (current_tokens + add_tokens) > max_tokens then
            return {0, current_calls, current_tokens}
        end

        local new_calls = redis.call('INCR', calls_key)
        local new_tokens = redis.call('INCRBY', tokens_key, add_tokens)

        local ttl_calls = redis.call('TTL', calls_key)
        if ttl_calls < 0 then
            redis.call('EXPIRE', calls_key, ttl)
        end
        local ttl_tokens = redis.call('TTL', tokens_key)
        if ttl_tokens < 0 then
            redis.call('EXPIRE', tokens_key, ttl)
        end

        return {1, new_calls, new_tokens}
        """

        allowed, used_calls, used_tokens = await redis_service.client.eval(
            lua,
            2,
            calls_key,
            tokens_key,
            int(spec.max_calls),
            int(spec.max_total_tokens),
            ttl_seconds,
            int(estimated_tokens),
        )
        if int(allowed) != 1:
            raise LLMQuotaExceededError(
                "LLM quota exceeded",
                spec=spec,
                used_calls=int(used_calls),
                used_tokens=int(used_tokens),
            )

    await _consume(f"llm_quota:{safe_tenant}:{bucket}:tenant:{safe_model}")
    await _consume(f"llm_quota:{safe_tenant}:{bucket}:user:{safe_user}:{safe_model}")
