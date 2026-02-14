"""
LLM Gateway (domain-neutral, enterprise-safe).

Design goals (see docs/LLM_INTEGRATION.md):
- Centralize LLM calls (prompt templates, caching, timeouts, audit).
- Treat LLM as Assist-only: outputs must be JSON constrained by a schema.
- Fail-open for non-critical features (callers decide fallback behavior).
- Never persist raw prompts or raw outputs in audit logs (store digests only).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from shared.config.settings import ApplicationSettings, get_settings
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.storage.redis_service import RedisService
from shared.utils.llm_safety import digest_for_audit, mask_pii_text, sha256_hex, stable_json_dumps

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

_ALLOWED_PROVIDER_EXTRA_BODY_KEYS: dict[str, set[str]] = {
    # OpenAI-compatible chat/completions endpoints commonly accept `user`.
    # Some gateways accept `store` as a vendor extension; we allow it when configured.
    "openai_compat": {"store", "user"},
    # Anthropic Messages API supports `metadata`.
    "anthropic": {"metadata"},
    # Google generateContent does not have a stable, portable set of extra body keys to support here.
    "google": set(),
    "mock": set(),
}


class LLMUnavailableError(RuntimeError):
    pass


class LLMRequestError(RuntimeError):
    pass


class LLMOutputValidationError(RuntimeError):
    pass


class LLMPolicyError(LLMRequestError):
    """Raised when a model/tool/policy guard blocks an LLM call."""


class LLMHTTPStatusError(LLMRequestError):
    def __init__(self, *, status_code: int, body_preview: str) -> None:
        super().__init__(f"LLM HTTP {int(status_code)}: {body_preview}")
        self.status_code = int(status_code)
        self.body_preview = str(body_preview)


@dataclass(frozen=True)
class LLMCallMeta:
    provider: str
    model: str
    cache_hit: bool
    latency_ms: int
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_estimate: Optional[float] = None


_PRICING_CACHE: Optional[tuple[str, dict[str, dict[str, float]]]] = None


def _load_pricing_table(raw: Optional[str]) -> dict[str, dict[str, float]]:
    global _PRICING_CACHE
    raw_value = str(raw or "").strip()
    if _PRICING_CACHE is not None and _PRICING_CACHE[0] == raw_value:
        return _PRICING_CACHE[1]
    if not raw_value:
        _PRICING_CACHE = (raw_value, {})
        return _PRICING_CACHE[1]
    if not raw:
        _PRICING_CACHE = (raw_value, {})
        return _PRICING_CACHE[1]
    try:
        parsed = json.loads(raw_value)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/llm_gateway.py:98", exc_info=True)
        _PRICING_CACHE = (raw_value, {})
        return _PRICING_CACHE[1]
    table: dict[str, dict[str, float]] = {}
    if isinstance(parsed, dict):
        for model_id, pricing in parsed.items():
            if not isinstance(pricing, dict):
                continue
            prompt_rate = pricing.get("prompt_per_1k")
            completion_rate = pricing.get("completion_per_1k")
            try:
                prompt_rate_f = float(prompt_rate)
                completion_rate_f = float(completion_rate)
            except (TypeError, ValueError):
                continue
            table[str(model_id)] = {"prompt_per_1k": prompt_rate_f, "completion_per_1k": completion_rate_f}
    _PRICING_CACHE = (raw_value, table)
    return _PRICING_CACHE[1]


def _estimate_cost(
    *, model: str, prompt_tokens: int, completion_tokens: int, pricing_json: Optional[str]
) -> Optional[float]:
    pricing = _load_pricing_table(pricing_json)
    rates = pricing.get(model)
    if not rates:
        return None
    prompt_rate = float(rates.get("prompt_per_1k") or 0.0)
    completion_rate = float(rates.get("completion_per_1k") or 0.0)
    return (float(prompt_tokens) / 1000.0) * prompt_rate + (float(completion_tokens) / 1000.0) * completion_rate


def _parse_provider_policies(raw: Optional[str]) -> dict[str, dict[str, Any]]:
    if raw is None:
        return {}
    text = str(raw).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/llm_gateway.py:138", exc_info=True)
        return {}
    if not isinstance(parsed, dict):
        return {}
    policies: dict[str, dict[str, Any]] = {}
    for key, value in parsed.items():
        provider = str(key or "").strip().lower()
        if not provider:
            continue
        policies[provider] = dict(value) if isinstance(value, dict) else {}
    return policies


def _coerce_provider_extra_headers(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    headers: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key or "").strip()
        val = str(raw_value or "").strip()
        if not key or not val:
            continue
        if "\n" in key or "\r" in key or "\n" in val or "\r" in val:
            continue
        headers[key] = val
    return headers


def _coerce_provider_extra_body(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    body: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        body[key] = raw_value
    return body


def _filter_provider_extra_body(*, provider: str, extra_body: dict[str, Any]) -> dict[str, Any]:
    allowed = _ALLOWED_PROVIDER_EXTRA_BODY_KEYS.get(str(provider or "").strip().lower(), set())
    if not allowed:
        return {}
    filtered: dict[str, Any] = {}
    for key, value in extra_body.items():
        if key in allowed:
            filtered[str(key)] = value
    return filtered


def _build_provider_send_overrides(
    *,
    provider: str,
    provider_policy: Any,
    audit_partition_key: Optional[str],
    audit_actor: Optional[str],
) -> tuple[dict[str, str], dict[str, Any], dict[str, Any]]:
    """
    Build provider send overrides (SEC-002).

    Returns (extra_headers, extra_body, policy_summary_for_audit).
    """
    policy_dict = provider_policy if isinstance(provider_policy, dict) else {}
    extra_headers = _coerce_provider_extra_headers(policy_dict.get("extra_headers"))

    extra_body_raw = _coerce_provider_extra_body(policy_dict.get("extra_body"))
    extra_body = _filter_provider_extra_body(provider=provider, extra_body=extra_body_raw)

    provider_value = str(provider or "").strip().lower()

    no_store = bool(policy_dict.get("no_store")) or policy_dict.get("store") is False
    if no_store and provider_value == "openai_compat":
        extra_body["store"] = False

    isolation_mode = str(policy_dict.get("session_isolation") or "").strip().lower()
    isolation_seed: Optional[str] = None
    if isolation_mode in {"partition", "partition_hash"} and audit_partition_key:
        isolation_seed = f"partition:{audit_partition_key}"
    elif isolation_mode in {"actor", "actor_hash"} and audit_actor:
        isolation_seed = f"actor:{audit_actor}"

    if isolation_seed:
        isolation_id = sha256_hex(isolation_seed)[:64]
        if provider_value == "openai_compat":
            extra_body.setdefault("user", isolation_id)
        elif provider_value == "anthropic":
            metadata = extra_body.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata.setdefault("user_id", isolation_id)
            extra_body["metadata"] = metadata

    summary: dict[str, Any] = {
        "disable_cache": bool(policy_dict.get("disable_cache")),
        "no_store": bool(no_store),
        "session_isolation": isolation_mode or None,
        "extra_headers": sorted(extra_headers.keys()),
        "extra_body": sorted(extra_body.keys()),
    }
    return extra_headers, extra_body, summary


def _extract_json_object(text: str) -> Dict[str, Any]:
    """
    Best-effort JSON object extraction.

    We intentionally do not accept arrays at the top-level to keep the contract strict.
    """
    text = (text or "").strip()
    if not text:
        raise LLMOutputValidationError("Empty LLM output")

    # Common failure mode: fenced code blocks
    if text.startswith("```"):
        # Try to strip code fences
        parts = text.split("```")
        # Find the largest chunk that looks like JSON
        candidates = [p.strip() for p in parts if "{" in p and "}" in p]
        if candidates:
            text = max(candidates, key=len)

    # Try direct parse first
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/llm_gateway.py:265", exc_info=True)
        pass

    # Fallback: find outermost braces
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise LLMOutputValidationError("LLM output is not a JSON object")
    snippet = text[start : end + 1]
    try:
        obj = json.loads(snippet)
    except Exception as e:
        raise LLMOutputValidationError(f"Failed to parse JSON object: {e}") from e
    if not isinstance(obj, dict):
        raise LLMOutputValidationError("LLM output JSON must be an object")
    return obj


def _tool_parameters_from_model(model: Type[BaseModel]) -> Dict[str, Any]:
    """
    Build an OpenAI tool/function `parameters` schema from a Pydantic model.

    We keep this intentionally permissive and rely on Pydantic validation on the
    returned arguments, because many OpenAI-compatible gateways implement only a
    subset of JSON Schema.
    """
    try:
        schema = model.model_json_schema()
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/llm_gateway.py:293", exc_info=True)
        schema = {}
    if not isinstance(schema, dict):
        schema = {}
    properties = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
    required = schema.get("required") if isinstance(schema.get("required"), list) else []
    parameters: Dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": True,
    }
    defs = schema.get("$defs")
    if isinstance(defs, dict) and defs:
        parameters["$defs"] = defs
    return parameters


def _openai_max_tokens_params(model: str, max_tokens: int) -> Dict[str, Any]:
    """
    OpenAI compatibility: newer models (ex: gpt-5) require max_completion_tokens.
    """
    model_name = str(model or "").strip().lower()
    if model_name.startswith("gpt-5"):
        return {"max_completion_tokens": int(max_tokens)}
    return {"max_tokens": int(max_tokens)}


def _openai_temperature_params(model: str, temperature: float) -> Dict[str, Any]:
    """
    OpenAI compatibility: gpt-5 only supports the default temperature (1).
    """
    model_name = str(model or "").strip().lower()
    if model_name.startswith("gpt-5"):
        return {}
    return {"temperature": float(temperature)}


def _openai_reasoning_params(model: str) -> Dict[str, Any]:
    """
    OpenAI responses API: gpt-5 needs low reasoning effort to emit output tokens.
    """
    model_name = str(model or "").strip().lower()
    if model_name.startswith("gpt-5"):
        return {"reasoning": {"effort": "low"}}
    return {}


def _use_openai_responses_api(model: str) -> bool:
    model_name = str(model or "").strip().lower()
    return model_name.startswith("gpt-5")


def _extract_openai_responses_text(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    output = data.get("output")
    texts: list[str] = []
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            if item_type == "output_text" and item.get("text"):
                texts.append(str(item.get("text")))
                continue
            if item_type != "message":
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                if part.get("type") in {"output_text", "text"} and part.get("text"):
                    texts.append(str(part.get("text")))
    return "".join(texts)


def _mask_payload(value: Any, *, max_chars: int) -> Any:
    if isinstance(value, dict):
        return {str(k): _mask_payload(v, max_chars=max_chars) for k, v in value.items()}
    if isinstance(value, list):
        return [_mask_payload(v, max_chars=max_chars) for v in value]
    if isinstance(value, str):
        return mask_pii_text(value, max_chars=max_chars)
    return value


def _log_llm_event(event: str, payload: Dict[str, Any], *, max_chars: int = 1000) -> None:
    try:
        safe_payload = _mask_payload(payload, max_chars=max_chars)
        logger.info("llm_event=%s payload=%s", event, json.dumps(safe_payload, ensure_ascii=True))
    except Exception as exc:
        logger.info("llm_event=%s payload_error=%s", event, exc)


class LLMGateway:
    """
    A thin, safe wrapper around an LLM provider.

    Provider is intentionally kept generic (OpenAI-compatible REST) so we can:
    - use OpenAI, Azure OpenAI, or a private gateway
    - use a local model that exposes the same contract
    """

    def __init__(self, settings: Optional[ApplicationSettings] = None) -> None:
        settings = settings or get_settings()
        llm = getattr(settings, "llm", None)

        provider_raw = str(getattr(llm, "provider", "") or "disabled").strip().lower()
        self.provider = provider_raw
        self.base_url = str(getattr(llm, "base_url", "") or "").strip().rstrip("/")
        self.api_key = str(getattr(llm, "api_key", "") or "").strip()
        self.model = str(getattr(llm, "model", "") or "").strip()

        self.anthropic_base_url = str(getattr(llm, "anthropic_base_url", "") or "https://api.anthropic.com").strip().rstrip("/")
        self.anthropic_api_key = str(getattr(llm, "anthropic_api_key_effective", "") or self.api_key).strip()
        self.anthropic_version = str(getattr(llm, "anthropic_version", "") or "2023-06-01").strip() or "2023-06-01"

        self.google_base_url = str(getattr(llm, "google_base_url", "") or "https://generativelanguage.googleapis.com").strip().rstrip("/")
        self.google_api_key = str(getattr(llm, "google_api_key_effective", "") or self.api_key).strip()

        self.timeout_s = float(getattr(llm, "timeout_seconds", 20.0) or 20.0)
        self.temperature = float(getattr(llm, "temperature", 0.0) or 0.0)
        self.max_tokens = int(getattr(llm, "max_tokens", 800) or 800)
        self.enable_json_mode = bool(getattr(llm, "enable_json_mode", True))
        self.enable_native_tool_calling = bool(getattr(llm, "native_tool_calling", False))

        self.provider_policies = _parse_provider_policies(getattr(llm, "provider_policies_json", None))

        self.enable_cache = bool(getattr(llm, "cache_enabled", True))
        self.cache_ttl_s = int(getattr(llm, "cache_ttl_seconds", 3600) or 3600)
        self.max_prompt_chars = int(getattr(llm, "max_prompt_chars", 0) or 0)

        self.retry_max_attempts = int(getattr(llm, "retry_max_attempts", 2) or 2)
        self.retry_base_delay_s = float(getattr(llm, "retry_base_delay_seconds", 0.5) or 0.5)
        self.retry_max_delay_s = float(getattr(llm, "retry_max_delay_seconds", 4.0) or 4.0)
        self.circuit_failure_threshold = int(getattr(llm, "circuit_failure_threshold", 5) or 5)
        self.circuit_open_seconds = float(getattr(llm, "circuit_open_seconds", 30.0) or 30.0)

        self.pricing_json = str(getattr(llm, "pricing_json", "") or "").strip() or None
        self._circuit: dict[str, dict[str, float]] = {}
        self._tool_calls_supported: dict[str, bool] = {}
        # Responses API JSON schema support is provider/model specific.
        # Cache failures to avoid paying a 4xx + fallback penalty on every call.
        self._responses_json_schema_supported: dict[str, bool] = {}
        # Mock provider supports deterministic sequences for E2E/CI runs.
        # Keyed by normalized task name and advanced per call.
        self._mock_sequence_cursors: dict[str, int] = {}

    def is_enabled(self) -> bool:
        if self.provider in {"", "disabled", "off", "none"}:
            return False
        if self.provider in {"openai_compat"}:
            return bool(self.base_url and self.model)
        if self.provider in {"anthropic"}:
            return bool(self.anthropic_api_key and self.model)
        if self.provider in {"google"}:
            return bool(self.google_api_key and self.model)
        if self.provider in {"mock"}:
            return True
        return False

    def _circuit_key(self, *, provider: str, model: str) -> str:
        return f"{provider}:{model or ''}".strip(":")

    def _is_circuit_open(self, *, circuit_key: str) -> bool:
        state = self._circuit.get(circuit_key) or {}
        opened_until = float(state.get("opened_until") or 0.0)
        return opened_until > time.time()

    def _record_circuit_success(self, *, circuit_key: str) -> None:
        if circuit_key in self._circuit:
            self._circuit[circuit_key] = {"failures": 0.0, "opened_until": 0.0}

    def _record_circuit_failure(self, *, circuit_key: str) -> None:
        state = self._circuit.get(circuit_key) or {"failures": 0.0, "opened_until": 0.0}
        failures = float(state.get("failures") or 0.0) + 1.0
        opened_until = float(state.get("opened_until") or 0.0)
        if failures >= float(self.circuit_failure_threshold):
            opened_until = max(opened_until, time.time() + float(self.circuit_open_seconds))
        self._circuit[circuit_key] = {"failures": failures, "opened_until": opened_until}

    def _retry_delay_s(self, *, prompt_hash: str, attempt: int) -> float:
        attempt = max(0, int(attempt))
        base = float(self.retry_base_delay_s) * (2.0 ** attempt)
        delay = min(float(self.retry_max_delay_s), max(0.0, base))
        # Deterministic jitter so retries are stable under replay/tests.
        jitter_seed = sha256_hex(f"{prompt_hash}:{attempt}")[:8]
        jitter_frac = int(jitter_seed, 16) / float(16**8)
        jitter = delay * 0.2 * jitter_frac
        return min(float(self.retry_max_delay_s), delay + jitter)

    def _should_retry(self, exc: Exception) -> bool:
        if isinstance(exc, LLMHTTPStatusError):
            return exc.status_code == 429 or 500 <= exc.status_code <= 599
        if isinstance(exc, (httpx.TimeoutException, httpx.RequestError)):
            return True
        return False

    async def _call_openai_compat_json(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        prompt_hash: str,
        extra_headers: Optional[Dict[str, str]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        url = f"{self.base_url}/chat/completions"
        _log_llm_event(
            "openai.request",
            {
                "mode": "json",
                "task": task,
                "model": model,
                "prompt_hash": prompt_hash,
                "system_prompt_chars": len(system_prompt),
                "user_prompt_chars": len(user_prompt),
                "max_tokens": max_tokens,
                "temperature": temperature,
                "json_mode": bool(self.enable_json_mode),
            },
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k).strip() and str(v).strip()})

        body: Dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        body.update(_openai_temperature_params(model, temperature))
        body.update(_openai_max_tokens_params(model, max_tokens))
        if self.enable_json_mode:
            body["response_format"] = {"type": "json_object"}
        if extra_body:
            for key, value in extra_body.items():
                if str(key).strip():
                    body[str(key)] = value

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=body)
        if resp.status_code >= 400:
            _log_llm_event(
                "openai.http_error",
                {"mode": "json", "task": task, "model": model, "status_code": resp.status_code, "body": resp.text[:500]},
            )
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        choice = (data.get("choices") or [{}])[0] if isinstance(data, dict) else {}
        message = (choice.get("message") or {}) if isinstance(choice, dict) else {}
        content = (message.get("content") or "") if isinstance(message, dict) else ""
        fallback_text = choice.get("text") if isinstance(choice, dict) else None
        if not content and isinstance(fallback_text, str):
            content = fallback_text
        _log_llm_event(
            "openai.response",
            {
                "mode": "json",
                "task": task,
                "model": model,
                "finish_reason": choice.get("finish_reason") if isinstance(choice, dict) else None,
                "choice_keys": sorted(choice.keys()) if isinstance(choice, dict) else [],
                "message_keys": sorted(message.keys()) if isinstance(message, dict) else [],
                "text_len": len(str(fallback_text or "")),
                "content_len": len(str(content or "")),
                "content_preview": content,
            },
        )

        if not content and isinstance(message, dict):
            tool_calls = message.get("tool_calls")
            if isinstance(tool_calls, list) and tool_calls:
                first = tool_calls[0]
                if isinstance(first, dict):
                    func = first.get("function")
                    if isinstance(func, dict):
                        content = func.get("arguments") or ""

        obj = _extract_json_object(str(content or ""))
        usage = data.get("usage") if isinstance(data, dict) else {}
        if isinstance(usage, dict) and ("input_tokens" in usage or "output_tokens" in usage):
            prompt_tokens = int(usage.get("input_tokens") or 0)
            completion_tokens = int(usage.get("output_tokens") or 0)
            usage = {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            }
        return obj, usage if isinstance(usage, dict) else {}

    async def _call_openai_compat_tool_call(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        prompt_hash: str,
        tool_name: str,
        tool_parameters: Dict[str, Any],
        extra_headers: Optional[Dict[str, str]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        url = f"{self.base_url}/chat/completions"
        _log_llm_event(
            "openai.request",
            {
                "mode": "tool_call",
                "task": task,
                "model": model,
                "prompt_hash": prompt_hash,
                "tool_name": tool_name,
                "system_prompt_chars": len(system_prompt),
                "user_prompt_chars": len(user_prompt),
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k).strip() and str(v).strip()})

        safe_tool_name = str(tool_name or "return_json").strip() or "return_json"
        tools = [
            {
                "type": "function",
                "function": {
                    "name": safe_tool_name,
                    "description": "Return the required JSON object as tool arguments.",
                    "parameters": tool_parameters or {"type": "object", "additionalProperties": True},
                },
            }
        ]

        body: Dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "tools": tools,
            "tool_choice": {"type": "function", "function": {"name": safe_tool_name}},
        }
        body.update(_openai_temperature_params(model, temperature))
        body.update(_openai_max_tokens_params(model, max_tokens))
        if extra_body:
            for key, value in extra_body.items():
                if str(key).strip():
                    body[str(key)] = value

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=body)
        if resp.status_code >= 400:
            _log_llm_event(
                "openai.http_error",
                {
                    "mode": "tool_call",
                    "task": task,
                    "model": model,
                    "status_code": resp.status_code,
                    "body": resp.text[:500],
                },
            )
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        choice = (data.get("choices") or [{}])[0] if isinstance(data, dict) else {}
        message = (choice.get("message") or {}) if isinstance(choice, dict) else {}

        content = (message.get("content") or "") if isinstance(message, dict) else ""
        fallback_text = choice.get("text") if isinstance(choice, dict) else None
        if not content and isinstance(fallback_text, str):
            content = fallback_text
        tool_calls = message.get("tool_calls") if isinstance(message, dict) else None
        arguments = None
        if isinstance(tool_calls, list) and tool_calls:
            first = tool_calls[0]
            if isinstance(first, dict):
                func = first.get("function")
                if isinstance(func, dict):
                    arguments = func.get("arguments")
        if arguments is None and isinstance(message, dict):
            legacy_call = message.get("function_call")
            if isinstance(legacy_call, dict):
                arguments = legacy_call.get("arguments")

        _log_llm_event(
            "openai.response",
            {
                "mode": "tool_call",
                "task": task,
                "model": model,
                "finish_reason": choice.get("finish_reason") if isinstance(choice, dict) else None,
                "choice_keys": sorted(choice.keys()) if isinstance(choice, dict) else [],
                "message_keys": sorted(message.keys()) if isinstance(message, dict) else [],
                "tool_calls_count": len(tool_calls) if isinstance(tool_calls, list) else 0,
                "content_len": len(str(content or "")),
                "arguments_len": len(str(arguments or "")) if arguments is not None else 0,
                "content_preview": content,
                "arguments_preview": arguments,
            },
        )

        if arguments is None:
            arguments = content

        obj = _extract_json_object(str(arguments or ""))
        usage = data.get("usage") if isinstance(data, dict) else {}
        return obj, usage if isinstance(usage, dict) else {}

    async def _call_openai_compat_responses_json(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        model: str,
        max_tokens: int,
        prompt_hash: str,
        tool_parameters: Optional[Dict[str, Any]] = None,
        schema_name: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        url = f"{self.base_url}/responses"
        use_schema = bool(tool_parameters and schema_name)
        if self._responses_json_schema_supported.get(str(model or "")) is False:
            use_schema = False
        _log_llm_event(
            "openai.request",
            {
                "mode": "responses_json",
                "task": task,
                "model": model,
                "prompt_hash": prompt_hash,
                "system_prompt_chars": len(system_prompt),
                "user_prompt_chars": len(user_prompt),
                "max_output_tokens": max_tokens,
                "format": "json_schema" if use_schema else "json_object",
                "schema_name": schema_name if use_schema else None,
            },
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k).strip() and str(v).strip()})

        text_format: Dict[str, Any]
        if use_schema:
            text_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": str(schema_name),
                    "schema": dict(tool_parameters or {}),
                    "strict": True,
                },
            }
        else:
            text_format = {"type": "json_object"}

        body: Dict[str, Any] = {
            "model": model,
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "text": {"format": text_format, "verbosity": "low"},
            "max_output_tokens": int(max_tokens),
        }
        body.update(_openai_reasoning_params(model))
        if extra_body:
            for key, value in extra_body.items():
                if str(key).strip():
                    body[str(key)] = value

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=body)
            # Some OpenAI-compatible gateways do not support json_schema response formatting.
            # Fall back to json_object format on 4xx errors.
            if resp.status_code in {400, 404} and use_schema:
                self._responses_json_schema_supported[str(model or "")] = False
                use_schema = False
                body["text"] = {"format": {"type": "json_object"}, "verbosity": "low"}
                _log_llm_event(
                    "openai.request",
                    {
                        "mode": "responses_json",
                        "task": task,
                        "model": model,
                        "prompt_hash": prompt_hash,
                        "max_output_tokens": max_tokens,
                        "format": "json_object",
                        "fallback_from": "json_schema",
                    },
                )
                resp = await client.post(url, headers=headers, json=body)
            elif resp.status_code < 400 and use_schema:
                self._responses_json_schema_supported[str(model or "")] = True
        if resp.status_code >= 400:
            _log_llm_event(
                "openai.http_error",
                {
                    "mode": "responses_json",
                    "task": task,
                    "model": model,
                    "status_code": resp.status_code,
                    "body": resp.text[:500],
                },
            )
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        status_value = str(data.get("status") or "").strip().lower() if isinstance(data, dict) else ""
        incomplete_details = data.get("incomplete_details") if isinstance(data, dict) else None
        content_text = _extract_openai_responses_text(data)

        # If we got a partial response (commonly due to output token budget), retry once with a larger output budget.
        if status_value == "incomplete":
            reason = ""
            if isinstance(incomplete_details, dict):
                reason = str(incomplete_details.get("reason") or incomplete_details.get("type") or "").strip().lower()
            should_bump = reason in {"max_output_tokens", "length", "output_limit"} or "output" in reason
            if should_bump and int(max_tokens) < 12000:
                bumped = min(12000, max(1, int(max_tokens)) * 2)
                _log_llm_event(
                    "openai.request",
                    {
                        "mode": "responses_json",
                        "task": task,
                        "model": model,
                        "prompt_hash": prompt_hash,
                        "max_output_tokens": bumped,
                        "format": "json_schema" if use_schema else "json_object",
                        "retry_reason": f"incomplete:{reason or 'unknown'}",
                    },
                )
                retry_body = dict(body)
                retry_body["max_output_tokens"] = int(bumped)
                async with httpx.AsyncClient(timeout=self.timeout_s) as retry_client:
                    resp = await retry_client.post(url, headers=headers, json=retry_body)
                if resp.status_code >= 400:
                    _log_llm_event(
                        "openai.http_error",
                        {
                            "mode": "responses_json",
                            "task": task,
                            "model": model,
                            "status_code": resp.status_code,
                            "body": resp.text[:500],
                        },
                    )
                    raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])
                data = resp.json()
                status_value = str(data.get("status") or "").strip().lower() if isinstance(data, dict) else ""
                incomplete_details = data.get("incomplete_details") if isinstance(data, dict) else None
                content_text = _extract_openai_responses_text(data)
        _log_llm_event(
            "openai.response",
            {
                "mode": "responses_json",
                "task": task,
                "model": model,
                "status": status_value or (data.get("status") if isinstance(data, dict) else None),
                "incomplete_details": incomplete_details,
                "error": data.get("error") if isinstance(data, dict) else None,
                "output_len": len(content_text or ""),
                "output_preview": content_text,
                "output_types": [
                    item.get("type")
                    for item in (data.get("output") or [])
                    if isinstance(item, dict) and item.get("type")
                ]
                if isinstance(data, dict)
                else [],
            },
        )

        if status_value == "incomplete":
            raise LLMOutputValidationError(f"Responses API returned incomplete output: {incomplete_details}")

        obj = _extract_json_object(str(content_text or ""))
        usage = data.get("usage") if isinstance(data, dict) else {}
        return obj, usage if isinstance(usage, dict) else {}

    async def _call_anthropic_json(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        prompt_hash: str,
        extra_headers: Optional[Dict[str, str]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        if not self.anthropic_api_key:
            raise LLMUnavailableError("Anthropic API key is not configured (set LLM_ANTHROPIC_API_KEY)")

        url = f"{self.anthropic_base_url}/v1/messages"
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "x-api-key": self.anthropic_api_key,
            "anthropic-version": self.anthropic_version,
            "Idempotency-Key": sha256_hex(f"llm:{task}:{model}:{prompt_hash}"),
        }
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k).strip() and str(v).strip()})
        body: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        if extra_body:
            for key, value in extra_body.items():
                if str(key).strip():
                    body[str(key)] = value
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=body)
        if resp.status_code >= 400:
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        blocks = data.get("content") if isinstance(data, dict) else None
        texts: list[str] = []
        if isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "text" and block.get("text"):
                    texts.append(str(block.get("text")))
        content = "\n".join(texts) if texts else str(data.get("content") or "")
        obj = _extract_json_object(content)
        usage = data.get("usage") if isinstance(data, dict) else {}
        return obj, usage if isinstance(usage, dict) else {}

    async def _call_google_json(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        prompt_hash: str,
        extra_headers: Optional[Dict[str, str]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        if not self.google_api_key:
            raise LLMUnavailableError("Google API key is not configured (set LLM_GOOGLE_API_KEY)")

        url = f"{self.google_base_url}/v1beta/models/{model}:generateContent"
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": sha256_hex(f"llm:{task}:{model}:{prompt_hash}"),
        }
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k).strip() and str(v).strip()})
        body: Dict[str, Any] = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{system_prompt}\n\n{user_prompt}"}],
                }
            ],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
        }
        if extra_body:
            for key, value in extra_body.items():
                if str(key).strip():
                    body[str(key)] = value
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, params={"key": self.google_api_key}, json=body)
        if resp.status_code >= 400:
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        candidates = data.get("candidates") if isinstance(data, dict) else None
        texts: list[str] = []
        if isinstance(candidates, list) and candidates:
            first = candidates[0]
            if isinstance(first, dict):
                content = first.get("content")
                if isinstance(content, dict):
                    parts = content.get("parts")
                    if isinstance(parts, list):
                        for part in parts:
                            if isinstance(part, dict) and part.get("text"):
                                texts.append(str(part.get("text")))
        content_text = "\n".join(texts)
        obj = _extract_json_object(content_text)
        usage = data.get("usageMetadata") if isinstance(data, dict) else {}
        return obj, usage if isinstance(usage, dict) else {}

    async def complete_json(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        response_model: Type[T],
        model: Optional[str] = None,
        allowed_models: Optional[list[str]] = None,
        use_native_tool_calling: Optional[bool] = None,
        redis_service: Optional[RedisService] = None,
        audit_store: Optional[AuditLogStore] = None,
        audit_partition_key: Optional[str] = None,
        audit_actor: Optional[str] = None,
        audit_resource_id: Optional[str] = None,
        audit_metadata: Optional[Dict[str, Any]] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> tuple[T, LLMCallMeta]:
        provider = self.provider
        if provider in {"", "disabled", "off", "none"}:
            raise LLMUnavailableError("LLM is disabled (set LLM_PROVIDER=openai_compat)")

        model_to_use = str(model or self.model or "").strip()
        allowed_set = {str(m).strip() for m in (allowed_models or []) if str(m).strip()} if allowed_models else None
        if allowed_set is not None and model_to_use and model_to_use not in allowed_set:
            raise LLMPolicyError(f"Model not allowed by policy: {model_to_use}")
        if provider == "openai_compat":
            if not self.base_url or not model_to_use:
                raise LLMUnavailableError(
                    "LLM provider is not configured (set LLM_BASE_URL and LLM_MODEL or pass model=...)"
                )
        elif provider in {"anthropic", "google"}:
            if not model_to_use:
                raise LLMUnavailableError("LLM_MODEL is required for this provider")
        elif provider == "mock":
            if not model_to_use:
                model_to_use = "mock"
        else:
            raise LLMUnavailableError(f"Unsupported LLM_PROVIDER: {provider}")

        temperature = self.temperature if temperature is None else float(temperature)
        max_tokens = self.max_tokens if max_tokens is None else int(max_tokens)

        # Hard safety caps (0 = no truncation, auto-detect in agent loop)
        pii_cap = self.max_prompt_chars if self.max_prompt_chars > 0 else None
        system_prompt = mask_pii_text(system_prompt, max_chars=pii_cap)
        user_prompt = mask_pii_text(user_prompt, max_chars=pii_cap)

        native_flag = self.enable_native_tool_calling if use_native_tool_calling is None else bool(use_native_tool_calling)
        prompt_obj = {
            "system": system_prompt,
            "user": user_prompt,
            "task": task,
            "model": model_to_use,
            "schema": getattr(response_model, "__name__", "schema"),
            "native_tool_calling": bool(native_flag),
        }
        prompt_hash = sha256_hex(stable_json_dumps(prompt_obj))
        input_digest = digest_for_audit(prompt_obj)

        cache_partition = audit_partition_key or "global"
        cache_partition_hash = sha256_hex(cache_partition)
        cache_key = f"llm:{provider}:{model_to_use}:{task}:{cache_partition_hash}:{prompt_hash}"

        provider_policy = (
            self.provider_policies.get(str(provider or "").strip().lower(), {})
            if isinstance(getattr(self, "provider_policies", None), dict)
            else {}
        )
        provider_extra_headers, provider_extra_body, provider_policy_summary = _build_provider_send_overrides(
            provider=provider,
            provider_policy=provider_policy,
            audit_partition_key=audit_partition_key,
            audit_actor=audit_actor,
        )

        cache_enabled = (
            bool(self.enable_cache and redis_service and audit_partition_key)
            and not bool(provider_policy.get("disable_cache"))
        )
        audit_metadata_effective = {**(audit_metadata or {}), "provider_policy": provider_policy_summary}

        if cache_enabled:
            try:
                cached = await redis_service.get_json(cache_key)
                if cached and isinstance(cached, dict) and "data" in cached:
                    parsed = response_model.model_validate(cached["data"])
                    cached_meta = cached.get("meta") if isinstance(cached.get("meta"), dict) else {}
                    prompt_tokens = int(cached_meta.get("prompt_tokens") or 0)
                    completion_tokens = int(cached_meta.get("completion_tokens") or 0)
                    total_tokens = int(cached_meta.get("total_tokens") or 0)
                    output_digest = digest_for_audit(cached["data"])
                    if audit_store and audit_partition_key:
                        try:
                            await audit_store.log(
                                partition_key=audit_partition_key,
                                actor=audit_actor,
                                action=f"LLM_{task}",
                                status="success",
                                resource_type="llm_request",
                                resource_id=audit_resource_id or f"llm:{task}:{prompt_hash[:12]}",
                                metadata={
                                    **audit_metadata_effective,
                                    "provider": provider,
                                    "model": model_to_use,
                                    "temperature": temperature,
                                    "max_tokens": max_tokens,
                                    "prompt_hash": prompt_hash,
                                    "cache_key": cache_key,
                                    "cache_hit": True,
                                    "input_digest": input_digest,
                                    "output_digest": output_digest,
                                    "latency_ms": 0,
                                    "attempts": 0,
                                    "prompt_tokens": prompt_tokens,
                                    "completion_tokens": completion_tokens,
                                    "total_tokens": total_tokens,
                                    "cost_estimate": 0.0,
                                },
                            )
                        except Exception as exc:
                            logger.warning("Failed to write LLM cache-hit audit log: %s", exc, exc_info=True)
                    return parsed, LLMCallMeta(
                        provider=provider,
                        model=model_to_use,
                        cache_hit=True,
                        latency_ms=0,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        total_tokens=total_tokens,
                        cost_estimate=0.0,
                    )
            except Exception as e:
                logger.debug(f"LLM cache read failed (non-fatal): {e}")

        started = time.time()
        error: Optional[str] = None
        out_model: Optional[T] = None
        output_digest: Optional[str] = None
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        cost_estimate: Optional[float] = None
        attempts = 0
        circuit_key = self._circuit_key(provider=provider, model=model_to_use)

        try:
            if self._is_circuit_open(circuit_key=circuit_key):
                raise LLMUnavailableError("LLM circuit breaker is open (try again later)")

            last_exc: Optional[Exception] = None
            max_attempts = max(1, int(self.retry_max_attempts))
            for attempt in range(max_attempts):
                attempts = attempt + 1
                if self._is_circuit_open(circuit_key=circuit_key):
                    raise LLMUnavailableError("LLM circuit breaker is open (try again later)")
                try:
                    if provider == "mock":
                        safe_task = re.sub(r"[^A-Z0-9]+", "_", (task or "").strip().upper()).strip("_")
                        task_key = f"LLM_MOCK_JSON_{safe_task}" if safe_task else ""
                        # IMPORTANT: mock sequences must not bleed across independent requests/runs.
                        # Scope the cursor by audit_partition_key when available (pipeline_agent:<run_id>, etc).
                        cursor_scope = str(audit_partition_key or "").strip() or "global"
                        cursor_key = f"{cursor_scope}:{safe_task}" if safe_task else cursor_scope
                        raw = (get_settings().llm.mock_json_for_task(task) or "").strip()
                        if not raw:
                            hint = task_key or "LLM_MOCK_JSON"
                            raise LLMUnavailableError(
                                f"LLM_PROVIDER=mock requires {hint} (or LLM_MOCK_JSON or LLM_MOCK_DIR) to be set"
                            )
                        parsed: Any
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/llm_gateway.py:1190", exc_info=True)
                            parsed = None

                        if isinstance(parsed, list):
                            if not parsed:
                                hint = task_key or "LLM_MOCK_JSON"
                                raise LLMUnavailableError(f"LLM_PROVIDER=mock requires {hint} to contain at least 1 item")
                            idx = int(self._mock_sequence_cursors.get(cursor_key, 0) or 0)
                            if idx < 0:
                                idx = 0
                            if idx >= len(parsed):
                                idx = len(parsed) - 1
                            selected = parsed[idx]
                            if not isinstance(selected, dict):
                                raise LLMOutputValidationError("LLM mock sequence items must be JSON objects")
                            obj = selected
                            # Advance cursor (cap at last element for repeated calls).
                            self._mock_sequence_cursors[cursor_key] = min(len(parsed) - 1, idx + 1)
                        elif isinstance(parsed, dict):
                            obj = parsed
                        else:
                            obj = _extract_json_object(raw)
                        usage = {}
                    elif provider == "openai_compat":
                        if _use_openai_responses_api(model_to_use):
                            safe_task = re.sub(r"[^a-zA-Z0-9_]+", "_", (task or "task").strip().lower()).strip("_")
                            schema_name = f"{safe_task or 'response'}_schema"[:64]
                            obj, usage = await self._call_openai_compat_responses_json(
                                task=task,
                                system_prompt=system_prompt,
                                user_prompt=user_prompt,
                                model=model_to_use,
                                max_tokens=max_tokens,
                                prompt_hash=prompt_hash,
                                tool_parameters=_tool_parameters_from_model(response_model),
                                schema_name=schema_name,
                                extra_headers=provider_extra_headers,
                                extra_body=provider_extra_body,
                            )
                            self._tool_calls_supported[model_to_use] = False
                        else:
                            tool_calls_supported = self._tool_calls_supported.get(model_to_use)
                            want_native = bool(native_flag) and tool_calls_supported is not False
                            if want_native:
                                safe_task = re.sub(r"[^a-zA-Z0-9_]+", "_", (task or "task").strip().lower()).strip("_")
                                tool_name = f"return_{safe_task}"[:64] if safe_task else "return_json"
                                try:
                                    obj, usage = await self._call_openai_compat_tool_call(
                                        task=task,
                                        system_prompt=system_prompt,
                                        user_prompt=user_prompt,
                                        model=model_to_use,
                                        temperature=temperature,
                                        max_tokens=max_tokens,
                                        prompt_hash=prompt_hash,
                                        tool_name=tool_name,
                                        tool_parameters=_tool_parameters_from_model(response_model),
                                        extra_headers=provider_extra_headers,
                                        extra_body=provider_extra_body,
                                    )
                                    self._tool_calls_supported[model_to_use] = True
                                except LLMHTTPStatusError as exc:
                                    # If the upstream does not support tools/tool_choice, fall back to prompt-based JSON.
                                    if exc.status_code in {400, 404}:
                                        self._tool_calls_supported[model_to_use] = False
                                        obj, usage = await self._call_openai_compat_json(
                                            task=task,
                                            system_prompt=system_prompt,
                                            user_prompt=user_prompt,
                                            model=model_to_use,
                                            temperature=temperature,
                                            max_tokens=max_tokens,
                                            prompt_hash=prompt_hash,
                                            extra_headers=provider_extra_headers,
                                            extra_body=provider_extra_body,
                                        )
                                    else:
                                        raise
                                except LLMOutputValidationError:
                                    # Tool calls can be flaky; fall back to strict JSON mode.
                                    obj, usage = await self._call_openai_compat_json(
                                        task=task,
                                        system_prompt=system_prompt,
                                        user_prompt=user_prompt,
                                        model=model_to_use,
                                        temperature=temperature,
                                        max_tokens=max_tokens,
                                        prompt_hash=prompt_hash,
                                        extra_headers=provider_extra_headers,
                                        extra_body=provider_extra_body,
                                    )
                            else:
                                obj, usage = await self._call_openai_compat_json(
                                    task=task,
                                    system_prompt=system_prompt,
                                    user_prompt=user_prompt,
                                    model=model_to_use,
                                    temperature=temperature,
                                    max_tokens=max_tokens,
                                    prompt_hash=prompt_hash,
                                    extra_headers=provider_extra_headers,
                                    extra_body=provider_extra_body,
                                )
                    elif provider == "anthropic":
                        obj, usage = await self._call_anthropic_json(
                            task=task,
                            system_prompt=system_prompt,
                            user_prompt=user_prompt,
                            model=model_to_use,
                            temperature=temperature,
                            max_tokens=max_tokens,
                            prompt_hash=prompt_hash,
                            extra_headers=provider_extra_headers,
                            extra_body=provider_extra_body,
                        )
                    elif provider == "google":
                        obj, usage = await self._call_google_json(
                            task=task,
                            system_prompt=system_prompt,
                            user_prompt=user_prompt,
                            model=model_to_use,
                            temperature=temperature,
                            max_tokens=max_tokens,
                            prompt_hash=prompt_hash,
                            extra_headers=provider_extra_headers,
                            extra_body=provider_extra_body,
                        )
                    else:
                        raise LLMUnavailableError(f"Unsupported LLM_PROVIDER: {provider}")

                    if provider == "anthropic":
                        prompt_tokens = int(usage.get("input_tokens") or 0)
                        completion_tokens = int(usage.get("output_tokens") or 0)
                        total_tokens = prompt_tokens + completion_tokens
                    elif provider == "google":
                        prompt_tokens = int(usage.get("promptTokenCount") or 0)
                        completion_tokens = int(usage.get("candidatesTokenCount") or 0)
                        total_tokens = int(usage.get("totalTokenCount") or (prompt_tokens + completion_tokens))
                    else:
                        prompt_tokens = int(usage.get("prompt_tokens") or 0)
                        completion_tokens = int(usage.get("completion_tokens") or 0)
                        total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))

                    cost_estimate = _estimate_cost(
                        model=model_to_use,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        pricing_json=self.pricing_json,
                    )
                    out_model = response_model.model_validate(obj)
                    output_digest = digest_for_audit(obj)
                    self._record_circuit_success(circuit_key=circuit_key)
                    break
                except (ValidationError, LLMOutputValidationError) as exc:
                    # Model responded but produced invalid output; allow limited retries for responses API.
                    last_exc = exc
                    if (
                        provider == "openai_compat"
                        and _use_openai_responses_api(model_to_use)
                        and attempt < (max_attempts - 1)
                    ):
                        delay_s = self._retry_delay_s(prompt_hash=prompt_hash, attempt=attempt)
                        await asyncio.sleep(delay_s)
                        continue
                    raise
                except Exception as exc:
                    last_exc = exc
                    if self._should_retry(exc):
                        self._record_circuit_failure(circuit_key=circuit_key)
                    if self._should_retry(exc) and attempt < (max_attempts - 1):
                        delay_s = self._retry_delay_s(prompt_hash=prompt_hash, attempt=attempt)
                        await asyncio.sleep(delay_s)
                        continue
                    raise

            if out_model is None and last_exc is not None:
                raise last_exc

            # Cache (best-effort)
            if cache_enabled and out_model is not None:
                try:
                    await redis_service.set_json(
                        cache_key,
                        {
                            "data": out_model.model_dump(mode="json"),
                            "meta": {
                                "prompt_tokens": prompt_tokens,
                                "completion_tokens": completion_tokens,
                                "total_tokens": total_tokens,
                                "cost_estimate": cost_estimate,
                            },
                            "cached_at": time.time(),
                        },
                        ttl=self.cache_ttl_s,
                    )
                except Exception as e:
                    logger.debug(f"LLM cache write failed (non-fatal): {e}")

        except (ValidationError, LLMOutputValidationError) as e:
            error = f"LLM output validation failed: {e}"
            raise LLMOutputValidationError(error) from e
        except httpx.RequestError as e:
            # Prevent raw httpx exceptions from escaping to global exception handlers (503),
            # so callers can handle LLM failures deterministically.
            raise LLMRequestError(f"LLM request failed: {e}") from e
        except Exception as e:
            error = str(e)
            raise
        finally:
            latency_ms = int((time.time() - started) * 1000)

            # Audit (best-effort; never store raw prompt/output)
            if audit_store and audit_partition_key:
                try:
                    await audit_store.log(
                        partition_key=audit_partition_key,
                        actor=audit_actor,
                        action=f"LLM_{task}",
                        status="success" if error is None else "failure",
                        resource_type="llm_request",
                        resource_id=audit_resource_id or f"llm:{task}:{prompt_hash[:12]}",
                        metadata={
                            **audit_metadata_effective,
                            "provider": provider,
                            "model": model_to_use,
                            "temperature": temperature,
                            "max_tokens": max_tokens,
                            "prompt_hash": prompt_hash,
                            "input_digest": input_digest,
                            "output_digest": output_digest,
                            "latency_ms": latency_ms,
                            "cache_key": cache_key,
                            "attempts": attempts,
                            "prompt_tokens": prompt_tokens,
                            "completion_tokens": completion_tokens,
                            "total_tokens": total_tokens,
                            "cost_estimate": cost_estimate,
                        },
                        error=error,
                    )
                except Exception as e:
                    logger.debug(f"LLM audit log failed (non-fatal): {e}")

        if out_model is None:
            raise LLMRequestError("LLM call failed without a parsed result")

        return out_model, LLMCallMeta(
            provider=provider,
            model=model_to_use,
            cache_hit=False,
            latency_ms=int((time.time() - started) * 1000),
            prompt_tokens=int(prompt_tokens),
            completion_tokens=int(completion_tokens),
            total_tokens=int(total_tokens),
            cost_estimate=cost_estimate,
        )


def create_llm_gateway(settings: ApplicationSettings) -> LLMGateway:
    return LLMGateway(settings)
