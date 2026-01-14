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
import contextlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from shared.config.settings import ApplicationSettings, get_settings
from shared.services.audit_log_store import AuditLogStore
from shared.services.redis_service import RedisService
from shared.utils.llm_safety import digest_for_audit, mask_pii_text, sha256_hex, stable_json_dumps

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


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
        self.max_prompt_chars = int(getattr(llm, "max_prompt_chars", 20000) or 20000)

        self.retry_max_attempts = int(getattr(llm, "retry_max_attempts", 2) or 2)
        self.retry_base_delay_s = float(getattr(llm, "retry_base_delay_seconds", 0.5) or 0.5)
        self.retry_max_delay_s = float(getattr(llm, "retry_max_delay_seconds", 4.0) or 4.0)
        self.circuit_failure_threshold = int(getattr(llm, "circuit_failure_threshold", 5) or 5)
        self.circuit_open_seconds = float(getattr(llm, "circuit_open_seconds", 30.0) or 30.0)

        self.pricing_json = str(getattr(llm, "pricing_json", "") or "").strip() or None
        self._circuit: dict[str, dict[str, float]] = {}
        self._tool_calls_supported: dict[str, bool] = {}

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
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        url = f"{self.base_url}/chat/completions"
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": sha256_hex(f"llm:{task}:{model}:{prompt_hash}"),
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        body: Dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if self.enable_json_mode:
            body["response_format"] = {"type": "json_object"}

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=body)
        if resp.status_code >= 400:
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        choice = (data.get("choices") or [{}])[0] if isinstance(data, dict) else {}
        message = (choice.get("message") or {}) if isinstance(choice, dict) else {}
        content = (message.get("content") or "") if isinstance(message, dict) else ""

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
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        url = f"{self.base_url}/chat/completions"
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": sha256_hex(f"llm:{task}:{model}:{prompt_hash}:tools"),
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

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
            "temperature": temperature,
            "max_tokens": max_tokens,
            "tools": tools,
            "tool_choice": {"type": "function", "function": {"name": safe_tool_name}},
        }

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=body)
        if resp.status_code >= 400:
            raise LLMHTTPStatusError(status_code=resp.status_code, body_preview=resp.text[:500])

        data = resp.json()
        choice = (data.get("choices") or [{}])[0] if isinstance(data, dict) else {}
        message = (choice.get("message") or {}) if isinstance(choice, dict) else {}

        content = (message.get("content") or "") if isinstance(message, dict) else ""
        arguments = None
        if isinstance(message, dict):
            tool_calls = message.get("tool_calls")
            if isinstance(tool_calls, list) and tool_calls:
                first = tool_calls[0]
                if isinstance(first, dict):
                    func = first.get("function")
                    if isinstance(func, dict):
                        arguments = func.get("arguments")

        if arguments is None:
            arguments = content

        obj = _extract_json_object(str(arguments or ""))
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
        body: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
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
    ) -> tuple[Dict[str, Any], dict[str, Any]]:
        if not self.google_api_key:
            raise LLMUnavailableError("Google API key is not configured (set LLM_GOOGLE_API_KEY)")

        url = f"{self.google_base_url}/v1beta/models/{model}:generateContent"
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": sha256_hex(f"llm:{task}:{model}:{prompt_hash}"),
        }
        body: Dict[str, Any] = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{system_prompt}\n\n{user_prompt}"}],
                }
            ],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
        }
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

        # Hard safety caps
        system_prompt = mask_pii_text(system_prompt, max_chars=self.max_prompt_chars)
        user_prompt = mask_pii_text(user_prompt, max_chars=self.max_prompt_chars)

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
        cache_enabled = bool(self.enable_cache and redis_service) and not bool(provider_policy.get("disable_cache"))

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
                        with contextlib.suppress(Exception):
                            await audit_store.log(
                                partition_key=audit_partition_key,
                                actor=audit_actor,
                                action=f"LLM_{task}",
                                status="success",
                                resource_type="llm_request",
                                resource_id=audit_resource_id or f"llm:{task}:{prompt_hash[:12]}",
                                metadata={
                                    **(audit_metadata or {}),
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
                        raw = (os.getenv(task_key) or os.getenv("LLM_MOCK_JSON") or "").strip()
                        if not raw:
                            hint = task_key or "LLM_MOCK_JSON"
                            raise LLMRequestError(f"LLM_PROVIDER=mock requires {hint} (or LLM_MOCK_JSON) to be set")
                        obj = _extract_json_object(raw)
                        usage: dict[str, Any] = {}
                    elif provider == "openai_compat":
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
                                    )
                                else:
                                    raise
                        else:
                            obj, usage = await self._call_openai_compat_json(
                                task=task,
                                system_prompt=system_prompt,
                                user_prompt=user_prompt,
                                model=model_to_use,
                                temperature=temperature,
                                max_tokens=max_tokens,
                                prompt_hash=prompt_hash,
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
                    # Model responded but produced invalid output; do not retry by default.
                    last_exc = exc
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
                            **(audit_metadata or {}),
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
