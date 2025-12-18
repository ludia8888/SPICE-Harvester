"""
LLM Gateway (domain-neutral, enterprise-safe).

Design goals (see docs/LLM_INTEGRATION.md):
- Centralize LLM calls (prompt templates, caching, timeouts, audit).
- Treat LLM as Assist-only: outputs must be JSON constrained by a schema.
- Fail-open for non-critical features (callers decide fallback behavior).
- Never persist raw prompts or raw outputs in audit logs (store digests only).
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from shared.models.audit_log import AuditStatus
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


@dataclass(frozen=True)
class LLMCallMeta:
    provider: str
    model: str
    cache_hit: bool
    latency_ms: int


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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


class LLMGateway:
    """
    A thin, safe wrapper around an LLM provider.

    Provider is intentionally kept generic (OpenAI-compatible REST) so we can:
    - use OpenAI, Azure OpenAI, or a private gateway
    - use a local model that exposes the same contract
    """

    def __init__(self) -> None:
        self.provider = os.getenv("LLM_PROVIDER", "disabled").strip().lower()
        self.base_url = (os.getenv("LLM_BASE_URL") or "").strip().rstrip("/")
        self.api_key = (os.getenv("LLM_API_KEY") or "").strip()
        self.model = (os.getenv("LLM_MODEL") or "").strip()

        self.timeout_s = float(os.getenv("LLM_TIMEOUT_SECONDS", "20"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0"))
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "800"))
        self.enable_json_mode = _env_bool("LLM_ENABLE_JSON_MODE", True)

        self.enable_cache = _env_bool("LLM_CACHE_ENABLED", True)
        self.cache_ttl_s = int(os.getenv("LLM_CACHE_TTL_SECONDS", "3600"))

        # Safety: cap prompt sizes even before provider caps kick in.
        self.max_prompt_chars = int(os.getenv("LLM_MAX_PROMPT_CHARS", "20000"))

    def is_enabled(self) -> bool:
        if self.provider in {"", "disabled", "off", "none"}:
            return False
        if self.provider in {"openai_compat"}:
            return bool(self.base_url and self.model)
        if self.provider in {"mock"}:
            return True
        return False

    async def complete_json(
        self,
        *,
        task: str,
        system_prompt: str,
        user_prompt: str,
        response_model: Type[T],
        redis_service: Optional[RedisService] = None,
        audit_store: Optional[AuditLogStore] = None,
        audit_partition_key: Optional[str] = None,
        audit_actor: Optional[str] = None,
        audit_resource_id: Optional[str] = None,
        audit_metadata: Optional[Dict[str, Any]] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> tuple[T, LLMCallMeta]:
        if not self.is_enabled():
            raise LLMUnavailableError(
                "LLM is disabled or not configured (set LLM_PROVIDER/openai_compat and LLM_BASE_URL/LLM_MODEL)"
            )

        temperature = self.temperature if temperature is None else float(temperature)
        max_tokens = self.max_tokens if max_tokens is None else int(max_tokens)

        # Hard safety caps
        system_prompt = mask_pii_text(system_prompt, max_chars=self.max_prompt_chars)
        user_prompt = mask_pii_text(user_prompt, max_chars=self.max_prompt_chars)

        prompt_obj = {"system": system_prompt, "user": user_prompt, "task": task, "model": self.model}
        prompt_hash = sha256_hex(stable_json_dumps(prompt_obj))
        input_digest = digest_for_audit(prompt_obj)

        cache_key = f"llm:{self.provider}:{self.model}:{task}:{prompt_hash}"

        if self.enable_cache and redis_service:
            try:
                cached = await redis_service.get_json(cache_key)
                if cached and isinstance(cached, dict) and "data" in cached:
                    parsed = response_model.model_validate(cached["data"])
                    return parsed, LLMCallMeta(
                        provider=self.provider, model=self.model, cache_hit=True, latency_ms=0
                    )
            except Exception as e:
                logger.debug(f"LLM cache read failed (non-fatal): {e}")

        started = time.time()
        error: Optional[str] = None
        out_model: Optional[T] = None
        output_digest: Optional[str] = None

        try:
            if self.provider == "mock":
                raw = os.getenv("LLM_MOCK_JSON", "").strip()
                if not raw:
                    raise LLMRequestError("LLM_PROVIDER=mock requires LLM_MOCK_JSON to be set")
                obj = _extract_json_object(raw)
                out_model = response_model.model_validate(obj)
                output_digest = digest_for_audit(obj)
            elif self.provider == "openai_compat":
                url = f"{self.base_url}/chat/completions"
                headers: Dict[str, str] = {"Content-Type": "application/json"}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"

                body: Dict[str, Any] = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                }
                if self.enable_json_mode:
                    # OpenAI-style JSON mode (best-effort; some gateways ignore it)
                    body["response_format"] = {"type": "json_object"}

                async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                    resp = await client.post(url, headers=headers, json=body)
                    if resp.status_code >= 400:
                        raise LLMRequestError(f"LLM HTTP {resp.status_code}: {resp.text[:500]}")

                    data = resp.json()
                    content = (
                        (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
                    )
                    obj = _extract_json_object(content)
                    out_model = response_model.model_validate(obj)
                    output_digest = digest_for_audit(obj)
            else:
                raise LLMUnavailableError(f"Unsupported LLM_PROVIDER: {self.provider}")

            # Cache (best-effort)
            if self.enable_cache and redis_service and out_model is not None:
                try:
                    await redis_service.set_json(
                        cache_key,
                        {"data": out_model.model_dump(mode="json"), "cached_at": time.time()},
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
                        status=AuditStatus.SUCCESS if error is None else AuditStatus.FAILURE,
                        resource_type="llm_request",
                        resource_id=audit_resource_id or f"llm:{task}:{prompt_hash[:12]}",
                        metadata={
                            **(audit_metadata or {}),
                            "provider": self.provider,
                            "model": self.model,
                            "temperature": temperature,
                            "max_tokens": max_tokens,
                            "prompt_hash": prompt_hash,
                            "input_digest": input_digest,
                            "output_digest": output_digest,
                            "latency_ms": latency_ms,
                            "cache_key": cache_key,
                        },
                        error=error,
                    )
                except Exception as e:
                    logger.debug(f"LLM audit log failed (non-fatal): {e}")

        if out_model is None:
            raise LLMRequestError("LLM call failed without a parsed result")

        return out_model, LLMCallMeta(
            provider=self.provider,
            model=self.model,
            cache_hit=False,
            latency_ms=int((time.time() - started) * 1000),
        )


def create_llm_gateway(_settings: Any = None) -> LLMGateway:
    # The gateway is configured via env vars (LLM_*). Settings object is unused for now.
    return LLMGateway()

