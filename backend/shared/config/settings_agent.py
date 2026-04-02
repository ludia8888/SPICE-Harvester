from __future__ import annotations

import logging
import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.config.settings_support import _ENV_FILE, _clamp_int, _strip_optional_text


logger = logging.getLogger(__name__)


class LLMSettings(BaseSettings):
    """LLM gateway settings (shared across services)."""

    model_config = SettingsConfigDict(
        env_prefix="LLM_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    provider: str = Field(default="disabled", description="LLM provider (LLM_PROVIDER)")
    base_url: Optional[str] = Field(default=None, description="OpenAI-compatible base URL (LLM_BASE_URL)")
    api_key: Optional[str] = Field(default=None, description="OpenAI-compatible API key (LLM_API_KEY)")
    model: Optional[str] = Field(default=None, description="Default model id (LLM_MODEL)")

    anthropic_base_url: str = Field(
        default="https://api.anthropic.com",
        description="Anthropic base URL (LLM_ANTHROPIC_BASE_URL)",
    )
    anthropic_api_key: Optional[str] = Field(
        default=None,
        description="Anthropic API key (LLM_ANTHROPIC_API_KEY; fallback ANTHROPIC_API_KEY/LLM_API_KEY)",
    )
    anthropic_version: str = Field(
        default="2023-06-01",
        description="Anthropic API version header (LLM_ANTHROPIC_VERSION)",
    )

    google_base_url: str = Field(
        default="https://generativelanguage.googleapis.com",
        description="Google Generative Language base URL (LLM_GOOGLE_BASE_URL)",
    )
    google_api_key: Optional[str] = Field(
        default=None,
        description="Google API key (LLM_GOOGLE_API_KEY; fallback GOOGLE_API_KEY/LLM_API_KEY)",
    )

    timeout_seconds: float = Field(default=60.0, description="LLM timeout seconds (LLM_TIMEOUT_SECONDS)")
    temperature: float = Field(default=0.0, description="LLM temperature (LLM_TEMPERATURE)")
    max_tokens: int = Field(default=800, description="LLM max tokens (LLM_MAX_TOKENS)")
    enable_json_mode: bool = Field(default=True, description="Enable JSON mode (LLM_ENABLE_JSON_MODE)")
    native_tool_calling: bool = Field(default=False, description="Enable native tool calling (LLM_NATIVE_TOOL_CALLING)")
    provider_policies_json: Optional[str] = Field(
        default=None,
        description="Provider send policies JSON (LLM_PROVIDER_POLICIES_JSON)",
    )

    cache_enabled: bool = Field(default=True, description="Enable Redis cache (LLM_CACHE_ENABLED)")
    cache_ttl_seconds: int = Field(default=3600, description="Cache TTL seconds (LLM_CACHE_TTL_SECONDS)")
    max_prompt_chars: int = Field(default=0, description="Prompt size cap chars; 0=auto-detect from model context window (LLM_MAX_PROMPT_CHARS)")

    retry_max_attempts: int = Field(default=2, description="Max retry attempts (LLM_RETRY_MAX_ATTEMPTS)")
    retry_base_delay_seconds: float = Field(default=0.5, description="Retry base delay seconds (LLM_RETRY_BASE_DELAY_SECONDS)")
    retry_max_delay_seconds: float = Field(default=4.0, description="Retry max delay seconds (LLM_RETRY_MAX_DELAY_SECONDS)")
    circuit_failure_threshold: int = Field(default=5, description="Circuit breaker threshold (LLM_CIRCUIT_FAILURE_THRESHOLD)")
    circuit_open_seconds: float = Field(default=30.0, description="Circuit breaker open seconds (LLM_CIRCUIT_OPEN_SECONDS)")

    pricing_json: Optional[str] = Field(default=None, description="Pricing table JSON (LLM_PRICING_JSON)")
    mock_json: Optional[str] = Field(
        default=None,
        description="Mock provider JSON payload (LLM_MOCK_JSON)",
    )
    mock_dir: Optional[str] = Field(
        default=None,
        description="Directory containing mock JSON files like agent_plan_compile_v1.json (LLM_MOCK_DIR)",
    )

    _normalize_provider_strings = field_validator(
        "provider",
        "base_url",
        "api_key",
        "model",
        "anthropic_base_url",
        "anthropic_api_key",
        "anthropic_version",
        "google_base_url",
        "google_api_key",
        "pricing_json",
        "mock_json",
        "mock_dir",
        "provider_policies_json",
        mode="before",
    )(_strip_optional_text)

    @field_validator("anthropic_api_key", mode="before")
    @classmethod
    def fallback_anthropic_api_key(cls, v):  # noqa: ANN001
        if v not in (None, ""):
            return v
        fallback = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
        return fallback or None

    @field_validator("google_api_key", mode="before")
    @classmethod
    def fallback_google_api_key(cls, v):  # noqa: ANN001
        if v not in (None, ""):
            return v
        fallback = (os.getenv("GOOGLE_API_KEY") or "").strip()
        return fallback or None

    @field_validator("api_key", mode="before")
    @classmethod
    def fallback_openai_api_key(cls, v):  # noqa: ANN001
        raw = str(v).strip() if v not in (None, "") else ""
        fallback = (os.getenv("OPENAI_API_KEY") or "").strip()
        if raw and fallback and raw != fallback:
            raise ValueError(
                "Conflicting API key env vars: LLM_API_KEY and OPENAI_API_KEY are both set to different values. "
                "Use only LLM_API_KEY."
            )
        if raw:
            return raw
        return fallback or None

    @field_validator("base_url", mode="before")
    @classmethod
    def fallback_openai_base_url(cls, v):  # noqa: ANN001
        raw = str(v).strip() if v not in (None, "") else ""
        fallback = (os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE") or "").strip()
        if raw and fallback and raw.rstrip("/") != fallback.rstrip("/"):
            raise ValueError(
                "Conflicting base URL env vars: LLM_BASE_URL and OPENAI_BASE_URL/OPENAI_API_BASE are both set to "
                "different values. Use only LLM_BASE_URL."
            )
        if raw:
            return raw
        return fallback or None

    @field_validator("model", mode="before")
    @classmethod
    def fallback_openai_model(cls, v):  # noqa: ANN001
        raw = str(v).strip() if v not in (None, "") else ""
        fallback = (os.getenv("OPENAI_MODEL") or "").strip()
        if raw and fallback and raw != fallback:
            raise ValueError(
                "Conflicting model env vars: LLM_MODEL and OPENAI_MODEL are both set to different values. "
                "Use only LLM_MODEL."
            )
        if raw:
            return raw
        return fallback or None

    @property
    def anthropic_api_key_effective(self) -> Optional[str]:
        return self.anthropic_api_key or self.api_key

    @property
    def google_api_key_effective(self) -> Optional[str]:
        return self.google_api_key or self.api_key

    def mock_json_for_task(self, task: Optional[str]) -> Optional[str]:
        """
        Resolve the mock provider JSON payload for a task.

        Priority:
        1) `LLM_MOCK_JSON_<TASK>` where TASK is uppercased and normalized
        2) `<LLM_MOCK_DIR>/<task>.json` (lowercased) when LLM_MOCK_DIR is set
        2) `LLM_MOCK_JSON`
        """
        from pathlib import Path

        safe_task = str(task or "").strip()
        if safe_task:
            import re

            token = re.sub(r"[^A-Z0-9]+", "_", safe_task.upper()).strip("_")
            if token:
                value = (os.getenv(f"LLM_MOCK_JSON_{token}") or "").strip()
                if value:
                    return value
                mock_dir = str(self.mock_dir or "").strip()
                if mock_dir:
                    candidate = Path(mock_dir) / f"{token.lower()}.json"
                    try:
                        file_value = candidate.read_text(encoding="utf-8").strip()
                        if file_value:
                            return file_value
                    except OSError as exc:
                        logger.warning("Failed to read task-scoped mock JSON %s: %s", candidate, exc, exc_info=True)
        fallback = str(self.mock_json or "").strip()
        if fallback:
            return fallback
        mock_dir = str(self.mock_dir or "").strip()
        if mock_dir:
            try:
                file_value = (Path(mock_dir) / "default.json").read_text(encoding="utf-8").strip()
                if file_value:
                    return file_value
            except OSError as exc:
                logger.warning("Failed to read default mock JSON from %s: %s", mock_dir, exc, exc_info=True)
        return None


class AgentRuntimeSettings(BaseSettings):
    """Agent runtime settings (agent service tool runner)."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    bff_base_url: Optional[str] = Field(
        default=None,
        description="Override BFF base URL for tool calls (AGENT_BFF_BASE_URL)",
    )
    bff_token: Optional[str] = Field(
        default=None,
        description="Bearer token for BFF tool calls (AGENT_BFF_TOKEN; fallback BFF_AGENT_TOKEN)",
    )
    service_name: str = Field(
        default="agent",
        description="Service name for audit/events (AGENT_SERVICE_NAME)",
    )
    run_max_steps: int = Field(
        default=50,
        description="Max steps per agent run (AGENT_RUN_MAX_STEPS)",
    )
    require_event_store: bool = Field(
        default=True,
        description="Require event store availability (AGENT_REQUIRE_EVENT_STORE)",
    )
    tool_timeout_seconds: float = Field(
        default=180.0,
        description="HTTP timeout for tool calls (AGENT_TOOL_TIMEOUT_SECONDS)",
    )
    tool_max_payload_bytes: int = Field(
        default=200000,
        description="Max tool payload bytes (AGENT_TOOL_MAX_PAYLOAD_BYTES)",
    )
    audit_max_preview_chars: int = Field(
        default=2000,
        description="Max chars stored in audit previews (AGENT_AUDIT_MAX_PREVIEW_CHARS)",
    )

    context_upload_max_bytes: int = Field(
        default=10 * 1024 * 1024,
        description="Max bytes for agent session context uploads (AGENT_CONTEXT_UPLOAD_MAX_BYTES)",
    )
    context_upload_max_text_chars: int = Field(
        default=20000,
        description="Max chars extracted from uploaded context files (AGENT_CONTEXT_UPLOAD_MAX_TEXT_CHARS)",
    )
    context_upload_clamav_host: Optional[str] = Field(
        default=None,
        description="ClamAV daemon host for context upload scanning (AGENT_CONTEXT_UPLOAD_CLAMAV_HOST)",
    )
    context_upload_clamav_port: int = Field(
        default=3310,
        description="ClamAV daemon port for context upload scanning (AGENT_CONTEXT_UPLOAD_CLAMAV_PORT)",
    )
    context_upload_clamav_timeout_seconds: float = Field(
        default=2.0,
        description="ClamAV scan timeout seconds (AGENT_CONTEXT_UPLOAD_CLAMAV_TIMEOUT_SECONDS)",
    )
    context_upload_clamav_required: bool = Field(
        default=False,
        description="Reject uploads when ClamAV unavailable (AGENT_CONTEXT_UPLOAD_CLAMAV_REQUIRED)",
    )

    command_timeout_seconds: float = Field(
        default=600.0,
        description=(
            "Wait timeout for async commands/pipeline jobs "
            "(AGENT_COMMAND_TIMEOUT_SECONDS; fallback PIPELINE_RUN_TIMEOUT_SECONDS/PIPELINE_RUN_TIMEOUT)"
        ),
    )
    command_poll_interval_seconds: float = Field(
        default=2.0,
        description="Poll interval seconds for command status (AGENT_COMMAND_POLL_INTERVAL_SECONDS)",
    )
    command_ws_idle_seconds: float = Field(
        default=5.0,
        description="WebSocket idle poll seconds (AGENT_COMMAND_WS_IDLE_SECONDS)",
    )
    command_ws_enabled: bool = Field(
        default=True,
        description="Enable command WebSocket progress (AGENT_COMMAND_WS_ENABLED)",
    )

    pipeline_wait_enabled: bool = Field(
        default=True,
        description="Wait for pipeline build/deploy job completion (AGENT_PIPELINE_WAIT_ENABLED)",
    )
    block_writes_on_overlay_degraded: bool = Field(
        default=True,
        description="Block writes when overlay is degraded (AGENT_BLOCK_WRITES_ON_OVERLAY_DEGRADED)",
    )
    allow_degraded_writes: bool = Field(
        default=False,
        description="Allow writes even if overlay degraded (AGENT_ALLOW_DEGRADED_WRITES)",
    )

    auto_retry_enabled: bool = Field(
        default=True,
        description="Enable auto retries for tool calls (AGENT_AUTO_RETRY_ENABLED)",
    )
    auto_retry_max_attempts: int = Field(
        default=3,
        description="Auto retry max attempts (AGENT_AUTO_RETRY_MAX_ATTEMPTS)",
    )
    auto_retry_base_delay_seconds: float = Field(
        default=0.5,
        description="Auto retry base delay seconds (AGENT_AUTO_RETRY_BASE_DELAY_SECONDS)",
    )
    auto_retry_max_delay_seconds: float = Field(
        default=8.0,
        description="Auto retry max delay seconds (AGENT_AUTO_RETRY_MAX_DELAY_SECONDS)",
    )
    auto_retry_allow_writes: bool = Field(
        default=False,
        description="Allow auto retries on write methods (AGENT_AUTO_RETRY_ALLOW_WRITES)",
    )

    @field_validator("context_upload_max_bytes", mode="before")
    @classmethod
    def clamp_context_upload_max_bytes(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10 * 1024 * 1024, min_value=0, max_value=100 * 1024 * 1024)

    @field_validator("context_upload_max_text_chars", mode="before")
    @classmethod
    def clamp_context_upload_max_text_chars(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=20000, min_value=0, max_value=2_000_000)

    _normalize_context_upload_clamav_host = field_validator(
        "context_upload_clamav_host",
        mode="before",
    )(_strip_optional_text)

    @field_validator("context_upload_clamav_port", mode="before")
    @classmethod
    def clamp_context_upload_clamav_port(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3310, min_value=1, max_value=65535)

    @field_validator("bff_token", mode="before")
    @classmethod
    def fallback_bff_token(cls, v):  # noqa: ANN001
        if os.getenv("AGENT_BFF_TOKEN") not in (None, ""):
            return v
        fallback = (os.getenv("BFF_AGENT_TOKEN") or "").strip()
        return fallback or v

    @field_validator("command_timeout_seconds", mode="before")
    @classmethod
    def fallback_command_timeout(cls, v):  # noqa: ANN001
        if os.getenv("AGENT_COMMAND_TIMEOUT_SECONDS") not in (None, ""):
            return v
        for key in ("PIPELINE_RUN_TIMEOUT_SECONDS", "PIPELINE_RUN_TIMEOUT"):
            raw = (os.getenv(key) or "").strip()
            if raw:
                return raw
        return v


class AgentPlanSettings(BaseSettings):
    """LLM-native control plane settings (planner + allowlist bootstrap)."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLAN_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    allowlist_bundle_path: Optional[str] = Field(
        default=None,
        description="Path to agent tool allowlist bundle JSON (AGENT_PLAN_ALLOWLIST_BUNDLE_PATH)",
    )
    allowlist_bootstrap_enabled: bool = Field(
        default=False,
        description="Bootstrap allowlist bundle into Postgres on startup (AGENT_PLAN_ALLOWLIST_BOOTSTRAP_ENABLED)",
    )
    allowlist_bootstrap_only_if_empty: bool = Field(
        default=True,
        description="Only bootstrap if DB table is empty (AGENT_PLAN_ALLOWLIST_BOOTSTRAP_ONLY_IF_EMPTY)",
    )

    _normalize_allowlist_bundle_path = field_validator(
        "allowlist_bundle_path",
        mode="before",
    )(_strip_optional_text)


class PipelinePlanSettings(BaseSettings):
    """Pipeline plan planner settings (LLM-backed pipeline definition proposals)."""

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_PLAN_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    llm_enabled: bool = Field(
        default=False,
        description="Enable pipeline planner LLM (PIPELINE_PLAN_LLM_ENABLED)",
    )


class ClientSettings(BaseSettings):
    """Internal service-to-service client settings (BFF/OMS/etc)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    oms_client_timeout_seconds: float = Field(
        default=60.0,
        description="OMS client timeout seconds (OMS_CLIENT_TIMEOUT_SECONDS)",
    )
    oms_client_token: Optional[str] = Field(
        default=None,
        description="OMS client admin token (OMS_CLIENT_TOKEN; fallback OMS_ADMIN_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)",
    )
    oms_client_debug_payload: bool = Field(
        default=False,
        description="Log OMS request payloads (OMS_CLIENT_DEBUG_PAYLOAD)",
    )
    agent_proxy_timeout_seconds: float = Field(
        default=30.0,
        description="BFF -> Agent proxy timeout seconds (AGENT_PROXY_TIMEOUT_SECONDS)",
    )
    bff_admin_token: Optional[str] = Field(
        default=None,
        description="BFF admin token for internal calls (BFF_ADMIN_TOKEN; fallback BFF_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)",
    )

    @field_validator("agent_proxy_timeout_seconds", mode="before")
    @classmethod
    def clamp_agent_proxy_timeout(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 30.0
        return max(1.0, min(value, 300.0))

    @field_validator("oms_client_token", mode="before")
    @classmethod
    def fallback_oms_client_token(cls, v):  # noqa: ANN001
        if os.getenv("OMS_CLIENT_TOKEN") not in (None, ""):
            return v
        for key in ("OMS_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
            value = (os.getenv(key) or "").strip()
            if value:
                return value
        return v

    @field_validator("bff_admin_token", mode="before")
    @classmethod
    def fallback_bff_admin_token(cls, v):  # noqa: ANN001
        if os.getenv("BFF_ADMIN_TOKEN") not in (None, ""):
            return v
        for key in ("BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
            value = (os.getenv(key) or "").strip()
            if value:
                return value
        return v


class MCPSettings(BaseSettings):
    """MCP integration settings (BFF/agent)."""

    model_config = SettingsConfigDict(
        env_prefix="MCP_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    config_path: Optional[str] = Field(
        default=None,
        description="Path to MCP config JSON (MCP_CONFIG_PATH)",
    )

    _normalize_config_path = field_validator(
        "config_path",
        mode="before",
    )(_strip_optional_text)
