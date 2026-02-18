"""
Centralized Configuration System for SPICE HARVESTER

This module provides a type-safe, centralized configuration system using Pydantic Settings
to replace scattered environment variable loading and eliminate anti-pattern 13.

Features:
- Type-safe configuration with validation
- Environment variable binding with defaults
- Hierarchical configuration structure
- Single source of truth for all settings
- Test-friendly configuration isolation
"""

import json
import logging
import os
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


def _is_docker_environment() -> bool:
    docker_env = (os.getenv("DOCKER_CONTAINER") or "").strip().lower()
    if docker_env in ("false", "0", "no", "off"):
        return False
    if docker_env in ("true", "1", "yes", "on"):
        return True
    return os.path.exists("/.dockerenv")


def _clamp_int(raw: Any, *, default: int, min_value: int = 0, max_value: int = 1_000_000) -> int:
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, value))


def _parse_boolish(raw: Any) -> Optional[bool]:
    if raw is None:
        return None
    value = str(raw).strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    return None


def _strip_optional_text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _strip_text_if_not_none(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    return str(raw).strip()


def _normalize_base_branch(raw: Any) -> str:
    return str(raw or "").strip() or "main"


def _clamp_flush_timeout_seconds(raw: Any, *, default: float = 10.0) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return max(0.1, min(value, 600.0))


def _env_truthy(name: str) -> bool:
    parsed = _parse_boolish(os.getenv(name))
    return parsed is True


def _should_load_dotenv() -> bool:
    """
    Whether settings should read from a local `.env` file.

    Security posture:
    - Disabled by default (avoid accidental secret loading/leaks).
    - Never load `.env` inside Docker.
    - Enable explicitly via SPICE_LOAD_DOTENV=true for local dev tooling.
    """

    if not _env_truthy("SPICE_LOAD_DOTENV"):
        return False
    if _is_docker_environment():
        return False
    return True


_ENV_FILE = ".env" if _should_load_dotenv() else None


class Environment(str, Enum):
    """Application environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"

class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Query guardrails
    query_max_scan_docs: int = Field(
        default=5000,
        description="Max scan docs for unsafe queries (QUERY_MAX_SCAN_DOCS)",
    )
    
    # PostgreSQL Configuration
    postgres_host: str = Field(
        default_factory=lambda: ("postgres" if _is_docker_environment() else "127.0.0.1"),
        description="PostgreSQL host"
    )
    postgres_port: int = Field(
        default_factory=lambda: (5432 if _is_docker_environment() else 5433),
        validation_alias=AliasChoices("POSTGRES_PORT", "POSTGRES_PORT_HOST"),
        description="PostgreSQL port"
    )
    postgres_user: str = Field(
        default="spiceadmin",
        description="PostgreSQL username"
    )
    postgres_password: str = Field(
        default="spicepass123",
        description="PostgreSQL password"
    )
    postgres_db: str = Field(
        default="spicedb",
        description="PostgreSQL database name"
    )
    
    # Redis Configuration
    redis_host: str = Field(
        default_factory=lambda: ("redis" if _is_docker_environment() else "127.0.0.1"),
        description="Redis host"
    )
    redis_port: int = Field(
        default=6379,
        validation_alias=AliasChoices("REDIS_PORT", "REDIS_PORT_HOST"),
        description="Redis port"
    )
    redis_password: Optional[str] = Field(
        default="spicepass123",
        description="Redis password"
    )
    
    # Elasticsearch Configuration
    elasticsearch_host: str = Field(
        default_factory=lambda: ("elasticsearch" if _is_docker_environment() else "127.0.0.1"),
        description="Elasticsearch host"
    )
    elasticsearch_port: int = Field(
        default=9200,
        validation_alias=AliasChoices("ELASTICSEARCH_PORT", "ELASTICSEARCH_PORT_HOST"),
        description="Elasticsearch port"
    )
    elasticsearch_request_timeout: int = Field(
        default=60,
        description="Elasticsearch request timeout in seconds"
    )
    elasticsearch_username: Optional[str] = Field(
        default=None,
        description="Elasticsearch username"
    )
    elasticsearch_password: Optional[str] = Field(
        default=None,
        description="Elasticsearch password"
    )
    elasticsearch_default_shards: int = Field(
        default=1,
        description="Default number of shards for new indices (ELASTICSEARCH_DEFAULT_SHARDS)",
    )
    elasticsearch_default_replicas: Optional[int] = Field(
        default=None,
        description="Default number of replicas for new indices (ELASTICSEARCH_DEFAULT_REPLICAS)",
    )
    
    # Kafka Configuration
    kafka_host: str = Field(
        default_factory=lambda: ("kafka" if _is_docker_environment() else "127.0.0.1"),
        description="Kafka host"
    )
    kafka_port: int = Field(
        default_factory=lambda: (29092 if _is_docker_environment() else 39092),
        validation_alias=AliasChoices("KAFKA_PORT", "KAFKA_PORT_HOST"),
        description="Kafka port"
    )
    kafka_bootstrap_servers: Optional[str] = Field(
        default=None,
        description="Kafka bootstrap servers (overrides host:port)"
    )

    @field_validator("elasticsearch_default_shards", mode="before")
    @classmethod
    def clamp_elasticsearch_default_shards(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1, min_value=1, max_value=1000)

    @field_validator("elasticsearch_default_replicas", mode="before")
    @classmethod
    def clamp_elasticsearch_default_replicas(cls, v):  # noqa: ANN001
        if v is None:
            return None
        raw = str(v).strip()
        if not raw:
            return None
        return _clamp_int(raw, default=0, min_value=0, max_value=1000)
    
    @property
    def postgres_url(self) -> str:
        """Construct PostgreSQL connection URL"""
        if override := (os.getenv("POSTGRES_URL") or "").strip():
            return override
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def kafka_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        if self.kafka_bootstrap_servers:
            return self.kafka_bootstrap_servers
        return f"{self.kafka_host}:{self.kafka_port}"
    
    @property
    def elasticsearch_url(self) -> str:
        """Construct Elasticsearch URL with authentication"""
        if override := (os.getenv("ELASTICSEARCH_URL") or "").strip():
            return override.rstrip("/")
        if self.elasticsearch_username and self.elasticsearch_password:
            return f"http://{self.elasticsearch_username}:{self.elasticsearch_password}@{self.elasticsearch_host}:{self.elasticsearch_port}"
        return f"http://{self.elasticsearch_host}:{self.elasticsearch_port}"
    
    @property
    def redis_url(self) -> str:
        """Construct Redis URL"""
        if override := (os.getenv("REDIS_URL") or "").strip():
            return override
        # If Redis password is empty, don't include auth
        if not self.redis_password:
            return f"redis://{self.redis_host}:{self.redis_port}"
        return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}"

class ServiceSettings(BaseSettings):
    """Service configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Service Hosts and Ports
    oms_host: str = Field(
        default_factory=lambda: ("oms" if _is_docker_environment() else "127.0.0.1"),
        description="OMS service host",
    )
    oms_port: int = Field(
        default=8000,
        description="OMS service port"
    )
    oms_base_url_override: Optional[str] = Field(
        default=None,
        description="OMS base URL override (OMS_BASE_URL)"
    )
    bff_host: str = Field(
        default_factory=lambda: ("bff" if _is_docker_environment() else "127.0.0.1"),
        description="BFF service host",
    )
    bff_port: int = Field(
        default=8002,
        description="BFF service port"
    )
    bff_base_url_override: Optional[str] = Field(
        default=None,
        description="BFF base URL override (BFF_BASE_URL)"
    )
    agent_host: str = Field(
        default_factory=lambda: ("agent" if _is_docker_environment() else "127.0.0.1"),
        description="Agent service host",
    )
    agent_port: int = Field(
        default=8004,
        description="Agent service port"
    )
    agent_base_url_override: Optional[str] = Field(
        default=None,
        description="Agent base URL override (AGENT_BASE_URL)"
    )
    
    # SSL Configuration
    use_https: bool = Field(
        default=False,
        description="Use HTTPS for service communication"
    )
    ssl_cert_path: str = Field(
        default="./ssl/common/server.crt",
        description="SSL certificate path"
    )
    ssl_key_path: str = Field(
        default="./ssl/common/server.key",
        description="SSL private key path"
    )
    ssl_ca_path: str = Field(
        default="./ssl/ca.crt",
        description="SSL CA certificate path"
    )
    verify_ssl: bool = Field(
        default=False,
        description="Verify SSL certificates"
    )
    
    # CORS Configuration
    cors_enabled: bool = Field(
        default=True,
        description="Enable CORS"
    )
    cors_origins: str = Field(
        default="",
        description="CORS allowed origins (JSON array string; CORS_ORIGINS). When unset, defaults apply."
    )
    enable_debug_endpoints: bool = Field(
        default=False,
        description="Enable opt-in debug endpoints (ENABLE_DEBUG_ENDPOINTS)"
    )
    funnel_excel_timeout_seconds: float = Field(
        default=120.0,
        description="Funnel excel timeout seconds (FUNNEL_EXCEL_TIMEOUT_SECONDS; fallback FUNNEL_CLIENT_TIMEOUT_SECONDS)",
    )
    funnel_infer_timeout_seconds: float = Field(
        default=8.0,
        description="Funnel dataset analysis timeout seconds (FUNNEL_INFER_TIMEOUT_SECONDS; fallback FUNNEL_CLIENT_TIMEOUT_SECONDS)",
    )

    @field_validator("oms_base_url_override", mode="before")
    @classmethod
    def get_oms_base_url_override(cls, v):
        if os.getenv("OMS_BASE_URL") not in (None, ""):
            return os.getenv("OMS_BASE_URL")
        fallback = (os.getenv("OMS_URL") or "").strip()
        return fallback or v

    @field_validator("bff_base_url_override", mode="before")
    @classmethod
    def get_bff_base_url_override(cls, v):
        if os.getenv("BFF_BASE_URL") not in (None, ""):
            return os.getenv("BFF_BASE_URL")
        fallback = (os.getenv("BFF_URL") or "").strip()
        return fallback or v

    @field_validator("agent_base_url_override", mode="before")
    @classmethod
    def get_agent_base_url_override(cls, v):
        if os.getenv("AGENT_BASE_URL") not in (None, ""):
            return os.getenv("AGENT_BASE_URL")
        fallback = (os.getenv("AGENT_URL") or "").strip()
        return fallback or v

    @field_validator("funnel_excel_timeout_seconds", mode="before")
    @classmethod
    def resolve_funnel_excel_timeout(cls, v):  # noqa: ANN001
        if os.getenv("FUNNEL_EXCEL_TIMEOUT_SECONDS") not in (None, ""):
            return v
        fallback = (os.getenv("FUNNEL_CLIENT_TIMEOUT_SECONDS") or "").strip()
        return fallback or v

    @field_validator("funnel_excel_timeout_seconds", mode="before")
    @classmethod
    def clamp_funnel_excel_timeout(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 120.0
        return max(5.0, min(value, 3600.0))

    @field_validator("funnel_infer_timeout_seconds", mode="before")
    @classmethod
    def resolve_funnel_infer_timeout(cls, v):  # noqa: ANN001
        if os.getenv("FUNNEL_INFER_TIMEOUT_SECONDS") not in (None, ""):
            return v
        fallback = (os.getenv("FUNNEL_CLIENT_TIMEOUT_SECONDS") or "").strip()
        return fallback or v

    @field_validator("funnel_infer_timeout_seconds", mode="before")
    @classmethod
    def clamp_funnel_infer_timeout(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 8.0
        return max(0.5, min(value, 120.0))

    @property
    def oms_base_url(self) -> str:
        """Construct OMS base URL"""
        if self.oms_base_url_override:
            return self.oms_base_url_override.rstrip("/")
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.oms_host}:{self.oms_port}"
    
    @property
    def bff_base_url(self) -> str:
        """Construct BFF base URL"""
        if self.bff_base_url_override:
            return self.bff_base_url_override.rstrip("/")
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.bff_host}:{self.bff_port}"
    
    @property
    def agent_base_url(self) -> str:
        """Construct Agent base URL"""
        if self.agent_base_url_override:
            return self.agent_base_url_override.rstrip("/")
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.agent_host}:{self.agent_port}"
    
    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS origins from JSON string"""
        value = str(self.cors_origins or "").strip()
        if not value:
            return []
        try:
            import json
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return ["*"]  # Fallback to allow all origins


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


class ObservabilitySettings(BaseSettings):
    """Logging/observability settings (shared across services/workers)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    log_level: str = Field(
        default="INFO",
        description="Python logging level (LOG_LEVEL)",
    )
    service_name: Optional[str] = Field(
        default=None,
        description="Service name override (SERVICE_NAME)",
    )
    hostname: Optional[str] = Field(
        default=None,
        description="Hostname override (HOSTNAME)",
    )
    enable_lineage: bool = Field(
        default=True,
        description="Enable lineage store integration (ENABLE_LINEAGE)",
    )
    lineage_fail_closed: bool = Field(
        default=True,
        description="Fail closed when lineage is unavailable/recording fails (LINEAGE_FAIL_CLOSED)",
    )
    lineage_fail_open_override: bool = Field(
        default=False,
        description="Emergency override to disable lineage fail-closed behavior (LINEAGE_FAIL_OPEN_OVERRIDE)",
    )
    enable_audit_logs: bool = Field(
        default=True,
        description="Enable audit log store integration (ENABLE_AUDIT_LOGS)",
    )
    run_id: Optional[str] = Field(
        default=None,
        description="Execution/run id (RUN_ID; fallback PIPELINE_RUN_ID/EXECUTION_ID)",
    )
    code_sha: Optional[str] = Field(
        default=None,
        description="Code SHA for audit/tracing (CODE_SHA; fallback GIT_SHA/COMMIT_SHA)",
    )
    source_version: Optional[str] = Field(
        default=None,
        description="Build/source version identifier (SOURCE_VERSION)",
    )
    enterprise_catalog_ref: Optional[str] = Field(
        default=None,
        description="Enterprise error catalog ref (ENTERPRISE_CATALOG_REF)",
    )

    # OpenTelemetry configuration (used by tracing/metrics helpers)
    otel_service_name: Optional[str] = Field(
        default=None,
        description="OpenTelemetry service name (OTEL_SERVICE_NAME)",
    )
    otel_service_version: str = Field(
        default="1.0.0",
        description="OpenTelemetry service version (OTEL_SERVICE_VERSION)",
    )
    otel_environment: Optional[str] = Field(
        default=None,
        description="OpenTelemetry environment override (OTEL_ENVIRONMENT)",
    )
    otel_exporter_otlp_endpoint: Optional[str] = Field(
        default=None,
        description="OTLP exporter endpoint (OTEL_EXPORTER_OTLP_ENDPOINT)",
    )
    jaeger_endpoint: str = Field(
        default="localhost:14250",
        description="Jaeger exporter endpoint (JAEGER_ENDPOINT)",
    )
    otel_trace_sample_rate: float = Field(
        default=1.0,
        description="Trace sample rate 0.0-1.0 (OTEL_TRACE_SAMPLE_RATE)",
    )
    otel_export_console: bool = Field(
        default=False,
        description="Export spans to console (OTEL_EXPORT_CONSOLE)",
    )
    otel_export_otlp: Optional[bool] = Field(
        default=None,
        description=(
            "Export spans via OTLP (OTEL_EXPORT_OTLP). "
            "If unset, enabled when OTEL_EXPORTER_OTLP_ENDPOINT is set."
        ),
    )
    otel_export_jaeger: bool = Field(
        default=False,
        description="Export spans via Jaeger (OTEL_EXPORT_JAEGER)",
    )
    otel_enable_tracing: bool = Field(
        default=True,
        description="Enable tracing (OTEL_ENABLE_TRACING)",
    )
    otel_instrument_asyncio: bool = Field(
        default=False,
        description="Enable asyncio instrumentation (OTEL_INSTRUMENT_ASYNCIO)",
    )
    otel_enable_metrics: bool = Field(
        default=False,
        description="Enable metrics (OTEL_ENABLE_METRICS)",
    )
    otel_metric_export_interval_seconds: float = Field(
        default=10.0,
        description="Metrics export interval seconds (OTEL_METRIC_EXPORT_INTERVAL_SECONDS)",
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, v):  # noqa: ANN001
        raw = str(v or "").strip().upper()
        return raw or "INFO"

    _normalize_optional_identity_fields = field_validator(
        "service_name",
        "hostname",
        "source_version",
        "enterprise_catalog_ref",
        "otel_service_name",
        "otel_environment",
        "otel_exporter_otlp_endpoint",
        mode="before",
    )(_strip_optional_text)

    @field_validator("run_id", mode="before")
    @classmethod
    def fallback_run_id(cls, v):  # noqa: ANN001
        if os.getenv("RUN_ID") not in (None, ""):
            return v
        for key in ("PIPELINE_RUN_ID", "EXECUTION_ID"):
            value = (os.getenv(key) or "").strip()
            if value:
                return value
        return v

    @field_validator("code_sha", mode="before")
    @classmethod
    def fallback_code_sha(cls, v):  # noqa: ANN001
        if os.getenv("CODE_SHA") not in (None, ""):
            return v
        for key in ("GIT_SHA", "COMMIT_SHA"):
            value = (os.getenv(key) or "").strip()
            if value:
                return value
        return v

    @field_validator("otel_service_version", mode="before")
    @classmethod
    def normalize_otel_service_version(cls, v):  # noqa: ANN001
        value = str(v or "").strip()
        return value or "1.0.0"

    @field_validator("jaeger_endpoint", mode="before")
    @classmethod
    def normalize_jaeger_endpoint(cls, v):  # noqa: ANN001
        value = str(v or "").strip()
        return value or "localhost:14250"

    @field_validator("otel_trace_sample_rate", mode="before")
    @classmethod
    def clamp_trace_sample_rate(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 1.0
        if value < 0.0:
            return 0.0
        if value > 1.0:
            return 1.0
        return value

    @field_validator("otel_export_otlp", mode="before")
    @classmethod
    def parse_otel_export_otlp(cls, v):  # noqa: ANN001
        return _parse_boolish(v)

    @field_validator("otel_metric_export_interval_seconds", mode="before")
    @classmethod
    def clamp_metric_export_interval_seconds(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 10.0
        return max(1.0, min(value, 3600.0))

    @property
    def service_name_effective(self) -> str:
        return (
            (self.service_name or "").strip()
            or (self.otel_service_name or "").strip()
            or "spice-harvester"
        )

    @property
    def lineage_required_effective(self) -> bool:
        return bool(
            self.enable_lineage
            and self.lineage_fail_closed
            and (not self.lineage_fail_open_override)
        )

    @property
    def enterprise_catalog_ref_effective(self) -> str:
        return (
            (self.enterprise_catalog_ref or "").strip()
            or (self.code_sha or "").strip()
            or (self.source_version or "").strip()
            or (self.otel_service_version or "").strip()
            or ""
        )

    @property
    def otel_export_otlp_effective(self) -> bool:
        if self.otel_export_otlp is not None:
            return bool(self.otel_export_otlp)
        return bool((self.otel_exporter_otlp_endpoint or "").strip())


class GraphQuerySettings(BaseSettings):
    """Graph query guardrails (graph federation)."""

    model_config = SettingsConfigDict(
        env_prefix="GRAPH_QUERY_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    max_hops: int = Field(default=10, description="Max hops allowed (GRAPH_QUERY_MAX_HOPS)")
    max_limit: int = Field(default=1000, description="Max limit allowed (GRAPH_QUERY_MAX_LIMIT)")
    max_paths: int = Field(default=2000, description="Max paths cap (GRAPH_QUERY_MAX_PATHS)")
    enforce_semantics: bool = Field(default=True, description="Enforce semantics (GRAPH_QUERY_ENFORCE_SEMANTICS)")

    @field_validator("max_hops", mode="before")
    @classmethod
    def clamp_max_hops(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10, min_value=1, max_value=1000)

    @field_validator("max_limit", mode="before")
    @classmethod
    def clamp_max_limit(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1000, min_value=1, max_value=1_000_000)

    @field_validator("max_paths", mode="before")
    @classmethod
    def clamp_max_paths(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2000, min_value=1, max_value=1_000_000)


class FeatureFlagsSettings(BaseSettings):
    """Feature flags / opt-in endpoints."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enable_pipeline_control_plane_events: bool = Field(
        default=True,
        description="Enable pipeline control-plane events (ENABLE_PIPELINE_CONTROL_PLANE_EVENTS)",
    )
    enable_sync_database_create_fallback: bool = Field(
        default=True,
        description="Enable synchronous database-create fallback (ENABLE_SYNC_DATABASE_CREATE_FALLBACK)",
    )
    enable_oms_rollback: bool = Field(
        default=False,
        description="Enable OMS rollback endpoints (ENABLE_OMS_ROLLBACK)",
    )
class PipelineSettings(BaseSettings):
    """Pipeline Builder + pipeline worker settings."""

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Pipeline Builder governance
    require_proposals: bool = Field(
        default=False,
        description="Require proposals on protected branches (PIPELINE_REQUIRE_PROPOSALS)",
    )
    protected_branches: str = Field(
        default="main",
        description="Comma-separated protected branches (PIPELINE_PROTECTED_BRANCHES)",
    )
    fallback_branches: str = Field(
        default="main",
        description="Comma-separated fallback branches for dataset resolution (PIPELINE_FALLBACK_BRANCHES)",
    )

    # Distributed locks (Redis)
    locks_enabled: bool = Field(
        default=True,
        description="Enable pipeline locks (PIPELINE_LOCKS_ENABLED)",
    )
    locks_required: bool = Field(
        default=True,
        description="Require lock acquisition (PIPELINE_LOCKS_REQUIRED)",
    )
    lock_ttl_seconds: int = Field(
        default=3600,
        description="Lock TTL seconds (PIPELINE_LOCK_TTL_SECONDS)",
    )
    lock_renew_seconds: int = Field(
        default=300,
        description="Lock renew interval seconds (PIPELINE_LOCK_RENEW_SECONDS)",
    )
    lock_retry_seconds: int = Field(
        default=5,
        description="Lock acquire retry sleep seconds (PIPELINE_LOCK_RETRY_SECONDS)",
    )
    lock_acquire_timeout_seconds: int = Field(
        default=3600,
        description="Worker lock acquire timeout seconds (PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS)",
    )
    publish_lock_acquire_timeout_seconds: int = Field(
        default=30,
        description=(
            "BFF publish lock acquire timeout seconds "
            "(PIPELINE_PUBLISH_LOCK_ACQUIRE_TIMEOUT_SECONDS; fallback PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS)"
        ),
    )

    # Pipeline execution toggles
    lakefs_diff_enabled: bool = Field(
        default=True,
        description="Enable lakeFS diff during pipeline execution (PIPELINE_LAKEFS_DIFF_ENABLED)",
    )
    spark_ansi_enabled: bool = Field(
        default=True,
        description="Enable Spark ANSI mode (PIPELINE_SPARK_ANSI_ENABLED)",
    )
    spark_adaptive_enabled: bool = Field(
        default=True,
        description="Enable Spark Adaptive Query Execution (PIPELINE_SPARK_ADAPTIVE_ENABLED)",
    )
    spark_shuffle_partitions: int = Field(
        default=16,
        description="Default Spark shuffle partitions (PIPELINE_SPARK_SHUFFLE_PARTITIONS)",
    )
    spark_console_progress: bool = Field(
        default=False,
        description="Show Spark console progress bars (PIPELINE_SPARK_CONSOLE_PROGRESS)",
    )
    spark_executor_threads: int = Field(
        default=1,
        description="Spark executor thread pool size (PIPELINE_SPARK_EXECUTOR_THREADS)",
    )
    spark_streaming_enabled: bool = Field(
        default=True,
        description="Enable Spark Structured Streaming input mode (PIPELINE_SPARK_STREAMING_ENABLED)",
    )
    spark_streaming_default_trigger: str = Field(
        default="available_now",
        description="Default trigger for streaming external inputs: available_now|once (PIPELINE_SPARK_STREAMING_DEFAULT_TRIGGER)",
    )
    spark_streaming_await_timeout_seconds: int = Field(
        default=120,
        description="Streaming query await timeout seconds for external inputs (PIPELINE_SPARK_STREAMING_AWAIT_TIMEOUT_SECONDS)",
    )
    kafka_schema_registry_timeout_seconds: int = Field(
        default=10,
        description=(
            "HTTP timeout seconds for kafka AVRO schema-registry resolution "
            "(PIPELINE_KAFKA_SCHEMA_REGISTRY_TIMEOUT_SECONDS)"
        ),
    )
    cast_mode: str = Field(
        default="SAFE_NULL",
        description="Casting policy: SAFE_NULL or STRICT (PIPELINE_CAST_MODE)",
    )
    udf_require_reference: bool = Field(
        default=True,
        description="Require UDF metadata to use udfId(+udfVersion) without inline udfCode (PIPELINE_UDF_REQUIRE_REFERENCE)",
    )
    udf_require_version_pinning: bool = Field(
        default=True,
        description="Require udfVersion pinning for UDF transforms during validation/preflight (PIPELINE_UDF_REQUIRE_VERSION_PINNING)",
    )
    udf_preflight_require_existence: bool = Field(
        default=True,
        description="Require preflight to verify UDF reference existence/version in registry (PIPELINE_UDF_PREFLIGHT_REQUIRE_EXISTENCE)",
    )
    udf_spark_parity_enabled: bool = Field(
        default=True,
        description="Enable Spark execution parity for UDF transforms (PIPELINE_UDF_SPARK_PARITY_ENABLED)",
    )
    preflight_fail_closed: bool = Field(
        default=True,
        description="Fail closed when pipeline preflight throws internal errors (PIPELINE_PREFLIGHT_FAIL_CLOSED)",
    )
    artifact_path: str = Field(
        default="data/pipeline_artifacts",
        description="Local pipeline artifact path root (PIPELINE_ARTIFACT_PATH)",
    )

    # Worker identity / retries
    jobs_group: str = Field(
        default="pipeline-worker-group",
        description="Kafka consumer group id (PIPELINE_JOBS_GROUP)",
    )
    job_queue_flush_timeout_seconds: float = Field(
        default=5.0,
        description="Kafka producer flush timeout seconds (PIPELINE_JOB_QUEUE_FLUSH_TIMEOUT_SECONDS)",
    )
    worker_handler: str = Field(
        default="pipeline_worker",
        description="Worker handler label (PIPELINE_WORKER_HANDLER)",
    )
    worker_name: str = Field(
        default="pipeline_worker",
        description="Worker service name label (PIPELINE_WORKER_NAME)",
    )
    jobs_max_retries: int = Field(
        default=5,
        description="Pipeline job max retries (PIPELINE_JOBS_MAX_RETRIES)",
    )
    jobs_backoff_base_seconds: int = Field(
        default=2,
        description="Pipeline job retry backoff base seconds (PIPELINE_JOBS_BACKOFF_BASE_SECONDS)",
    )
    jobs_backoff_max_seconds: int = Field(
        default=60,
        description="Pipeline job retry backoff max seconds (PIPELINE_JOBS_BACKOFF_MAX_SECONDS)",
    )
    jobs_max_poll_interval_ms: int = Field(
        default=3600000,
        description="Kafka max.poll.interval.ms for pipeline worker jobs (PIPELINE_JOBS_MAX_POLL_INTERVAL_MS)",
    )
    processed_event_lease_timeout_seconds: int = Field(
        default=120,
        description=(
            "Processed-event lease timeout seconds for pipeline worker idempotency "
            "(PIPELINE_PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS)"
        ),
    )

    # Scheduler
    scheduler_poll_seconds: int = Field(
        default=30,
        description="Pipeline scheduler poll interval seconds (PIPELINE_SCHEDULER_POLL_SECONDS)",
    )

    _normalize_pipeline_strings = field_validator(
        "protected_branches",
        "fallback_branches",
        "jobs_group",
        "artifact_path",
        "worker_handler",
        "worker_name",
        "spark_streaming_default_trigger",
        mode="before",
    )(_strip_text_if_not_none)

    @field_validator("publish_lock_acquire_timeout_seconds", mode="before")
    @classmethod
    def fallback_publish_lock_timeout(cls, v):  # noqa: ANN001
        # Backward compatible: historically BFF used PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS.
        if os.getenv("PIPELINE_PUBLISH_LOCK_ACQUIRE_TIMEOUT_SECONDS") not in (None, ""):
            return v
        compat_fallback = (os.getenv("PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS") or "").strip()
        return compat_fallback or v

    @field_validator("jobs_max_retries", mode="before")
    @classmethod
    def clamp_jobs_max_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)

    @field_validator("job_queue_flush_timeout_seconds", mode="before")
    @classmethod
    def clamp_job_queue_flush_timeout_seconds(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 5.0
        return max(0.1, min(value, 60.0))

    @field_validator("jobs_backoff_base_seconds", mode="before")
    @classmethod
    def clamp_jobs_backoff_base_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2, min_value=0, max_value=300)

    @field_validator("jobs_backoff_max_seconds", mode="before")
    @classmethod
    def clamp_jobs_backoff_max_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)

    @field_validator("spark_executor_threads", mode="before")
    @classmethod
    def clamp_spark_executor_threads(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1, min_value=1, max_value=128)

    @field_validator("spark_shuffle_partitions", mode="before")
    @classmethod
    def clamp_spark_shuffle_partitions(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=16, min_value=1, max_value=5000)

    @field_validator("spark_streaming_await_timeout_seconds", mode="before")
    @classmethod
    def clamp_spark_streaming_await_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=120, min_value=1, max_value=86_400)

    @field_validator("kafka_schema_registry_timeout_seconds", mode="before")
    @classmethod
    def clamp_kafka_schema_registry_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10, min_value=1, max_value=120)

    @field_validator("spark_streaming_default_trigger", mode="before")
    @classmethod
    def normalize_spark_streaming_default_trigger(cls, v):  # noqa: ANN001
        text = str(v or "").strip().lower() or "available_now"
        if text in {"available_now", "availablenow", "available-now"}:
            return "available_now"
        if text == "once":
            return "once"
        return "available_now"

    @field_validator("lock_ttl_seconds", mode="before")
    @classmethod
    def clamp_lock_ttl_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=60, max_value=86_400)

    @field_validator("lock_renew_seconds", mode="before")
    @classmethod
    def clamp_lock_renew_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=300, min_value=10, max_value=3_600)

    @field_validator("lock_retry_seconds", mode="before")
    @classmethod
    def clamp_lock_retry_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=600)

    @field_validator("lock_acquire_timeout_seconds", mode="before")
    @classmethod
    def clamp_lock_acquire_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=30, max_value=86_400)

    @field_validator("publish_lock_acquire_timeout_seconds", mode="before")
    @classmethod
    def clamp_publish_lock_acquire_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30, min_value=5, max_value=3_600)

    @field_validator("scheduler_poll_seconds", mode="before")
    @classmethod
    def clamp_scheduler_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30, min_value=1, max_value=3_600)

    @property
    def protected_branches_set(self) -> set[str]:
        raw = str(self.protected_branches or "").strip() or "main"
        branches = {branch.strip() for branch in raw.split(",") if branch.strip()}
        branches.add("main")
        return branches

    @property
    def fallback_branches_list(self) -> List[str]:
        raw = str(self.fallback_branches or "").strip() or "main"
        branches = [branch.strip() for branch in raw.split(",") if branch.strip()]
        if "main" not in branches:
            branches.append("main")
        return branches


class OntologySettings(BaseSettings):
    """Ontology API + linter governance settings."""

    model_config = SettingsConfigDict(
        env_prefix="ONTOLOGY_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    protected_branches: str = Field(
        default="main,master,production,prod",
        description="Protected branches (ONTOLOGY_PROTECTED_BRANCHES)",
    )
    require_proposals: bool = Field(
        default=True,
        description="Require proposals on protected branches (ONTOLOGY_REQUIRE_PROPOSALS)",
    )
    validate_relationships: bool = Field(
        default=True,
        description="Validate relationship structure (ONTOLOGY_VALIDATE_RELATIONSHIPS)",
    )
    resource_strict: bool = Field(
        default=True,
        description="Strict resource checks (ONTOLOGY_RESOURCE_STRICT)",
    )
    require_health_gate: bool = Field(
        default=True,
        description="Require health gate for destructive ops (ONTOLOGY_REQUIRE_HEALTH_GATE)",
    )
    resource_storage_backend: str = Field(
        default="postgres",
        description=(
            "Ontology resource storage backend "
            "(ONTOLOGY_RESOURCE_STORAGE_BACKEND: postgres)"
        ),
    )

    # Linter strictness defaults (domain-neutral)
    require_primary_key: bool = Field(
        default=True,
        description="Require primary key (ONTOLOGY_REQUIRE_PRIMARY_KEY)",
    )
    allow_implicit_primary_key: Optional[bool] = Field(
        default=None,
        description=(
            "Allow implicit primary key (ONTOLOGY_ALLOW_IMPLICIT_PRIMARY_KEY). "
            "When unset, defaults to false in production and on protected branches requiring proposals."
        ),
    )
    require_title_key: bool = Field(
        default=True,
        description="Require title key (ONTOLOGY_REQUIRE_TITLE_KEY)",
    )
    allow_implicit_title_key: Optional[bool] = Field(
        default=None,
        description=(
            "Allow implicit title key (ONTOLOGY_ALLOW_IMPLICIT_TITLE_KEY). "
            "When unset, defaults to false in production and on protected branches requiring proposals."
        ),
    )
    block_event_like_class_names: bool = Field(
        default=False,
        description="Block event-like class names (ONTOLOGY_BLOCK_EVENT_LIKE_CLASS)",
    )
    enforce_snake_case_fields: bool = Field(
        default=False,
        description="Enforce snake_case field names (ONTOLOGY_ENFORCE_SNAKE_CASE_FIELDS)",
    )

    @field_validator("protected_branches", mode="before")
    @classmethod
    def strip_protected_branches(cls, v):  # noqa: ANN001
        return str(v or "").strip() or "main,master,production,prod"

    @field_validator("allow_implicit_primary_key", "allow_implicit_title_key", mode="before")
    @classmethod
    def parse_optional_bool(cls, v):  # noqa: ANN001
        return _parse_boolish(v)

    @field_validator("resource_storage_backend", mode="before")
    @classmethod
    def normalize_resource_storage_backend(cls, v):  # noqa: ANN001
        backend = str(v or "").strip().lower()
        if backend and backend != "postgres":
            logger.warning(
                "Ignoring unsupported ontology backend profile (%s); runtime backend is fixed to postgres",
                backend,
            )
        return "postgres"

    @property
    def protected_branches_set(self) -> set[str]:
        raw = str(self.protected_branches or "").strip()
        branches = {b.strip() for b in raw.split(",") if b.strip()}
        return branches or {"main", "master", "production", "prod"}

    def allow_implicit_primary_key_effective(self, *, is_production: bool, branch: Optional[str] = None) -> bool:
        if self.allow_implicit_primary_key is not None:
            return bool(self.allow_implicit_primary_key)
        if is_production:
            return False
        if self.require_proposals and branch and branch in self.protected_branches_set:
            return False
        return True

    def allow_implicit_title_key_effective(self, *, is_production: bool, branch: Optional[str] = None) -> bool:
        if self.allow_implicit_title_key is not None:
            return bool(self.allow_implicit_title_key)
        if is_production:
            return False
        if self.require_proposals and branch and branch in self.protected_branches_set:
            return False
        return True


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

    # Context uploads (CTX-006)
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


class AuthSettings(BaseSettings):
    """Service auth configuration (BFF/OMS)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Token sources (inbound auth)
    bff_admin_token: Optional[str] = Field(
        default=None,
        description="BFF admin token(s), comma-separated for rotation (BFF_ADMIN_TOKEN)",
    )
    bff_write_token: Optional[str] = Field(
        default=None,
        description="BFF write token(s), comma-separated for rotation (BFF_WRITE_TOKEN)",
    )
    bff_agent_token: Optional[str] = Field(
        default=None,
        description="BFF internal agent token(s), comma-separated for rotation (BFF_AGENT_TOKEN)",
    )

    oms_admin_token: Optional[str] = Field(
        default=None,
        description="OMS admin token(s), comma-separated for rotation (OMS_ADMIN_TOKEN)",
    )
    oms_write_token: Optional[str] = Field(
        default=None,
        description="OMS write token(s), comma-separated for rotation (OMS_WRITE_TOKEN)",
    )

    admin_api_key: Optional[str] = Field(
        default=None,
        description="Unified admin API key(s), comma-separated for rotation (ADMIN_API_KEY)",
    )
    admin_token: Optional[str] = Field(
        default=None,
        description="Unified admin token(s), comma-separated for rotation (ADMIN_TOKEN)",
    )

    # End-user auth (JWT/OIDC) - enterprise mode
    user_jwt_enabled: bool = Field(
        default=False,
        description="Enable end-user JWT auth (USER_JWT_ENABLED)",
    )
    user_jwt_issuer: Optional[str] = Field(
        default=None,
        description="Expected JWT issuer (USER_JWT_ISSUER)",
    )
    user_jwt_audience: Optional[str] = Field(
        default=None,
        description="Expected JWT audience (USER_JWT_AUDIENCE)",
    )
    user_jwt_jwks_url: Optional[str] = Field(
        default=None,
        description="JWKS URL for JWT verification (USER_JWT_JWKS_URL)",
    )
    user_jwt_public_key: Optional[str] = Field(
        default=None,
        description="PEM public key for JWT verification (USER_JWT_PUBLIC_KEY)",
    )
    user_jwt_hs256_secret: Optional[str] = Field(
        default=None,
        description="HS256 shared secret(s), comma-separated for rotation (USER_JWT_HS256_SECRET)",
    )
    user_jwt_algorithms: Optional[str] = Field(
        default=None,
        description="Comma-separated accepted JWT algorithms (USER_JWT_ALGORITHMS)",
    )

    # Require flags (Optional so 'unset' can use heuristics)
    bff_require_auth: Optional[bool] = Field(
        default=None,
        description="Require auth for BFF (BFF_REQUIRE_AUTH)",
    )
    oms_require_auth: Optional[bool] = Field(
        default=None,
        description="Require auth for OMS (OMS_REQUIRE_AUTH)",
    )
    bff_require_db_scope: bool = Field(
        default=False,
        description="Require db scope headers for BFF writes (BFF_REQUIRE_DB_SCOPE)",
    )
    bff_require_db_access: Optional[bool] = Field(
        default=None,
        description="Require database access roles for BFF writes (BFF_REQUIRE_DB_ACCESS)",
    )

    # Disable-approval flags (fail-closed)
    allow_insecure_bff_auth_disable: bool = Field(
        default=False,
        description="Allow disabling BFF auth (ALLOW_INSECURE_BFF_AUTH_DISABLE)",
    )
    allow_insecure_oms_auth_disable: bool = Field(
        default=False,
        description="Allow disabling OMS auth (ALLOW_INSECURE_OMS_AUTH_DISABLE)",
    )
    allow_insecure_auth_disable: bool = Field(
        default=False,
        description="Allow disabling auth globally (ALLOW_INSECURE_AUTH_DISABLE)",
    )

    # Development-only auth bypass (master principal)
    dev_master_auth_enabled: bool = Field(
        default=False,
        description="Development-only: allow bypassing auth failures by attaching a master principal (DEV_MASTER_AUTH_ENABLED)",
    )
    dev_master_user_id: str = Field(
        default="dev-admin",
        description="Development-only: master principal user id (DEV_MASTER_USER_ID)",
    )
    dev_master_user_type: str = Field(
        default="user",
        description="Development-only: master principal type (DEV_MASTER_USER_TYPE)",
    )
    dev_master_roles: Optional[str] = Field(
        default="admin,platform_admin",
        description="Development-only: comma-separated roles for the master principal (DEV_MASTER_ROLES)",
    )

    # Exempt paths
    bff_auth_exempt_paths: Optional[str] = Field(
        default=None,
        description="Comma-separated exempt paths for BFF (BFF_AUTH_EXEMPT_PATHS)",
    )
    oms_auth_exempt_paths: Optional[str] = Field(
        default=None,
        description="Comma-separated exempt paths for OMS (OMS_AUTH_EXEMPT_PATHS)",
    )

    _normalize_auth_strings = field_validator(
        "bff_admin_token",
        "bff_write_token",
        "bff_agent_token",
        "oms_admin_token",
        "oms_write_token",
        "admin_api_key",
        "admin_token",
        "user_jwt_issuer",
        "user_jwt_audience",
        "user_jwt_jwks_url",
        "user_jwt_public_key",
        "user_jwt_hs256_secret",
        "user_jwt_algorithms",
        "bff_auth_exempt_paths",
        "oms_auth_exempt_paths",
        "dev_master_user_id",
        "dev_master_user_type",
        "dev_master_roles",
        mode="before",
    )(_strip_optional_text)

    @property
    def bff_auth_disable_allowed(self) -> bool:
        return bool(self.allow_insecure_bff_auth_disable or self.allow_insecure_auth_disable)

    @property
    def oms_auth_disable_allowed(self) -> bool:
        return bool(self.allow_insecure_oms_auth_disable or self.allow_insecure_auth_disable)

    @staticmethod
    def _split_tokens(raw: Optional[str]) -> tuple[str, ...]:
        value = str(raw or "").strip()
        if not value:
            return ()
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @classmethod
    def _tokens_from_values(cls, *values: Optional[str]) -> tuple[str, ...]:
        tokens: list[str] = []
        for value in values:
            tokens.extend(cls._split_tokens(value))
        return tuple(tokens)

    @property
    def bff_expected_tokens(self) -> tuple[str, ...]:
        return self._tokens_from_values(self.bff_admin_token, self.bff_write_token, self.admin_api_key, self.admin_token)

    @property
    def bff_agent_tokens(self) -> tuple[str, ...]:
        return self._split_tokens(self.bff_agent_token)

    @property
    def oms_expected_tokens(self) -> tuple[str, ...]:
        return self._tokens_from_values(self.oms_admin_token, self.oms_write_token, self.admin_api_key, self.admin_token)

    @property
    def bff_expected_token(self) -> Optional[str]:
        tokens = self.bff_expected_tokens
        if tokens:
            return tokens[0]
        agent_tokens = self.bff_agent_tokens
        if agent_tokens:
            return agent_tokens[0]
        return None

    @property
    def bff_admin_only_token(self) -> Optional[str]:
        tokens = self._tokens_from_values(self.bff_admin_token, self.admin_api_key, self.admin_token)
        return tokens[0] if tokens else None

    @property
    def oms_expected_token(self) -> Optional[str]:
        tokens = self.oms_expected_tokens
        return tokens[0] if tokens else None

    @property
    def admin_bypass_tokens(self) -> set[str]:
        tokens = self._tokens_from_values(
            self.bff_admin_token,
            self.bff_write_token,
            self.oms_admin_token,
            self.oms_write_token,
            self.admin_api_key,
            self.admin_token,
        )
        return set(tokens)

    def is_bff_auth_required(self, *, allow_pytest: bool, default_required: bool = True) -> bool:
        if self.bff_require_auth is not None:
            return bool(self.bff_require_auth)
        if allow_pytest and os.environ.get("PYTEST_CURRENT_TEST"):
            return False
        if self.bff_expected_tokens or self.bff_agent_tokens:
            return True
        return bool(default_required)

    def is_oms_auth_required(self, *, default_required: bool = True) -> bool:
        if self.oms_require_auth is not None:
            return bool(self.oms_require_auth)
        if self.oms_expected_tokens:
            return True
        return bool(default_required)

    @staticmethod
    def _parse_exempt_paths(raw: Optional[str], defaults: tuple[str, ...]) -> set[str]:
        value = (raw or "").strip()
        if not value:
            return set(defaults)
        paths = {path.strip() for path in value.split(",") if path.strip()}
        return paths or set(defaults)

    def resolve_bff_exempt_paths(self, *, defaults: tuple[str, ...]) -> set[str]:
        return self._parse_exempt_paths(self.bff_auth_exempt_paths, defaults)

    def resolve_oms_exempt_paths(self, *, defaults: tuple[str, ...]) -> set[str]:
        return self._parse_exempt_paths(self.oms_auth_exempt_paths, defaults)

    @property
    def dev_master_role_set(self) -> tuple[str, ...]:
        return self._split_tokens(self.dev_master_roles)


class RateLimitSettings(BaseSettings):
    """Rate limiter runtime configuration."""

    model_config = SettingsConfigDict(
        env_prefix="RATE_LIMIT_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    fail_open: bool = Field(
        default=False,
        description="Fail-open when Redis is unavailable (RATE_LIMIT_FAIL_OPEN)",
    )
    local_max_entries: int = Field(
        default=10_000,
        description="Local fallback cache max entries (RATE_LIMIT_LOCAL_MAX_ENTRIES)",
    )

    @field_validator("local_max_entries", mode="before")
    @classmethod
    def clamp_local_max_entries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10_000, min_value=0, max_value=1_000_000)


class MessagingSettings(BaseSettings):
    """Kafka topic/group configuration settings"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Topics (keep env var names stable: *_TOPIC)
    instance_events_topic: str = Field(
        default="instance_events",
        description="Kafka topic for instance domain events (INSTANCE_EVENTS_TOPIC)",
    )
    ontology_events_topic: str = Field(
        default="ontology_events",
        description="Kafka topic for ontology domain events (ONTOLOGY_EVENTS_TOPIC)",
    )
    action_events_topic: str = Field(
        default="action_events",
        description="Kafka topic for action events (ACTION_EVENTS_TOPIC)",
    )
    projection_dlq_topic: str = Field(
        default="projection_failures_dlq",
        description="Kafka DLQ topic for projection failures (PROJECTION_DLQ_TOPIC)",
    )
    connector_updates_topic: str = Field(
        default="connector-updates",
        description="Kafka topic for connector updates (CONNECTOR_UPDATES_TOPIC)",
    )
    connector_updates_dlq_topic: str = Field(
        default="connector-updates-dlq",
        description="Kafka DLQ topic for connector updates (CONNECTOR_UPDATES_DLQ_TOPIC)",
    )
    pipeline_jobs_topic: str = Field(
        default="pipeline-jobs",
        description="Kafka topic for pipeline jobs (PIPELINE_JOBS_TOPIC)",
    )
    pipeline_jobs_dlq_topic: str = Field(
        default="pipeline-jobs-dlq",
        description="Kafka DLQ topic for pipeline jobs (PIPELINE_JOBS_DLQ_TOPIC)",
    )
    pipeline_events_topic: str = Field(
        default="pipeline-events",
        description="Kafka topic for pipeline control-plane events (PIPELINE_EVENTS_TOPIC)",
    )
    dataset_ingest_outbox_dlq_topic: str = Field(
        default="dataset-ingest-outbox-dlq",
        description="Kafka DLQ topic for dataset ingest outbox (DATASET_INGEST_OUTBOX_DLQ_TOPIC)",
    )
    objectify_jobs_topic: str = Field(
        default="objectify-jobs",
        description="Kafka topic for objectify jobs (OBJECTIFY_JOBS_TOPIC)",
    )
    objectify_jobs_dlq_topic: str = Field(
        default="objectify-jobs-dlq",
        description="Kafka DLQ topic for objectify jobs (OBJECTIFY_JOBS_DLQ_TOPIC)",
    )

    # Command topics
    instance_commands_topic: str = Field(
        default="instance_commands",
        description="Kafka topic for instance commands (INSTANCE_COMMANDS_TOPIC)",
    )
    ontology_commands_topic: str = Field(
        default="ontology_commands",
        description="Kafka topic for ontology commands (ONTOLOGY_COMMANDS_TOPIC)",
    )
    database_commands_topic: str = Field(
        default="database_commands",
        description="Kafka topic for database commands (DATABASE_COMMANDS_TOPIC)",
    )
    action_commands_topic: str = Field(
        default="action_commands",
        description="Kafka topic for action commands (ACTION_COMMANDS_TOPIC)",
    )
    instance_commands_dlq_topic: str = Field(
        default="instance-commands-dlq",
        description="Kafka DLQ topic for instance commands (INSTANCE_COMMANDS_DLQ_TOPIC)",
    )
    ontology_commands_dlq_topic: str = Field(
        default="ontology-commands-dlq",
        description="Kafka DLQ topic for ontology commands (ONTOLOGY_COMMANDS_DLQ_TOPIC)",
    )
    action_commands_dlq_topic: str = Field(
        default="action-commands-dlq",
        description="Kafka DLQ topic for action commands (ACTION_COMMANDS_DLQ_TOPIC)",
    )

    # Consumer groups
    projection_worker_group: str = Field(
        default="projection-worker-group",
        description="Kafka consumer group for projection worker (PROJECTION_WORKER_GROUP)",
    )
    message_relay_group: str = Field(
        default="message-relay-group",
        description="Kafka consumer group for message relay (MESSAGE_RELAY_GROUP)",
    )
    instance_worker_group: str = Field(
        default="instance-worker-group",
        description="Kafka consumer group for instance worker (INSTANCE_WORKER_GROUP)",
    )
    ontology_worker_group: str = Field(
        default="ontology-worker-group",
        description="Kafka consumer group for ontology worker (ONTOLOGY_WORKER_GROUP)",
    )
    action_worker_group: str = Field(
        default="action-worker-group",
        description="Kafka consumer group for action worker (ACTION_WORKER_GROUP)",
    )
    objectify_jobs_group: str = Field(
        default="objectify-worker-group",
        description="Kafka consumer group for objectify jobs (OBJECTIFY_JOBS_GROUP)",
    )

class StorageSettings(BaseSettings):
    """Storage configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # MinIO/S3 Configuration
    minio_endpoint_url: str = Field(
        default_factory=lambda: (
            "http://spice-minio:9000" if _is_docker_environment() else "http://127.0.0.1:9000"
        ),
        validation_alias=AliasChoices("MINIO_ENDPOINT_URL", "MINIO_PORT_HOST"),
        description="MinIO/S3 endpoint URL"
    )
    minio_access_key: str = Field(
        default="minioadmin",
        description="MinIO/S3 access key"
    )
    minio_secret_key: str = Field(
        default="minioadmin123",
        description="MinIO/S3 secret key"
    )
    minio_ssl_verify: Optional[bool] = Field(
        default=None,
        description=(
            "Verify SSL certificates for MinIO/S3 when using https (MINIO_SSL_VERIFY). "
            "When unset, botocore defaults apply."
        ),
    )
    minio_ssl_ca_bundle: Optional[str] = Field(
        default=None,
        description="CA bundle path for MinIO/S3 TLS verification (MINIO_SSL_CA_BUNDLE).",
    )

    # lakeFS (S3 gateway + REST auth shares credentials)
    lakefs_api_url: Optional[str] = Field(
        default=None,
        description="lakeFS API base URL (e.g. http://localhost:48080)",
    )
    lakefs_s3_endpoint_url: Optional[str] = Field(
        default=None,
        description="lakeFS S3 gateway endpoint URL (e.g. http://localhost:8000)",
    )
    lakefs_api_port: int = Field(
        default_factory=lambda: (8000 if _is_docker_environment() else 48080),
        validation_alias=AliasChoices("LAKEFS_API_PORT", "LAKEFS_PORT_HOST"),
        description="lakeFS API port (used when LAKEFS_API_URL is unset)",
    )

    @field_validator("minio_endpoint_url", mode="before")
    @classmethod
    def normalize_minio_endpoint_url(cls, v):  # noqa: ANN001
        if v is None:
            return v
        value = str(v).strip()
        if not value:
            return value
        if value.isdigit() and not _is_docker_environment():
            port = _clamp_int(value, default=9000, min_value=1, max_value=65535)
            return f"http://127.0.0.1:{port}"
        return value
    lakefs_access_key_id: Optional[str] = Field(
        default=None,
        description="lakeFS access key id",
    )
    lakefs_secret_access_key: Optional[str] = Field(
        default=None,
        description="lakeFS secret access key",
    )
    lakefs_s3_ssl_verify: Optional[bool] = Field(
        default=None,
        description="Verify SSL certificates for lakeFS S3 gateway when using https (LAKEFS_S3_SSL_VERIFY).",
    )
    lakefs_s3_ssl_ca_bundle: Optional[str] = Field(
        default=None,
        description="CA bundle path for lakeFS S3 gateway TLS verification (LAKEFS_S3_SSL_CA_BUNDLE).",
    )
    lakefs_client_timeout_seconds: float = Field(
        default=120.0,
        description="lakeFS REST client timeout seconds (LAKEFS_CLIENT_TIMEOUT_SECONDS; fallback LAKEFS_TIMEOUT_SECONDS)",
    )
    lakefs_credentials_source: Optional[str] = Field(
        default=None,
        description="Where to load lakeFS credentials from (LAKEFS_CREDENTIALS_SOURCE: env|db)",
    )
    lakefs_service_principal: Optional[str] = Field(
        default=None,
        description="Service principal identifier for lakeFS credentials (LAKEFS_SERVICE_PRINCIPAL)",
    )
    lakefs_credentials_encryption_key: Optional[str] = Field(
        default=None,
        description="Fernet key for encrypting lakeFS secrets in Postgres (LAKEFS_CREDENTIALS_ENCRYPTION_KEY)",
    )

    lakefs_raw_repository: str = Field(
        default="raw-datasets",
        description="lakeFS repository for raw datasets (LAKEFS_RAW_REPOSITORY)",
    )
    lakefs_artifacts_repository: str = Field(
        default="pipeline-artifacts",
        description="lakeFS repository for pipeline artifacts (LAKEFS_ARTIFACTS_REPOSITORY)",
    )
    
    # S3 Buckets
    event_store_bucket: str = Field(
        default="spice-event-store",
        description="S3 bucket for immutable event store (EVENT_STORE_BUCKET)",
    )
    instance_bucket: str = Field(
        default="instance-events",
        description="S3 bucket for instance events"
    )
    timeseries_bucket: str = Field(
        default="timeseries-data",
        description="S3 bucket for time series property data (TIMESERIES_BUCKET)",
    )
    attachments_bucket: str = Field(
        default="attachments-data",
        description="S3 bucket for attachment property files (ATTACHMENTS_BUCKET)",
    )

    # Local SQLite / filesystem paths
    label_mappings_db_path: str = Field(
        default="data/label_mappings.db",
        description="SQLite path for label mappings (LABEL_MAPPINGS_DB_PATH)",
    )

    @field_validator("lakefs_client_timeout_seconds", mode="before")
    @classmethod
    def clamp_lakefs_client_timeout(cls, v):  # noqa: ANN001
        if os.getenv("LAKEFS_CLIENT_TIMEOUT_SECONDS") not in (None, ""):
            return v
        fallback = (os.getenv("LAKEFS_TIMEOUT_SECONDS") or "").strip()
        return fallback or v

    @field_validator("lakefs_credentials_source", mode="before")
    @classmethod
    def normalize_lakefs_credentials_source(cls, v):  # noqa: ANN001
        value = str(v or "").strip().lower()
        if not value:
            return None
        if value in {"db", "database"}:
            return "db"
        if value in {"env", "environment"}:
            return "env"
        return None
    
    @property
    def use_ssl(self) -> bool:
        """Determine if SSL should be used based on endpoint URL"""
        return self.minio_endpoint_url.startswith("https://")

    @property
    def lakefs_api_url_effective(self) -> str:
        """Return lakeFS API base URL (without /api/v1)."""
        if self.lakefs_api_url:
            return self.lakefs_api_url.rstrip("/")
        host = "lakefs" if _is_docker_environment() else "127.0.0.1"
        return f"http://{host}:{self.lakefs_api_port}"

    @property
    def lakefs_s3_endpoint_effective(self) -> str:
        """Return lakeFS S3 Gateway endpoint URL."""
        if self.lakefs_s3_endpoint_url:
            return self.lakefs_s3_endpoint_url.rstrip("/")
        return self.lakefs_api_url_effective


class CacheSettings(BaseSettings):
    """Cache and TTL configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Cache TTL Settings (in seconds)
    class_label_cache_ttl: int = Field(
        default=3600,
        description="Class label cache TTL in seconds"
    )
    command_status_cache_ttl: int = Field(
        default=86400,
        description="Command status cache TTL in seconds"
    )
    command_status_ttl_seconds: int = Field(
        default=0,
        description="TTL seconds for command status entries (COMMAND_STATUS_TTL_SECONDS; 0=never expire)",
    )
    user_session_cache_ttl: int = Field(
        default=7200,
        description="User session cache TTL in seconds"
    )
    websocket_connection_ttl: int = Field(
        default=3600,
        description="WebSocket connection TTL in seconds"
    )
    mapping_cache_ttl: int = Field(
        default=1800,
        description="Mapping cache TTL in seconds"
    )

    @field_validator("command_status_ttl_seconds", mode="before")
    @classmethod
    def clamp_command_status_ttl_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=0, min_value=0, max_value=31_536_000)


class SecuritySettings(BaseSettings):
    """Security configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # JWT Configuration
    secret_key: str = Field(
        default="your-secret-key-change-this-in-production",
        description="JWT secret key"
    )
    algorithm: str = Field(
        default="HS256",
        description="JWT algorithm"
    )
    access_token_expire_minutes: int = Field(
        default=30,
        description="JWT access token expiry in minutes"
    )

    # Data protection (SEC-004/SEC-005)
    data_encryption_keys: str = Field(
        default="",
        description="Comma-separated base64/hex keys for encrypting stored data at rest (DATA_ENCRYPTION_KEYS)",
    )

    # Input sanitizer limits
    input_sanitizer_max_dict_keys: int = Field(
        default=100,
        description="Max dict keys to sanitize (INPUT_SANITIZER_MAX_DICT_KEYS)",
    )
    input_sanitizer_max_list_items: int = Field(
        default=1000,
        description="Max list items to sanitize (INPUT_SANITIZER_MAX_LIST_ITEMS)",
    )

    # Input validation limits
    max_search_query_length: int = Field(
        default=100,
        description="Maximum search query length (MAX_SEARCH_QUERY_LENGTH)",
    )
    max_db_name_length: int = Field(
        default=50,
        description="Maximum database name length (MAX_DB_NAME_LENGTH)",
    )
    max_class_id_length: int = Field(
        default=100,
        description="Maximum class id length (MAX_CLASS_ID_LENGTH)",
    )
    max_instance_id_length: int = Field(
        default=255,
        description="Maximum instance id length (MAX_INSTANCE_ID_LENGTH)",
    )
    max_client_id_length: int = Field(
        default=50,
        description="Maximum websocket client id length (MAX_CLIENT_ID_LENGTH)",
    )
    max_user_id_length: int = Field(
        default=50,
        description="Maximum user id length (MAX_USER_ID_LENGTH)",
    )


class PerformanceSettings(BaseSettings):
    """Performance and optimization settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Connection Pool Settings
    database_pool_size: int = Field(
        default=20,
        description="Database connection pool size"
    )
    database_max_overflow: int = Field(
        default=30,
        description="Database max overflow connections"
    )
    
    # Rate Limiting
    enable_rate_limiting: bool = Field(
        default=True,
        description="Enable API rate limiting"
    )
    requests_per_minute: int = Field(
        default=60,
        description="Requests per minute limit"
    )

    # Concurrency limits
    max_concurrent_commands: int = Field(
        default=100,
        description="Max concurrent commands (MAX_CONCURRENT_COMMANDS)",
    )
    max_websocket_connections: int = Field(
        default=1000,
        description="Max websocket connections (MAX_WEBSOCKET_CONNECTIONS)",
    )

    # Payload size limits (bytes)
    max_ontology_size: int = Field(
        default=10485760,
        description="Max ontology payload size in bytes (MAX_ONTOLOGY_SIZE)",
    )
    max_instance_size: int = Field(
        default=1048576,
        description="Max instance payload size in bytes (MAX_INSTANCE_SIZE)",
    )

    # Postgres pool tuning (per-registry overrides)
    action_log_pg_pool_min: int = Field(default=1, description="ACTION_LOG_PG_POOL_MIN")
    action_log_pg_pool_max: int = Field(default=5, description="ACTION_LOG_PG_POOL_MAX")
    action_log_pg_command_timeout_seconds: int = Field(
        default=30,
        description="ACTION_LOG_PG_COMMAND_TIMEOUT",
        validation_alias="ACTION_LOG_PG_COMMAND_TIMEOUT",
    )

    connector_registry_pg_pool_min: int = Field(default=1, description="CONNECTOR_REGISTRY_PG_POOL_MIN")
    connector_registry_pg_pool_max: int = Field(default=5, description="CONNECTOR_REGISTRY_PG_POOL_MAX")
    connector_registry_pg_command_timeout_seconds: int = Field(
        default=30,
        description="CONNECTOR_REGISTRY_PG_COMMAND_TIMEOUT",
        validation_alias="CONNECTOR_REGISTRY_PG_COMMAND_TIMEOUT",
    )

    dataset_registry_pg_pool_min: int = Field(default=1, description="DATASET_REGISTRY_PG_POOL_MIN")
    dataset_registry_pg_pool_max: int = Field(default=5, description="DATASET_REGISTRY_PG_POOL_MAX")
    dataset_registry_pg_command_timeout_seconds: int = Field(
        default=30,
        description="DATASET_REGISTRY_PG_COMMAND_TIMEOUT",
        validation_alias="DATASET_REGISTRY_PG_COMMAND_TIMEOUT",
    )

    pipeline_registry_pg_pool_min: int = Field(default=1, description="PIPELINE_REGISTRY_PG_POOL_MIN")
    pipeline_registry_pg_pool_max: int = Field(default=5, description="PIPELINE_REGISTRY_PG_POOL_MAX")
    pipeline_registry_pg_command_timeout_seconds: int = Field(
        default=30,
        description="PIPELINE_REGISTRY_PG_COMMAND_TIMEOUT",
        validation_alias="PIPELINE_REGISTRY_PG_COMMAND_TIMEOUT",
    )

    objectify_pg_pool_min: int = Field(default=1, description="OBJECTIFY_PG_POOL_MIN")
    objectify_pg_pool_max: int = Field(default=5, description="OBJECTIFY_PG_POOL_MAX")
    objectify_pg_command_timeout_seconds: int = Field(
        default=30,
        description="OBJECTIFY_PG_COMMAND_TIMEOUT",
        validation_alias="OBJECTIFY_PG_COMMAND_TIMEOUT",
    )

    audit_pg_pool_min: int = Field(default=1, description="AUDIT_PG_POOL_MIN")
    audit_pg_pool_max: int = Field(default=5, description="AUDIT_PG_POOL_MAX")
    audit_pg_command_timeout_seconds: int = Field(
        default=30,
        description="AUDIT_PG_COMMAND_TIMEOUT",
        validation_alias="AUDIT_PG_COMMAND_TIMEOUT",
    )

    lineage_pg_pool_min: int = Field(default=1, description="LINEAGE_PG_POOL_MIN")
    lineage_pg_pool_max: int = Field(default=5, description="LINEAGE_PG_POOL_MAX")
    lineage_pg_command_timeout_seconds: int = Field(
        default=30,
        description="LINEAGE_PG_COMMAND_TIMEOUT",
        validation_alias="LINEAGE_PG_COMMAND_TIMEOUT",
    )
    lineage_latest_edges_max_ids: int = Field(default=5000, description="LINEAGE_LATEST_EDGES_MAX_IDS")

    agg_seq_pg_pool_min: int = Field(default=1, description="AGG_SEQ_PG_POOL_MIN")
    agg_seq_pg_pool_max: int = Field(default=5, description="AGG_SEQ_PG_POOL_MAX")
    agg_seq_pg_command_timeout_seconds: int = Field(
        default=30,
        description="AGG_SEQ_PG_COMMAND_TIMEOUT",
        validation_alias="AGG_SEQ_PG_COMMAND_TIMEOUT",
    )

    @field_validator(
        "action_log_pg_pool_min",
        "connector_registry_pg_pool_min",
        "dataset_registry_pg_pool_min",
        "pipeline_registry_pg_pool_min",
        "objectify_pg_pool_min",
        "audit_pg_pool_min",
        "lineage_pg_pool_min",
        "agg_seq_pg_pool_min",
        mode="before",
    )
    @classmethod
    def clamp_pg_pool_min(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1, min_value=1, max_value=100)

    @field_validator(
        "action_log_pg_pool_max",
        "connector_registry_pg_pool_max",
        "dataset_registry_pg_pool_max",
        "pipeline_registry_pg_pool_max",
        "objectify_pg_pool_max",
        "audit_pg_pool_max",
        "lineage_pg_pool_max",
        "agg_seq_pg_pool_max",
        mode="before",
    )
    @classmethod
    def clamp_pg_pool_max(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=500)

    @field_validator(
        "action_log_pg_command_timeout_seconds",
        "connector_registry_pg_command_timeout_seconds",
        "dataset_registry_pg_command_timeout_seconds",
        "pipeline_registry_pg_command_timeout_seconds",
        "objectify_pg_command_timeout_seconds",
        "audit_pg_command_timeout_seconds",
        "lineage_pg_command_timeout_seconds",
        "agg_seq_pg_command_timeout_seconds",
        mode="before",
    )
    @classmethod
    def clamp_pg_command_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30, min_value=1, max_value=3600)

    @field_validator("lineage_latest_edges_max_ids", mode="before")
    @classmethod
    def clamp_lineage_latest_edges_max_ids(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5000, min_value=1, max_value=5_000_000)


class EventSourcingSettings(BaseSettings):
    """Event sourcing / CQRS tuning settings"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    event_store_retention_days: int = Field(
        default=365,
        description="Event store retention in days (EVENT_STORE_RETENTION_DAYS)",
    )
    projection_batch_size: int = Field(
        default=1000,
        description="Projection worker batch size (PROJECTION_BATCH_SIZE)",
    )
    projection_scroll_timeout: str = Field(
        default="5m",
        description="Elasticsearch scroll timeout for projections (PROJECTION_SCROLL_TIMEOUT)",
    )
    command_timeout_seconds: int = Field(
        default=300,
        description="Command processing timeout in seconds (COMMAND_TIMEOUT_SECONDS)",
    )
    command_retry_count: int = Field(
        default=3,
        description="Command retry count (COMMAND_RETRY_COUNT)",
    )
    enable_event_sourcing: bool = Field(
        default=True,
        description="Enable event sourcing (ENABLE_EVENT_SOURCING)",
    )
    enable_processed_event_registry: bool = Field(
        default=True,
        description="Enable durable processed-event registry (ENABLE_PROCESSED_EVENT_REGISTRY)",
    )
    processed_event_lease_timeout_seconds: int = Field(
        default=900,
        description="Processed-event lease timeout seconds (PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS)",
    )
    processed_event_heartbeat_interval_seconds: int = Field(
        default=30,
        description="Processed-event heartbeat interval seconds (PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS)",
    )
    processed_event_pg_pool_min: int = Field(
        default=1,
        description="Processed-event Postgres pool min size (PROCESSED_EVENT_PG_POOL_MIN)",
    )
    processed_event_pg_pool_max: int = Field(
        default=5,
        description="Processed-event Postgres pool max size (PROCESSED_EVENT_PG_POOL_MAX)",
    )
    processed_event_pg_command_timeout: int = Field(
        default=30,
        description="Processed-event Postgres command timeout seconds (PROCESSED_EVENT_PG_COMMAND_TIMEOUT)",
    )
    processed_event_owner: Optional[str] = Field(
        default=None,
        description="Override processed-event registry owner id (PROCESSED_EVENT_OWNER)",
    )

    # Event Store (S3/MinIO) runtime controls
    event_store_sequence_allocator_mode: str = Field(
        default="postgres",
        description="Sequence allocator mode (EVENT_STORE_SEQUENCE_ALLOCATOR_MODE)",
    )
    event_store_sequence_schema: str = Field(
        default="spice_event_registry",
        description="Sequence schema name (EVENT_STORE_SEQUENCE_SCHEMA)",
    )
    event_store_sequence_handler_prefix: str = Field(
        default="write_side",
        description="Sequence handler prefix (EVENT_STORE_SEQUENCE_HANDLER_PREFIX)",
    )
    event_store_idempotency_mismatch_mode: str = Field(
        default="error",
        description="Idempotency mismatch handling (EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE)",
    )
    event_store_require_tls: bool = Field(
        default=False,
        description="Require TLS for event store endpoint (EVENT_STORE_REQUIRE_TLS)",
    )
    event_store_ssl_verify: bool = Field(
        default=True,
        description="Verify TLS certs for event store (EVENT_STORE_SSL_VERIFY)",
    )
    event_store_s3_addressing_style: Optional[str] = Field(
        default=None,
        description="Force S3 addressing style (EVENT_STORE_S3_ADDRESSING_STYLE)",
    )

    @field_validator(
        "event_store_sequence_allocator_mode",
        "event_store_sequence_schema",
        "event_store_sequence_handler_prefix",
        "event_store_idempotency_mismatch_mode",
        "event_store_s3_addressing_style",
        mode="before",
    )
    @classmethod
    def normalize_event_store_strings(cls, v):  # noqa: ANN001
        if v is None:
            return None
        return str(v).strip()


class BranchVirtualizationSettings(BaseSettings):
    """Branch virtualization defaults (OCC seeding)."""

    model_config = SettingsConfigDict(
        env_prefix="BRANCH_VIRTUALIZATION_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    base_branch: str = Field(
        default="main",
        description="Base branch to seed OCC when branch has no local events (BRANCH_VIRTUALIZATION_BASE_BRANCH)",
    )
    _normalize_base_branch = field_validator("base_branch", mode="before")(_normalize_base_branch)


class InstanceWorkerSettings(BaseSettings):
    """Instance worker runtime settings."""

    model_config = SettingsConfigDict(
        env_prefix="INSTANCE_WORKER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    dlq_flush_timeout_seconds: float = Field(
        default=10.0,
        description="DLQ flush timeout seconds (INSTANCE_WORKER_DLQ_FLUSH_TIMEOUT_SECONDS)",
    )
    max_retry_attempts: int = Field(
        default=5,
        description="Max retry attempts for command processing (INSTANCE_WORKER_MAX_RETRY_ATTEMPTS)",
    )
    allow_pk_generation: bool = Field(
        default=False,
        description="Allow generating missing PKs (INSTANCE_ALLOW_PK_GENERATION)",
    )
    relationship_strict: bool = Field(
        default=True,
        description="Strict relationship schema enforcement (INSTANCE_RELATIONSHIP_STRICT)",
    )
    untyped_ref_max_retry_attempts: int = Field(
        default=30,
        description="Max retry attempts for untyped ref witness errors (INSTANCE_WORKER_UNTYPED_REF_MAX_RETRY_ATTEMPTS)",
    )
    untyped_ref_backoff_max_seconds: float = Field(
        default=15.0,
        description="Max backoff seconds for untyped ref witness errors (INSTANCE_WORKER_UNTYPED_REF_BACKOFF_MAX_SECONDS)",
    )

    @field_validator("allow_pk_generation", mode="before")
    @classmethod
    def fallback_allow_pk_generation(cls, v):  # noqa: ANN001
        compat_fallback = _parse_boolish(os.getenv("INSTANCE_ALLOW_PK_GENERATION"))
        return compat_fallback if compat_fallback is not None else v

    @field_validator("relationship_strict", mode="before")
    @classmethod
    def fallback_relationship_strict(cls, v):  # noqa: ANN001
        compat_fallback = _parse_boolish(os.getenv("INSTANCE_RELATIONSHIP_STRICT"))
        return compat_fallback if compat_fallback is not None else v

    @field_validator("max_retry_attempts", mode="before")
    @classmethod
    def clamp_max_retry_attempts(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)

    @field_validator("untyped_ref_max_retry_attempts", mode="before")
    @classmethod
    def clamp_untyped_ref_max_retry_attempts(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30, min_value=1, max_value=500)

    @field_validator("untyped_ref_backoff_max_seconds", mode="before")
    @classmethod
    def clamp_untyped_ref_backoff_max_seconds(cls, v):  # noqa: ANN001
        try:
            value = float(v)
        except (TypeError, ValueError):
            return 15.0
        return max(0.1, min(value, 300.0))


class OntologyWorkerSettings(BaseSettings):
    """Ontology worker runtime settings."""

    model_config = SettingsConfigDict(
        env_prefix="ONTOLOGY_WORKER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    dlq_flush_timeout_seconds: float = Field(
        default=10.0,
        description="DLQ flush timeout seconds (ONTOLOGY_WORKER_DLQ_FLUSH_TIMEOUT_SECONDS)",
    )
    db_ready_poll_seconds: float = Field(
        default=0.5,
        description="DB readiness poll seconds (ONTOLOGY_WORKER_DB_READY_POLL_SECONDS)",
    )
    max_retry_attempts: int = Field(
        default=5,
        description="Max retry attempts (ONTOLOGY_WORKER_MAX_RETRY_ATTEMPTS)",
    )

    @field_validator("max_retry_attempts", mode="before")
    @classmethod
    def clamp_max_retry_attempts(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)


class ProjectionWorkerSettings(BaseSettings):
    """Projection worker runtime settings."""

    model_config = SettingsConfigDict(
        env_prefix="PROJECTION_WORKER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    dlq_flush_timeout_seconds: float = Field(
        default=10.0,
        description="DLQ flush timeout seconds (PROJECTION_WORKER_DLQ_FLUSH_TIMEOUT_SECONDS)",
    )
    max_retries: int = Field(
        default=5,
        description="Max retries for projection worker (PROJECTION_WORKER_MAX_RETRIES)",
    )

    @field_validator("max_retries", mode="before")
    @classmethod
    def clamp_max_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)


class ActionWorkerSettings(BaseSettings):
    """Action worker runtime settings."""

    model_config = SettingsConfigDict(
        env_prefix="ACTION_WORKER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    dlq_flush_timeout_seconds: float = Field(
        default=10.0,
        description="DLQ flush timeout seconds (ACTION_WORKER_DLQ_FLUSH_TIMEOUT_SECONDS)",
    )
    dlq_retries: int = Field(
        default=10,
        description="DLQ producer retries (ACTION_WORKER_DLQ_RETRIES)",
    )
    max_retry_attempts: int = Field(
        default=5,
        description="Max retry attempts (ACTION_WORKER_MAX_RETRY_ATTEMPTS)",
    )

    @field_validator("dlq_retries", mode="before")
    @classmethod
    def clamp_dlq_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10, min_value=0, max_value=1_000_000)

    @field_validator("max_retry_attempts", mode="before")
    @classmethod
    def clamp_max_retry_attempts(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)


class ActionOutboxSettings(BaseSettings):
    """Action outbox worker settings."""

    model_config = SettingsConfigDict(
        env_prefix="ACTION_OUTBOX_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    poll_seconds: float = Field(
        default=2.0,
        description="Poll interval seconds (ACTION_OUTBOX_POLL_SECONDS)",
    )
    batch_size: int = Field(
        default=100,
        description="Batch size for each scan (ACTION_OUTBOX_BATCH_SIZE)",
    )

    @field_validator("batch_size", mode="before")
    @classmethod
    def clamp_batch_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=100, min_value=1, max_value=10_000_000)


class OntologyDeployOutboxSettings(BaseSettings):
    """Ontology deployment outbox worker settings (OMS embedded worker)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=True,
        description="Enable ontology deploy outbox worker (ENABLE_ONTOLOGY_DEPLOY_OUTBOX_WORKER)",
        validation_alias="ENABLE_ONTOLOGY_DEPLOY_OUTBOX_WORKER",
    )
    use_deployments_v2: bool = Field(
        default=True,
        description="Use deployments registry v2 (ONTOLOGY_DEPLOYMENTS_V2)",
        validation_alias="ONTOLOGY_DEPLOYMENTS_V2",
    )
    poll_seconds: int = Field(
        default=5,
        description="Poll interval seconds (ONTOLOGY_DEPLOY_OUTBOX_POLL_SECONDS)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_POLL_SECONDS",
    )
    batch_size: int = Field(
        default=50,
        description="Batch size (ONTOLOGY_DEPLOY_OUTBOX_BATCH)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_BATCH",
    )
    claim_timeout_seconds: int = Field(
        default=300,
        description="Claim timeout seconds (ONTOLOGY_DEPLOY_OUTBOX_CLAIM_TIMEOUT_SECONDS)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_CLAIM_TIMEOUT_SECONDS",
    )
    backoff_base_seconds: int = Field(
        default=2,
        description="Backoff base seconds (ONTOLOGY_DEPLOY_OUTBOX_BACKOFF_BASE_SECONDS)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_BACKOFF_BASE_SECONDS",
    )
    backoff_max_seconds: int = Field(
        default=60,
        description="Backoff max seconds (ONTOLOGY_DEPLOY_OUTBOX_BACKOFF_MAX_SECONDS)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_BACKOFF_MAX_SECONDS",
    )
    retention_days: int = Field(
        default=7,
        description="Retention days (ONTOLOGY_DEPLOY_OUTBOX_RETENTION_DAYS)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_RETENTION_DAYS",
    )
    purge_interval_seconds: int = Field(
        default=3600,
        description="Purge interval seconds (ONTOLOGY_DEPLOY_OUTBOX_PURGE_INTERVAL_SECONDS)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_PURGE_INTERVAL_SECONDS",
    )
    purge_limit: int = Field(
        default=10_000,
        description="Purge batch limit (ONTOLOGY_DEPLOY_OUTBOX_PURGE_LIMIT)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_PURGE_LIMIT",
    )
    worker_id: Optional[str] = Field(
        default=None,
        description="Worker id override (ONTOLOGY_DEPLOY_OUTBOX_WORKER_ID)",
        validation_alias="ONTOLOGY_DEPLOY_OUTBOX_WORKER_ID",
    )

    @field_validator("poll_seconds", mode="before")
    @classmethod
    def clamp_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=3600)

    @field_validator("batch_size", mode="before")
    @classmethod
    def clamp_batch_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=50, min_value=1, max_value=5000)

    @field_validator("claim_timeout_seconds", mode="before")
    @classmethod
    def clamp_claim_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=300, min_value=0, max_value=86_400)

    @field_validator("backoff_base_seconds", mode="before")
    @classmethod
    def clamp_backoff_base_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2, min_value=1, max_value=300)

    @field_validator("backoff_max_seconds", mode="before")
    @classmethod
    def clamp_backoff_max_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)

    @field_validator("retention_days", mode="before")
    @classmethod
    def clamp_retention_days(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=7, min_value=0, max_value=365)

    @field_validator("purge_interval_seconds", mode="before")
    @classmethod
    def clamp_purge_interval_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=300, max_value=86_400)

    @field_validator("purge_limit", mode="before")
    @classmethod
    def clamp_purge_limit(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10_000, min_value=1, max_value=100_000)


class ConnectorSyncSettings(BaseSettings):
    """Connector sync worker settings."""

    model_config = SettingsConfigDict(
        env_prefix="CONNECTOR_SYNC_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    group: str = Field(
        default="connector-sync-worker-group",
        description="Kafka consumer group id (CONNECTOR_SYNC_GROUP)",
    )
    handler: str = Field(
        default="connector_sync_worker",
        description="Handler label (CONNECTOR_SYNC_HANDLER)",
    )
    max_retries: int = Field(
        default=5,
        description="Max retries (CONNECTOR_SYNC_MAX_RETRIES)",
    )
    backoff_base_seconds: int = Field(
        default=2,
        description="Backoff base seconds (CONNECTOR_SYNC_BACKOFF_BASE_SECONDS)",
    )
    backoff_max_seconds: int = Field(
        default=60,
        description="Backoff max seconds (CONNECTOR_SYNC_BACKOFF_MAX_SECONDS)",
    )

    _normalize_sync_identity = field_validator(
        "group",
        "handler",
        mode="before",
    )(_strip_text_if_not_none)

    @field_validator("max_retries", mode="before")
    @classmethod
    def clamp_max_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)

    @field_validator("backoff_base_seconds", mode="before")
    @classmethod
    def clamp_backoff_base_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2, min_value=0, max_value=300)

    @field_validator("backoff_max_seconds", mode="before")
    @classmethod
    def clamp_backoff_max_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)


class ConnectorTriggerSettings(BaseSettings):
    """Connector trigger service settings."""

    model_config = SettingsConfigDict(
        env_prefix="CONNECTOR_TRIGGER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    source_type: str = Field(
        default="google_sheets",
        description="Connector trigger source type (CONNECTOR_TRIGGER_SOURCE_TYPE)",
    )
    tick_seconds: int = Field(
        default=5,
        description="Tick interval seconds (CONNECTOR_TRIGGER_TICK_SECONDS)",
    )
    poll_concurrency: int = Field(
        default=5,
        description="Poll concurrency (CONNECTOR_TRIGGER_POLL_CONCURRENCY)",
    )
    outbox_batch: int = Field(
        default=50,
        description="Outbox batch size (CONNECTOR_TRIGGER_OUTBOX_BATCH)",
    )

    @field_validator("source_type", mode="before")
    @classmethod
    def strip_source_type(cls, v):  # noqa: ANN001
        return str(v or "").strip() or "google_sheets"

    @field_validator("tick_seconds", mode="before")
    @classmethod
    def clamp_tick_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=3600)

    @field_validator("poll_concurrency", mode="before")
    @classmethod
    def clamp_poll_concurrency(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)

    @field_validator("outbox_batch", mode="before")
    @classmethod
    def clamp_outbox_batch(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=50, min_value=1, max_value=500)


class ObjectifySettings(BaseSettings):
    """Objectify worker settings."""

    model_config = SettingsConfigDict(
        env_prefix="OBJECTIFY_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    worker_handler: str = Field(
        default="objectify_worker",
        description="Worker handler label (OBJECTIFY_WORKER_HANDLER)",
    )
    batch_size: int = Field(
        default=500,
        description="Default batch size (OBJECTIFY_BATCH_SIZE)",
    )
    row_batch_size: int = Field(
        default=1000,
        description="Row batch size (OBJECTIFY_ROW_BATCH_SIZE)",
    )
    bulk_update_batch_size: Optional[int] = Field(
        default=None,
        description="Bulk update batch size (OBJECTIFY_BULK_UPDATE_BATCH_SIZE; default OBJECTIFY_BATCH_SIZE)",
    )
    list_page_size: int = Field(
        default=1000,
        description="Pagination page size (OBJECTIFY_LIST_PAGE_SIZE)",
    )
    max_rows: int = Field(
        default=0,
        description="Max rows limit (0=unlimited) (OBJECTIFY_MAX_ROWS)",
    )
    lineage_max_links: int = Field(
        default=1000,
        description="Max lineage links emitted (OBJECTIFY_LINEAGE_MAX_LINKS)",
    )
    max_retries: int = Field(
        default=5,
        description="Max retries (OBJECTIFY_MAX_RETRIES)",
    )
    backoff_base_seconds: int = Field(
        default=2,
        description="Backoff base seconds (OBJECTIFY_BACKOFF_BASE_SECONDS)",
    )
    backoff_max_seconds: int = Field(
        default=60,
        description="Backoff max seconds (OBJECTIFY_BACKOFF_MAX_SECONDS)",
    )
    ontology_pk_validation_mode: str = Field(
        default="warn",
        description=(
            "When ontology schema declares primaryKey, validate object_type pk_spec matches "
            "(fields + order). Modes: off|warn|fail (OBJECTIFY_ONTOLOGY_PK_VALIDATION_MODE)."
        ),
    )
    dataset_primary_index_chunk_size: int = Field(
        default=500,
        description="Chunk size for dataset-primary ES bulk indexing (OBJECTIFY_DATASET_PRIMARY_INDEX_CHUNK_SIZE).",
    )
    dataset_primary_refresh: bool = Field(
        default=False,
        description="Refresh ES index after dataset-primary writes (OBJECTIFY_DATASET_PRIMARY_REFRESH).",
    )
    dataset_primary_prune_stale_on_full: bool = Field(
        default=True,
        description=(
            "When execution_mode=full, delete stale class instances not present in current dataset snapshot "
            "(OBJECTIFY_DATASET_PRIMARY_PRUNE_STALE_ON_FULL)."
        ),
    )

    @field_validator("worker_handler", mode="before")
    @classmethod
    def strip_worker_handler(cls, v):  # noqa: ANN001
        return str(v or "").strip() or "objectify_worker"

    @field_validator("batch_size", mode="before")
    @classmethod
    def clamp_batch_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=500, min_value=1, max_value=5000)

    @field_validator("row_batch_size", mode="before")
    @classmethod
    def clamp_row_batch_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1000, min_value=1, max_value=50_000)

    @field_validator("bulk_update_batch_size", mode="before")
    @classmethod
    def clamp_bulk_update_batch_size(cls, v):  # noqa: ANN001
        if v in (None, ""):
            return None
        return _clamp_int(v, default=500, min_value=1, max_value=5000)

    @field_validator("list_page_size", mode="before")
    @classmethod
    def clamp_list_page_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1000, min_value=10, max_value=10_000)

    @field_validator("max_rows", mode="before")
    @classmethod
    def clamp_max_rows(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=0, min_value=0, max_value=10_000_000)

    @field_validator("lineage_max_links", mode="before")
    @classmethod
    def clamp_lineage_max_links(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1000, min_value=0, max_value=100_000)

    @field_validator("max_retries", mode="before")
    @classmethod
    def clamp_max_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)

    @field_validator("backoff_base_seconds", mode="before")
    @classmethod
    def clamp_backoff_base_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2, min_value=0, max_value=300)

    @field_validator("backoff_max_seconds", mode="before")
    @classmethod
    def clamp_backoff_max_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)

    @field_validator("ontology_pk_validation_mode", mode="before")
    @classmethod
    def normalize_ontology_pk_validation_mode(cls, v):  # noqa: ANN001
        raw = str(v or "").strip().lower()
        if not raw:
            return "warn"
        if raw in {"0", "false", "off", "disabled", "none"}:
            return "off"
        if raw in {"1", "true", "on", "enabled", "warn", "warning"}:
            return "warn"
        if raw in {"fail", "error", "strict"}:
            return "fail"
        raise ValueError("ontology_pk_validation_mode must be one of: off, warn, fail")

    @field_validator("dataset_primary_index_chunk_size", mode="before")
    @classmethod
    def clamp_dataset_primary_index_chunk_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=500, min_value=1, max_value=10_000)

    @property
    def bulk_update_batch_size_effective(self) -> int:
        return int(self.bulk_update_batch_size or self.batch_size)


class IngestReconcilerSettings(BaseSettings):
    """Dataset ingest reconciler worker settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=True,
        description="Enable ingest reconciler (ENABLE_DATASET_INGEST_RECONCILER)",
        validation_alias="ENABLE_DATASET_INGEST_RECONCILER",
    )
    poll_seconds: int = Field(
        default=60,
        description="Poll interval seconds (DATASET_INGEST_RECONCILER_POLL_SECONDS)",
        validation_alias="DATASET_INGEST_RECONCILER_POLL_SECONDS",
    )
    stale_seconds: int = Field(
        default=3600,
        description="Stale cutoff seconds (DATASET_INGEST_RECONCILER_STALE_SECONDS)",
        validation_alias="DATASET_INGEST_RECONCILER_STALE_SECONDS",
    )
    limit: int = Field(
        default=200,
        description="Max ingest rows per run (DATASET_INGEST_RECONCILER_LIMIT)",
        validation_alias="DATASET_INGEST_RECONCILER_LIMIT",
    )
    lock_key: int = Field(
        default=910214,
        description="Postgres advisory lock key (DATASET_INGEST_RECONCILER_LOCK_KEY)",
        validation_alias="DATASET_INGEST_RECONCILER_LOCK_KEY",
    )
    alert_published_threshold: int = Field(
        default=1,
        description="Alert threshold for published count (INGEST_RECONCILER_ALERT_PUBLISHED_THRESHOLD)",
    )
    alert_aborted_threshold: int = Field(
        default=1,
        description="Alert threshold for aborted count (INGEST_RECONCILER_ALERT_ABORTED_THRESHOLD)",
    )
    alert_on_error: bool = Field(
        default=True,
        description="Alert on exceptions (INGEST_RECONCILER_ALERT_ON_ERROR)",
    )
    alert_cooldown_seconds: int = Field(
        default=300,
        description="Alert cooldown seconds (INGEST_RECONCILER_ALERT_COOLDOWN_SECONDS)",
    )
    alert_webhook_url: Optional[str] = Field(
        default=None,
        description="Webhook URL (INGEST_RECONCILER_ALERT_WEBHOOK_URL; fallback ALERT_WEBHOOK_URL)",
    )
    port: int = Field(
        default=8012,
        description="Worker port (INGEST_RECONCILER_PORT)",
        validation_alias="INGEST_RECONCILER_PORT",
    )

    @field_validator("alert_webhook_url", mode="before")
    @classmethod
    def fallback_alert_webhook_url(cls, v):  # noqa: ANN001
        value = str(v or "").strip()
        if value:
            return value
        fallback = str(os.getenv("ALERT_WEBHOOK_URL") or "").strip()
        return fallback or None

    @field_validator("poll_seconds", mode="before")
    @classmethod
    def clamp_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=5, max_value=3600)

    @field_validator("stale_seconds", mode="before")
    @classmethod
    def clamp_stale_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=60, max_value=86_400)

    @field_validator("limit", mode="before")
    @classmethod
    def clamp_limit(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=200, min_value=1, max_value=5000)

    @field_validator("lock_key", mode="before")
    @classmethod
    def clamp_lock_key(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=910214, min_value=1, max_value=2_000_000_000)

    @field_validator("alert_published_threshold", mode="before")
    @classmethod
    def clamp_alert_published_threshold(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1, min_value=0, max_value=1_000_000)

    @field_validator("alert_aborted_threshold", mode="before")
    @classmethod
    def clamp_alert_aborted_threshold(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=1, min_value=0, max_value=1_000_000)

    @field_validator("alert_cooldown_seconds", mode="before")
    @classmethod
    def clamp_alert_cooldown_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=300, min_value=0, max_value=86_400)


class DatasetIngestOutboxSettings(BaseSettings):
    """Dataset ingest outbox worker settings (BFF embedded worker)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=True,
        description="Enable dataset ingest outbox worker (ENABLE_DATASET_INGEST_OUTBOX_WORKER)",
        validation_alias="ENABLE_DATASET_INGEST_OUTBOX_WORKER",
    )
    poll_seconds: int = Field(
        default=5,
        description="Poll interval seconds (DATASET_INGEST_OUTBOX_POLL_SECONDS)",
        validation_alias="DATASET_INGEST_OUTBOX_POLL_SECONDS",
    )
    flush_timeout_seconds: float = Field(
        default=10.0,
        description="Kafka flush timeout seconds (DATASET_INGEST_OUTBOX_FLUSH_TIMEOUT_SECONDS)",
        validation_alias="DATASET_INGEST_OUTBOX_FLUSH_TIMEOUT_SECONDS",
    )
    backoff_base_seconds: int = Field(
        default=2,
        description="Retry backoff base seconds (DATASET_INGEST_OUTBOX_BACKOFF_BASE_SECONDS)",
        validation_alias="DATASET_INGEST_OUTBOX_BACKOFF_BASE_SECONDS",
    )
    backoff_max_seconds: int = Field(
        default=60,
        description="Retry backoff max seconds (DATASET_INGEST_OUTBOX_BACKOFF_MAX_SECONDS)",
        validation_alias="DATASET_INGEST_OUTBOX_BACKOFF_MAX_SECONDS",
    )
    max_retries: int = Field(
        default=5,
        description="Max retries before DLQ (DATASET_INGEST_OUTBOX_MAX_RETRIES)",
        validation_alias="DATASET_INGEST_OUTBOX_MAX_RETRIES",
    )
    claim_timeout_seconds: int = Field(
        default=300,
        description="Claim timeout seconds (DATASET_INGEST_OUTBOX_CLAIM_TIMEOUT_SECONDS)",
        validation_alias="DATASET_INGEST_OUTBOX_CLAIM_TIMEOUT_SECONDS",
    )
    worker_id: Optional[str] = Field(
        default=None,
        description="Worker id override (DATASET_INGEST_OUTBOX_WORKER_ID)",
        validation_alias="DATASET_INGEST_OUTBOX_WORKER_ID",
    )
    purge_interval_seconds: int = Field(
        default=3600,
        description="Purge interval seconds (DATASET_INGEST_OUTBOX_PURGE_INTERVAL_SECONDS)",
        validation_alias="DATASET_INGEST_OUTBOX_PURGE_INTERVAL_SECONDS",
    )
    retention_days: int = Field(
        default=7,
        description="Retention days (DATASET_INGEST_OUTBOX_RETENTION_DAYS)",
        validation_alias="DATASET_INGEST_OUTBOX_RETENTION_DAYS",
    )
    purge_limit: int = Field(
        default=10_000,
        description="Purge batch limit (DATASET_INGEST_OUTBOX_PURGE_LIMIT)",
        validation_alias="DATASET_INGEST_OUTBOX_PURGE_LIMIT",
    )
    enable_dlq: bool = Field(
        default=True,
        description="Enable DLQ publishing (ENABLE_DATASET_INGEST_OUTBOX_DLQ)",
        validation_alias="ENABLE_DATASET_INGEST_OUTBOX_DLQ",
    )
    dlq_max_in_flight: int = Field(
        default=5,
        description="DLQ producer max in-flight requests (DATASET_INGEST_OUTBOX_MAX_IN_FLIGHT)",
        validation_alias="DATASET_INGEST_OUTBOX_MAX_IN_FLIGHT",
    )
    dlq_delivery_timeout_ms: int = Field(
        default=120_000,
        description="DLQ producer delivery.timeout.ms (DATASET_INGEST_OUTBOX_DELIVERY_TIMEOUT_MS)",
        validation_alias="DATASET_INGEST_OUTBOX_DELIVERY_TIMEOUT_MS",
    )
    dlq_request_timeout_ms: int = Field(
        default=30_000,
        description="DLQ producer request.timeout.ms (DATASET_INGEST_OUTBOX_REQUEST_TIMEOUT_MS)",
        validation_alias="DATASET_INGEST_OUTBOX_REQUEST_TIMEOUT_MS",
    )
    dlq_retries: int = Field(
        default=5,
        description="DLQ producer retries (DATASET_INGEST_OUTBOX_RETRIES)",
        validation_alias="DATASET_INGEST_OUTBOX_RETRIES",
    )

    @field_validator("poll_seconds", mode="before")
    @classmethod
    def clamp_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=3600)

    _normalize_flush_timeout_seconds = field_validator(
        "flush_timeout_seconds",
        mode="before",
    )(_clamp_flush_timeout_seconds)

    @field_validator("backoff_base_seconds", mode="before")
    @classmethod
    def clamp_backoff_base_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2, min_value=1, max_value=300)

    @field_validator("backoff_max_seconds", mode="before")
    @classmethod
    def clamp_backoff_max_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)

    @field_validator("max_retries", mode="before")
    @classmethod
    def clamp_max_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=100)

    @field_validator("claim_timeout_seconds", mode="before")
    @classmethod
    def clamp_claim_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=300, min_value=0, max_value=86_400)

    @field_validator("purge_interval_seconds", mode="before")
    @classmethod
    def clamp_purge_interval_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=300, max_value=86_400)

    @field_validator("retention_days", mode="before")
    @classmethod
    def clamp_retention_days(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=7, min_value=0, max_value=365)

    @field_validator("purge_limit", mode="before")
    @classmethod
    def clamp_purge_limit(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10_000, min_value=1, max_value=100_000)

    @field_validator("dlq_max_in_flight", mode="before")
    @classmethod
    def clamp_dlq_max_in_flight(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=5)

    @field_validator("dlq_delivery_timeout_ms", mode="before")
    @classmethod
    def clamp_dlq_delivery_timeout_ms(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=120_000, min_value=10_000, max_value=1_800_000)

    @field_validator("dlq_request_timeout_ms", mode="before")
    @classmethod
    def clamp_dlq_request_timeout_ms(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30_000, min_value=5_000, max_value=600_000)

    @field_validator("dlq_retries", mode="before")
    @classmethod
    def clamp_dlq_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=0, max_value=100)


class ObjectifyOutboxWorkerSettings(BaseSettings):
    """Objectify outbox worker settings (BFF embedded worker)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=True,
        description="Enable objectify outbox worker (ENABLE_OBJECTIFY_OUTBOX_WORKER)",
        validation_alias="ENABLE_OBJECTIFY_OUTBOX_WORKER",
    )
    poll_seconds: int = Field(
        default=5,
        description="Poll interval seconds (OBJECTIFY_OUTBOX_POLL_SECONDS)",
        validation_alias="OBJECTIFY_OUTBOX_POLL_SECONDS",
    )
    batch_size: int = Field(
        default=50,
        description="Batch size per scan (OBJECTIFY_OUTBOX_BATCH)",
        validation_alias="OBJECTIFY_OUTBOX_BATCH",
    )
    flush_timeout_seconds: float = Field(
        default=10.0,
        description="Kafka flush timeout seconds (OBJECTIFY_OUTBOX_FLUSH_TIMEOUT_SECONDS)",
        validation_alias="OBJECTIFY_OUTBOX_FLUSH_TIMEOUT_SECONDS",
    )
    backoff_base_seconds: int = Field(
        default=2,
        description="Retry backoff base seconds (OBJECTIFY_OUTBOX_BACKOFF_BASE_SECONDS)",
        validation_alias="OBJECTIFY_OUTBOX_BACKOFF_BASE_SECONDS",
    )
    backoff_max_seconds: int = Field(
        default=60,
        description="Retry backoff max seconds (OBJECTIFY_OUTBOX_BACKOFF_MAX_SECONDS)",
        validation_alias="OBJECTIFY_OUTBOX_BACKOFF_MAX_SECONDS",
    )
    claim_timeout_seconds: int = Field(
        default=300,
        description="Claim timeout seconds (OBJECTIFY_OUTBOX_CLAIM_TIMEOUT_SECONDS)",
        validation_alias="OBJECTIFY_OUTBOX_CLAIM_TIMEOUT_SECONDS",
    )
    worker_id: Optional[str] = Field(
        default=None,
        description="Worker id override (OBJECTIFY_OUTBOX_WORKER_ID)",
        validation_alias="OBJECTIFY_OUTBOX_WORKER_ID",
    )
    purge_interval_seconds: int = Field(
        default=3600,
        description="Purge interval seconds (OBJECTIFY_OUTBOX_PURGE_INTERVAL_SECONDS)",
        validation_alias="OBJECTIFY_OUTBOX_PURGE_INTERVAL_SECONDS",
    )
    retention_days: int = Field(
        default=7,
        description="Retention days (OBJECTIFY_OUTBOX_RETENTION_DAYS)",
        validation_alias="OBJECTIFY_OUTBOX_RETENTION_DAYS",
    )
    purge_limit: int = Field(
        default=10_000,
        description="Purge batch limit (OBJECTIFY_OUTBOX_PURGE_LIMIT)",
        validation_alias="OBJECTIFY_OUTBOX_PURGE_LIMIT",
    )
    producer_max_in_flight: int = Field(
        default=5,
        description="Kafka producer max in-flight requests (OBJECTIFY_OUTBOX_MAX_IN_FLIGHT)",
        validation_alias="OBJECTIFY_OUTBOX_MAX_IN_FLIGHT",
    )
    producer_delivery_timeout_ms: int = Field(
        default=120_000,
        description="Kafka producer delivery.timeout.ms (OBJECTIFY_OUTBOX_DELIVERY_TIMEOUT_MS)",
        validation_alias="OBJECTIFY_OUTBOX_DELIVERY_TIMEOUT_MS",
    )
    producer_request_timeout_ms: int = Field(
        default=30_000,
        description="Kafka producer request.timeout.ms (OBJECTIFY_OUTBOX_REQUEST_TIMEOUT_MS)",
        validation_alias="OBJECTIFY_OUTBOX_REQUEST_TIMEOUT_MS",
    )
    producer_retries: int = Field(
        default=5,
        description="Kafka producer retries (OBJECTIFY_OUTBOX_RETRIES)",
        validation_alias="OBJECTIFY_OUTBOX_RETRIES",
    )

    @field_validator("poll_seconds", mode="before")
    @classmethod
    def clamp_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=3600)

    @field_validator("batch_size", mode="before")
    @classmethod
    def clamp_batch_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=50, min_value=1, max_value=5000)

    _normalize_flush_timeout_seconds = field_validator(
        "flush_timeout_seconds",
        mode="before",
    )(_clamp_flush_timeout_seconds)

    @field_validator("backoff_base_seconds", mode="before")
    @classmethod
    def clamp_backoff_base_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2, min_value=1, max_value=300)

    @field_validator("backoff_max_seconds", mode="before")
    @classmethod
    def clamp_backoff_max_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)

    @field_validator("claim_timeout_seconds", mode="before")
    @classmethod
    def clamp_claim_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=300, min_value=0, max_value=86_400)

    @field_validator("purge_interval_seconds", mode="before")
    @classmethod
    def clamp_purge_interval_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=300, max_value=86_400)

    @field_validator("retention_days", mode="before")
    @classmethod
    def clamp_retention_days(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=7, min_value=0, max_value=365)

    @field_validator("purge_limit", mode="before")
    @classmethod
    def clamp_purge_limit(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10_000, min_value=1, max_value=100_000)

    @field_validator("producer_max_in_flight", mode="before")
    @classmethod
    def clamp_producer_max_in_flight(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=1, max_value=5)

    @field_validator("producer_delivery_timeout_ms", mode="before")
    @classmethod
    def clamp_producer_delivery_timeout_ms(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=120_000, min_value=10_000, max_value=1_800_000)

    @field_validator("producer_request_timeout_ms", mode="before")
    @classmethod
    def clamp_producer_request_timeout_ms(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30_000, min_value=5_000, max_value=600_000)

    @field_validator("producer_retries", mode="before")
    @classmethod
    def clamp_producer_retries(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=5, min_value=0, max_value=100)


class ObjectifyReconcilerSettings(BaseSettings):
    """Objectify reconciler worker settings (BFF embedded worker)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=True,
        description="Enable objectify reconciler (ENABLE_OBJECTIFY_RECONCILER)",
        validation_alias="ENABLE_OBJECTIFY_RECONCILER",
    )
    poll_seconds: int = Field(
        default=60,
        description="Poll interval seconds (OBJECTIFY_RECONCILER_POLL_SECONDS)",
        validation_alias="OBJECTIFY_RECONCILER_POLL_SECONDS",
    )
    stale_after_seconds: int = Field(
        default=600,
        description="Stale cutoff seconds (OBJECTIFY_RECONCILER_STALE_SECONDS)",
        validation_alias="OBJECTIFY_RECONCILER_STALE_SECONDS",
    )
    enqueued_stale_seconds: int = Field(
        default=900,
        description="Stale cutoff seconds for QUEUED runs (OBJECTIFY_RECONCILER_ENQUEUED_STALE_SECONDS; 0=disabled)",
        validation_alias="OBJECTIFY_RECONCILER_ENQUEUED_STALE_SECONDS",
    )
    lock_key: int = Field(
        default=910215,
        description="Postgres advisory lock key (OBJECTIFY_RECONCILER_LOCK_KEY)",
        validation_alias="OBJECTIFY_RECONCILER_LOCK_KEY",
    )

    @field_validator("poll_seconds", mode="before")
    @classmethod
    def clamp_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=60, min_value=1, max_value=3600)

    @field_validator("stale_after_seconds", mode="before")
    @classmethod
    def clamp_stale_after_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=600, min_value=0, max_value=86_400)

    @field_validator("enqueued_stale_seconds", mode="before")
    @classmethod
    def clamp_enqueued_stale_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=900, min_value=0, max_value=86_400)

    @field_validator("lock_key", mode="before")
    @classmethod
    def clamp_lock_key(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=910215, min_value=1, max_value=2_000_000_000)

    @property
    def enqueued_stale_seconds_effective(self) -> Optional[int]:
        value = int(self.enqueued_stale_seconds or 0)
        return value if value > 0 else None


class WritebackMaterializerSettings(BaseSettings):
    """Writeback materializer worker settings."""

    model_config = SettingsConfigDict(
        env_prefix="WRITEBACK_MATERIALIZER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    base_branch: str = Field(
        default="main",
        description="Base branch (WRITEBACK_MATERIALIZER_BASE_BRANCH)",
    )
    interval_seconds: float = Field(
        default=float(6 * 60 * 60),
        description="Run interval seconds (WRITEBACK_MATERIALIZER_INTERVAL_SECONDS)",
    )
    run_once: bool = Field(
        default=False,
        description="Run once and exit (WRITEBACK_MATERIALIZER_RUN_ONCE)",
    )
    db_names: Optional[str] = Field(
        default=None,
        description="Comma-separated db names to materialize (WRITEBACK_MATERIALIZER_DB_NAMES)",
    )
    _normalize_base_branch = field_validator("base_branch", mode="before")(_normalize_base_branch)

    _normalize_db_names = field_validator("db_names", mode="before")(_strip_optional_text)

    @property
    def db_names_list(self) -> list[str]:
        raw = str(self.db_names or "").strip()
        if not raw:
            return []
        return [part.strip() for part in raw.split(",") if part.strip()]


class EventPublisherSettings(BaseSettings):
    """Event publisher (message relay) settings."""

    model_config = SettingsConfigDict(
        env_prefix="EVENT_PUBLISHER_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    checkpoint_key: str = Field(
        default="checkpoints/event_publisher.json",
        description="Checkpoint S3 key (EVENT_PUBLISHER_CHECKPOINT_KEY)",
    )
    poll_interval: int = Field(
        default=3,
        description="Poll interval seconds (EVENT_PUBLISHER_POLL_INTERVAL; compatibility MESSAGE_RELAY_POLL_INTERVAL)",
    )
    batch_size: int = Field(
        default=200,
        description="Batch size (EVENT_PUBLISHER_BATCH_SIZE; compatibility MESSAGE_RELAY_BATCH_SIZE)",
    )
    kafka_flush_batch_size: Optional[int] = Field(
        default=None,
        description="Kafka flush batch size (EVENT_PUBLISHER_KAFKA_FLUSH_BATCH_SIZE; default batch_size)",
    )
    kafka_flush_timeout_seconds: float = Field(
        default=10.0,
        description="Kafka flush timeout seconds (EVENT_PUBLISHER_KAFKA_FLUSH_TIMEOUT_SECONDS)",
    )
    metrics_log_interval_seconds: int = Field(
        default=30,
        description="Metrics log interval seconds (EVENT_PUBLISHER_METRICS_LOG_INTERVAL_SECONDS)",
    )
    lookback_seconds: int = Field(
        default=600,
        description="Lookback seconds (EVENT_PUBLISHER_LOOKBACK_SECONDS)",
    )
    lookback_max_keys: int = Field(
        default=2000,
        description="Lookback max keys (EVENT_PUBLISHER_LOOKBACK_MAX_KEYS)",
    )
    dedup_max_events: int = Field(
        default=10_000,
        description="Dedup max events (EVENT_PUBLISHER_DEDUP_MAX_EVENTS)",
    )
    dedup_checkpoint_max_events: int = Field(
        default=2000,
        description="Dedup checkpoint max events (EVENT_PUBLISHER_DEDUP_CHECKPOINT_MAX_EVENTS)",
    )
    start_timestamp: Optional[str] = Field(
        default=None,
        description="Optional start timestamp ISO8601 (EVENT_PUBLISHER_START_TIMESTAMP)",
    )
    kafka_topic_bootstrap_timeout_seconds: int = Field(
        default=120,
        description="Kafka topic bootstrap timeout seconds (KAFKA_TOPIC_BOOTSTRAP_TIMEOUT_SECONDS)",
    )

    @field_validator("poll_interval", mode="before")
    @classmethod
    def fallback_poll_interval(cls, v):  # noqa: ANN001
        if (os.getenv("EVENT_PUBLISHER_POLL_INTERVAL") or "").strip():
            return v
        compat_fallback = (os.getenv("MESSAGE_RELAY_POLL_INTERVAL") or "").strip()
        return compat_fallback or v

    @field_validator("batch_size", mode="before")
    @classmethod
    def fallback_batch_size(cls, v):  # noqa: ANN001
        if (os.getenv("EVENT_PUBLISHER_BATCH_SIZE") or "").strip():
            return v
        compat_fallback = (os.getenv("MESSAGE_RELAY_BATCH_SIZE") or "").strip()
        return compat_fallback or v

    @field_validator("kafka_topic_bootstrap_timeout_seconds", mode="before")
    @classmethod
    def fallback_topic_bootstrap_timeout(cls, v):  # noqa: ANN001
        raw = (os.getenv("KAFKA_TOPIC_BOOTSTRAP_TIMEOUT_SECONDS") or "").strip()
        return raw or v

    @field_validator("poll_interval", mode="before")
    @classmethod
    def clamp_poll_interval_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3, min_value=1, max_value=3600)

    @field_validator("batch_size", mode="before")
    @classmethod
    def clamp_batch_size(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=200, min_value=1, max_value=100_000)

    @field_validator("kafka_flush_batch_size", mode="before")
    @classmethod
    def clamp_kafka_flush_batch_size(cls, v):  # noqa: ANN001
        if v in (None, ""):
            return None
        return _clamp_int(v, default=200, min_value=1, max_value=1_000_000)

    @field_validator("metrics_log_interval_seconds", mode="before")
    @classmethod
    def clamp_metrics_log_interval_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30, min_value=1, max_value=3600)

    @field_validator("lookback_seconds", mode="before")
    @classmethod
    def clamp_lookback_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=600, min_value=0, max_value=86_400)

    @field_validator("lookback_max_keys", mode="before")
    @classmethod
    def clamp_lookback_max_keys(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2000, min_value=0, max_value=1_000_000)

    @field_validator("dedup_max_events", mode="before")
    @classmethod
    def clamp_dedup_max_events(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10_000, min_value=0, max_value=1_000_000)

    @field_validator("dedup_checkpoint_max_events", mode="before")
    @classmethod
    def clamp_dedup_checkpoint_max_events(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=2000, min_value=0, max_value=1_000_000)

    @field_validator("kafka_topic_bootstrap_timeout_seconds", mode="before")
    @classmethod
    def clamp_topic_bootstrap_timeout_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=120, min_value=1, max_value=3600)

    @property
    def kafka_flush_batch_size_effective(self) -> int:
        return int(self.kafka_flush_batch_size or self.batch_size)


class AgentRetentionWorkerSettings(BaseSettings):
    """Agent session retention worker settings (SEC-005)."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_RETENTION_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(default=False, description="Enable agent retention worker (AGENT_RETENTION_ENABLED)")
    poll_seconds: int = Field(default=3600, description="Retention worker poll seconds (AGENT_RETENTION_POLL_SECONDS)")
    retention_days: int = Field(default=30, description="Retention days for agent session data (AGENT_RETENTION_DAYS)")
    action: str = Field(default="redact", description="Retention action redact|delete (AGENT_RETENTION_ACTION)")
    policy_json: Optional[str] = Field(
        default=None,
        description="Retention policy JSON per object type (AGENT_RETENTION_POLICY_JSON)",
    )

    @field_validator("poll_seconds", mode="before")
    @classmethod
    def clamp_poll_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=10, max_value=86_400)

    @field_validator("retention_days", mode="before")
    @classmethod
    def clamp_retention_days(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=30, min_value=0, max_value=3650)

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, v):  # noqa: ANN001
        if v is None:
            return "redact"
        value = str(v).strip().lower() or "redact"
        if value not in {"redact", "delete"}:
            return "redact"
        return value

    _normalize_policy_json = field_validator("policy_json", mode="before")(_strip_optional_text)


class SchemaChangeMonitorSettings(BaseSettings):
    """Schema change monitor settings for proactive drift detection."""

    model_config = SettingsConfigDict(
        env_prefix="SCHEMA_MONITOR_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=False,
        description="Enable schema change monitor (SCHEMA_MONITOR_ENABLED)",
    )
    check_interval_seconds: int = Field(
        default=300,
        description="Schema check interval in seconds (SCHEMA_MONITOR_CHECK_INTERVAL_SECONDS)",
    )
    cooldown_seconds: int = Field(
        default=3600,
        description="Notification cooldown in seconds (SCHEMA_MONITOR_COOLDOWN_SECONDS)",
    )

    @field_validator("check_interval_seconds", mode="before")
    @classmethod
    def clamp_check_interval(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=300, min_value=60, max_value=86_400)

    @field_validator("cooldown_seconds", mode="before")
    @classmethod
    def clamp_cooldown(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=60, max_value=86_400)


class ChaosSettings(BaseSettings):
    """Chaos/fault injection settings (test-only)."""

    model_config = SettingsConfigDict(
        env_prefix="CHAOS_",
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    enabled: bool = Field(
        default=False,
        validation_alias="ENABLE_CHAOS_INJECTION",
        description="Enable chaos injection (ENABLE_CHAOS_INJECTION)",
    )
    crash_point: Optional[str] = Field(
        default=None,
        description="Crash point selector (CHAOS_CRASH_POINT)",
    )
    crash_once: bool = Field(
        default=True,
        description="Crash only once per container (CHAOS_CRASH_ONCE)",
    )
    crash_exit_code: int = Field(
        default=42,
        description="Process exit code when crashing (CHAOS_CRASH_EXIT_CODE)",
    )

    @field_validator("enabled", mode="before")
    @classmethod
    def coerce_enabled(cls, v):  # noqa: ANN001
        value = str(v).strip() if v is not None else ""
        if not value:
            return False
        parsed = _parse_boolish(value)
        return parsed if parsed is not None else False

    _normalize_crash_point = field_validator("crash_point", mode="before")(_strip_optional_text)

    @field_validator("crash_once", mode="before")
    @classmethod
    def coerce_crash_once(cls, v):  # noqa: ANN001
        if v is None:
            return True
        raw = str(v).strip()
        if not raw:
            return True
        parsed = _parse_boolish(raw)
        return parsed if parsed is not None else False

    @field_validator("crash_exit_code", mode="before")
    @classmethod
    def clamp_crash_exit_code(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=42, min_value=0, max_value=255)


class WorkersSettings(BaseSettings):
    """Workers/services runtime settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    instance: InstanceWorkerSettings = Field(default_factory=InstanceWorkerSettings)
    ontology: OntologyWorkerSettings = Field(default_factory=OntologyWorkerSettings)
    ontology_deploy_outbox: OntologyDeployOutboxSettings = Field(default_factory=OntologyDeployOutboxSettings)
    projection: ProjectionWorkerSettings = Field(default_factory=ProjectionWorkerSettings)
    action: ActionWorkerSettings = Field(default_factory=ActionWorkerSettings)
    action_outbox: ActionOutboxSettings = Field(default_factory=ActionOutboxSettings)
    connector_sync: ConnectorSyncSettings = Field(default_factory=ConnectorSyncSettings)
    connector_trigger: ConnectorTriggerSettings = Field(default_factory=ConnectorTriggerSettings)
    objectify: ObjectifySettings = Field(default_factory=ObjectifySettings)
    dataset_ingest_outbox: DatasetIngestOutboxSettings = Field(default_factory=DatasetIngestOutboxSettings)
    ingest_reconciler: IngestReconcilerSettings = Field(default_factory=IngestReconcilerSettings)
    objectify_outbox: ObjectifyOutboxWorkerSettings = Field(default_factory=ObjectifyOutboxWorkerSettings)
    objectify_reconciler: ObjectifyReconcilerSettings = Field(default_factory=ObjectifyReconcilerSettings)
    writeback_materializer: WritebackMaterializerSettings = Field(default_factory=WritebackMaterializerSettings)
    event_publisher: EventPublisherSettings = Field(default_factory=EventPublisherSettings)
    agent_retention: AgentRetentionWorkerSettings = Field(default_factory=AgentRetentionWorkerSettings)
    schema_change_monitor: SchemaChangeMonitorSettings = Field(default_factory=SchemaChangeMonitorSettings)



class WritebackSettings(BaseSettings):
    """Ontology writeback + read overlay settings"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    ontology_writeback_repo: str = Field(
        default="ontology-writeback",
        description="lakeFS repository for writeback (ONTOLOGY_WRITEBACK_REPO)",
    )
    ontology_writeback_branch_prefix: str = Field(
        default="writeback",
        description="Branch prefix for writeback branches (ONTOLOGY_WRITEBACK_BRANCH_PREFIX)",
    )
    ontology_writeback_dataset_id: Optional[str] = Field(
        default=None,
        description="Optional dataset id for writeback scope (ONTOLOGY_WRITEBACK_DATASET_ID)",
    )

    # Writeback feature flags (ACTION_WRITEBACK_DESIGN.md)
    writeback_enforce: bool = Field(
        default=False,
        description="Enforce writeback policy checks (WRITEBACK_ENFORCE)",
    )
    writeback_enforce_governance: bool = Field(
        default=False,
        description="Enforce governance checks for writeback (WRITEBACK_ENFORCE_GOVERNANCE)",
    )
    writeback_enforce_action_data_access: bool = Field(
        default=True,
        description=(
            "Enforce action target data_access policy checks on submit/simulate/worker "
            "(WRITEBACK_ENFORCE_ACTION_DATA_ACCESS)"
        ),
    )
    writeback_read_overlay: bool = Field(
        default=False,
        description="Enable overlay reads from lakeFS branches (WRITEBACK_READ_OVERLAY)",
    )
    writeback_enabled_object_types: str = Field(
        default="",
        description="Comma-separated allowed object types for writeback/overlay (WRITEBACK_ENABLED_OBJECT_TYPES)",
    )
    writeback_dataset_acl_scope: str = Field(
        default="dataset_acl",
        description="ACL scope name for writeback dataset access (WRITEBACK_DATASET_ACL_SCOPE)",
    )
    writeback_submission_snapshot_max_targets: int = Field(
        default=200,
        description="Max targets to snapshot during writeback submission (WRITEBACK_SUBMISSION_SNAPSHOT_MAX_TARGETS)",
    )

    _normalize_writeback_strings = field_validator(
        "ontology_writeback_repo",
        "ontology_writeback_branch_prefix",
        "writeback_enabled_object_types",
        "writeback_dataset_acl_scope",
        mode="before",
    )(_strip_text_if_not_none)

    _normalize_writeback_dataset_id = field_validator(
        "ontology_writeback_dataset_id",
        mode="before",
    )(_strip_optional_text)


class TestSettings(BaseSettings):
    """Test environment configuration"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    test_database_url: str = Field(
        default="sqlite:///./test.db",
        description="Test database URL"
    )
    test_timeout: int = Field(
        default=30,
        description="Test timeout in seconds"
    )
    oms_query_wait_seconds: float = Field(
        default=60.0,
        description="OMS query wait seconds (OMS_QUERY_WAIT_SECONDS)",
    )


class GoogleSheetsSettings(BaseSettings):
    """Google Sheets integration settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    google_sheets_api_key: Optional[str] = Field(
        default=None,
        description="Google Sheets API key"
    )
    google_sheets_credentials_path: Optional[str] = Field(
        default=None,
        description="Google Sheets service account credentials path"
    )
    oauth_client_id: Optional[str] = Field(
        default=None,
        validation_alias="GOOGLE_CLIENT_ID",
        description="Google OAuth client id (GOOGLE_CLIENT_ID)",
    )
    oauth_client_secret: Optional[str] = Field(
        default=None,
        validation_alias="GOOGLE_CLIENT_SECRET",
        description="Google OAuth client secret (GOOGLE_CLIENT_SECRET)",
    )
    oauth_redirect_uri: str = Field(
        default="http://localhost:8002/api/v1/data-connectors/google-sheets/oauth/callback",
        validation_alias="GOOGLE_REDIRECT_URI",
        description="Google OAuth redirect URI (GOOGLE_REDIRECT_URI)",
    )

    @field_validator("google_sheets_api_key", mode="before")
    @classmethod
    def fallback_google_api_key(cls, v):  # noqa: ANN001
        if os.getenv("GOOGLE_SHEETS_API_KEY") not in (None, ""):
            return v
        fallback = (os.getenv("GOOGLE_API_KEY") or "").strip()
        return fallback or v


class ApplicationSettings(BaseSettings):
    """Main application settings - aggregates all other settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Environment and basic settings
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        validation_alias=AliasChoices("ENVIRONMENT", "SPICE_ENV"),
        description="Application environment"
    )
    debug: bool = Field(
        default=True,
        description="Enable debug mode"
    )
    
    # Nested settings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    services: ServiceSettings = Field(default_factory=ServiceSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    graph_query: GraphQuerySettings = Field(default_factory=GraphQuerySettings)
    features: FeatureFlagsSettings = Field(default_factory=FeatureFlagsSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    pipeline_plan: PipelinePlanSettings = Field(default_factory=PipelinePlanSettings)
    ontology: OntologySettings = Field(default_factory=OntologySettings)
    agent: AgentRuntimeSettings = Field(default_factory=AgentRuntimeSettings)
    agent_plan: AgentPlanSettings = Field(default_factory=AgentPlanSettings)
    clients: ClientSettings = Field(default_factory=ClientSettings)
    mcp: MCPSettings = Field(default_factory=MCPSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    messaging: MessagingSettings = Field(default_factory=MessagingSettings)
    event_sourcing: EventSourcingSettings = Field(default_factory=EventSourcingSettings)
    branch_virtualization: BranchVirtualizationSettings = Field(default_factory=BranchVirtualizationSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    workers: WorkersSettings = Field(default_factory=WorkersSettings)
    chaos: ChaosSettings = Field(default_factory=ChaosSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    writeback: WritebackSettings = Field(default_factory=WritebackSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    performance: PerformanceSettings = Field(default_factory=PerformanceSettings)
    test: TestSettings = Field(default_factory=TestSettings)
    google_sheets: GoogleSheetsSettings = Field(default_factory=GoogleSheetsSettings)

    @field_validator("environment", mode="before")
    @classmethod
    def normalize_environment(cls, v):
        if isinstance(v, Environment):
            return v
        if isinstance(v, str):
            normalized = v.strip().lower()
            aliases = {
                "dev": Environment.DEVELOPMENT,
                "development": Environment.DEVELOPMENT,
                "local": Environment.DEVELOPMENT,
                "stage": Environment.STAGING,
                "staging": Environment.STAGING,
                "prod": Environment.PRODUCTION,
                "production": Environment.PRODUCTION,
                "test": Environment.TEST,
                "testing": Environment.TEST,
            }
            if normalized in aliases:
                return aliases[normalized]
        return v
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.environment == Environment.DEVELOPMENT
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.environment == Environment.PRODUCTION
    
    @property
    def is_test(self) -> bool:
        """Check if running in test mode"""
        return self.environment == Environment.TEST

    @property
    def is_pytest(self) -> bool:
        """Check if running under pytest (PYTEST_CURRENT_TEST set)."""
        return bool(os.environ.get("PYTEST_CURRENT_TEST"))


# This replaces all scattered ServiceConfig() and AppConfig() instantiations
settings = ApplicationSettings()

def get_settings() -> ApplicationSettings:
    """
    Get the global settings instance
    
    This function provides access to the centralized configuration
    and can be used with FastAPI's Depends() for dependency injection.
    
    Returns:
        ApplicationSettings: The global settings instance
    """
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return ApplicationSettings()
    return settings

def reload_settings() -> ApplicationSettings:
    """
    Reload settings from environment (useful for testing)
    
    Returns:
        ApplicationSettings: New settings instance with reloaded values
    """
    global settings
    settings = ApplicationSettings()
    return settings


def build_client_ssl_config(settings: Optional[ApplicationSettings] = None) -> Dict[str, Any]:
    """
    SSL config for HTTP clients (httpx/requests/etc).

    Mirrors compatibility behavior:
    - In production: always verify.
    - In non-prod: follow `services.verify_ssl`.
    - If verifying and CA path exists, use it.
    """
    cfg = settings or get_settings()
    services = cfg.services

    verify_ssl = True if cfg.is_production else bool(services.verify_ssl)
    client_cfg: Dict[str, Any] = {"verify": verify_ssl}

    ca_path = str(services.ssl_ca_path or "").strip()
    if verify_ssl and ca_path and os.path.exists(ca_path):
        client_cfg["verify"] = ca_path
    return client_cfg


def build_server_ssl_config(settings: Optional[ApplicationSettings] = None) -> Dict[str, Any]:
    """
    SSL config for uvicorn (server-side TLS).

    Returns uvicorn kwargs like `ssl_certfile`/`ssl_keyfile`, or `{}` when disabled.
    """
    cfg = settings or get_settings()
    services = cfg.services
    if not bool(services.use_https):
        return {}

    ssl_cfg: Dict[str, Any] = {}
    cert_path = str(services.ssl_cert_path or "").strip()
    key_path = str(services.ssl_key_path or "").strip()

    if cert_path:
        if os.path.exists(cert_path):
            ssl_cfg["ssl_certfile"] = cert_path
        else:
            logger.warning("SSL certificate not found at %s", cert_path)

    if key_path:
        if os.path.exists(key_path):
            ssl_cfg["ssl_keyfile"] = key_path
        else:
            logger.warning("SSL key not found at %s", key_path)

    return ssl_cfg


def _get_dev_cors_origins() -> List[str]:
    common_ports = [3000, 3001, 3002, 5173, 5174, 8080, 8081, 8082, 4200, 4201]
    origins: List[str] = []
    for port in common_ports:
        origins.extend(
            [
                f"http://localhost:{port}",
                f"http://127.0.0.1:{port}",
                f"https://localhost:{port}",
                f"https://127.0.0.1:{port}",
            ]
        )
    origins.append("*")
    return origins


def _get_environment_default_origins(settings: ApplicationSettings) -> List[str]:
    if settings.is_production:
        return [
            "https://app.spice-harvester.com",
            "https://www.spice-harvester.com",
            "https://spice-harvester.com",
        ]
    return _get_dev_cors_origins()


def resolve_cors_origins(settings: Optional[ApplicationSettings] = None) -> List[str]:
    """
    Resolve CORS origins with production safety.

    Rules:
    - If CORS_ORIGINS is a JSON array:
      - empty list => fall back to environment defaults
      - in production: filter wildcard '*' (and fall back if it removes all)
    - Else: fall back to environment defaults.
    """
    cfg = settings or get_settings()
    raw = str(cfg.services.cors_origins or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
        except Exception as exc:
            logger.warning("Invalid CORS_ORIGINS JSON: %s", exc)
            parsed = None

        if isinstance(parsed, list):
            if len(parsed) == 0:
                return _get_environment_default_origins(cfg)

            if cfg.is_production:
                filtered: List[str] = []
                for origin in parsed:
                    if origin == "*":
                        logger.error(
                            "SECURITY WARNING: Wildcard (*) CORS origin is not allowed in production"
                        )
                        continue
                    filtered.append(origin)
                return filtered or _get_environment_default_origins(cfg)

            return parsed

        if parsed is not None:
            logger.warning("CORS_ORIGINS must be a JSON array, got=%s", type(parsed))

    return _get_environment_default_origins(cfg)


def build_cors_middleware_config(settings: Optional[ApplicationSettings] = None) -> Dict[str, Any]:
    cfg = settings or get_settings()
    origins = resolve_cors_origins(cfg)

    cors_cfg: Dict[str, Any] = {
        "allow_origins": origins,
        "allow_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        "allow_headers": ["*"],
        "expose_headers": ["*"],
        "allow_credentials": True,
        "max_age": 3600,
    }

    if cfg.is_production:
        cors_cfg["allow_headers"] = [
            "Accept",
            "Accept-Language",
            "Authorization",
            "Content-Type",
            "DNT",
            "Origin",
            "User-Agent",
            "X-Requested-With",
        ]
        cors_cfg["expose_headers"] = ["Content-Length", "Content-Type", "X-Request-ID"]
        cors_cfg["max_age"] = 86400

    return cors_cfg


def get_cors_debug_info(settings: Optional[ApplicationSettings] = None) -> Dict[str, Any]:
    cfg = settings or get_settings()
    env_value = getattr(cfg.environment, "value", str(cfg.environment))
    return {
        "enabled": bool(cfg.services.cors_enabled),
        "origins": resolve_cors_origins(cfg),
        "environment": env_value,
        "is_production": cfg.is_production,
        "config": build_cors_middleware_config(cfg),
    }
