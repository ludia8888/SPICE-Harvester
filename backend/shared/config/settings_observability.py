from __future__ import annotations

import logging
import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.config.settings_support import (
    _ENV_FILE,
    _clamp_int,
    _parse_boolish,
    _strip_optional_text,
)


logger = logging.getLogger(__name__)


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
        default=False,
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
