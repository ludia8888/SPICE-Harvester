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

from shared.config.settings_support import (
    _ENV_FILE,
    _clamp_flush_timeout_seconds,
    _clamp_int,
    _is_docker_environment,
    _normalize_base_branch,
    _parse_boolish,
    _strip_optional_text,
    _strip_text_if_not_none,
)
from shared.config.settings_agent import (
    AgentPlanSettings,
    AgentRuntimeSettings,
    ClientSettings,
    LLMSettings,
    MCPSettings,
    PipelinePlanSettings,
)
from shared.config.settings_infra import CacheSettings, PerformanceSettings
from shared.config.settings_observability import ChaosSettings, ObservabilitySettings
from shared.config.settings_security import GoogleSheetsSettings, SecuritySettings, TestSettings
from shared.config.settings_workers import (
    ActionOutboxSettings,
    ActionWorkerSettings,
    AgentRetentionWorkerSettings,
    ConnectorSyncSettings,
    ConnectorTriggerSettings,
    DatasetIngestOutboxSettings,
    EventPublisherSettings,
    IngestReconcilerSettings,
    InstanceWorkerSettings,
    ObjectifyOutboxWorkerSettings,
    ObjectifyReconcilerSettings,
    ObjectifySettings,
    OntologyDeployOutboxSettings,
    OntologyWorkerSettings,
    ProjectionWorkerSettings,
    SchemaChangeMonitorSettings,
    WorkersSettings,
    WritebackMaterializerSettings,
)

logger = logging.getLogger(__name__)


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
    allow_runtime_ddl_bootstrap_override: Optional[bool] = Field(
        default=None,
        validation_alias=AliasChoices("ALLOW_RUNTIME_DDL_BOOTSTRAP", "DATABASE_ALLOW_RUNTIME_DDL_BOOTSTRAP"),
        description=(
            "Allow runtime schema/table bootstrap when migrations are missing. "
            "Defaults to true in development/test and false in staging/production."
        ),
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
    elasticsearch_compat_version: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("ELASTICSEARCH_COMPAT_VERSION", "ES_COMPAT_VERSION"),
        description="Override elasticsearch-py compatibility header (ELASTICSEARCH_COMPAT_VERSION / ES_COMPAT_VERSION)",
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
    oms_grpc_port: int = Field(
        default=50051,
        description="OMS gRPC service port (OMS_GRPC_PORT)",
    )
    oms_grpc_target_override: Optional[str] = Field(
        default=None,
        description="OMS gRPC target override (OMS_GRPC_TARGET)",
    )
    oms_grpc_enabled: bool = Field(
        default=True,
        description="Enable OMS gRPC bridge server (OMS_GRPC_ENABLED)",
    )
    oms_grpc_bind_host: str = Field(
        default="0.0.0.0",
        description="OMS gRPC bind host (OMS_GRPC_BIND_HOST)",
    )
    oms_grpc_use_tls: bool = Field(
        default=False,
        description="Use TLS for OMS gRPC client channels (OMS_GRPC_USE_TLS)",
    )
    oms_grpc_server_use_tls: bool = Field(
        default=False,
        description="Use TLS for OMS gRPC server listeners (OMS_GRPC_SERVER_USE_TLS)",
    )
    oms_grpc_require_mtls: bool = Field(
        default=False,
        description="Require mTLS client certificates for OMS gRPC server listeners (OMS_GRPC_REQUIRE_MTLS)",
    )
    oms_grpc_server_name: Optional[str] = Field(
        default=None,
        description="SNI/server name override for OMS gRPC TLS (OMS_GRPC_SERVER_NAME)",
    )
    oms_grpc_client_ca_path: Optional[str] = Field(
        default=None,
        description="CA bundle path for OMS gRPC client TLS (OMS_GRPC_CLIENT_CA_PATH; fallback SSL_CA_PATH)",
    )
    oms_grpc_client_cert_path: Optional[str] = Field(
        default=None,
        description="Client cert path for OMS gRPC mTLS (OMS_GRPC_CLIENT_CERT_PATH)",
    )
    oms_grpc_client_key_path: Optional[str] = Field(
        default=None,
        description="Client key path for OMS gRPC mTLS (OMS_GRPC_CLIENT_KEY_PATH)",
    )
    oms_grpc_server_cert_path: Optional[str] = Field(
        default=None,
        description="Server cert path for OMS gRPC TLS (OMS_GRPC_SERVER_CERT_PATH; fallback SSL_CERT_PATH)",
    )
    oms_grpc_server_key_path: Optional[str] = Field(
        default=None,
        description="Server key path for OMS gRPC TLS (OMS_GRPC_SERVER_KEY_PATH; fallback SSL_KEY_PATH)",
    )
    oms_grpc_server_ca_path: Optional[str] = Field(
        default=None,
        description="CA bundle path for OMS gRPC server mTLS (OMS_GRPC_SERVER_CA_PATH; fallback SSL_CA_PATH)",
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

    _normalize_optional_service_strings = field_validator(
        "oms_grpc_target_override",
        "oms_grpc_bind_host",
        "oms_grpc_server_name",
        "oms_grpc_client_ca_path",
        "oms_grpc_client_cert_path",
        "oms_grpc_client_key_path",
        "oms_grpc_server_cert_path",
        "oms_grpc_server_key_path",
        "oms_grpc_server_ca_path",
        "bff_base_url_override",
        "agent_base_url_override",
        mode="before",
    )(_strip_optional_text)

    @field_validator("oms_base_url_override", mode="before")
    @classmethod
    def get_oms_base_url_override(cls, v):
        if os.getenv("OMS_BASE_URL") not in (None, ""):
            return os.getenv("OMS_BASE_URL")
        fallback = (os.getenv("OMS_URL") or "").strip()
        return fallback or v

    @field_validator("oms_grpc_target_override", mode="before")
    @classmethod
    def get_oms_grpc_target_override(cls, v):
        override = (os.getenv("OMS_GRPC_TARGET") or "").strip()
        return override or v

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
    def oms_grpc_target(self) -> str:
        """Construct OMS gRPC target host:port."""
        if self.oms_grpc_target_override:
            return self.oms_grpc_target_override.strip()
        return f"{self.oms_host}:{int(self.oms_grpc_port)}"
    
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
    enable_foundry_connectivity_jdbc: bool = Field(
        default=False,
        description="Enable Foundry connectivity JDBC connectors (ENABLE_FOUNDRY_CONNECTIVITY_JDBC)",
    )
    foundry_connectivity_jdbc_db_allowlist: str = Field(
        default="",
        description=(
            "Comma-separated ontology DB allowlist for JDBC connectivity when global flag is off "
            "(FOUNDRY_CONNECTIVITY_JDBC_DB_ALLOWLIST)"
        ),
    )
    enable_foundry_connectivity_cdc: bool = Field(
        default=False,
        description="Enable Foundry connectivity CDC mode (ENABLE_FOUNDRY_CONNECTIVITY_CDC)",
    )
    foundry_connectivity_cdc_db_allowlist: str = Field(
        default="",
        description=(
            "Comma-separated ontology DB allowlist for CDC mode when global flag is off "
            "(FOUNDRY_CONNECTIVITY_CDC_DB_ALLOWLIST)"
        ),
    )

    _normalize_connectivity_allowlists = field_validator(
        "foundry_connectivity_jdbc_db_allowlist",
        "foundry_connectivity_cdc_db_allowlist",
        mode="before",
    )(_strip_text_if_not_none)

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
    spark_driver_memory: str = Field(
        default="512m",
        description="Spark driver heap size (PIPELINE_SPARK_DRIVER_MEMORY)",
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

    @field_validator("spark_driver_memory", mode="before")
    @classmethod
    def normalize_spark_driver_memory(cls, v):  # noqa: ANN001
        text = str(v or "").strip()
        return text or "512m"

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
        default="main,production,prod",
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
    search_around_spark_threshold: int = Field(
        default=100_000,
        description=(
            "Search Around request object-count threshold for Spark on-demand routing "
            "(ONTOLOGY_SEARCH_AROUND_SPARK_THRESHOLD)"
        ),
    )
    writeback_spark_threshold: int = Field(
        default=10_000,
        description=(
            "Writeback request object-count threshold for Spark on-demand routing "
            "(ONTOLOGY_WRITEBACK_SPARK_THRESHOLD)"
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
        return str(v or "").strip() or "main,production,prod"

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

    @field_validator("search_around_spark_threshold", mode="before")
    @classmethod
    def clamp_search_around_spark_threshold(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=100_000, min_value=1, max_value=10_000_000)

    @field_validator("writeback_spark_threshold", mode="before")
    @classmethod
    def clamp_writeback_spark_threshold(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=10_000, min_value=1, max_value=10_000_000)

    @property
    def protected_branches_set(self) -> set[str]:
        raw = str(self.protected_branches or "").strip()
        branches = {b.strip() for b in raw.split(",") if b.strip()}
        return branches or {"main", "production", "prod"}

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

    # Login / local auth settings
    auth_login_enabled: bool = Field(
        default=True,
        description="Enable /auth/login endpoint (AUTH_LOGIN_ENABLED)",
    )
    auth_users: Optional[str] = Field(
        default=None,
        description="JSON user list for local auth (AUTH_USERS)",
    )
    auth_access_token_ttl: int = Field(
        default=3600,
        description="Access-token lifetime in seconds (AUTH_ACCESS_TOKEN_TTL)",
    )
    auth_refresh_token_ttl: int = Field(
        default=604800,
        description="Refresh-token lifetime in seconds (AUTH_REFRESH_TOKEN_TTL)",
    )

    # Require flags (Optional so 'unset' can use heuristics)
    agent_require_auth: Optional[bool] = Field(
        default=None,
        description="Require auth for Agent (AGENT_REQUIRE_AUTH)",
    )
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
    agent_auth_exempt_paths: Optional[str] = Field(
        default=None,
        description="Comma-separated exempt paths for Agent (AGENT_AUTH_EXEMPT_PATHS)",
    )
    oms_auth_exempt_paths: Optional[str] = Field(
        default=None,
        description="Comma-separated exempt paths for OMS (OMS_AUTH_EXEMPT_PATHS)",
    )
    oms_grpc_service_token: Optional[str] = Field(
        default=None,
        description="Dedicated client token for OMS gRPC bridge calls (OMS_GRPC_SERVICE_TOKEN)",
    )
    oms_grpc_service_tokens: Optional[str] = Field(
        default=None,
        description="Dedicated allowed token list for OMS gRPC bridge server (OMS_GRPC_SERVICE_TOKENS)",
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
        "agent_auth_exempt_paths",
        "oms_auth_exempt_paths",
        "oms_grpc_service_token",
        "oms_grpc_service_tokens",
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
    def oms_grpc_service_token_effective(self) -> Optional[str]:
        tokens = self._split_tokens(self.oms_grpc_service_token)
        if tokens:
            return tokens[0]
        tokens = self.oms_grpc_expected_service_tokens
        return tokens[0] if tokens else None

    @property
    def oms_grpc_expected_service_tokens(self) -> tuple[str, ...]:
        tokens = self._split_tokens(self.oms_grpc_service_tokens)
        if tokens:
            return tokens
        return tuple(
            token
            for token in (
                *self.bff_agent_tokens,
                *self.bff_expected_tokens,
                *self.oms_expected_tokens,
            )
            if str(token or "").strip()
        )

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

    def is_agent_auth_required(self, *, default_required: bool = True) -> bool:
        if self.agent_require_auth is not None:
            return bool(self.agent_require_auth)
        if self.allow_insecure_auth_disable:
            return False
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

    def resolve_agent_exempt_paths(self, *, defaults: tuple[str, ...]) -> set[str]:
        return self._parse_exempt_paths(self.agent_auth_exempt_paths, defaults)

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


class ApplicationSettings(BaseSettings):
    """Main application settings - aggregates all other settings"""
    
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
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

    @property
    def allow_runtime_ddl_bootstrap(self) -> bool:
        override = self.database.allow_runtime_ddl_bootstrap_override
        if override is not None:
            return bool(override)
        return self.is_development or self.is_test or self.is_pytest


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
