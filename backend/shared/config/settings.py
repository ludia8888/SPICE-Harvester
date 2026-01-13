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

import os
from typing import Optional, List, Dict, Any
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum


def _is_docker_environment() -> bool:
    docker_env = (os.getenv("DOCKER_CONTAINER") or "").strip().lower()
    if docker_env in ("false", "0", "no", "off"):
        return False
    if docker_env in ("true", "1", "yes", "on"):
        return True
    return os.path.exists("/.dockerenv")


class Environment(str, Enum):
    """Application environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"

class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # TerminusDB Configuration
    terminus_url: str = Field(
        default_factory=lambda: (
            "http://terminusdb:6363" if _is_docker_environment() else "http://127.0.0.1:6363"
        ),
        description="TerminusDB server URL"
    )
    terminus_user: str = Field(
        default="admin",
        description="TerminusDB username"
    )
    terminus_password: str = Field(
        default="admin",
        description="TerminusDB password/key"
    )
    terminus_account: str = Field(
        default="admin",
        description="TerminusDB account"
    )
    
    @field_validator("terminus_url", mode="before")
    @classmethod
    def get_terminus_url(cls, v):
        return os.getenv("TERMINUS_SERVER_URL", v or "http://localhost:6363")

    @field_validator("terminus_user", mode="before")
    @classmethod
    def get_terminus_user(cls, v):
        return os.getenv("TERMINUS_USER", v or "anonymous")

    @field_validator("terminus_password", mode="before")
    @classmethod
    def get_terminus_password(cls, v):
        return os.getenv("TERMINUS_KEY", v or "admin")

    @field_validator("terminus_account", mode="before")
    @classmethod
    def get_terminus_account(cls, v):
        return os.getenv("TERMINUS_ACCOUNT", v or "admin")
    terminus_timeout: int = Field(
        default=30,
        description="TerminusDB connection timeout in seconds"
    )
    terminus_retry_attempts: int = Field(
        default=3,
        description="TerminusDB retry attempts"
    )
    terminus_retry_delay: float = Field(
        default=1.0,
        description="TerminusDB retry delay in seconds"
    )
    terminus_ssl_verify: bool = Field(
        default=True,
        description="Verify SSL certificates for TerminusDB"
    )
    terminus_use_ssl: bool = Field(
        default=False,
        description="Use SSL for TerminusDB connections"
    )
    
    # PostgreSQL Configuration
    postgres_host: str = Field(
        default_factory=lambda: ("postgres" if _is_docker_environment() else "127.0.0.1"),
        description="PostgreSQL host"
    )
    postgres_port: int = Field(
        default_factory=lambda: (5432 if _is_docker_environment() else 5433),
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
    
    # Kafka Configuration
    kafka_host: str = Field(
        default_factory=lambda: ("kafka" if _is_docker_environment() else "127.0.0.1"),
        description="Kafka host"
    )
    kafka_port: int = Field(
        default_factory=lambda: (29092 if _is_docker_environment() else 9092),
        description="Kafka port"
    )
    kafka_bootstrap_servers: Optional[str] = Field(
        default=None,
        description="Kafka bootstrap servers (overrides host:port)"
    )
    
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
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Service Hosts and Ports
    oms_host: str = Field(
        default="127.0.0.1",
        description="OMS service host"
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
        default="127.0.0.1",
        description="BFF service host"
    )
    bff_port: int = Field(
        default=8002,
        description="BFF service port"
    )
    bff_base_url_override: Optional[str] = Field(
        default=None,
        description="BFF base URL override (BFF_BASE_URL)"
    )
    funnel_host: str = Field(
        default="127.0.0.1",
        description="Funnel service host"
    )
    funnel_port: int = Field(
        default=8003,
        description="Funnel service port"
    )
    funnel_base_url_override: Optional[str] = Field(
        default=None,
        description="Funnel base URL override (FUNNEL_BASE_URL)"
    )
    agent_host: str = Field(
        default="127.0.0.1",
        description="Agent service host"
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
        default='["http://localhost:3000", "http://localhost:3001", "http://localhost:8080"]',
        description="CORS allowed origins (JSON array string)"
    )
    enable_debug_endpoints: bool = Field(
        default=False,
        description="Enable opt-in debug endpoints (ENABLE_DEBUG_ENDPOINTS)"
    )

    @field_validator("oms_base_url_override", mode="before")
    @classmethod
    def get_oms_base_url_override(cls, v):
        return os.getenv("OMS_BASE_URL", v)

    @field_validator("bff_base_url_override", mode="before")
    @classmethod
    def get_bff_base_url_override(cls, v):
        return os.getenv("BFF_BASE_URL", v)

    @field_validator("funnel_base_url_override", mode="before")
    @classmethod
    def get_funnel_base_url_override(cls, v):
        return os.getenv("FUNNEL_BASE_URL", v)

    @field_validator("agent_base_url_override", mode="before")
    @classmethod
    def get_agent_base_url_override(cls, v):
        return os.getenv("AGENT_BASE_URL", v)
    
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
    def funnel_base_url(self) -> str:
        """Construct Funnel base URL"""
        if self.funnel_base_url_override:
            return self.funnel_base_url_override.rstrip("/")
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.funnel_host}:{self.funnel_port}"

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
        try:
            import json
            return json.loads(self.cors_origins)
        except (json.JSONDecodeError, TypeError):
            return ["*"]  # Fallback to allow all origins


class MessagingSettings(BaseSettings):
    """Kafka topic/group configuration settings"""

    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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
    search_projection_dlq_topic: str = Field(
        default="projection_failures_dlq",
        description="Kafka DLQ topic for search projection failures (SEARCH_PROJECTION_DLQ_TOPIC)",
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
    search_projection_group: str = Field(
        default="search-projection-worker",
        description="Kafka consumer group for search projection worker (SEARCH_PROJECTION_GROUP)",
    )

class StorageSettings(BaseSettings):
    """Storage configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # MinIO/S3 Configuration
    minio_endpoint_url: str = Field(
        default_factory=lambda: (
            "http://spice-minio:9000" if _is_docker_environment() else "http://127.0.0.1:9000"
        ),
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
        description="lakeFS API port (used when LAKEFS_API_URL is unset)",
    )
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
    
    # S3 Buckets
    event_store_bucket: str = Field(
        default="spice-event-store",
        description="S3 bucket for immutable event store (EVENT_STORE_BUCKET)",
    )
    instance_bucket: str = Field(
        default="instance-events",
        description="S3 bucket for instance events"
    )
    
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
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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


class SecuritySettings(BaseSettings):
    """Security configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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


class EventSourcingSettings(BaseSettings):
    """Event sourcing / CQRS tuning settings"""

    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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


class WritebackSettings(BaseSettings):
    """Ontology writeback + read overlay settings"""

    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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

    @field_validator(
        "ontology_writeback_repo",
        "ontology_writeback_branch_prefix",
        "writeback_enabled_object_types",
        "writeback_dataset_acl_scope",
        mode="before",
    )
    @classmethod
    def strip_strings(cls, v):  # noqa: ANN001
        if v is None:
            return v
        return str(v).strip()

    @field_validator("ontology_writeback_dataset_id", mode="before")
    @classmethod
    def blank_to_none(cls, v):  # noqa: ANN001
        if v is None:
            return None
        value = str(v).strip()
        return value or None


class TestSettings(BaseSettings):
    """Test environment configuration"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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


class GoogleSheetsSettings(BaseSettings):
    """Google Sheets integration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
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


class ApplicationSettings(BaseSettings):
    """Main application settings - aggregates all other settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Environment and basic settings
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Application environment"
    )
    debug: bool = Field(
        default=True,
        description="Enable debug mode"
    )
    
    # Nested settings
    database: DatabaseSettings = DatabaseSettings()
    services: ServiceSettings = ServiceSettings()
    messaging: MessagingSettings = MessagingSettings()
    event_sourcing: EventSourcingSettings = EventSourcingSettings()
    storage: StorageSettings = StorageSettings()
    cache: CacheSettings = CacheSettings()
    writeback: WritebackSettings = WritebackSettings()
    security: SecuritySettings = SecuritySettings()
    performance: PerformanceSettings = PerformanceSettings()
    test: TestSettings = TestSettings()
    google_sheets: GoogleSheetsSettings = GoogleSheetsSettings()

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
