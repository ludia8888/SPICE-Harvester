from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.config.settings_support import _ENV_FILE, _clamp_int


class CacheSettings(BaseSettings):
    """Cache and TTL configuration settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    class_label_cache_ttl: int = Field(
        default=3600,
        description="Class label cache TTL in seconds",
    )
    command_status_cache_ttl: int = Field(
        default=86400,
        description="Command status cache TTL in seconds",
    )
    command_status_ttl_seconds: int = Field(
        default=0,
        description="TTL seconds for command status entries (COMMAND_STATUS_TTL_SECONDS; 0=never expire)",
    )
    foundry_temp_object_set_ttl_seconds: int = Field(
        default=3600,
        description="TTL seconds for temporary Foundry object sets (FOUNDRY_TEMP_OBJECT_SET_TTL_SECONDS)",
    )
    user_session_cache_ttl: int = Field(
        default=7200,
        description="User session cache TTL in seconds",
    )
    websocket_connection_ttl: int = Field(
        default=3600,
        description="WebSocket connection TTL in seconds",
    )
    mapping_cache_ttl: int = Field(
        default=1800,
        description="Mapping cache TTL in seconds",
    )

    @field_validator("command_status_ttl_seconds", mode="before")
    @classmethod
    def clamp_command_status_ttl_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=0, min_value=0, max_value=31_536_000)

    @field_validator("foundry_temp_object_set_ttl_seconds", mode="before")
    @classmethod
    def clamp_foundry_temp_object_set_ttl_seconds(cls, v):  # noqa: ANN001
        return _clamp_int(v, default=3600, min_value=1, max_value=31_536_000)


class PerformanceSettings(BaseSettings):
    """Performance and optimization settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_pool_size: int = Field(
        default=20,
        description="Database connection pool size",
    )
    database_max_overflow: int = Field(
        default=30,
        description="Database max overflow connections",
    )
    enable_rate_limiting: bool = Field(
        default=True,
        description="Enable API rate limiting",
    )
    requests_per_minute: int = Field(
        default=60,
        description="Requests per minute limit",
    )
    max_concurrent_commands: int = Field(
        default=100,
        description="Max concurrent commands (MAX_CONCURRENT_COMMANDS)",
    )
    max_websocket_connections: int = Field(
        default=1000,
        description="Max websocket connections (MAX_WEBSOCKET_CONNECTIONS)",
    )
    max_ontology_size: int = Field(
        default=10485760,
        description="Max ontology payload size in bytes (MAX_ONTOLOGY_SIZE)",
    )
    max_instance_size: int = Field(
        default=1048576,
        description="Max instance payload size in bytes (MAX_INSTANCE_SIZE)",
    )
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
