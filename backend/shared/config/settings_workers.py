from __future__ import annotations

import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.config.settings_support import (
    _ENV_FILE,
    _clamp_flush_timeout_seconds,
    _clamp_int,
    _normalize_base_branch,
    _parse_boolish,
    _strip_optional_text,
    _strip_text_if_not_none,
)


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
