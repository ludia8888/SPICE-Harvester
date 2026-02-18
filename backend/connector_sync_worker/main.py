"""
Connector Sync Worker (shared runtime).

Consumes Kafka `connector-updates` events (EventEnvelope; metadata.kind='connector_update').

When a confirmed mapping exists, extracts rows via connector adapters and writes through
shared connector ingest service (dataset version + objectify enqueue).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, Optional

from confluent_kafka import Producer

from data_connector.adapters.factory import (
    ConnectorAdapterFactory,
    SUPPORTED_CONNECTOR_KINDS,
    connector_kind_from_source_type,
    file_import_source_type_for_kind,
    table_import_source_type_for_kind,
)
from data_connector.adapters.runtime_credentials import resolve_source_runtime_credentials
from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited, record_lineage_or_raise
from shared.models.event_envelope import EventEnvelope
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.core.connector_ingest_service import ConnectorIngestService
from shared.services.kafka.dlq_publisher import (
    EnvelopeDlqSpec,
)
from shared.services.kafka.processed_event_worker import StrictHeartbeatEventEnvelopeKafkaWorker
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import ExecutorKafkaProducerOps, KafkaProducerOps, close_kafka_producer
from shared.services.kafka.safe_consumer import SafeKafkaConsumer
from shared.services.registries.connector_registry import ConnectorRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.utils.app_logger import configure_logging
from shared.utils.time_utils import utcnow
from shared.utils.worker_runner import run_component_lifecycle

logger = logging.getLogger(__name__)


class ConnectorSyncWorker(StrictHeartbeatEventEnvelopeKafkaWorker[Optional[str]]):
    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.connector_sync

        self.service_name = "connector-sync-worker"
        self.running = False

        self.topic = AppConfig.CONNECTOR_UPDATES_TOPIC
        self.dlq_topic = AppConfig.CONNECTOR_UPDATES_DLQ_TOPIC
        self.group_id = str(cfg.group or "connector-sync-worker-group").strip() or "connector-sync-worker-group"
        self.handler = str(cfg.handler or "connector_sync_worker").strip() or "connector_sync_worker"

        self.max_retries = int(cfg.max_retries)
        self.backoff_base = int(cfg.backoff_base_seconds)
        self.backoff_max = int(cfg.backoff_max_seconds)
        self._dlq_spec = EnvelopeDlqSpec(
            dlq_topic=self.dlq_topic,
            service_name=self.service_name,
            kind="connector_update_dlq",
            failed_event_type="CONNECTOR_UPDATE_FAILED",
            span_name="connector_sync.dlq_produce",
        )
        self.tracing = get_tracing_service(self.service_name)
        self.metrics = get_metrics_collector(self.service_name)

        self.consumer: Optional[SafeKafkaConsumer] = None
        self.consumer_ops = None
        self.dlq_producer: Optional[Producer] = None
        self.dlq_producer_ops: Optional[KafkaProducerOps] = None

        self.registry: Optional[ConnectorRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage: Optional[LineageStore] = None
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.lineage_required = bool(settings.observability.lineage_required_effective and self.enable_lineage)

        self.sheets: Optional[GoogleSheetsService] = None
        self.adapter_factory: Optional[ConnectorAdapterFactory] = None

        self.dataset_registry: Optional[DatasetRegistry] = None
        self.pipeline_registry: Optional[PipelineRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.objectify_job_queue: Optional[ObjectifyJobQueue] = None
        self.ingest_service: Optional[ConnectorIngestService] = None

    async def initialize(self) -> None:
        settings = get_settings()

        self.registry = ConnectorRegistry()
        await self.registry.initialize()

        self.processed = await create_processed_event_registry()

        self.lineage = None
        if self.enable_lineage:
            self.lineage = LineageStore()
            try:
                await self.lineage.initialize()
            except Exception as e:
                if self.lineage_required:
                    raise RuntimeError("LineageStore unavailable") from e
                log_exception_rate_limited(
                    logger,
                    zone=RuntimeZone.OBSERVABILITY,
                    operation="connector_sync_worker.initialize.lineage",
                    exc=e,
                    code=ErrorCode.LINEAGE_UNAVAILABLE,
                    category=ErrorCategory.UPSTREAM,
                )
                self.lineage = None

        self.sheets = GoogleSheetsService(api_key=settings.google_sheets.google_sheets_api_key)
        self.adapter_factory = ConnectorAdapterFactory(google_sheets_service=self.sheets)

        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()
        self.pipeline_registry = PipelineRegistry()
        await self.pipeline_registry.initialize()
        self.objectify_registry = ObjectifyRegistry()
        await self.objectify_registry.initialize()
        self.objectify_job_queue = ObjectifyJobQueue(objectify_registry=self.objectify_registry)

        self.ingest_service = ConnectorIngestService(
            dataset_registry=self.dataset_registry,
            pipeline_registry=self.pipeline_registry,
            objectify_registry=self.objectify_registry,
            objectify_job_queue=self.objectify_job_queue,
        )

        self._initialize_safe_consumer_runtime(
            group_id=self.group_id,
            topics=[self.topic],
            service_name="connector-sync-worker",
            thread_name_prefix="connector-sync-worker-kafka",
        )

        self.dlq_producer = create_kafka_dlq_producer(
            bootstrap_servers=settings.database.kafka_servers,
            client_id=settings.observability.service_name or "connector-sync-worker",
        )
        self.dlq_producer_ops = ExecutorKafkaProducerOps(
            self.dlq_producer,
            thread_name_prefix="connector-sync-worker-kafka-producer",
        )

        logger.info(f"✅ ConnectorSyncWorker initialized (topic={self.topic}, group={self.group_id})")

    async def close(self) -> None:
        if self.sheets:
            await self.sheets.close()
            self.sheets = None
        self.adapter_factory = None

        if self.objectify_job_queue:
            await self.objectify_job_queue.close()
            self.objectify_job_queue = None
        if self.objectify_registry:
            await self.objectify_registry.close()
            self.objectify_registry = None
        if self.pipeline_registry:
            await self.pipeline_registry.close()
            self.pipeline_registry = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None
        self.ingest_service = None

        if self.lineage:
            await self.lineage.close()
            self.lineage = None
        if self.processed:
            await self.processed.close()
            self.processed = None
        if self.registry:
            await self.registry.close()
            self.registry = None

        await self._close_consumer_runtime()
        await close_kafka_producer(
            producer_ops=self.dlq_producer_ops,
            timeout_s=5.0,
            warning_logger=logger,
            warning_message="DLQ producer flush failed during shutdown: %s",
        )
        self.dlq_producer_ops = None
        self.dlq_producer = None

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: EventEnvelope,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        await self._send_envelope_failure_to_dlq(
            msg=msg,
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            producer_ops=self.dlq_producer_ops,
            spec=self._dlq_spec,
            key_fallback="connector_sync",
            missing_producer_message="DLQ producer not configured; dropping connector sync DLQ payload: %s",
        )

    async def _process_payload(self, payload: EventEnvelope) -> Optional[str]:  # type: ignore[override]
        return await self._handle_envelope(payload)

    def _span_name(self, *, payload: EventEnvelope) -> str:  # type: ignore[override]
        return "connector_sync.process_event"

    def _is_retryable_error(self, exc: Exception, *, payload: EventEnvelope) -> bool:  # type: ignore[override]
        return True

    def _in_progress_sleep_seconds(self, *, claim, payload: EventEnvelope) -> float:  # type: ignore[override]
        return 1.0

    def _should_seek_on_in_progress(self, *, claim, payload: EventEnvelope) -> bool:  # type: ignore[override]
        return False

    def _should_seek_on_retry(self, *, attempt_count: int, payload: EventEnvelope) -> bool:  # type: ignore[override]
        return False

    def _should_mark_done_after_dlq(self, *, payload: EventEnvelope, error: str) -> bool:  # type: ignore[override]
        return True

    async def _on_success(self, *, payload: EventEnvelope, result: Optional[str], duration_s: float) -> None:  # type: ignore[override]
        if self.registry:
            try:
                await self.registry.record_sync_outcome(
                    source_type=str((payload.data or {}).get("source_type") or ""),
                    source_id=str((payload.data or {}).get("source_id") or ""),
                    success=True,
                    command_id=result,
                )
            except Exception as exc:
                logger.warning("Best-effort record_sync_outcome failed: %s", exc, exc_info=True)
        await super()._on_success(payload=payload, result=result, duration_s=duration_s)

    async def _on_retry_scheduled(
        self,
        *,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        if not self.registry:
            return
        try:
            await self.registry.record_sync_outcome(
                source_type=str((payload.data or {}).get("source_type") or ""),
                source_id=str((payload.data or {}).get("source_id") or ""),
                success=False,
                error=error,
                next_retry_at=utcnow() + timedelta(seconds=int(backoff_s)),
            )
        except Exception as exc:
            logger.warning("Best-effort record_sync_outcome failed: %s", exc, exc_info=True)

    async def _on_terminal_failure(
        self,
        *,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        if not self.registry:
            return
        try:
            backoff_s = self._backoff_seconds(attempt_count=attempt_count, payload=payload)
            await self.registry.record_sync_outcome(
                source_type=str((payload.data or {}).get("source_type") or ""),
                source_id=str((payload.data or {}).get("source_id") or ""),
                success=False,
                error=error,
                next_retry_at=utcnow() + timedelta(seconds=int(backoff_s)),
            )
        except Exception as exc:
            logger.warning("Best-effort record_sync_outcome failed: %s", exc, exc_info=True)

    async def _process_connector_update(self, envelope: EventEnvelope) -> Optional[str]:
        if not self.registry or not self.adapter_factory or not self.ingest_service:
            raise RuntimeError("Worker not initialized")

        data = envelope.data or {}
        source_type = str(data.get("source_type") or "").strip()
        source_id = str(data.get("source_id") or "").strip()
        if not source_type or not source_id:
            raise ValueError("connector_update missing source_type/source_id")

        source = await self.registry.get_source(source_type=source_type, source_id=source_id)
        if not source or not source.enabled:
            logger.info("Skipping disabled/unregistered source (%s:%s)", source_type, source_id)
            return None

        mapping = await self.registry.get_mapping(source_type=source_type, source_id=source_id)
        if not mapping or not mapping.enabled:
            logger.info("Skipping auto-import (mapping not enabled) for %s:%s", source_type, source_id)
            return None

        db_name = (mapping.target_db_name or "").strip()
        branch = (mapping.target_branch or "main").strip() or "main"
        if not db_name:
            logger.info("Skipping auto-import (target DB not configured) for %s:%s", source_type, source_id)
            return None

        connector_kind = connector_kind_from_source_type(source_type)
        adapter = self.adapter_factory.get_adapter(connector_kind)
        source_cfg = dict(source.config_json or {})
        import_mode = str(source_cfg.get("import_mode") or "SNAPSHOT").strip().upper()
        import_config = source_cfg.get("table_import_config") if isinstance(source_cfg.get("table_import_config"), dict) else {}

        runtime_config, secrets = await resolve_source_runtime_credentials(
            connector_registry=self.registry,
            source_type=source_type,
            source_config=source_cfg,
        )

        sync_state = await self.registry.get_sync_state(source_type=source_type, source_id=source_id)
        sync_state_json = dict(sync_state.sync_state_json or {}) if sync_state else {}

        if import_mode == "SNAPSHOT":
            extract = await adapter.snapshot_extract(
                config=runtime_config,
                secrets=secrets,
                import_config=import_config,
            )
        elif import_mode == "INCREMENTAL":
            extract = await adapter.incremental_extract(
                config=runtime_config,
                secrets=secrets,
                import_config=import_config,
                sync_state=sync_state_json,
            )
        elif import_mode == "CDC":
            extract = await adapter.cdc_extract(
                config=runtime_config,
                secrets=secrets,
                import_config=import_config,
                sync_state=sync_state_json,
            )
        elif import_mode in {"APPEND", "UPDATE"}:
            extract = await adapter.snapshot_extract(
                config=runtime_config,
                secrets=secrets,
                import_config=import_config,
            )
        else:
            raise ValueError("Unsupported import mode")

        ingest = await self.ingest_service.ingest_rows(
            db_name=db_name,
            source_type=source_type,
            source_id=source_id,
            columns=list(extract.columns or []),
            rows=list(extract.rows or []),
            branch=branch,
            dataset_name=str(source_cfg.get("display_name") or f"{connector_kind}_{source_id}"),
            import_mode=import_mode,
            primary_key_column=(
                str(import_config.get("primaryKeyColumn") or import_config.get("primary_key_column") or "").strip() or None
            ),
            actor_user_id="connector_sync_worker",
            source_ref=f"{source_type}:{source_id}",
        )

        if extract.next_state:
            await self.registry.upsert_sync_state_json(
                source_type=source_type,
                source_id=source_id,
                sync_state_json=dict(extract.next_state),
                merge=True,
            )

        dataset = ingest.get("dataset") if isinstance(ingest, dict) else {}
        version = ingest.get("version") if isinstance(ingest, dict) else {}
        command_id = str(version.get("version_id") or "").strip() or None

        if command_id and self.lineage and dataset:
            dataset_id = str(dataset.get("dataset_id") or "").strip() or None

            async def _record_connector_command_lineage() -> None:
                await self.lineage.record_link(  # type: ignore[union-attr]
                    from_node_id=LineageStore.node_event(str(envelope.event_id)),
                    to_node_id=LineageStore.node_aggregate("Dataset", dataset_id or command_id),
                    edge_type="connector_triggered_command",
                    occurred_at=envelope.occurred_at,
                    db_name=db_name,
                    edge_metadata={
                        "db_name": db_name,
                        "source_type": source_type,
                        "source_id": source_id,
                        "event_id": str(envelope.event_id),
                        "command_id": str(command_id),
                    },
                )

            await record_lineage_or_raise(
                lineage_store=self.lineage,
                required=self.lineage_required,
                record_call=_record_connector_command_lineage,
                logger=logger,
                operation="connector_sync_worker.record_connector_command_lineage",
                zone=RuntimeZone.CORE,
                context={
                    "db_name": db_name,
                    "source_id": source_id,
                    "event_id": str(envelope.event_id),
                    "command_id": str(command_id),
                },
            )

        logger.info(
            "✅ Connector sync completed (source=%s:%s, db=%s, mode=%s, rows=%s, version_id=%s)",
            source_type,
            source_id,
            db_name,
            import_mode,
            len(extract.rows or []),
            command_id,
        )
        return command_id

    async def _handle_envelope(self, envelope: EventEnvelope) -> Optional[str]:
        kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
        if kind != "connector_update":
            raise ValueError(f"Unexpected envelope kind: {kind}")

        source_type = str((envelope.data or {}).get("source_type") or "").strip()
        supported_source_types = {
            table_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS
        }
        supported_source_types.update(
            {file_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS}
        )
        if source_type not in supported_source_types:
            raise ValueError(f"Unsupported source_type: {source_type}")
        return await self._process_connector_update(envelope)

    async def run(self) -> None:
        if not self.consumer or not self.processed or not self.registry:
            raise RuntimeError("Worker not initialized")

        self.running = True
        logger.info("🚀 ConnectorSyncWorker started")
        await self.run_loop(poll_timeout=1.0, idle_sleep=None, catch_exceptions=False)


if __name__ == "__main__":
    configure_logging(get_settings().observability.log_level)
    asyncio.run(run_component_lifecycle(ConnectorSyncWorker()))
