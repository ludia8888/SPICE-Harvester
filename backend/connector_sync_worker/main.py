"""
Connector Sync Worker (shared runtime).

Consumes Kafka `connector-updates` events (EventEnvelope; metadata.kind='connector_update').

When a confirmed mapping exists, fetches + normalizes data via connector adapters and submits
write commands to the platform engine (BFF/OMS async 202 path).

Core policies (operational tradeoffs):
- At-least-once + idempotent: ProcessedEventRegistry claims side-effects.
- Mapping-gated: no mapping → no writes.
- Queueing + backoff + DLQ: defaults to safe operational behavior.
- Lineage: connector event → submitted command_id edge is recorded (fail-closed by default).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, Optional

import httpx
from confluent_kafka import Producer

from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.utils import normalize_sheet_data
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited, record_lineage_or_raise
from shared.models.event_envelope import EventEnvelope
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.kafka.dlq_publisher import (
    EnvelopeDlqSpec,
)
from shared.services.kafka.processed_event_worker import StrictHeartbeatEventEnvelopeKafkaWorker
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import ExecutorKafkaProducerOps, KafkaProducerOps, close_kafka_producer
from shared.services.kafka.safe_consumer import SafeKafkaConsumer
from shared.services.registries.connector_registry import ConnectorRegistry
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.core.sheet_import_service import FieldMapping, SheetImportService
from shared.security.auth_utils import BFF_TOKEN_ENV_KEYS, get_expected_token
from shared.utils.app_logger import configure_logging
from shared.utils.import_type_normalization import normalize_import_target_type
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
        self.http: Optional[httpx.AsyncClient] = None

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

        # Connector adapter (v1: google sheets)
        api_key = settings.google_sheets.google_sheets_api_key
        self.sheets = GoogleSheetsService(api_key=api_key)

        # HTTP client to call BFF (auth is fail-closed in prod).
        token = get_expected_token(BFF_TOKEN_ENV_KEYS)
        headers: Dict[str, str] = {}
        if token:
            headers["X-Admin-Token"] = token
        else:
            logger.warning("No BFF auth token configured; BFF calls may fail (set ADMIN_TOKEN/BFF_ADMIN_TOKEN).")
        self.http = httpx.AsyncClient(timeout=60.0, headers=headers)

        self._initialize_safe_consumer_runtime(
            group_id=self.group_id,
            topics=[self.topic],
            service_name="connector-sync-worker",
            thread_name_prefix="connector-sync-worker-kafka",
        )

        # DLQ producer (best-effort)
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
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.sheets:
            await self.sheets.close()
            self.sheets = None
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

    # --- EventEnvelopeKafkaWorker hooks ---
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

    def _bff_scope_headers(self, *, db_name: str) -> Dict[str, str]:
        scope = (db_name or "").strip()
        if not scope:
            return {}
        return {"X-DB-Name": scope, "X-Project": scope}

    async def _fetch_ontology_schema(self, *, db_name: str, class_label: str, branch: str) -> Dict[str, Any]:
        if not self.http:
            raise RuntimeError("HTTP client not initialized")
        bff_url = get_settings().services.bff_base_url
        url = f"{bff_url}/api/v1/databases/{db_name}/ontology/{class_label}/schema"
        headers = self._bff_scope_headers(db_name=db_name)
        resp = await self.http.get(url, params={"format": "json", "branch": branch}, headers=headers or None)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError("Ontology schema response is not an object")
        return data

    async def _target_field_types(self, *, db_name: str, class_label: str, branch: str) -> Dict[str, str]:
        schema = await self._fetch_ontology_schema(db_name=db_name, class_label=class_label, branch=branch)
        props = schema.get("properties") or []
        types: Dict[str, str] = {}
        if isinstance(props, list):
            for p in props:
                if not isinstance(p, dict):
                    continue
                name = (p.get("name") or "").strip()
                label = (p.get("label") or "").strip()
                typ = normalize_import_target_type(p.get("type"))
                if name:
                    types[name] = typ
                if label:
                    types[label] = typ
        return types

    async def _process_google_sheets_update(self, envelope: EventEnvelope) -> Optional[str]:
        if not self.registry or not self.sheets or not self.http:
            raise RuntimeError("Worker not initialized")

        data = envelope.data or {}
        source_id = str(data.get("source_id") or "").strip()
        if not source_id:
            raise ValueError("connector_update missing source_id")

        source = await self.registry.get_source(source_type="google_sheets", source_id=source_id)
        if not source or not source.enabled:
            logger.info(f"Skipping disabled/unregistered source (google_sheets:{source_id})")
            return None

        mapping = await self.registry.get_mapping(source_type="google_sheets", source_id=source_id)
        if not mapping or not mapping.enabled:
            logger.info(f"Skipping auto-import (mapping not enabled) for google_sheets:{source_id}")
            return None

        db_name = (mapping.target_db_name or "").strip()
        class_label = (mapping.target_class_label or "").strip()
        branch = (mapping.target_branch or "main").strip() or "main"
        if not db_name or not class_label:
            logger.info(f"Skipping auto-import (target not configured) for google_sheets:{source_id}")
            return None

        cfg = source.config_json or {}
        sheet_url = (cfg.get("sheet_url") or "").strip()
        worksheet_name = (cfg.get("worksheet_name") or "").strip() or None
        if not sheet_url:
            raise ValueError("Registered source is missing sheet_url")

        access_token = (cfg.get("access_token") or "").strip() or None
        if not access_token:
            refresh_token = (cfg.get("refresh_token") or "").strip() or None
            if refresh_token:
                from data_connector.google_sheets.auth import GoogleOAuth2Client

                oauth_client = GoogleOAuth2Client()
                if oauth_client.client_id and oauth_client.client_secret:
                    refreshed = await oauth_client.refresh_access_token(refresh_token)
                    access_token = (refreshed.get("access_token") or "").strip() or None
                    cfg.update(
                        {
                            "access_token": refreshed.get("access_token"),
                            "refresh_token": refreshed.get("refresh_token", refresh_token),
                            "expires_at": refreshed.get("expires_at"),
                        }
                    )
                    await self.registry.upsert_source(
                        source_type=source.source_type,
                        source_id=source.source_id,
                        enabled=True,
                        config_json=cfg,
                    )

        _, _, _, _, values = await self.sheets.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name,
            access_token=access_token,
        )
        columns, rows = normalize_sheet_data(values)

        max_rows = cfg.get("max_import_rows")
        try:
            max_rows = int(max_rows) if max_rows is not None else None
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at connector_sync_worker/main.py:365", exc_info=True)
            max_rows = None
        if max_rows is not None and max_rows > 0:
            rows = rows[: int(max_rows)]

        if not columns or not rows:
            logger.info(f"No data to import (google_sheets:{source_id})")
            return None

        # Build mappings (identity by default)
        mappings: list[FieldMapping] = []
        fm_raw = mapping.field_mappings or []
        if fm_raw:
            for m in fm_raw:
                if not isinstance(m, dict):
                    continue
                sf = (m.get("source_field") or "").strip()
                tf = (m.get("target_field") or "").strip()
                if sf and tf:
                    mappings.append(FieldMapping(source_field=sf, target_field=tf))
        else:
            for c in columns:
                name = str(c).strip()
                if name:
                    mappings.append(FieldMapping(source_field=name, target_field=name))

        if not mappings:
            raise ValueError("No field mappings available for auto-import")

        target_types = await self._target_field_types(db_name=db_name, class_label=class_label, branch=branch)
        build = SheetImportService.build_instances(
            columns=columns,
            rows=rows,
            mappings=mappings,
            target_field_types=target_types,
        )

        instances = build.get("instances") or []
        errors = build.get("errors") or []
        warnings = build.get("warnings") or []

        if not instances:
            raise ValueError(f"No valid instances to import (errors={len(errors)})")

        bff_url = get_settings().services.bff_base_url
        url = f"{bff_url}/api/v1/databases/{db_name}/instances/{class_label}/bulk-create"
        payload = {
            "instances": instances,
            "metadata": {
                "source": "connector_sync_worker",
                "connector": {
                    "source_type": "google_sheets",
                    "source_id": source_id,
                    "event_id": str(envelope.event_id),
                    "sequence_number": envelope.sequence_number,
                    "cursor": data.get("cursor"),
                    "previous_cursor": data.get("previous_cursor"),
                },
                "import": {
                    "warnings": warnings[:200],
                    "errors": errors[:200],
                    "stats": build.get("stats") or {},
                },
            },
        }

        headers = self._bff_scope_headers(db_name=db_name)
        resp = await self.http.post(url, params={"branch": branch}, json=payload, headers=headers or None)
        resp.raise_for_status()
        result = resp.json()

        command_id: Optional[str] = None
        if isinstance(result, dict):
            command_id = (
                str(result.get("command_id") or "").strip()
                or str(result.get("commandId") or "").strip()
                or str(result.get("id") or "").strip()
            )
            if not command_id:
                # Some endpoints may return nested structures; keep best-effort.
                data_obj = result.get("data") if isinstance(result.get("data"), dict) else None
                if data_obj:
                    command_id = str(data_obj.get("command_id") or data_obj.get("commandId") or "").strip() or None

        if command_id:
            async def _record_connector_command_lineage() -> None:
                await self.lineage.record_link(  # type: ignore[union-attr]
                    from_node_id=LineageStore.node_event(str(envelope.event_id)),
                    to_node_id=LineageStore.node_event(str(command_id)),
                    edge_type="connector_triggered_command",
                    occurred_at=envelope.occurred_at,
                    db_name=db_name,
                    edge_metadata={
                        "db_name": db_name,
                        "source_type": "google_sheets",
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
            "✅ Auto-import submitted (source=google_sheets:%s, db=%s, class=%s, instances=%s, command_id=%s)",
            source_id,
            db_name,
            class_label,
            len(instances),
            command_id,
        )
        return command_id

    async def _handle_envelope(self, envelope: EventEnvelope) -> Optional[str]:
        kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
        if kind != "connector_update":
            raise ValueError(f"Unexpected envelope kind: {kind}")

        source_type = str((envelope.data or {}).get("source_type") or "").strip()
        if source_type == "google_sheets":
            return await self._process_google_sheets_update(envelope)
        raise ValueError(f"Unsupported source_type: {source_type}")

    async def run(self) -> None:
        if not self.consumer or not self.processed or not self.registry:
            raise RuntimeError("Worker not initialized")

        self.running = True
        logger.info("🚀 ConnectorSyncWorker started")

        await self.run_loop(poll_timeout=1.0, idle_sleep=None, catch_exceptions=False)


if __name__ == "__main__":
    configure_logging(get_settings().observability.log_level)
    asyncio.run(run_component_lifecycle(ConnectorSyncWorker()))
