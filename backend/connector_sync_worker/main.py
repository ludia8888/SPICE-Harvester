"""
Connector Sync Worker (Foundry-style shared runtime).

Consumes Kafka `connector-updates` events (EventEnvelope; metadata.kind='connector_update').

When a confirmed mapping exists, fetches + normalizes data via connector adapters and submits
write commands to the platform engine (BFF/OMS async 202 path).

Core policies (Foundry-style tradeoffs):
- At-least-once + idempotent: ProcessedEventRegistry claims side-effects.
- Mapping-gated: no mapping → no writes.
- Queueing + backoff + DLQ: defaults to safe operational behavior.
- Lineage: connector event → submitted command_id edge is recorded (best-effort).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import httpx
from confluent_kafka import Consumer, KafkaError, Producer

from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.utils import normalize_sheet_data
from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.connector_registry import ConnectorRegistry
from shared.services.lineage_store import LineageStore
from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry
from shared.services.sheet_import_service import FieldMapping, SheetImportService
from shared.utils.import_type_normalization import normalize_import_target_type

logger = logging.getLogger(__name__)


def _parse_int(name: str, default: int, *, min_value: int = 0, max_value: int = 1_000_000) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return max(min_value, min(max_value, value))


def _get_bff_token() -> Optional[str]:
    for key in ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ConnectorSyncWorker:
    def __init__(self) -> None:
        self.running = False

        self.topic = (os.getenv("CONNECTOR_UPDATES_TOPIC") or "connector-updates").strip() or "connector-updates"
        self.dlq_topic = (
            (os.getenv("CONNECTOR_UPDATES_DLQ_TOPIC") or "connector-updates-dlq").strip() or "connector-updates-dlq"
        )
        self.group_id = (os.getenv("CONNECTOR_SYNC_GROUP") or "connector-sync-worker-group").strip()
        self.handler = (os.getenv("CONNECTOR_SYNC_HANDLER") or "connector_sync_worker").strip()

        self.max_retries = _parse_int("CONNECTOR_SYNC_MAX_RETRIES", 5, min_value=1, max_value=100)
        self.backoff_base = _parse_int("CONNECTOR_SYNC_BACKOFF_BASE_SECONDS", 2, min_value=0, max_value=300)
        self.backoff_max = _parse_int("CONNECTOR_SYNC_BACKOFF_MAX_SECONDS", 60, min_value=1, max_value=3600)

        self._consumer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-consumer")
        self._producer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-producer")

        self.consumer: Optional[Consumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.registry: Optional[ConnectorRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage: Optional[LineageStore] = None
        self.sheets: Optional[GoogleSheetsService] = None
        self.http: Optional[httpx.AsyncClient] = None

    async def _consumer_call(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._consumer_executor, lambda: func(*args, **kwargs))

    async def _producer_call(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._producer_executor, lambda: func(*args, **kwargs))

    async def initialize(self) -> None:
        self.registry = ConnectorRegistry()
        await self.registry.initialize()

        self.processed = ProcessedEventRegistry()
        await self.processed.initialize()

        # Lineage is best-effort; do not fail worker startup if it's unavailable.
        self.lineage = LineageStore()
        try:
            await self.lineage.initialize()
        except Exception as e:
            logger.warning(f"LineageStore unavailable (continuing without lineage): {e}")
            self.lineage = None

        # Connector adapter (v1: google sheets)
        api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_SHEETS_API_KEY") or "").strip() or None
        self.sheets = GoogleSheetsService(api_key=api_key)

        # HTTP client to call BFF (auth is fail-closed in prod).
        token = _get_bff_token()
        headers: Dict[str, str] = {}
        if token:
            headers["X-Admin-Token"] = token
        else:
            logger.warning("No BFF auth token configured; BFF calls may fail (set ADMIN_TOKEN/BFF_ADMIN_TOKEN).")
        self.http = httpx.AsyncClient(timeout=60.0, headers=headers)

        # Kafka consumer
        self.consumer = Consumer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 300000,
            }
        )
        await self._consumer_call(self.consumer.subscribe, [self.topic])

        # DLQ producer (best-effort)
        self.dlq_producer = Producer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "client.id": os.getenv("SERVICE_NAME") or "connector-sync-worker",
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 100,
                "linger.ms": 20,
                "compression.type": "snappy",
            }
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
        if self.consumer:
            try:
                await self._consumer_call(self.consumer.close)
            except Exception:
                pass
            self.consumer = None
        if self.dlq_producer:
            try:
                await self._producer_call(self.dlq_producer.flush, 5)
            except Exception:
                pass
            self.dlq_producer = None

        self._consumer_executor.shutdown(wait=False, cancel_futures=True)
        self._producer_executor.shutdown(wait=False, cancel_futures=True)

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        if not self.processed:
            return
        interval = _parse_int("PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS", 30, min_value=1, max_value=600)
        while True:
            await asyncio.sleep(interval)
            ok = await self.processed.heartbeat(handler=handler, event_id=event_id)
            if not ok:
                return

    async def _send_to_dlq(self, envelope: EventEnvelope, *, error: str, attempt_count: int) -> None:
        if not self.dlq_producer:
            return

        dlq_env = envelope.model_copy(deep=True)
        if not isinstance(dlq_env.metadata, dict):
            dlq_env.metadata = {}
        dlq_env.metadata.update(
            {
                "kind": "connector_update_dlq",
                "dlq_error": (error or "").strip()[:4000],
                "dlq_attempt_count": int(attempt_count),
                "dlq_topic": self.dlq_topic,
            }
        )
        dlq_env.event_type = "CONNECTOR_UPDATE_FAILED"

        key = dlq_env.aggregate_id.encode("utf-8")
        value = dlq_env.model_dump_json().encode("utf-8")
        await self._producer_call(self.dlq_producer.produce, topic=self.dlq_topic, key=key, value=value)
        await self._producer_call(self.dlq_producer.flush, 10)

    async def _fetch_ontology_schema(self, *, db_name: str, class_label: str, branch: str) -> Dict[str, Any]:
        if not self.http:
            raise RuntimeError("HTTP client not initialized")
        bff_url = ServiceConfig.get_bff_url()
        url = f"{bff_url}/api/v1/database/{db_name}/ontology/{class_label}/schema"
        resp = await self.http.get(url, params={"format": "json", "branch": branch})
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

        _, _, _, _, values = await self.sheets.fetch_sheet_values(sheet_url, worksheet_name=worksheet_name)
        columns, rows = normalize_sheet_data(values)

        max_rows = cfg.get("max_import_rows")
        try:
            max_rows = int(max_rows) if max_rows is not None else None
        except Exception:
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

        bff_url = ServiceConfig.get_bff_url()
        url = f"{bff_url}/api/v1/database/{db_name}/instances/{class_label}/bulk-create"
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

        resp = await self.http.post(url, params={"branch": branch}, json=payload)
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

        if command_id and self.lineage:
            try:
                await self.lineage.record_link(
                    from_node_id=self.lineage.node_event(str(envelope.event_id)),
                    to_node_id=self.lineage.node_event(str(command_id)),
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
            except Exception as e:
                logger.warning(f"Failed to record lineage edge: {e}")

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

        while self.running:
            msg = await self._consumer_call(self.consumer.poll, timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            envelope: Optional[EventEnvelope] = None
            claim = None
            heartbeat_task = None
            try:
                raw = msg.value().decode("utf-8")
                payload = json.loads(raw)
                envelope = EventEnvelope.model_validate(payload)

                claim = await self.processed.claim(
                    handler=self.handler,
                    event_id=str(envelope.event_id),
                    aggregate_id=str(envelope.aggregate_id),
                    sequence_number=envelope.sequence_number,
                )

                if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                    await self._consumer_call(self.consumer.commit, msg, asynchronous=False)
                    continue

                if claim.decision == ClaimDecision.IN_PROGRESS:
                    # Another worker holds the lease; do not commit to avoid losing the event.
                    await asyncio.sleep(1)
                    continue

                heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(handler=self.handler, event_id=str(envelope.event_id))
                )

                command_id = await self._handle_envelope(envelope)

                # Best-effort sync outcome tracking for ops/UI.
                try:
                    await self.registry.record_sync_outcome(
                        source_type=str((envelope.data or {}).get("source_type") or ""),
                        source_id=str((envelope.data or {}).get("source_id") or ""),
                        success=True,
                        command_id=command_id,
                    )
                except Exception:
                    pass

                await self.processed.mark_done(
                    handler=self.handler,
                    event_id=str(envelope.event_id),
                    aggregate_id=str(envelope.aggregate_id),
                    sequence_number=envelope.sequence_number,
                )
                await self._consumer_call(self.consumer.commit, msg, asynchronous=False)

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in connector-updates (dropping): {e}")
                await self._consumer_call(self.consumer.commit, msg, asynchronous=False)
            except Exception as e:
                err = str(e)
                attempt_count = int(getattr(claim, "attempt_count", 1) or 1)

                if envelope:
                    try:
                        await self.processed.mark_failed(handler=self.handler, event_id=str(envelope.event_id), error=err)
                    except Exception as mark_err:
                        logger.error(f"Failed to mark processed event failed: {mark_err}")

                    # Best-effort sync outcome tracking for ops/UI.
                    try:
                        backoff_s = min(self.backoff_max, int(self.backoff_base * (2 ** max(0, attempt_count - 1))))
                        await self.registry.record_sync_outcome(
                            source_type=str((envelope.data or {}).get("source_type") or ""),
                            source_id=str((envelope.data or {}).get("source_id") or ""),
                            success=False,
                            error=err,
                            next_retry_at=_utcnow() + timedelta(seconds=backoff_s),
                        )
                    except Exception:
                        pass

                    if attempt_count >= self.max_retries:
                        logger.error(f"Max retries exceeded; sending to DLQ (event_id={envelope.event_id})")
                        try:
                            await self._send_to_dlq(envelope, error=err, attempt_count=attempt_count)
                        except Exception as dlq_err:
                            logger.error(f"Failed to send to DLQ: {dlq_err}")

                        try:
                            await self.processed.mark_done(
                                handler=self.handler,
                                event_id=str(envelope.event_id),
                                aggregate_id=str(envelope.aggregate_id),
                                sequence_number=envelope.sequence_number,
                            )
                        except Exception:
                            pass
                        await self._consumer_call(self.consumer.commit, msg, asynchronous=False)
                        continue

                backoff_s = min(self.backoff_max, int(self.backoff_base * (2 ** max(0, attempt_count - 1))))
                await asyncio.sleep(backoff_s)
            finally:
                if heartbeat_task:
                    heartbeat_task.cancel()


async def _main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    worker = ConnectorSyncWorker()
    await worker.initialize()
    try:
        await worker.run()
    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(_main())

