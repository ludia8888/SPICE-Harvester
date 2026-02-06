"""
Ontology Worker Service
Command를 처리하여 실제 TerminusDB 작업을 수행하는 워커 서비스
"""

import asyncio
import json
import logging
import os
import signal
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from confluent_kafka import Producer

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.commands import (
    CommandType,
)
from shared.models.events import (
    BaseEvent, EventType,
    OntologyEvent, DatabaseEvent,
    CommandFailedEvent
)
from shared.models.event_envelope import EventEnvelope
from shared.models.config import ConnectionConfig
from oms.services.async_terminus import AsyncTerminusService
from oms.services.event_store import EventStore
from shared.services.storage.redis_service import RedisService, create_redis_service
from shared.services.core.command_status_service import CommandStatus, CommandStatusService
from shared.services.kafka.consumer_ops import ExecutorKafkaConsumerOps
from shared.services.kafka.processed_event_worker import RegistryKey, StrictHeartbeatKafkaWorker
from shared.services.kafka.producer_factory import create_kafka_producer
from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
)
from shared.services.kafka.safe_consumer import SafeKafkaConsumer, create_safe_consumer
from shared.services.registries.lineage_store import LineageStore
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.ontology_key_spec_registry import OntologyKeySpecRegistry
from shared.services.core.worker_stores import initialize_worker_stores
from shared.security.input_sanitizer import validate_branch_name
from shared.utils.spice_event_ids import spice_event_id
from shared.utils.ontology_version import resolve_ontology_version
from oms.exceptions import DuplicateOntologyError, DatabaseError

# Observability imports
from shared.observability.tracing import get_tracing_service
from shared.observability.metrics import get_metrics_collector
from shared.observability.context_propagation import (
    attach_context_from_kafka,
    kafka_headers_from_current_context,
)
from shared.utils.app_logger import configure_logging

# 로깅 설정
_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _OntologyCommandPayload:
    command: Dict[str, Any]
    envelope_metadata: Optional[Dict[str, Any]] = None


class _OntologyCommandParseError(ValueError):
    def __init__(
        self,
        *,
        stage: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        fallback_metadata: Optional[Dict[str, Any]],
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.stage = str(stage)
        self.payload_text = payload_text
        self.payload_obj = payload_obj
        self.fallback_metadata = fallback_metadata
        self.cause = cause


class OntologyWorker(StrictHeartbeatKafkaWorker[_OntologyCommandPayload, None]):
    """온톨로지 Command를 처리하는 워커"""
    
    def __init__(self):
        settings = get_settings()
        worker_cfg = settings.workers.ontology

        self.running = False
        self.service_name = "ontology-worker"
        self.handler = "ontology_worker"
        self.kafka_servers = settings.database.kafka_servers
        self.enable_event_sourcing = bool(settings.event_sourcing.enable_event_sourcing)
        self.enable_processed_event_registry = bool(settings.event_sourcing.enable_processed_event_registry)
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.enable_audit_logs = bool(settings.observability.enable_audit_logs)
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.producer: Optional[Producer] = None
        self.dlq_producer: Optional[Producer] = None
        self.dlq_topic = AppConfig.ONTOLOGY_COMMANDS_DLQ_TOPIC
        self.dlq_flush_timeout_seconds = float(worker_cfg.dlq_flush_timeout_seconds)
        self.max_retry_attempts = int(worker_cfg.max_retry_attempts)
        self.max_retries = int(self.max_retry_attempts)
        self.backoff_base = 1
        self.backoff_max = 60
        self.db_ready_poll_seconds = float(worker_cfg.db_ready_poll_seconds)
        self.terminus_service: Optional[AsyncTerminusService] = None
        self.redis_service: Optional[RedisService] = None
        self.command_status_service: Optional[CommandStatusService] = None
        self.event_store: Optional[EventStore] = None
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None
        self.key_spec_registry: Optional[OntologyKeySpecRegistry] = OntologyKeySpecRegistry()
        self.tracing_service = None
        self.metrics_collector = None
        self.tracing = None
        self.metrics = None
        self.consumer_ops = None

    @staticmethod
    def _extract_key_spec_from_payload(payload: Dict[str, Any]) -> tuple[list[str], list[str]]:
        """
        Extract ordered primary/title keys from an ontology payload.

        Policy (enterprise-safe):
        - Prefer explicit per-property flags (primaryKey/titleKey) to preserve order.
        - If flags are missing but payload.metadata.key_spec exists, use it as a fallback.
        """
        props = payload.get("properties") or []
        pk_fields: list[str] = []
        title_fields: list[str] = []
        if isinstance(props, list):
            for item in props:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if not name:
                    continue
                if bool(item.get("primaryKey") or item.get("primary_key")):
                    pk_fields.append(name)
                if bool(item.get("titleKey") or item.get("title_key")):
                    title_fields.append(name)

        if pk_fields or title_fields:
            return pk_fields, title_fields

        meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        key_spec = meta.get("key_spec") if isinstance(meta, dict) else None
        if isinstance(key_spec, dict):
            raw_pk = key_spec.get("primary_key") or key_spec.get("primaryKey") or []
            raw_title = key_spec.get("title_key") or key_spec.get("titleKey") or []
            if isinstance(raw_pk, list):
                pk_fields = [str(v).strip() for v in raw_pk if str(v).strip()]
            if isinstance(raw_title, list):
                title_fields = [str(v).strip() for v in raw_title if str(v).strip()]
        return pk_fields, title_fields

    async def _wait_for_database_exists(self, *, db_name: str, expected: bool, timeout_seconds: int = 20) -> None:
        if not self.terminus_service:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(timeout_seconds)
        poll = float(self.db_ready_poll_seconds)
        last = None
        while loop.time() < deadline:
            last = await self.terminus_service.database_exists(db_name)
            if bool(last) is bool(expected):
                return
            await asyncio.sleep(poll)
        raise RuntimeError(
            f"Database '{db_name}' existence check timed out (expected={expected}, last_exists={last})"
        )

    async def initialize(self):
        """워커 초기화"""
        settings = get_settings()

        group_id = (AppConfig.ONTOLOGY_WORKER_GROUP or "ontology-worker-group").strip()

        # Kafka Consumer (strong consistency: read_committed + rebalance-safe offsets)
        self.consumer = create_safe_consumer(
            group_id=group_id,
            topics=[AppConfig.ONTOLOGY_COMMANDS_TOPIC, AppConfig.DATABASE_COMMANDS_TOPIC],
            service_name="ontology-worker",
        )
        self.consumer_ops = ExecutorKafkaConsumerOps(
            self.consumer,
            thread_name_prefix="ontology-worker-kafka",
        )
        
        # Kafka Producer 설정 (Event 발행용)
        self.producer = create_kafka_producer(
            bootstrap_servers=self.kafka_servers,
            client_id="ontology-worker",
        )
        self.dlq_producer = self.producer
        
        # TerminusDB 연결 설정 - 올바른 인증 정보 사용
        connection_info = ConnectionConfig(
            server_url=settings.database.terminus_url.rstrip("/"),
            user=settings.database.terminus_user,
            account=settings.database.terminus_account,
            key=settings.database.terminus_password,
        )
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()
        
        # Redis 연결 설정 (선택적 - 실패해도 계속 진행)
        try:
            self.redis_service = create_redis_service(settings)
            await self.redis_service.connect()
            self.command_status_service = CommandStatusService(self.redis_service)
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed, continuing without command status tracking: {e}")
            self.redis_service = None
            self.command_status_service = None

        # Event Sourcing is now mandatory for write-side correctness.
        # Legacy "direct Kafka publish" paths are removed because they break SSoT.
        if not self.enable_event_sourcing:
            raise RuntimeError(
                "ENABLE_EVENT_SOURCING=false is no longer supported. "
                "Enable Event Sourcing and provide a reachable Event Store (MinIO/S3)."
            )

        self.event_store = EventStore()
        await self.event_store.connect()
        logger.info("✅ Event Store connected (domain events will be appended to S3/MinIO)")

        stores = await initialize_worker_stores(
            enable_lineage=self.enable_lineage,
            enable_audit_logs=self.enable_audit_logs,
            logger=logger,
        )
        self.processed_event_registry = stores.processed
        self.processed = stores.processed
        self.lineage_store = stores.lineage_store
        self.audit_store = stores.audit_store
        
        # Initialize OpenTelemetry
        self.tracing_service = get_tracing_service("ontology-worker")
        self.metrics_collector = get_metrics_collector("ontology-worker")
        self.tracing = self.tracing_service
        self.metrics = self.metrics_collector
        
        logger.info("Ontology Worker initialized successfully")

    async def _publish_to_dlq(
        self,
        *,
        msg: Any,
        stage: str,
        error: str,
        attempt_count: int,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.dlq_producer:
            raise RuntimeError("DLQ producer not configured")

        try:
            original_value = payload_text if payload_text is not None else msg.value().decode("utf-8", errors="replace")
        except Exception:
            original_value = "<unavailable>"

        dlq_message: Dict[str, Any] = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "original_timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "original_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
            "original_value": original_value,
            "stage": stage,
            "error": (error or "").strip()[:4000],
            "attempt_count": int(attempt_count),
            "worker": "ontology-worker",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if payload_obj is not None:
            dlq_message["parsed_payload"] = payload_obj

        key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}".encode("utf-8")
        value = json.dumps(dlq_message, ensure_ascii=False, default=str).encode("utf-8")

        with attach_context_from_kafka(
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata if isinstance(fallback_metadata, dict) else None,
            service_name="ontology-worker",
        ):
            headers = kafka_headers_from_current_context()
            with self.tracing_service.span(
                "ontology_worker.dlq_produce",
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": self.dlq_topic,
                    "messaging.destination_kind": "topic",
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.offset": msg.offset(),
                },
            ):
                self.dlq_producer.produce(self.dlq_topic, key=key, value=value, headers=headers or None)
                await asyncio.to_thread(self.dlq_producer.flush, self.dlq_flush_timeout_seconds)

        try:
            self.metrics_collector.record_event("ONTOLOGY_COMMAND_DLQ", action="published")
        except Exception:
            pass

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        payload: Optional[_OntologyCommandPayload] = None,
        raw_payload: Optional[str] = None,
        stage: str = "process_command",
        payload_text: Optional[str] = None,
        payload_obj: Optional[Dict[str, Any]] = None,
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if kafka_headers is None:
            with suppress(Exception):
                kafka_headers = msg.headers()

        if payload_text is None:
            payload_text = raw_payload

        if payload_obj is None and payload is not None:
            payload_obj = payload.command if isinstance(payload.command, dict) else None

        if fallback_metadata is None and payload is not None:
            fallback_metadata = payload.envelope_metadata

        await self._publish_to_dlq(
            msg=msg,
            stage=str(stage),
            error=error,
            attempt_count=int(attempt_count),
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
        )
        
    async def process_command(self, command_data: Dict[str, Any]) -> None:
        """Command 처리"""
        command_id = command_data.get('command_id')

        # Idempotency guard: if this command is already completed, skip processing.
        if command_id and self.command_status_service:
            try:
                existing = await self.command_status_service.redis.get_command_status(str(command_id))
                if existing and existing.get("status") == "COMPLETED":
                    logger.info(f"Skipping already completed command {command_id}")
                    return
            except Exception as e:
                # Never block command processing on Redis issues; downstream operations must be idempotent too.
                logger.warning(f"Failed to read command status for {command_id}: {e}")

        # Redis에 처리 시작 상태 업데이트
        if command_id and self.command_status_service:
            await self.command_status_service.start_processing(
                command_id=str(command_id),
                worker_id=f"worker-{os.getpid()}",
            )

        command_type = command_data.get('command_type')

        import sys
        sys.stdout.flush()
        sys.stdout.flush()
        sys.stdout.flush()

        # Command 유형별 처리
        if command_type == CommandType.CREATE_ONTOLOGY_CLASS:
            sys.stdout.flush()
            await self.handle_create_ontology(command_data)
            sys.stdout.flush()
        elif command_type == CommandType.UPDATE_ONTOLOGY_CLASS:
            await self.handle_update_ontology(command_data)
        elif command_type == CommandType.DELETE_ONTOLOGY_CLASS:
            await self.handle_delete_ontology(command_data)
        elif command_type == CommandType.CREATE_DATABASE:
            await self.handle_create_database(command_data)
        elif command_type == CommandType.DELETE_DATABASE:
            await self.handle_delete_database(command_data)
        else:
            raise ValueError(f"Unknown command type: {command_type}")

    async def handle_create_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 생성 처리"""
        import sys
        sys.stdout.flush()
        sys.stdout.flush()
        
        payload = command_data.get('payload', {}) or {}
        db_name = payload.get('db_name') or command_data.get("db_name")  # payload-first (historical)
        command_id = command_data.get('command_id')
        branch = validate_branch_name(payload.get("branch") or command_data.get("branch") or "main")
        
        sys.stdout.flush()
        sys.stdout.flush()
        sys.stdout.flush()
        
        
        try:
            # TerminusDB에 온톨로지 생성
            from shared.models.ontology import OntologyBase
            ontology_obj = OntologyBase(
                id=payload.get('class_id'),
                label=payload.get('label'),
                description=payload.get('description'),
                properties=payload.get('properties', []),
                relationships=payload.get('relationships', []),
                parent_class=payload.get('parent_class'),
                abstract=payload.get('abstract', False),
                metadata=payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {},
            )
            
            # Idempotent create: if it already exists, treat as success.
            try:
                advanced = payload.get("advanced_options") or {}
                if advanced:
                    await self.terminus_service.create_ontology_with_advanced_relationships(
                        db_name=db_name,
                        ontology_data=ontology_obj,
                        branch=branch,
                        auto_generate_inverse=bool(advanced.get("auto_generate_inverse", False)),
                        validate_relationships=bool(advanced.get("validate_relationships", True)),
                        check_circular_references=bool(advanced.get("check_circular_references", True)),
                    )
                else:
                    await self.terminus_service.create_ontology(
                        db_name, ontology_obj, branch=branch
                    )  # Fixed: call create_ontology with OntologyBase
            except Exception as e:
                existing = await self.terminus_service.get_ontology(
                    db_name, payload.get("class_id"), branch=branch
                )
                if existing:
                    logger.info(f"Ontology '{payload.get('class_id')}' already exists; treating create as idempotent success")
                else:
                    if command_id and self.audit_store:
                        try:
                            await self.audit_store.log(
                                partition_key=f"db:{db_name}",
                                actor="ontology_worker",
                                action="ONTOLOGY_TERMINUS_WRITE",
                                status="failure",
                                resource_type="terminus_schema",
                                resource_id=f"terminus:{db_name}:{branch}:ontology:{payload.get('class_id')}",
                                event_id=str(command_id),
                                command_id=str(command_id),
                                metadata={"db_name": db_name, "branch": branch, "class_id": payload.get("class_id"), "operation": "create"},
                                error=str(e),
                                occurred_at=datetime.now(timezone.utc),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Audit log write failed (operation=create status=failure db=%s branch=%s class_id=%s): %s",
                                db_name,
                                branch,
                                payload.get("class_id"),
                                exc,
                                exc_info=True,
                            )
                    raise

            class_id = payload.get("class_id")
            if db_name and class_id and self.key_spec_registry:
                try:
                    pk_fields, title_fields = self._extract_key_spec_from_payload(payload)
                    await self.key_spec_registry.upsert_key_spec(
                        db_name=str(db_name),
                        branch=str(branch),
                        class_id=str(class_id),
                        primary_key=pk_fields,
                        title_key=title_fields,
                    )
                except Exception as exc:
                    # Hard fail: without key_spec we cannot provide a reliable primaryKey/titleKey contract.
                    # This is retry-safe because Terminus writes are idempotent and the next retry will
                    # re-attempt the key_spec upsert.
                    logger.error(
                        "Failed to upsert ontology key_spec (db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )
                    raise RuntimeError("ontology_key_spec_upsert_failed") from exc
            ontology_version = await resolve_ontology_version(
                self.terminus_service, db_name=db_name, branch=branch, logger=logger
            )
            if command_id and class_id and self.lineage_store:
                try:
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, f"ontology:{class_id}"),
                        edge_type="event_wrote_terminus_document",
                        occurred_at=datetime.now(timezone.utc),
                        to_label=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                        edge_metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "artifact": "ontology_class",
                            "ontology": ontology_version,
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "Lineage record_link failed (operation=create db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )

            if command_id and class_id and self.audit_store:
                try:
                    await self.audit_store.log(
                        partition_key=f"db:{db_name}",
                        actor="ontology_worker",
                        action="ONTOLOGY_TERMINUS_WRITE",
                        status="success",
                        resource_type="terminus_schema",
                        resource_id=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "operation": "create",
                            "ontology": ontology_version,
                        },
                        occurred_at=datetime.now(timezone.utc),
                    )
                except Exception as exc:
                    logger.warning(
                        "Audit log write failed (operation=create status=success db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )
            
            
            # 성공 이벤트 생성
            event = OntologyEvent(
                event_type=EventType.ONTOLOGY_CLASS_CREATED,
                db_name=db_name,
                branch=branch,
                class_id=payload.get('class_id'),
                command_id=command_id,
                metadata={"ontology": ontology_version},
                data={
                    "db_name": db_name,
                    "branch": branch,
                    "class_id": payload.get('class_id'),
                    "label": payload.get('label'),
                    "description": payload.get('description'),
                    "properties": payload.get('properties', []),
                    "relationships": payload.get('relationships', []),
                    "parent_class": payload.get('parent_class'),
                    "abstract": payload.get('abstract', False),
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            
            # Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "class_id": payload.get('class_id'),
                        "message": f"Successfully created ontology class: {payload.get('class_id')}"
                        }
                )
            
            logger.info(f"Successfully created ontology class: {payload.get('class_id')}")
            
        except Exception as e:
            logger.error(f"Failed to create ontology class: {e}")
            raise
            
    async def handle_update_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 업데이트 처리"""
        payload = command_data.get('payload', {}) or {}
        db_name = payload.get('db_name') or command_data.get("db_name")
        branch = validate_branch_name(payload.get("branch") or command_data.get("branch") or "main")
        class_id = payload.get('class_id')
        updates = payload.get('updates', {})
        command_id = command_data.get('command_id')
        
        logger.info(f"Updating ontology class: {class_id} in database: {db_name}")
        
        try:
            # 기존 데이터 조회
            existing = await self.terminus_service.get_ontology(db_name, class_id, branch=branch)
            if not existing:
                raise Exception(f"Ontology class '{class_id}' not found")

            existing_dict = (
                existing.model_dump() if hasattr(existing, "model_dump") else existing
            )
            
            # 업데이트 데이터 병합
            merged_data = {**existing_dict, **updates}
            merged_data["id"] = class_id  # ID는 변경 불가

            # LocalizedText merge: allow partial updates like {"en": "..."} without dropping other translations.
            for key in ("label", "description"):
                incoming = updates.get(key)
                existing_value = existing_dict.get(key) if isinstance(existing_dict, dict) else None
                if isinstance(existing_value, dict) and isinstance(incoming, dict):
                    merged = dict(existing_value)
                    merged.update(incoming)
                    merged_data[key] = merged
            
            # TerminusDB 업데이트
            from shared.models.ontology import OntologyBase
            ontology_obj = OntologyBase(**merged_data)
            try:
                await self.terminus_service.update_ontology(db_name, class_id, ontology_obj, branch=branch)
            except Exception as e:
                # Best-effort idempotency: if the current ontology already reflects the requested update, continue.
                current = await self.terminus_service.get_ontology(db_name, class_id, branch=branch)
                current_dict = current.model_dump() if hasattr(current, "model_dump") else (current or {})
                already_applied = True
                if isinstance(updates, dict):
                    for k, v in updates.items():
                        if current_dict.get(k) != v:
                            already_applied = False
                            break
                if already_applied:
                    logger.info(f"Ontology '{class_id}' update appears already applied; treating as idempotent success")
                else:
                    if command_id and self.audit_store:
                        try:
                            await self.audit_store.log(
                                partition_key=f"db:{db_name}",
                                actor="ontology_worker",
                                action="ONTOLOGY_TERMINUS_WRITE",
                                status="failure",
                                resource_type="terminus_schema",
                                resource_id=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                                event_id=str(command_id),
                                command_id=str(command_id),
                                metadata={"db_name": db_name, "branch": branch, "class_id": class_id, "operation": "update"},
                                error=str(e),
                                occurred_at=datetime.now(timezone.utc),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Audit log write failed (operation=update status=failure db=%s branch=%s class_id=%s): %s",
                                db_name,
                                branch,
                                class_id,
                                exc,
                                exc_info=True,
                            )
                    raise

            ontology_version = await resolve_ontology_version(
                self.terminus_service, db_name=db_name, branch=branch, logger=logger
            )

            if db_name and class_id and self.key_spec_registry:
                try:
                    pk_fields, title_fields = self._extract_key_spec_from_payload(merged_data)
                    await self.key_spec_registry.upsert_key_spec(
                        db_name=str(db_name),
                        branch=str(branch),
                        class_id=str(class_id),
                        primary_key=pk_fields,
                        title_key=title_fields,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to upsert ontology key_spec after update (db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )
                    raise RuntimeError("ontology_key_spec_upsert_failed") from exc
            if command_id and class_id and self.lineage_store:
                try:
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, f"ontology:{class_id}"),
                        edge_type="event_wrote_terminus_document",
                        occurred_at=datetime.now(timezone.utc),
                        to_label=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                        edge_metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "artifact": "ontology_class",
                            "ontology": ontology_version,
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "Lineage record_link failed (operation=update db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )

            if command_id and class_id and self.audit_store:
                try:
                    await self.audit_store.log(
                        partition_key=f"db:{db_name}",
                        actor="ontology_worker",
                        action="ONTOLOGY_TERMINUS_WRITE",
                        status="success",
                        resource_type="terminus_schema",
                        resource_id=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "operation": "update",
                            "ontology": ontology_version,
                        },
                        occurred_at=datetime.now(timezone.utc),
                    )
                except Exception as exc:
                    logger.warning(
                        "Audit log write failed (operation=update status=success db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )
            
            # 성공 이벤트 생성
            event = OntologyEvent(
                event_type=EventType.ONTOLOGY_CLASS_UPDATED,
                db_name=db_name,
                branch=branch,
                class_id=class_id,
                command_id=command_id,
                metadata={"ontology": ontology_version},
                data={
                    "db_name": db_name,
                    "branch": branch,
                    "class_id": class_id,
                    "updates": updates,
                    "merged_data": merged_data,
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            
            # Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "class_id": class_id,
                        "message": f"Successfully updated ontology class: {class_id}",
                        "updates": updates,
                        }
                )
            
            logger.info(f"Successfully updated ontology class: {class_id}")
            
        except Exception as e:
            logger.error(f"Failed to update ontology class: {e}")
            raise
            
    async def handle_delete_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 삭제 처리"""
        payload = command_data.get('payload', {}) or {}
        db_name = payload.get('db_name') or command_data.get("db_name")
        branch = validate_branch_name(payload.get("branch") or command_data.get("branch") or "main")
        class_id = payload.get('class_id')
        command_id = command_data.get('command_id')
        
        logger.info(f"Deleting ontology class: {class_id} from database: {db_name}")
        
        try:
            # TerminusDB에서 삭제
            # Idempotent delete: "not found" is success (already deleted).
            try:
                success = await self.terminus_service.delete_ontology(db_name, class_id, branch=branch)
            except Exception:
                success = False

            already_missing = False
            if not success:
                existing = await self.terminus_service.get_ontology(db_name, class_id, branch=branch)
                if existing:
                    if command_id and self.audit_store:
                        try:
                            await self.audit_store.log(
                                partition_key=f"db:{db_name}",
                                actor="ontology_worker",
                                action="ONTOLOGY_TERMINUS_DELETE",
                                status="failure",
                                resource_type="terminus_schema",
                                resource_id=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                                event_id=str(command_id),
                                command_id=str(command_id),
                                metadata={"db_name": db_name, "branch": branch, "class_id": class_id, "operation": "delete"},
                                error="delete_failed",
                                occurred_at=datetime.now(timezone.utc),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Audit log write failed (operation=delete status=failure db=%s branch=%s class_id=%s): %s",
                                db_name,
                                branch,
                                class_id,
                                exc,
                                exc_info=True,
                            )
                    raise Exception(f"Failed to delete ontology class '{class_id}'")
                logger.info(f"Ontology '{class_id}' already deleted; treating delete as idempotent success")
                already_missing = True

            ontology_version = await resolve_ontology_version(
                self.terminus_service, db_name=db_name, branch=branch, logger=logger
            )

            if db_name and class_id and self.key_spec_registry:
                try:
                    await self.key_spec_registry.delete_key_spec(
                        db_name=str(db_name),
                        branch=str(branch),
                        class_id=str(class_id),
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to delete ontology key_spec (db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )
                    raise RuntimeError("ontology_key_spec_delete_failed") from exc

            if command_id and class_id and self.lineage_store:
                try:
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, f"ontology:{class_id}"),
                        edge_type="event_deleted_terminus_document",
                        occurred_at=datetime.now(timezone.utc),
                        to_label=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                        edge_metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "artifact": "ontology_class",
                            "ontology": ontology_version,
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "Lineage record_link failed (operation=delete db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )

            if command_id and class_id and self.audit_store:
                try:
                    await self.audit_store.log(
                        partition_key=f"db:{db_name}",
                        actor="ontology_worker",
                        action="ONTOLOGY_TERMINUS_DELETE",
                        status="success",
                        resource_type="terminus_schema",
                        resource_id=f"terminus:{db_name}:{branch}:ontology:{class_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "class_id": class_id,
                            "operation": "delete",
                            "already_missing": already_missing,
                            "ontology": ontology_version,
                        },
                        occurred_at=datetime.now(timezone.utc),
                    )
                except Exception as exc:
                    logger.warning(
                        "Audit log write failed (operation=delete status=success db=%s branch=%s class_id=%s): %s",
                        db_name,
                        branch,
                        class_id,
                        exc,
                        exc_info=True,
                    )
            
            # 성공 이벤트 생성
            event = OntologyEvent(
                event_type=EventType.ONTOLOGY_CLASS_DELETED,
                db_name=db_name,
                branch=branch,
                class_id=class_id,
                command_id=command_id,
                metadata={"ontology": ontology_version},
                data={
                    "db_name": db_name,
                    "branch": branch,
                    "class_id": class_id
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            
            # Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "class_id": class_id,
                        "message": f"Successfully deleted ontology class: {class_id}"
                    }
                )
            
            logger.info(f"Successfully deleted ontology class: {class_id}")
            
        except Exception as e:
            logger.error(f"Failed to delete ontology class: {e}")
            raise
            
    async def handle_create_database(self, command_data: Dict[str, Any]) -> None:
        """데이터베이스 생성 처리"""
        payload = command_data.get('payload', {}) or {}
        db_name = payload.get('database_name')  # Fixed: was 'name', should be 'database_name'
        command_id = command_data.get('command_id')
        
        if not db_name:
            raise ValueError("database_name is required")

        logger.info(f"Creating database: {db_name}")
        
        try:
            # Idempotent create: if it already exists, treat as success.
            try:
                created = await self.terminus_service.create_database(
                    db_name,
                    payload.get('description', '')  # Fixed: only pass 2 args, no label
                )
            except DuplicateOntologyError:
                # Already exists -> treat as idempotent success
                created = False
            except DatabaseError as exc:
                if "already exists" in str(exc).lower():
                    created = False
                else:
                    raise

            if not created:
                exists = await self.terminus_service.database_exists(db_name)
                if not exists:
                    raise Exception(f"Failed to create database '{db_name}'")
                logger.info(f"Database '{db_name}' already exists; treating create as idempotent success")

            # TerminusDB can be briefly eventually-consistent right after creation; ensure it is visible
            # before marking the async command COMPLETED.
            await self._wait_for_database_exists(db_name=db_name, expected=True)
            
            # 성공 이벤트 생성
            event = DatabaseEvent(
                event_type=EventType.DATABASE_CREATED,
                db_name=db_name,
                command_id=command_id,
                data={
                    "db_name": db_name,
                    "label": payload.get('label'),
                    "description": payload.get('description'),
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)

            # Redis에 완료 상태 업데이트 (202 Accepted observability contract)
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=str(command_id),
                    result={
                        "db_name": db_name,
                        "message": f"Successfully created database: {db_name}",
                    },
                )
            logger.info(f"Successfully created database: {db_name}")
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise
            
    async def handle_delete_database(self, command_data: Dict[str, Any]) -> None:
        """데이터베이스 삭제 처리"""
        payload = command_data.get('payload', {}) or {}
        db_name = payload.get('database_name')  # Fixed: was 'name', should be 'database_name'
        command_id = command_data.get('command_id')
        
        if not db_name:
            raise ValueError("database_name is required")

        logger.info(f"Deleting database: {db_name}")
        
        try:
            # Idempotent delete: "not found" is success (already deleted).
            success = await self.terminus_service.delete_database(db_name)
            if not success:
                exists = await self.terminus_service.database_exists(db_name)
                if exists:
                    raise Exception(f"Failed to delete database '{db_name}'")
                logger.info(f"Database '{db_name}' already deleted; treating delete as idempotent success")

            await self._wait_for_database_exists(db_name=db_name, expected=False)
            
            # 성공 이벤트 생성
            event = DatabaseEvent(
                event_type=EventType.DATABASE_DELETED,
                db_name=db_name,
                command_id=command_id,
                data={
                    "db_name": db_name
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)

            # Redis에 완료 상태 업데이트 (202 Accepted observability contract)
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=str(command_id),
                    result={
                        "db_name": db_name,
                        "message": f"Successfully deleted database: {db_name}",
                    },
                )
            logger.info(f"Successfully deleted database: {db_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete database: {e}")
            raise
            
    def _to_domain_envelope(self, event: BaseEvent, *, kafka_topic: str) -> EventEnvelope:
        event_type = event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type)
        data: Dict[str, Any] = dict(event.data) if isinstance(event.data, dict) else {}

        # Preserve useful context for downstream projections
        if hasattr(event, "db_name") and getattr(event, "db_name"):
            data.setdefault("db_name", getattr(event, "db_name"))
        if hasattr(event, "branch") and getattr(event, "branch"):
            data.setdefault("branch", getattr(event, "branch"))
        if hasattr(event, "class_id") and getattr(event, "class_id"):
            data.setdefault("class_id", getattr(event, "class_id"))
        if getattr(event, "command_id", None):
            data.setdefault("command_id", str(event.command_id))

        obs = get_settings().observability
        metadata: Dict[str, Any] = {
            "kind": "domain",
            "kafka_topic": kafka_topic,
            "service": "ontology_worker",
            "run_id": obs.run_id,
            "code_sha": obs.code_sha,
        }
        if isinstance(event.metadata, dict):
            metadata.update(event.metadata)

        event_id = str(event.event_id)
        if getattr(event, "command_id", None):
            command_id_str = str(event.command_id)
            metadata.setdefault("command_id", command_id_str)
            event_id = spice_event_id(
                command_id=command_id_str,
                event_type=event_type,
                aggregate_id=str(event.aggregate_id),
            )

        return EventEnvelope(
            event_id=event_id,
            event_type=event_type,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            occurred_at=event.occurred_at,
            actor=event.occurred_by,
            data=data,
            metadata=metadata,
            schema_version=event.schema_version or "1",
            sequence_number=event.sequence_number,
        )

    async def publish_event(self, event: BaseEvent) -> None:
        """이벤트 발행 (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka)."""
        if not self.event_store:
            raise RuntimeError("Event Store not initialized (ENABLE_EVENT_SOURCING requires Event Store)")

        envelope = self._to_domain_envelope(event, kafka_topic=AppConfig.ONTOLOGY_EVENTS_TOPIC)
        await self.event_store.append_event(envelope)
        logger.info(
            f"✅ Stored domain event {envelope.event_id} [{envelope.event_type}] "
            f"(seq={envelope.sequence_number}) for {envelope.aggregate_type}/{envelope.aggregate_id}"
        )
        
    async def publish_failure_event(self, command_data: Dict[str, Any], error: str) -> None:
        """실패 이벤트 발행"""
        event = CommandFailedEvent(
            aggregate_type=command_data.get('aggregate_type', 'Unknown'),
            aggregate_id=command_data.get('aggregate_id', 'Unknown'),
            command_id=command_data.get('command_id'),
            command_type=command_data.get('command_type', 'Unknown'),
            error_message=error,
            error_details={
                "command_data": command_data
            },
            retry_count=command_data.get('retry_count', 0),
            data={  # 필수 필드 추가
                "command_type": command_data.get('command_type', 'Unknown'),
                "error": error,
                "command_data": command_data
            }
        )
        
        await self.publish_event(event)

    # --- ProcessedEventKafkaWorker hooks ---
    def _parse_payload(self, payload: Any) -> _OntologyCommandPayload:  # type: ignore[override]
        if not isinstance(payload, (bytes, bytearray)):
            raise _OntologyCommandParseError(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=TypeError("Kafka payload must be bytes"),
            )

        try:
            raw_text = payload.decode("utf-8")
        except Exception as exc:
            raise _OntologyCommandParseError(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=exc,
            ) from exc

        raw_message: Any
        try:
            raw_message = json.loads(raw_text)
        except Exception as exc:
            raise _OntologyCommandParseError(
                stage="parse_json",
                payload_text=raw_text,
                payload_obj=None,
                fallback_metadata=None,
                cause=exc,
            ) from exc

        fallback_metadata = raw_message.get("metadata") if isinstance(raw_message, dict) else None
        fallback_metadata = fallback_metadata if isinstance(fallback_metadata, dict) else None

        try:
            envelope = EventEnvelope.model_validate(raw_message if isinstance(raw_message, dict) else {})
        except Exception as exc:
            payload_obj = raw_message if isinstance(raw_message, dict) else None
            raise _OntologyCommandParseError(
                stage="parse_envelope",
                payload_text=raw_text,
                payload_obj=payload_obj,
                fallback_metadata=fallback_metadata,
                cause=exc,
            ) from exc

        kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
        if kind != "command":
            payload_obj = envelope.model_dump(mode="json")
            raise _OntologyCommandParseError(
                stage="validate_envelope",
                payload_text=raw_text,
                payload_obj=payload_obj if isinstance(payload_obj, dict) else None,
                fallback_metadata=envelope.metadata if isinstance(envelope.metadata, dict) else fallback_metadata,
                cause=ValueError(f"unexpected_envelope_kind:{kind}"),
            )

        command_data = dict(envelope.data or {})
        if command_data.get("aggregate_id") and command_data.get("aggregate_id") != envelope.aggregate_id:
            raise _OntologyCommandParseError(
                stage="validate_envelope",
                payload_text=raw_text,
                payload_obj=envelope.model_dump(mode="json"),
                fallback_metadata=envelope.metadata if isinstance(envelope.metadata, dict) else fallback_metadata,
                cause=ValueError(
                    f"aggregate_id mismatch: command.aggregate_id={command_data.get('aggregate_id')} "
                    f"envelope.aggregate_id={envelope.aggregate_id}"
                ),
            )
        command_data["aggregate_id"] = envelope.aggregate_id
        command_data.setdefault("event_id", envelope.event_id)
        command_data.setdefault("sequence_number", envelope.sequence_number)
        return _OntologyCommandPayload(command=command_data, envelope_metadata=envelope.metadata)

    def _fallback_metadata(self, payload: _OntologyCommandPayload) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        return payload.envelope_metadata

    def _registry_key(self, payload: _OntologyCommandPayload) -> RegistryKey:  # type: ignore[override]
        command = payload.command if isinstance(payload.command, dict) else {}

        registry_event_id = command.get("event_id") or command.get("command_id")
        registry_event_id = str(registry_event_id or "").strip()
        if not registry_event_id:
            raise ValueError("event_id is required")

        aggregate_id = command.get("aggregate_id")
        aggregate_id = str(aggregate_id).strip() if aggregate_id is not None else None
        if not aggregate_id:
            aggregate_id = None

        sequence_number = command.get("sequence_number")
        try:
            seq_int = int(sequence_number) if sequence_number is not None else None
        except Exception:
            seq_int = None

        return RegistryKey(event_id=registry_event_id, aggregate_id=aggregate_id, sequence_number=seq_int)

    async def _process_payload(self, payload: _OntologyCommandPayload) -> None:  # type: ignore[override]
        command = payload.command if isinstance(payload.command, dict) else {}
        await self.process_command(command)

    def _span_name(self, *, payload: _OntologyCommandPayload) -> str:  # type: ignore[override]
        return "ontology_worker.process_command"

    def _span_attributes(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: _OntologyCommandPayload,
        registry_key: RegistryKey,
    ) -> Dict[str, Any]:
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        command = payload.command if isinstance(payload.command, dict) else {}
        db_name = command.get("db_name")
        payload_obj = command.get("payload")
        if not db_name and isinstance(payload_obj, dict):
            db_name = payload_obj.get("db_name")
        attrs.update(
            {
                "command.type": command.get("command_type"),
                "command.id": str(command.get("command_id")) if command.get("command_id") is not None else None,
                "db.name": db_name,
            }
        )
        return attrs

    @staticmethod
    def _is_retryable_error(exc: Exception, *, payload: Optional[_OntologyCommandPayload] = None) -> bool:
        msg = str(exc).lower()
        non_retryable_markers = [
            "schema check failure",
            "not_a_class_or_base_type",
            "security violation",
            "invalid",
            "bad request",
            "api error: 400",
        ]
        return not any(marker in msg for marker in non_retryable_markers)

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:  # type: ignore[override]
        stage = "parse"
        payload_text = raw_payload
        payload_obj = None
        fallback_metadata = None
        cause = error

        if isinstance(error, _OntologyCommandParseError):
            stage = error.stage
            payload_text = error.payload_text if error.payload_text is not None else raw_payload
            payload_obj = error.payload_obj
            fallback_metadata = error.fallback_metadata
            cause = error.cause

        kafka_headers = None
        with suppress(Exception):
            kafka_headers = msg.headers()
        try:
            await self._publish_to_dlq(
                msg=msg,
                stage=stage,
                error=str(cause),
                attempt_count=1,
                payload_text=payload_text,
                payload_obj=payload_obj,
                kafka_headers=kafka_headers,
                fallback_metadata=fallback_metadata,
            )
        except Exception as dlq_err:
            logger.error("Failed to publish invalid ontology payload to DLQ; retrying: %s", dlq_err, exc_info=True)
            raise
        logger.exception("Invalid ontology payload; skipping: %s", cause)

    async def _on_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: _OntologyCommandPayload,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        command = payload.command if isinstance(payload.command, dict) else {}
        command_id = str(command.get("command_id") or "").strip()
        if command_id and self.command_status_service:
            with suppress(Exception):
                await self.command_status_service.update_status(
                    command_id,
                    CommandStatus.RETRYING,
                    message="Retrying command after transient failure",
                    error=error,
                )

        with suppress(Exception):
            await self.publish_failure_event(command, error)

        logger.warning(
            "Retrying ontology command in %ss (attempt %s): %s",
            int(backoff_s),
            attempt_count,
            error,
        )

    async def _on_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: _OntologyCommandPayload,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        command = payload.command if isinstance(payload.command, dict) else {}
        command_id = str(command.get("command_id") or "").strip()
        if command_id and self.command_status_service:
            with suppress(Exception):
                await self.command_status_service.fail_command(command_id, error, retry_count=int(attempt_count))

        with suppress(Exception):
            await self.publish_failure_event(command, error)

        logger.error(
            "Ontology command failed after %s attempts (retryable=%s): %s",
            attempt_count,
            retryable,
            error,
        )
        
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        
        logger.info("Ontology Worker started")
        
        try:
            await self.run_loop(poll_timeout=1.0, idle_sleep=None)
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        finally:
            await self.shutdown()
            
    async def shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Ontology Worker...")
        self.running = False
        
        await self._close_consumer_runtime()
            
        if self.terminus_service:
            await self.terminus_service.disconnect()
            
        if self.redis_service:
            await self.redis_service.disconnect()

        if self.processed_event_registry:
            await self.processed_event_registry.close()
        if self.key_spec_registry:
            with suppress(Exception):
                await self.key_spec_registry.close()
            
        logger.info("Ontology Worker shut down successfully")


async def main():
    """메인 진입점"""
    worker = OntologyWorker()
    
    # 종료 시그널 핸들러
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        worker.running = False
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
