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
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from uuid import uuid5, NAMESPACE_URL

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition
import asyncpg

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig
from shared.config.settings import ApplicationSettings
from shared.models.commands import (
    BaseCommand, CommandType, CommandStatus, 
    OntologyCommand, DatabaseCommand, BranchCommand
)
from shared.models.events import (
    BaseEvent, EventType,
    OntologyEvent, DatabaseEvent, BranchEvent,
    CommandFailedEvent
)
from shared.models.event_envelope import EventEnvelope
from shared.models.config import ConnectionConfig
from oms.services.async_terminus import AsyncTerminusService
from oms.services.event_store import EventStore
from shared.services.redis_service import RedisService, create_redis_service
from shared.services.command_status_service import CommandStatusService
from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry
from shared.services.lineage_store import LineageStore
from shared.services.audit_log_store import AuditLogStore
from shared.security.input_sanitizer import validate_branch_name
from shared.utils.ontology_version import build_ontology_version

# Observability imports
from shared.observability.tracing import get_tracing_service, trace_endpoint
from shared.observability.metrics import get_metrics_collector
from shared.observability.context_propagation import ContextPropagator

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OntologyWorker:
    """온톨로지 Command를 처리하는 워커"""
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.enable_event_sourcing = os.getenv("ENABLE_EVENT_SOURCING", "true").lower() == "true"
        self.enable_processed_event_registry = (
            os.getenv("ENABLE_PROCESSED_EVENT_REGISTRY", "true").lower() == "true"
        )
        self.enable_lineage = os.getenv("ENABLE_LINEAGE", "true").strip().lower() in {"1", "true", "yes", "on"}
        self.enable_audit_logs = os.getenv("ENABLE_AUDIT_LOGS", "true").strip().lower() in {"1", "true", "yes", "on"}
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.terminus_service: Optional[AsyncTerminusService] = None
        self.redis_service: Optional[RedisService] = None
        self.command_status_service: Optional[CommandStatusService] = None
        self.event_store: Optional[EventStore] = None
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None
        self.tracing_service = None
        self.metrics_collector = None
        self.context_propagator = ContextPropagator()

    @staticmethod
    def _coerce_commit_id(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip() or None
        if isinstance(value, dict):
            for key in ("commit", "commit_id", "identifier", "id", "@id", "head"):
                candidate = value.get(key)
                if candidate:
                    return str(candidate).strip() or None
        return str(value).strip() or None

    async def _resolve_ontology_version(self, db_name: str, branch: str) -> Dict[str, str]:
        """
        Resolve the current ontology semantic contract version (ref + commit).

        This stamp is attached to:
        - Terminus side-effects (lineage/audit)
        - Domain events (so projections inherit it)
        """
        commit: Optional[str] = None
        if self.terminus_service:
            branches = await self.terminus_service.version_control_service.list_branches(db_name)
            for item in branches or []:
                if not isinstance(item, dict):
                    continue
                if item.get("name") == branch:
                    commit = self._coerce_commit_id(item.get("head"))
                    break
        return build_ontology_version(branch=branch, commit=commit)

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        if not self.processed_event_registry:
            return
        interval = int(os.getenv("PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS", "30"))
        while True:
            await asyncio.sleep(interval)
            ok = await self.processed_event_registry.heartbeat(handler=handler, event_id=event_id)
            if not ok:
                return
        
    async def initialize(self):
        """워커 초기화"""
        # Kafka Consumer 설정
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'ontology-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5분
            'session.timeout.ms': 45000,  # 45초
        })
        
        # Kafka Producer 설정 (Event 발행용)
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'ontology-worker',
            'acks': 'all',
            'retries': 3,
            'compression.type': 'snappy',
        })
        
        # TerminusDB 연결 설정 - 올바른 인증 정보 사용
        connection_info = ConnectionConfig(
            server_url=ServiceConfig.get_terminus_url(),  # Use ServiceConfig for correct endpoint
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin"),
        )
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()
        
        # Redis 연결 설정 (선택적 - 실패해도 계속 진행)
        try:
            settings = ApplicationSettings()
            self.redis_service = create_redis_service(settings)
            await self.redis_service.connect()
            self.command_status_service = CommandStatusService(self.redis_service)
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed, continuing without command status tracking: {e}")
            self.redis_service = None
            self.command_status_service = None

        if self.enable_event_sourcing:
            try:
                self.event_store = EventStore()
                await self.event_store.connect()
                logger.info("✅ Event Store connected (domain events will be appended to S3/MinIO)")
            except Exception as e:
                logger.warning(f"⚠️ Event Store connection failed, falling back to direct Kafka publish: {e}")
                self.event_store = None

        # Durable processed-events registry (idempotency + ordering guard)
        if self.enable_processed_event_registry:
            self.processed_event_registry = ProcessedEventRegistry()
            await self.processed_event_registry.connect()
            logger.info("✅ ProcessedEventRegistry connected (Postgres)")
        else:
            logger.warning("⚠️ ProcessedEventRegistry disabled (duplicates may re-apply side-effects)")

        # First-class lineage/audit (best-effort; do not fail the worker)
        if self.enable_lineage:
            try:
                self.lineage_store = LineageStore()
                await self.lineage_store.initialize()
                logger.info("✅ LineageStore connected (Postgres)")
            except Exception as e:
                logger.warning(f"⚠️ LineageStore unavailable (continuing without lineage): {e}")
                self.lineage_store = None

        if self.enable_audit_logs:
            try:
                self.audit_store = AuditLogStore()
                await self.audit_store.initialize()
                logger.info("✅ AuditLogStore connected (Postgres)")
            except Exception as e:
                logger.warning(f"⚠️ AuditLogStore unavailable (continuing without audit logs): {e}")
                self.audit_store = None
        
        # Command 토픽 구독 - 온톨로지와 데이터베이스 명령 모두 처리
        self.consumer.subscribe([
            AppConfig.ONTOLOGY_COMMANDS_TOPIC,
            AppConfig.DATABASE_COMMANDS_TOPIC
        ])
        
        # Initialize OpenTelemetry
        self.tracing_service = get_tracing_service("ontology-worker")
        self.metrics_collector = get_metrics_collector("ontology-worker")
        
        logger.info("Ontology Worker initialized successfully")
        
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

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
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
            
    async def handle_create_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 생성 처리"""
        import sys
        sys.stdout.flush()
        sys.stdout.flush()
        
        payload = command_data.get('payload', {}) or {}
        db_name = payload.get('db_name')  # Fixed: get db_name from payload
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
                abstract=payload.get('abstract', False)
            )
            
            # Idempotent create: if it already exists, treat as success.
            try:
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
                        except Exception:
                            pass
                    raise

            class_id = payload.get("class_id")
            ontology_version = await self._resolve_ontology_version(db_name, branch)
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
                except Exception:
                    pass

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
                except Exception:
                    pass
            
            
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
        db_name = command_data.get('db_name')
        payload = command_data.get('payload', {}) or {}
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
                        except Exception:
                            pass
                    raise

            ontology_version = await self._resolve_ontology_version(db_name, branch)
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
                except Exception:
                    pass

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
                except Exception:
                    pass
            
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
        db_name = command_data.get('db_name')
        payload = command_data.get('payload', {}) or {}
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
                        except Exception:
                            pass
                    raise Exception(f"Failed to delete ontology class '{class_id}'")
                logger.info(f"Ontology '{class_id}' already deleted; treating delete as idempotent success")
                already_missing = True

            ontology_version = await self._resolve_ontology_version(db_name, branch)

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
                except Exception:
                    pass

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
                except Exception:
                    pass
            
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
        
        logger.info(f"Creating database: {db_name}")
        
        try:
            # Idempotent create: if it already exists, treat as success.
            created = await self.terminus_service.create_database(
                db_name,
                payload.get('description', '')  # Fixed: only pass 2 args, no label
            )
            if not created:
                exists = await self.terminus_service.database_exists(db_name)
                if not exists:
                    raise Exception(f"Failed to create database '{db_name}'")
                logger.info(f"Database '{db_name}' already exists; treating create as idempotent success")
            
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
        
        logger.info(f"Deleting database: {db_name}")
        
        try:
            # Idempotent delete: "not found" is success (already deleted).
            success = await self.terminus_service.delete_database(db_name)
            if not success:
                exists = await self.terminus_service.database_exists(db_name)
                if exists:
                    raise Exception(f"Failed to delete database '{db_name}'")
                logger.info(f"Database '{db_name}' already deleted; treating delete as idempotent success")
            
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

        metadata: Dict[str, Any] = {
            "kind": "domain",
            "kafka_topic": kafka_topic,
            "service": "ontology_worker",
            "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
            "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
        }
        if isinstance(event.metadata, dict):
            metadata.update(event.metadata)

        event_id = str(event.event_id)
        if getattr(event, "command_id", None):
            command_id_str = str(event.command_id)
            metadata.setdefault("command_id", command_id_str)
            event_id = str(uuid5(NAMESPACE_URL, f"spice:{command_id_str}:{event_type}:{event.aggregate_id}"))

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
        if self.enable_event_sourcing and self.event_store:
            envelope = self._to_domain_envelope(event, kafka_topic=AppConfig.ONTOLOGY_EVENTS_TOPIC)
            await self.event_store.append_event(envelope)
            logger.info(
                f"✅ Stored domain event {envelope.event_id} [{envelope.event_type}] "
                f"(seq={envelope.sequence_number}) for {envelope.aggregate_type}/{envelope.aggregate_id}"
            )
            return

        # Fallback: direct Kafka publish (legacy)
        partition_key = (
            event.aggregate_id if hasattr(event, "aggregate_id") and event.aggregate_id else str(event.event_id)
        )
        self.producer.produce(
            topic=AppConfig.ONTOLOGY_EVENTS_TOPIC,
            value=event.model_dump_json(),
            key=partition_key.encode("utf-8"),
        )
        self.producer.flush()
        
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
        
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        
        logger.info("Ontology Worker started")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    # 메시지 파싱
                    registry_event_id = None
                    registry_aggregate_id = None
                    registry_sequence = None
                    registry_claimed = False
                    registry_attempt_count = 1
                    command_data: Dict[str, Any] = {}

                    try:
                        envelope = EventEnvelope.model_validate_json(msg.value())
                    except Exception as e:
                        logger.error(f"Failed to parse EventEnvelope JSON: {e}")
                        # Poison pill: commit to avoid infinite retry loop.
                        self.consumer.commit(asynchronous=False)
                        continue

                    kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
                    if kind != "command":
                        logger.error(f"Unexpected envelope kind for command topic: {kind}")
                        # Poison pill: commit to avoid infinite retry loop.
                        self.consumer.commit(asynchronous=False)
                        continue

                    command_data = dict(envelope.data or {})
                    if command_data.get("aggregate_id") and command_data.get("aggregate_id") != envelope.aggregate_id:
                        raise ValueError(
                            f"aggregate_id mismatch: command.aggregate_id={command_data.get('aggregate_id')} "
                            f"envelope.aggregate_id={envelope.aggregate_id}"
                        )
                    command_data["aggregate_id"] = envelope.aggregate_id
                    command_data.setdefault("event_id", envelope.event_id)
                    command_data.setdefault("sequence_number", envelope.sequence_number)
                    logger.info(f"Unwrapped command event: {envelope.event_type}")

                    # Durable idempotency + ordering guard (Postgres)
                    registry_event_id = command_data.get("event_id") or command_data.get("command_id")
                    registry_aggregate_id = command_data.get("aggregate_id")
                    registry_sequence = command_data.get("sequence_number")
                    if self.processed_event_registry and registry_event_id:
                        claim = await self.processed_event_registry.claim(
                            handler="ontology_worker",
                            event_id=str(registry_event_id),
                            aggregate_id=str(registry_aggregate_id) if registry_aggregate_id else None,
                            sequence_number=int(registry_sequence) if registry_sequence is not None else None,
                        )
                        if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                            logger.info(
                                f"Skipping {claim.decision.value} command event_id={registry_event_id} "
                                f"(aggregate_id={registry_aggregate_id}, seq={registry_sequence})"
                            )
                            self.consumer.commit(asynchronous=False)
                            continue
                        if claim.decision == ClaimDecision.IN_PROGRESS:
                            logger.info(
                                f"Command {registry_event_id} is in progress elsewhere; retrying later"
                            )
                            await asyncio.sleep(2)
                            self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                            continue
                        registry_claimed = True
                        registry_attempt_count = int(claim.attempt_count or 1)

                    heartbeat_task = None
                    if registry_claimed and self.processed_event_registry and registry_event_id:
                        heartbeat_task = asyncio.create_task(
                            self._heartbeat_loop(handler="ontology_worker", event_id=str(registry_event_id))
                        )
                    
                    try:
                        logger.info(f"Processing command: {command_data.get('command_type')} "
                                   f"for {command_data.get('aggregate_id')}")
                        
                        # Command 처리
                        await self.process_command(command_data)

                        if registry_claimed and self.processed_event_registry and registry_event_id:
                            await self.processed_event_registry.mark_done(
                                handler="ontology_worker",
                                event_id=str(registry_event_id),
                                aggregate_id=str(registry_aggregate_id) if registry_aggregate_id else None,
                                sequence_number=int(registry_sequence) if registry_sequence is not None else None,
                            )
                        
                        # 처리 성공 시 오프셋 커밋
                        self.consumer.commit(asynchronous=False)
                    finally:
                        if heartbeat_task:
                            heartbeat_task.cancel()
                            with suppress(asyncio.CancelledError):
                                await heartbeat_task
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                    # 파싱 실패해도 오프셋은 커밋 (무한 루프 방지)
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error(f"Error processing command: {e}")
                    if registry_claimed and self.processed_event_registry and registry_event_id:
                        try:
                            await self.processed_event_registry.mark_failed(
                                handler="ontology_worker",
                                event_id=str(registry_event_id),
                                error=str(e),
                            )
                        except Exception as reg_err:
                            logger.warning(f"Failed to mark event failed in registry: {reg_err}")
                    # Surface failure to user-facing command status (if available)
                    command_id = command_data.get("command_id") if isinstance(command_data, dict) else None
                    if command_id and self.command_status_service:
                        try:
                            await self.command_status_service.fail_command(str(command_id), str(e))
                        except Exception as cs_err:
                            logger.warning(f"Failed to update command status for {command_id}: {cs_err}")
                    # Emit a failure event for observability (best-effort; never crash on failure publishing).
                    try:
                        await self.publish_failure_event(command_data, str(e))
                    except Exception as pub_err:
                        logger.warning(f"Failed to publish failure event: {pub_err}")

                    # Decide retry vs. skip (avoid poison-pill crash loops).
                    attempt_count = int(registry_attempt_count or 1)

                    max_attempts = int(os.getenv("ONTOLOGY_WORKER_MAX_RETRY_ATTEMPTS", "5"))
                    retryable = self._is_retryable_error(e)

                    if retryable and attempt_count < max_attempts:
                        backoff_s = min(2 ** max(attempt_count - 1, 0), 60)
                        logger.warning(
                            f"Retrying command event_id={registry_event_id} in {backoff_s}s "
                            f"(attempt {attempt_count}/{max_attempts})"
                        )
                        await asyncio.sleep(backoff_s)
                        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                        continue

                    logger.error(
                        f"Skipping failed command event_id={registry_event_id} after {attempt_count} attempts "
                        f"(retryable={retryable}); committing offset to avoid poison pill"
                    )
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        finally:
            await self.shutdown()
            
    async def shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Ontology Worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        if self.terminus_service:
            await self.terminus_service.disconnect()
            
        if self.redis_service:
            await self.redis_service.disconnect()

        if self.processed_event_registry:
            await self.processed_event_registry.close()
            
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
