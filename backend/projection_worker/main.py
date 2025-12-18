"""
Projection Worker Service
Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커 서비스
"""

import asyncio
import json
import logging
import os
import signal
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition

from shared.config.service_config import ServiceConfig
from shared.config.search_config import (
    get_instances_index_name,
    get_ontologies_index_name,
    DEFAULT_INDEX_SETTINGS
)
from shared.config.app_config import AppConfig
from shared.config.settings import ApplicationSettings
from shared.models.event_envelope import EventEnvelope
from shared.models.events import (
    BaseEvent, EventType,
    InstanceEvent,
    OntologyEvent
)
from shared.services.redis_service import RedisService, create_redis_service
from shared.services.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
from shared.services.projection_manager import ProjectionManager
from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry
from shared.services.lineage_store import LineageStore
from shared.services.audit_log_store import AuditLogStore
from shared.utils.chaos import maybe_crash

# Observability imports
from shared.observability.tracing import get_tracing_service
from shared.observability.metrics import get_metrics_collector
from shared.observability.context_propagation import ContextPropagator

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class _InProgressLeaseError(RuntimeError):
    """Raised when another worker holds the processed_events lease for this event."""


class ProjectionWorker:
    """Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커

    Kafka message contract:
    - Projection topics carry EventEnvelope JSON (metadata.kind == "domain")
    """

    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.redis_service: Optional[RedisService] = None
        self.elasticsearch_service: Optional[ElasticsearchService] = None
        self.projection_manager: Optional[ProjectionManager] = None
        self.tracing_service = None
        self.metrics_collector = None
        self.context_propagator = ContextPropagator()

        # Durable idempotency (Postgres)
        self.enable_processed_event_registry = (
            os.getenv("ENABLE_PROCESSED_EVENT_REGISTRY", "true").lower() == "true"
        )
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None

        # First-class provenance/audit (fail-open by default)
        self.enable_lineage = os.getenv("ENABLE_LINEAGE", "true").strip().lower() in {"1", "true", "yes", "on"}
        self.enable_audit_logs = os.getenv("ENABLE_AUDIT_LOGS", "true").strip().lower() in {"1", "true", "yes", "on"}
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None

        # 생성된 인덱스 캐시 (중복 생성 방지)
        self.created_indices = set()

        # DLQ 토픽
        self.dlq_topic = AppConfig.PROJECTION_DLQ_TOPIC

        # 재시도 설정
        self.max_retries = int(os.getenv("PROJECTION_WORKER_MAX_RETRIES", "5"))
        self.retry_count = {}

        # Cache Stampede 방지 모니터링 메트릭
        self.cache_metrics = {
            "cache_hits": 0,
            "cache_misses": 0,
            "negative_cache_hits": 0,
            "lock_acquisitions": 0,
            "lock_failures": 0,
            "elasticsearch_queries": 0,
            "fallback_queries": 0,
            "total_lock_wait_time": 0.0,
        }

    @staticmethod
    def _is_es_version_conflict(error: Exception) -> bool:
        status = getattr(error, "status_code", None)
        meta = getattr(error, "meta", None)
        if meta is not None:
            status = getattr(meta, "status", status)
        return status == 409

    @staticmethod
    def _parse_sequence(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _extract_envelope_metadata(event_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        metadata = event_data.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        command_id = metadata.get("command_id")
        trace_id = metadata.get("trace_id")
        correlation_id = metadata.get("correlation_id")
        service = metadata.get("service")
        return {
            "command_id": str(command_id) if command_id else None,
            "trace_id": str(trace_id) if trace_id else None,
            "correlation_id": str(correlation_id) if correlation_id else None,
            "origin_service": str(service) if service else None,
        }

    async def _record_es_side_effect(
        self,
        *,
        event_id: str,
        event_data: Dict[str, Any],
        db_name: str,
        index_name: str,
        doc_id: str,
        operation: str,
        status: str,
        record_lineage: bool,
        skip_reason: Optional[str] = None,
        error: Optional[str] = None,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record projection side-effects for provenance (lineage) + audit.

        - Lineage: domain event -> ES artifact (only when record_lineage=True)
        - Audit: structured log (success/failure, skip_reason, ids)
        """
        occurred_at = datetime.now(timezone.utc)
        meta = self._extract_envelope_metadata(event_data)
        seq = self._parse_sequence(event_data.get("sequence_number"))

        if self.audit_store:
            try:
                action = "PROJECTION_ES_INDEX" if operation == "index" else "PROJECTION_ES_DELETE"
                audit_metadata = {
                    "db_name": db_name,
                    "index": index_name,
                    "doc_id": doc_id,
                    "operation": operation,
                    "event_type": event_data.get("event_type"),
                    "aggregate_id": event_data.get("aggregate_id"),
                    "sequence_number": seq,
                    "skipped": bool(skip_reason),
                    "skip_reason": skip_reason,
                    "origin_service": meta.get("origin_service"),
                    "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
                    "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
                }
                if isinstance(extra_metadata, dict) and extra_metadata:
                    audit_metadata.update(extra_metadata)
                await self.audit_store.log(
                    partition_key=f"db:{db_name}",
                    actor="projection_worker",
                    action=action,
                    status=status,
                    resource_type="es_document",
                    resource_id=f"{index_name}/{doc_id}",
                    event_id=str(event_id) if event_id else None,
                    command_id=meta.get("command_id"),
                    trace_id=meta.get("trace_id"),
                    correlation_id=meta.get("correlation_id"),
                    metadata=audit_metadata,
                    error=error,
                    occurred_at=occurred_at,
                )
            except Exception as e:
                logger.debug(f"Audit record failed (non-fatal): {e}")

        if self.lineage_store and record_lineage:
            try:
                edge_type = "event_deleted_es_document" if operation == "delete" else "event_materialized_es_document"
                await self.lineage_store.record_link(
                    from_node_id=self.lineage_store.node_event(str(event_id)),
                    to_node_id=self.lineage_store.node_artifact("es", index_name, doc_id),
                    edge_type=edge_type,
                    occurred_at=occurred_at,
                    to_label=f"es:{index_name}/{doc_id}",
                    edge_metadata={
                        "db_name": db_name,
                        "index": index_name,
                        "doc_id": doc_id,
                        "operation": operation,
                        "sequence_number": seq,
                    },
                )
            except Exception as e:
                logger.debug(f"Lineage record failed (non-fatal): {e}")

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
        # Kafka Consumer 설정 (멀티 토픽 구독)
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'projection-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5분
            'session.timeout.ms': 45000,  # 45초
        })
        
        # Kafka Producer 설정 (실패 이벤트 발행용)
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'projection-worker',
            'acks': 'all',
            'retries': 3,
            'compression.type': 'snappy',
        })
        
        # Redis 연결 설정 (온톨로지 캐싱용)
        settings = ApplicationSettings()
        self.redis_service = create_redis_service(settings)
        await self.redis_service.connect()
        logger.info("Redis connection established")
        
        # Elasticsearch 연결 설정
        self.elasticsearch_service = create_elasticsearch_service(settings)
        await self.elasticsearch_service.connect()
        logger.info("Elasticsearch connection established")

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
        
        # 인덱스 생성 및 매핑 설정
        await self._setup_indices()
        
        # 토픽 구독
        topics = [AppConfig.INSTANCE_EVENTS_TOPIC, AppConfig.ONTOLOGY_EVENTS_TOPIC]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
        # Initialize OpenTelemetry
        self.tracing_service = get_tracing_service("projection-worker")
        self.metrics_collector = get_metrics_collector("projection-worker")
        
        # 🎯 Initialize ProjectionManager for materialized views
        try:
            # ProjectionManager는 GraphFederationServiceWOQL이 필요하므로
            # 실제 프로덕션에서는 별도로 초기화하도록 설계됨
            # 여기서는 스켈레톤만 준비
            logger.info("🎯 ProjectionManager ready for initialization when graph service is available")
            # TODO: Initialize ProjectionManager when GraphFederationServiceWOQL is available
            # self.projection_manager = ProjectionManager(
            #     graph_service=graph_service,
            #     es_service=self.elasticsearch_service,
            #     redis_service=self.redis_service
            # )
        except Exception as e:
            logger.warning(f"ProjectionManager initialization skipped: {e}")
        
    async def _setup_indices(self):
        """매핑 파일 로드 (인덱스는 DB별로 동적 생성)"""
        try:
            # 매핑 파일만 미리 로드
            self.instances_mapping = await self._load_mapping('instances_mapping.json')
            self.ontologies_mapping = await self._load_mapping('ontologies_mapping.json')
            logger.info("Loaded index mappings successfully")
                
        except Exception as e:
            logger.error(f"Failed to load mappings: {e}")
            raise
            
    async def _ensure_index_exists(self, db_name: str, index_type: str = "instances"):
        """특정 데이터베이스의 인덱스가 존재하는지 확인하고 없으면 생성"""
        if index_type == "instances":
            index_name = get_instances_index_name(db_name)
            mapping = self.instances_mapping
        else:
            index_name = get_ontologies_index_name(db_name)
            mapping = self.ontologies_mapping
            
        # 이미 생성된 인덱스는 스킵
        if index_name in self.created_indices:
            return index_name
            
        try:
            if not await self.elasticsearch_service.index_exists(index_name):
                # 설정 병합 (매핑 파일 설정 + 기본 설정)
                settings = mapping.get('settings', {}).copy()
                settings.update(DEFAULT_INDEX_SETTINGS)
                
                await self.elasticsearch_service.create_index(
                    index_name,
                    mappings=mapping['mappings'],
                    settings=settings
                )
                logger.info(f"Created index: {index_name} for database: {db_name}")
                
            self.created_indices.add(index_name)
            return index_name
            
        except Exception as e:
            logger.error(f"Failed to ensure index exists for {db_name}: {e}")
            raise
            
    async def _load_mapping(self, filename: str) -> Dict[str, Any]:
        """매핑 파일 로드"""
        mapping_path = os.path.join(
            os.path.dirname(__file__), 
            "mappings", 
            filename
        )
        try:
            with open(mapping_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load mapping {filename}: {e}")
            raise
            
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        logger.info("Projection Worker started")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                        
                try:
                    # 이벤트 처리
                    await self._process_event(msg)
                    # 성공 시 오프셋 커밋
                    maybe_crash("projection_worker:before_commit", logger=logger)
                    self.consumer.commit(msg)
                    # Clear retry state for this offset
                    key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
                    self.retry_count.pop(key, None)
                    
                except Exception as e:
                    logger.error(f"Failed to process event: {e}")
                    # 재시도 로직
                    await self._handle_retry(msg, e)
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        finally:
            await self._shutdown()
            
    async def _process_event(self, msg):
        """이벤트 처리"""
        try:
            registry_event_id = None
            registry_aggregate_id = None
            registry_sequence = None
            registry_claimed = False

            try:
                envelope = EventEnvelope.model_validate_json(msg.value())
            except Exception as e:
                raise ValueError(f"Invalid EventEnvelope JSON: {e}") from e

            kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
            if kind != "domain":
                raise ValueError(f"Unexpected envelope kind for projection topic {msg.topic()}: {kind}")

            event_data = envelope.model_dump(mode="json")
            event_type = envelope.event_type
            topic = msg.topic()
            
            logger.info(f"Processing event: {event_type} from topic: {topic}")
            
            # Durable idempotency + ordering guard (Postgres)
            registry_event_id = envelope.event_id
            registry_aggregate_id = envelope.aggregate_id
            registry_sequence = envelope.sequence_number
            handler = f"projection_worker:{topic}"

            if self.processed_event_registry and registry_event_id:
                claim = await self.processed_event_registry.claim(
                    handler=handler,
                    event_id=str(registry_event_id),
                    aggregate_id=str(registry_aggregate_id) if registry_aggregate_id else None,
                    sequence_number=int(registry_sequence) if registry_sequence is not None else None,
                )
                if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                    logger.info(
                        f"Skipping {claim.decision.value} event_id={registry_event_id} "
                        f"(aggregate_id={registry_aggregate_id}, seq={registry_sequence})"
                    )
                    return
                if claim.decision == ClaimDecision.IN_PROGRESS:
                    raise _InProgressLeaseError(
                        f"Event {registry_event_id} is already in progress elsewhere (lease not expired)"
                    )
                registry_claimed = True
                maybe_crash("projection_worker:after_claim", logger=logger)

            heartbeat_task = None
            if registry_claimed and self.processed_event_registry and registry_event_id:
                heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(handler=handler, event_id=str(registry_event_id))
                )
            
            try:
                maybe_crash("projection_worker:before_side_effect", logger=logger)
                if topic == AppConfig.INSTANCE_EVENTS_TOPIC:
                    await self._handle_instance_event(event_data)
                elif topic == AppConfig.ONTOLOGY_EVENTS_TOPIC:
                    await self._handle_ontology_event(event_data)
                else:
                    logger.warning(f"Unknown topic: {topic}")
                maybe_crash("projection_worker:after_side_effect", logger=logger)

                if registry_claimed and self.processed_event_registry and registry_event_id:
                    maybe_crash("projection_worker:before_mark_done", logger=logger)
                    await self.processed_event_registry.mark_done(
                        handler=handler,
                        event_id=str(registry_event_id),
                        aggregate_id=str(registry_aggregate_id) if registry_aggregate_id else None,
                        sequence_number=int(registry_sequence) if registry_sequence is not None else None,
                    )
            finally:
                if heartbeat_task:
                    heartbeat_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await heartbeat_task
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse event JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            if (
                registry_claimed
                and self.processed_event_registry
                and registry_event_id
                and "handler" in locals()
            ):
                try:
                    await self.processed_event_registry.mark_failed(
                        handler=handler,
                        event_id=str(registry_event_id),
                        error=str(e),
                    )
                except Exception as reg_err:
                    logger.warning(f"Failed to mark event failed in registry: {reg_err}")
            raise
            
    async def _handle_instance_event(self, event_data: Dict[str, Any]):
        """인스턴스 이벤트 처리"""
        try:
            event_type = event_data.get('event_type')
            instance_data = event_data.get('data', {})
            event_id = event_data.get('event_id')
            
            if event_type == EventType.INSTANCE_CREATED.value:
                await self._handle_instance_created(instance_data, event_id, event_data)
            elif event_type == EventType.INSTANCE_UPDATED.value:
                await self._handle_instance_updated(instance_data, event_id, event_data)
            elif event_type == EventType.INSTANCE_DELETED.value:
                await self._handle_instance_deleted(instance_data, event_id, event_data)
            else:
                logger.warning(f"Unknown instance event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error handling instance event: {e}")
            raise
            
    async def _handle_ontology_event(self, event_data: Dict[str, Any]):
        """온톨로지 이벤트 처리"""
        try:
            event_type = event_data.get('event_type')
            ontology_data = event_data.get('data', {})
            event_id = event_data.get('event_id')
            
            if event_type == EventType.ONTOLOGY_CLASS_CREATED.value:
                await self._handle_ontology_class_created(ontology_data, event_id, event_data)
            elif event_type == EventType.ONTOLOGY_CLASS_UPDATED.value:
                await self._handle_ontology_class_updated(ontology_data, event_id, event_data)
            elif event_type == EventType.ONTOLOGY_CLASS_DELETED.value:
                await self._handle_ontology_class_deleted(ontology_data, event_id, event_data)
            elif event_type == EventType.DATABASE_CREATED.value:
                await self._handle_database_created(ontology_data, event_id, event_data)
            elif event_type == EventType.DATABASE_DELETED.value:
                await self._handle_database_deleted(ontology_data, event_id, event_data)
            else:
                logger.warning(f"Unknown ontology event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error handling ontology event: {e}")
            raise
            
    async def _handle_instance_created(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 생성 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance creation")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "instances")
            
            # 클래스 라벨 조회 (Redis 캐시 활용)
            class_label = await self._get_class_label(instance_data.get('class_id'), db_name)
            
            # Elasticsearch 문서 구성
            instance_id = instance_data.get('instance_id')
            if not instance_id:
                raise ValueError("instance_id is required for instance creation")

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
            if incoming_seq is None:
                existing_doc = await self.elasticsearch_service.get_document(index_name, instance_id)
                if existing_doc:
                    if existing_doc.get("event_id") == event_id:
                        logger.info(
                            f"Skipping duplicate instance create event (event_id={event_id}, instance_id={instance_id})"
                        )
                        await self._record_es_side_effect(
                            event_id=str(event_id),
                            event_data=event_data,
                            db_name=db_name,
                            index_name=index_name,
                            doc_id=str(instance_id),
                            operation="index",
                            status="success",
                            record_lineage=True,
                            skip_reason="duplicate",
                        )
                        return
                    logger.info(
                        f"Instance already exists; skipping create without sequence_number (instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="already_exists_no_sequence",
                    )
                    return

            doc = {
                'instance_id': instance_id,
                'class_id': instance_data.get('class_id'),
                'class_label': class_label,
                'properties': self._normalize_properties(instance_data.get('properties', [])),
                'data': instance_data,  # 원본 데이터 (enabled: false)
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'version': int(incoming_seq) if incoming_seq is not None else 1,
                'db_name': db_name,
                'branch': instance_data.get('branch'),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # instance_id를 문서 ID로 사용 (업데이트/삭제 정합성)
            try:
                await self.elasticsearch_service.index_document(
                    index_name,
                    doc,
                    doc_id=instance_id,
                    refresh=True,
                    version=incoming_seq,
                    version_type="external_gte" if incoming_seq is not None else None,
                    op_type="create" if incoming_seq is None else None,
                )
            except Exception as e:
                if incoming_seq is None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping instance create due to ES create conflict "
                        f"(instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="es_create_conflict",
                    )
                    return
                if incoming_seq is not None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping stale instance create event via ES version conflict "
                        f"(seq={incoming_seq}, instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="stale_version_conflict",
                    )
                    return
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(instance_id),
                    operation="index",
                    status="failure",
                    record_lineage=False,
                    error=str(e),
                )
                raise
            
            logger.info(f"Instance created in Elasticsearch: {instance_id} in index: {index_name}")
            await self._record_es_side_effect(
                event_id=str(event_id),
                event_data=event_data,
                db_name=db_name,
                index_name=index_name,
                doc_id=str(instance_id),
                operation="index",
                status="success",
                record_lineage=True,
            )
            
        except Exception as e:
            logger.error(f"Failed to handle instance created: {e}")
            raise
            
    async def _handle_instance_updated(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 업데이트 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance update")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "instances")
            
            # 클래스 라벨 조회
            class_label = await self._get_class_label(instance_data.get('class_id'), db_name)
            
            # 기존 문서 조회
            instance_id = instance_data.get('instance_id')
            if not instance_id:
                raise ValueError("instance_id is required for instance update")

            existing_doc = await self.elasticsearch_service.get_document(
                index_name,
                instance_id
            )

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
            if existing_doc:
                if existing_doc.get("event_id") == event_id:
                    logger.info(
                        f"Skipping duplicate instance update event (event_id={event_id}, instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="index",
                        status="success",
                        record_lineage=True,
                        skip_reason="duplicate",
                    )
                    return

            if incoming_seq is None and existing_doc:
                logger.warning(
                    f"Refusing to update instance without sequence_number (instance_id={instance_id})"
                )
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(instance_id),
                    operation="index",
                    status="success",
                    record_lineage=False,
                    skip_reason="missing_sequence_number",
                )
                return
            
            version = 1
            if incoming_seq is not None:
                version = int(incoming_seq)

            created_at = None
            if existing_doc:
                created_at = existing_doc.get("created_at")
            
            # 업데이트 문서 구성
            doc = {
                'instance_id': instance_id,
                'class_id': instance_data.get('class_id'),
                'class_label': class_label,
                'properties': self._normalize_properties(instance_data.get('properties', [])),
                'data': instance_data,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'version': version,
                'db_name': db_name,
                'branch': instance_data.get('branch'),
                'created_at': created_at or datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            try:
                if incoming_seq is not None:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=instance_id,
                        refresh=True,
                        version=incoming_seq,
                        version_type="external_gte",
                    )
                else:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=instance_id,
                        refresh=True,
                        op_type="create",
                    )
            except Exception as e:
                if incoming_seq is not None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping stale instance update event via ES version conflict "
                        f"(seq={incoming_seq}, instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="stale_version_conflict",
                    )
                    return
                if incoming_seq is None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping instance update create due to ES conflict "
                        f"(instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="es_create_conflict",
                    )
                    return
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(instance_id),
                    operation="index",
                    status="failure",
                    record_lineage=False,
                    error=str(e),
                )
                raise
            
            logger.info(f"Instance updated in Elasticsearch: {instance_id} in index: {index_name}")
            await self._record_es_side_effect(
                event_id=str(event_id),
                event_data=event_data,
                db_name=db_name,
                index_name=index_name,
                doc_id=str(instance_id),
                operation="index",
                status="success",
                record_lineage=True,
            )
            
        except Exception as e:
            logger.error(f"Failed to handle instance updated: {e}")
            raise
            
    async def _handle_instance_deleted(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 삭제 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance deletion")
                
            # 인덱스 이름 결정
            index_name = get_instances_index_name(db_name)
            instance_id = instance_data.get('instance_id')
            if not instance_id:
                raise ValueError("instance_id is required for instance deletion")

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
            if incoming_seq is None:
                existing_doc = await self.elasticsearch_service.get_document(index_name, instance_id)
                if not existing_doc:
                    logger.info(
                        f"Instance already deleted (instance_id={instance_id}); treating delete as idempotent success"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="delete",
                        status="success",
                        record_lineage=False,
                        skip_reason="already_deleted_no_sequence",
                    )
                    return
                if existing_doc.get("event_id") == event_id:
                    logger.info(
                        f"Skipping duplicate instance delete event (event_id={event_id}, instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="delete",
                        status="success",
                        record_lineage=False,
                        skip_reason="duplicate",
                    )
                    return

                logger.warning(
                    f"Refusing to delete instance without sequence_number (instance_id={instance_id})"
                )
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(instance_id),
                    operation="delete",
                    status="success",
                    record_lineage=False,
                    skip_reason="missing_sequence_number",
                )
                return

            # 문서 삭제 (external version guard)
            try:
                success = await self.elasticsearch_service.delete_document(
                    index_name,
                    instance_id,
                    refresh=True,
                    version=incoming_seq,
                    version_type="external_gte",
                )
            except Exception as e:
                if self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping stale instance delete event via ES version conflict "
                        f"(seq={incoming_seq}, instance_id={instance_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(instance_id),
                        operation="delete",
                        status="success",
                        record_lineage=False,
                        skip_reason="stale_version_conflict",
                    )
                    return
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(instance_id),
                    operation="delete",
                    status="failure",
                    record_lineage=False,
                    error=str(e),
                )
                raise
            
            if success:
                logger.info(f"Instance deleted from Elasticsearch: {instance_id} from index: {index_name}")
            else:
                logger.warning(f"Instance not found for deletion: {instance_id} in index: {index_name}")
            await self._record_es_side_effect(
                event_id=str(event_id),
                event_data=event_data,
                db_name=db_name,
                index_name=index_name,
                doc_id=str(instance_id),
                operation="delete",
                status="success",
                record_lineage=True,
                extra_metadata={"deleted": bool(success)},
            )
                
        except Exception as e:
            logger.error(f"Failed to handle instance deleted: {e}")
            raise
            
    async def _handle_ontology_class_created(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 생성 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or ontology_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for ontology class creation")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "ontologies")
            
            # Elasticsearch 문서 구성
            class_id = ontology_data.get('class_id') or ontology_data.get('id')
            if not class_id:
                raise ValueError("class_id is required for ontology class creation")

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
            if incoming_seq is None:
                existing_doc = await self.elasticsearch_service.get_document(index_name, class_id)
                if existing_doc:
                    if existing_doc.get("event_id") == event_id:
                        logger.info(
                            f"Skipping duplicate ontology create event (event_id={event_id}, class_id={class_id})"
                        )
                        await self._record_es_side_effect(
                            event_id=str(event_id),
                            event_data=event_data,
                            db_name=db_name,
                            index_name=index_name,
                            doc_id=str(class_id),
                            operation="index",
                            status="success",
                            record_lineage=True,
                            skip_reason="duplicate",
                        )
                        return
                    logger.info(
                        f"Ontology already exists; skipping create without sequence_number (class_id={class_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="already_exists_no_sequence",
                    )
                    return

            doc = {
                'class_id': class_id,
                'label': ontology_data.get('label'),
                'description': ontology_data.get('description'),
                'properties': ontology_data.get('properties', []),
                'relationships': ontology_data.get('relationships', []),
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': db_name,
                'branch': ontology_data.get('branch'),
                'version': int(incoming_seq) if incoming_seq is not None else 1,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # 인덱싱 (external version guard)
            try:
                await self.elasticsearch_service.index_document(
                    index_name,
                    doc,
                    doc_id=class_id,
                    refresh=True,
                    version=incoming_seq,
                    version_type="external_gte" if incoming_seq is not None else None,
                    op_type="create" if incoming_seq is None else None,
                )
            except Exception as e:
                if incoming_seq is None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping ontology create due to ES create conflict "
                        f"(class_id={class_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="es_create_conflict",
                    )
                    return
                if incoming_seq is not None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping stale ontology create event via ES version conflict "
                        f"(seq={incoming_seq}, class_id={class_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="stale_version_conflict",
                    )
                    return
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(class_id),
                    operation="index",
                    status="failure",
                    record_lineage=False,
                    error=str(e),
                )
                raise
            
            # Redis에 클래스 라벨 캐싱 (DB별로 키 구분)
            await self._cache_class_label(
                class_id,
                ontology_data.get('label'),
                db_name
            )
            
            logger.info(f"Ontology class created in Elasticsearch: {class_id} in index: {index_name}")
            await self._record_es_side_effect(
                event_id=str(event_id),
                event_data=event_data,
                db_name=db_name,
                index_name=index_name,
                doc_id=str(class_id),
                operation="index",
                status="success",
                record_lineage=True,
            )
            
        except Exception as e:
            logger.error(f"Failed to handle ontology class created: {e}")
            raise
            
    async def _handle_ontology_class_updated(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 업데이트 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or ontology_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for ontology class update")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "ontologies")

            class_id = ontology_data.get('class_id') or ontology_data.get('id')
            
            # 기존 문서 조회
            existing_doc = await self.elasticsearch_service.get_document(
                index_name,
                class_id
            )

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
            if existing_doc:
                if existing_doc.get("event_id") == event_id:
                    logger.info(f"Skipping duplicate ontology update event (event_id={event_id}, class_id={class_id})")
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=True,
                        skip_reason="duplicate",
                    )
                    return

            if incoming_seq is None and existing_doc:
                logger.warning(
                    f"Refusing to update ontology class without sequence_number (class_id={class_id})"
                )
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(class_id),
                    operation="index",
                    status="success",
                    record_lineage=False,
                    skip_reason="missing_sequence_number",
                )
                return
            
            version = 1
            if incoming_seq is not None:
                version = int(incoming_seq)

            created_at = None
            if existing_doc:
                created_at = existing_doc.get("created_at")
            
            # 업데이트 문서 구성
            doc = {
                'class_id': class_id,
                'label': ontology_data.get('label'),
                'description': ontology_data.get('description'),
                'properties': ontology_data.get('properties', []),
                'relationships': ontology_data.get('relationships', []),
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': db_name,
                'branch': ontology_data.get('branch'),
                'version': version,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'created_at': created_at or datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            try:
                if incoming_seq is not None:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=class_id,
                        refresh=True,
                        version=incoming_seq,
                        version_type="external_gte",
                    )
                else:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=class_id,
                        refresh=True,
                        op_type="create",
                    )
            except Exception as e:
                if incoming_seq is not None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping stale ontology update event via ES version conflict "
                        f"(seq={incoming_seq}, class_id={class_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="stale_version_conflict",
                    )
                    return
                if incoming_seq is None and self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping ontology update create due to ES conflict "
                        f"(class_id={class_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="es_create_conflict",
                    )
                    return
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(class_id),
                    operation="index",
                    status="failure",
                    record_lineage=False,
                    error=str(e),
                )
                raise
            
            # Redis 캐시 업데이트 (DB별로 키 구분)
            await self._cache_class_label(
                class_id,
                ontology_data.get('label'),
                db_name
            )
            
            logger.info(f"Ontology class updated in Elasticsearch: {class_id} in index: {index_name}")
            await self._record_es_side_effect(
                event_id=str(event_id),
                event_data=event_data,
                db_name=db_name,
                index_name=index_name,
                doc_id=str(class_id),
                operation="index",
                status="success",
                record_lineage=True,
            )
            
        except Exception as e:
            logger.error(f"Failed to handle ontology class updated: {e}")
            raise
            
    async def _handle_ontology_class_deleted(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 삭제 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or ontology_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for ontology class deletion")
                
            # 인덱스 이름 결정
            index_name = get_ontologies_index_name(db_name)
            class_id = ontology_data.get('class_id') or ontology_data.get('id')
            if not class_id:
                raise ValueError("class_id is required for ontology class deletion")

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
            if incoming_seq is None:
                existing_doc = await self.elasticsearch_service.get_document(index_name, class_id)
                if not existing_doc:
                    logger.info(
                        f"Ontology class already deleted (class_id={class_id}); treating delete as idempotent success"
                    )
                    await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id))
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="delete",
                        status="success",
                        record_lineage=False,
                        skip_reason="already_deleted_no_sequence",
                    )
                    return
                if existing_doc.get("event_id") == event_id:
                    logger.info(
                        f"Skipping duplicate ontology delete event (event_id={event_id}, class_id={class_id})"
                    )
                    await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id))
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="delete",
                        status="success",
                        record_lineage=False,
                        skip_reason="duplicate",
                    )
                    return

                logger.warning(
                    f"Refusing to delete ontology class without sequence_number (class_id={class_id})"
                )
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(class_id),
                    operation="delete",
                    status="success",
                    record_lineage=False,
                    skip_reason="missing_sequence_number",
                )
                return

            # 문서 삭제 (external version guard)
            try:
                success = await self.elasticsearch_service.delete_document(
                    index_name,
                    class_id,
                    refresh=True,
                    version=incoming_seq,
                    version_type="external_gte",
                )
            except Exception as e:
                if self._is_es_version_conflict(e):
                    logger.info(
                        f"Skipping stale ontology delete event via ES version conflict "
                        f"(seq={incoming_seq}, class_id={class_id})"
                    )
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="delete",
                        status="success",
                        record_lineage=False,
                        skip_reason="stale_version_conflict",
                    )
                    return
                await self._record_es_side_effect(
                    event_id=str(event_id),
                    event_data=event_data,
                    db_name=db_name,
                    index_name=index_name,
                    doc_id=str(class_id),
                    operation="delete",
                    status="failure",
                    record_lineage=False,
                    error=str(e),
                )
                raise
            
            # Redis 캐시 삭제 (DB별로 키 구분)
            await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id))
            
            if success:
                logger.info(f"Ontology class deleted from Elasticsearch: {class_id} from index: {index_name}")
            else:
                logger.warning(f"Ontology class not found for deletion: {class_id} in index: {index_name}")
            await self._record_es_side_effect(
                event_id=str(event_id),
                event_data=event_data,
                db_name=db_name,
                index_name=index_name,
                doc_id=str(class_id),
                operation="delete",
                status="success",
                record_lineage=True,
                extra_metadata={"deleted": bool(success)},
            )
                
        except Exception as e:
            logger.error(f"Failed to handle ontology class deleted: {e}")
            raise
            
    async def _handle_database_created(self, db_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """데이터베이스 생성 이벤트 처리"""
        try:
            db_name = db_data.get('db_name') or event_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for database creation")
                
            # 데이터베이스 생성 시 기본 인덱스들을 미리 준비
            logger.info(f"Database created: {db_name}, preparing Elasticsearch indices")
            
            # 인스턴스와 온톨로지 인덱스를 미리 생성
            await self._ensure_index_exists(db_name, "instances")
            await self._ensure_index_exists(db_name, "ontologies")
            
            # 데이터베이스 메타데이터 문서 생성 (검색 가능한 데이터베이스 목록 관리)
            metadata_index = "spice_database_metadata"
            metadata_doc = {
                'database_name': db_name,
                'description': db_data.get('description', ''),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'created_by': event_data.get('occurred_by', 'system'),
                'event_id': event_id,
                'status': 'active'
            }
            
            # 메타데이터 인덱스 확인 및 생성
            if not await self.elasticsearch_service.index_exists(metadata_index):
                await self.elasticsearch_service.create_index(
                    metadata_index,
                    mappings={
                        "properties": {
                            "database_name": {"type": "keyword"},
                            "description": {"type": "text"},
                            "created_at": {"type": "date"},
                            "created_by": {"type": "keyword"},
                            "event_id": {"type": "keyword"},
                            "status": {"type": "keyword"}
                        }
                    },
                    settings=DEFAULT_INDEX_SETTINGS
                )
            
            # 메타데이터 문서 인덱싱
            await self.elasticsearch_service.index_document(
                metadata_index,
                metadata_doc,
                doc_id=db_name,
                refresh=True
            )
            
            logger.info(f"Database creation processed: {db_name}, indices prepared and metadata indexed")
            
        except Exception as e:
            logger.error(f"Failed to handle database created: {e}")
            raise
            
    async def _handle_database_deleted(self, db_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """데이터베이스 삭제 이벤트 처리"""
        try:
            db_name = db_data.get('db_name') or event_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for database deletion")
                
            logger.info(f"Database deleted: {db_name}, cleaning up Elasticsearch indices")
            
            # 관련 인덱스들 삭제
            instances_index = get_instances_index_name(db_name)
            ontologies_index = get_ontologies_index_name(db_name)
            
            # 인덱스 삭제 (존재하는 경우에만)
            if await self.elasticsearch_service.index_exists(instances_index):
                await self.elasticsearch_service.delete_index(instances_index)
                logger.info(f"Deleted instances index: {instances_index}")
                
            if await self.elasticsearch_service.index_exists(ontologies_index):
                await self.elasticsearch_service.delete_index(ontologies_index)
                logger.info(f"Deleted ontologies index: {ontologies_index}")
            
            # 메타데이터에서 데이터베이스 상태 업데이트 (완전 삭제 대신 비활성화)
            metadata_index = "spice_database_metadata"
            if await self.elasticsearch_service.index_exists(metadata_index):
                await self.elasticsearch_service.update_document(
                    metadata_index,
                    db_name,
                    doc={
                        'status': 'deleted',
                        'deleted_at': datetime.now(timezone.utc).isoformat(),
                        'deleted_by': event_data.get('occurred_by', 'system'),
                        'deletion_event_id': event_id
                    },
                    refresh=True
                )
            
            # 생성된 인덱스 캐시에서 제거
            self.created_indices.discard(instances_index)
            self.created_indices.discard(ontologies_index)
            
            logger.info(f"Database deletion processed: {db_name}, indices cleaned up and metadata updated")
            
        except Exception as e:
            logger.error(f"Failed to handle database deleted: {e}")
            raise
            
    async def _get_class_label(self, class_id: str, db_name: str) -> Optional[str]:
        """
        Redis에서 클래스 라벨 조회 (Cache Stampede 방지)
        
        분산 락을 사용하여 동시에 여러 요청이 들어와도 
        Elasticsearch에는 한 번만 요청하도록 최적화합니다.
        """
        try:
            if not class_id or not db_name:
                return None
                
            cache_key = AppConfig.get_class_label_key(db_name, class_id)
            lock_key = f"lock:{cache_key}"
            
            # 캐시 stampede 방지를 위한 분산 락 메커니즘
            max_wait_time = 5.0  # 최대 5초 대기
            lock_timeout = 10    # 락 타임아웃 10초
            retry_interval = 0.05  # 50ms 간격으로 재시도
            
            start_time = asyncio.get_event_loop().time()
            
            while (asyncio.get_event_loop().time() - start_time) < max_wait_time:
                # 1. 캐시에서 조회 시도
                cached_label = await self.redis_service.client.get(cache_key)
                if cached_label:
                    # Negative caching 처리
                    if cached_label == "__NONE__":
                        self.cache_metrics['negative_cache_hits'] += 1
                        return None
                    self.cache_metrics['cache_hits'] += 1
                    return cached_label
                
                # 2. 분산 락 획득 시도 (SETNX with TTL)
                lock_acquired = await self.redis_service.client.set(
                    lock_key, 
                    "1", 
                    ex=lock_timeout,  # TTL 설정으로 데드락 방지
                    nx=True  # SET if Not eXists
                )
                
                if lock_acquired:
                    # 3. 락을 획득한 요청만 Elasticsearch에서 데이터 조회
                    self.cache_metrics['lock_acquisitions'] += 1
                    try:
                        # 락 획득 후 다시 한번 캐시 확인 (다른 요청이 이미 저장했을 수 있음)
                        cached_label = await self.redis_service.client.get(cache_key)
                        if cached_label:
                            # Negative caching 처리
                            if cached_label == "__NONE__":
                                self.cache_metrics['negative_cache_hits'] += 1
                                return None
                            self.cache_metrics['cache_hits'] += 1
                            return cached_label
                        
                        # Elasticsearch에서 조회
                        self.cache_metrics['cache_misses'] += 1
                        self.cache_metrics['elasticsearch_queries'] += 1
                        index_name = get_ontologies_index_name(db_name)
                        doc = await self.elasticsearch_service.get_document(
                            index_name,
                            class_id
                        )
                        
                        if doc:
                            label = doc.get('label')
                            if label:
                                # 캐시에 저장 (1시간 TTL)
                                await self.redis_service.client.setex(
                                    cache_key,
                                    AppConfig.CLASS_LABEL_CACHE_TTL,
                                    label
                                )
                                logger.debug(f"Cached class label for {class_id} in {db_name}: {label}")
                                return label
                        
                        # 결과가 없는 경우도 짧은 시간 캐싱 (negative caching)
                        await self.redis_service.client.setex(
                            cache_key,
                            300,  # 5분간 negative 캐싱
                            "__NONE__"  # 빈 값 표시자
                        )
                        return None
                        
                    finally:
                        # 4. 락 해제 (반드시 실행)
                        await self.redis_service.client.delete(lock_key)
                        
                else:
                    # 5. 락 획득 실패 시 잠시 대기 후 재시도
                    self.cache_metrics['lock_failures'] += 1
                    self.cache_metrics['total_lock_wait_time'] += retry_interval
                    await asyncio.sleep(retry_interval)
                    
            # 최대 대기 시간 초과 시 fallback (락 없이 직접 조회)
            logger.warning(f"Lock wait timeout for class_label {class_id} in {db_name}, falling back to direct query")
            return await self._get_class_label_fallback(class_id, db_name)
            
        except Exception as e:
            logger.error(f"Failed to get class label for {class_id} in {db_name}: {e}")
            return None
    
    async def _get_class_label_fallback(self, class_id: str, db_name: str) -> Optional[str]:
        """
        락 획득 실패 시 fallback 조회 (성능보다 안정성 우선)
        """
        try:
            self.cache_metrics['fallback_queries'] += 1
            self.cache_metrics['elasticsearch_queries'] += 1
            
            index_name = get_ontologies_index_name(db_name)
            doc = await self.elasticsearch_service.get_document(
                index_name,
                class_id
            )
            
            if doc:
                label = doc.get('label')
                if label:
                    # 짧은 시간만 캐싱 (경합 상황이므로)
                    cache_key = AppConfig.get_class_label_key(db_name, class_id)
                    await self.redis_service.client.setex(
                        cache_key,
                        60,  # 1분만 캐싱
                        label
                    )
                    return label
                    
            return None
            
        except Exception as e:
            logger.error(f"Fallback query failed for {class_id} in {db_name}: {e}")
            return None
    
    def get_cache_efficiency_metrics(self) -> Dict[str, Any]:
        """
        캐시 효율성 및 락 경합 메트릭 반환
        
        Returns:
            메트릭 딕셔너리
        """
        total_requests = (
            self.cache_metrics['cache_hits'] + 
            self.cache_metrics['cache_misses'] + 
            self.cache_metrics['negative_cache_hits']
        )
        
        if total_requests == 0:
            return {
                'cache_hit_rate': 0.0,
                'elasticsearch_query_rate': 0.0,
                'lock_contention_rate': 0.0,
                'average_lock_wait_time': 0.0,
                **self.cache_metrics
            }
        
        cache_hit_rate = (
            self.cache_metrics['cache_hits'] + 
            self.cache_metrics['negative_cache_hits']
        ) / total_requests
        
        total_lock_attempts = (
            self.cache_metrics['lock_acquisitions'] + 
            self.cache_metrics['lock_failures']
        )
        
        lock_contention_rate = (
            self.cache_metrics['lock_failures'] / total_lock_attempts 
            if total_lock_attempts > 0 else 0.0
        )
        
        avg_lock_wait_time = (
            self.cache_metrics['total_lock_wait_time'] / self.cache_metrics['lock_failures']
            if self.cache_metrics['lock_failures'] > 0 else 0.0
        )
        
        return {
            'cache_hit_rate': round(cache_hit_rate * 100, 2),  # 백분율
            'elasticsearch_query_rate': round(
                (self.cache_metrics['elasticsearch_queries'] / total_requests) * 100, 2
            ),
            'lock_contention_rate': round(lock_contention_rate * 100, 2),
            'average_lock_wait_time': round(avg_lock_wait_time * 1000, 2),  # ms 단위
            'total_requests': total_requests,
            **self.cache_metrics
        }
    
    def log_cache_metrics(self):
        """캐시 메트릭을 로그로 출력"""
        metrics = self.get_cache_efficiency_metrics()
        
        logger.info(
            f"Cache Efficiency Metrics - "
            f"Hit Rate: {metrics['cache_hit_rate']}%, "
            f"ES Query Rate: {metrics['elasticsearch_query_rate']}%, "
            f"Lock Contention: {metrics['lock_contention_rate']}%, "
            f"Avg Lock Wait: {metrics['average_lock_wait_time']}ms, "
            f"Total Requests: {metrics['total_requests']}"
        )
        
        if metrics['fallback_queries'] > 0:
            logger.warning(
                f"Fallback queries detected: {metrics['fallback_queries']} "
                f"(indicates high lock contention)"
            )
            
    async def _cache_class_label(self, class_id: str, label: str, db_name: str):
        """클래스 라벨을 Redis에 캐싱"""
        try:
            if not class_id or not label or not db_name:
                return
                
            cache_key = AppConfig.get_class_label_key(db_name, class_id)
            await self.redis_service.client.setex(
                cache_key,
                3600,  # 1시간 TTL
                label
            )
        except Exception as e:
            logger.error(f"Failed to cache class label for {class_id} in {db_name}: {e}")
            
    def _normalize_properties(self, properties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """속성을 검색 최적화된 형태로 정규화"""
        normalized = []
        for prop in properties:
            normalized.append({
                'name': prop.get('name'),
                'value': str(prop.get('value', '')),
                'type': prop.get('type')
            })
        return normalized

    @staticmethod
    def _is_transient_infra_error(error: Exception) -> bool:
        """
        Return True for errors that are expected to recover via retry (e.g. ES outage).

        Projection is a read-model: for transient infra failures we prefer "retry until success"
        over DLQ/commit, to guarantee convergence after recovery.
        """
        msg = str(error).lower()
        transient_markers = [
            # Elasticsearch / aiohttp connection errors (common during ES restart)
            "cannot connect to host elasticsearch",
            "clientconnectorerror",
            "connectionerror",
            "connection error",
            "connect call failed",
            "connection refused",
            "connection reset",
            "temporarily unavailable",
            "service unavailable",
            "timeout",
        ]
        return any(marker in msg for marker in transient_markers)
        
    async def _handle_retry(self, msg, error):
        """재시도 처리"""
        try:
            key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"

            # Lease contention is not a "failure": another worker is already processing this event_id.
            # Never send this to DLQ; just retry later without consuming retry budget.
            if isinstance(error, _InProgressLeaseError):
                self.retry_count.pop(key, None)
                logger.info(f"Lease in progress elsewhere; retrying later: {key}")
                await asyncio.sleep(2)
                if self.consumer:
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                return

            # Transient infra failures (e.g. Elasticsearch down): retry indefinitely with capped backoff.
            if self._is_transient_infra_error(error):
                retry_count = self.retry_count.get(key, 0) + 1
                self.retry_count[key] = retry_count
                backoff_s = min(max(1, retry_count * 2), 30)
                logger.warning(
                    f"Transient infra error; retrying without DLQ (attempt {retry_count}, backoff={backoff_s}s): {key}"
                )
                await asyncio.sleep(backoff_s)
                if self.consumer:
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                return

            retry_count = self.retry_count.get(key, 0) + 1
            
            if retry_count <= self.max_retries:
                self.retry_count[key] = retry_count
                logger.warning(f"Retrying message (attempt {retry_count}/{self.max_retries}): {key}")
                await asyncio.sleep(min(retry_count * 2, 30))  # capped backoff (avoid max.poll.interval issues)
                # Rewind to the failed offset so we don't accidentally commit past it.
                if self.consumer:
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                return
                
            # 최대 재시도 횟수 초과 시 DLQ로 전송
            logger.error(f"Max retries exceeded for message: {key}, sending to DLQ")
            await self._send_to_dlq(msg, error)
            
            # 재시도 카운트 제거
            if key in self.retry_count:
                del self.retry_count[key]
                
            # 오프셋 커밋 (DLQ 전송 후)
            self.consumer.commit(msg)
            
        except Exception as e:
            logger.error(f"Error in retry handling: {e}")
            
    async def _send_to_dlq(self, msg, error):
        """실패한 메시지를 DLQ로 전송"""
        try:
            dlq_message = {
                'original_topic': msg.topic(),
                'original_partition': msg.partition(),
                'original_offset': msg.offset(),
                'original_value': msg.value().decode('utf-8'),
                'error': str(error),
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'worker': 'projection-worker'
            }
            
            self.producer.produce(
                self.dlq_topic,
                key=f"{msg.topic()}:{msg.partition()}:{msg.offset()}",
                value=json.dumps(dlq_message)
            )
            self.producer.flush()
            
            logger.info(f"Message sent to DLQ: {dlq_message}")
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
            
    async def _shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Projection Worker...")
        
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        if self.producer:
            self.producer.flush()
            
        if self.elasticsearch_service:
            await self.elasticsearch_service.disconnect()

        if self.processed_event_registry:
            await self.processed_event_registry.close()
            
        if self.redis_service:
            await self.redis_service.disconnect()
            
        logger.info("Projection Worker stopped")


async def main():
    """메인 함수"""
    worker = ProjectionWorker()
    
    # 시그널 핸들러 설정
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        worker.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
