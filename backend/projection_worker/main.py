"""
Projection Worker Service
Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커 서비스
"""

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
import signal
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from confluent_kafka import Producer, TopicPartition

from shared.config.search_config import (
    get_instances_index_name,
    get_ontologies_index_name,
    sanitize_index_name,
    get_default_index_settings,
)
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.models.events import (
    EventType,
)
from shared.services.storage.redis_service import RedisService, create_redis_service
from shared.services.storage.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
from shared.services.storage.lakefs_storage_service import LakeFSStorageService, create_lakefs_storage_service
from shared.services.core.projection_manager import ProjectionManager
from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
)
from shared.services.kafka.processed_event_worker import RegistryKey, StrictHeartbeatEventEnvelopeKafkaWorker
from shared.services.kafka.producer_factory import create_kafka_producer
from shared.services.kafka.safe_consumer import SafeKafkaConsumer, create_safe_consumer
from shared.services.registries.lineage_store import LineageStore
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.utils.chaos import maybe_crash
from shared.utils.ontology_version import split_ref_commit
from shared.utils.language import coerce_localized_text, select_localized_text, get_default_language
from shared.utils.resource_rid import strip_rid_revision
from shared.utils.writeback_paths import ref_key, writeback_patchset_key
from shared.utils.writeback_lifecycle import overlay_doc_id
from shared.utils.executor_utils import call_in_executor

# Observability imports
from shared.observability.tracing import get_tracing_service
from shared.observability.metrics import get_metrics_collector
from shared.observability.context_propagation import attach_context_from_kafka, kafka_headers_from_current_context
from shared.utils.app_logger import configure_logging

# 로깅 설정
_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)


class ProjectionWorker(StrictHeartbeatEventEnvelopeKafkaWorker[None]):
    """Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커

    Kafka message contract:
    - Projection topics carry EventEnvelope JSON (metadata.kind == "domain")
    """

    expected_envelope_kind = "domain"

    def __init__(self):
        settings = get_settings()
        worker_cfg = settings.workers.projection

        self.service_name = "projection-worker"
        self.running = False
        self.kafka_servers = settings.database.kafka_servers
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.producer: Optional[Producer] = None
        self.redis_service: Optional[RedisService] = None
        self.elasticsearch_service: Optional[ElasticsearchService] = None
        self.projection_manager: Optional[ProjectionManager] = None
        self.tracing_service = None
        self.metrics_collector = None
        self._consumer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="projection-worker-kafka")
        self.lakefs_storage: Optional[LakeFSStorageService] = None

        # Durable idempotency (Postgres)
        self.enable_processed_event_registry = bool(settings.event_sourcing.enable_processed_event_registry)
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None

        # First-class provenance/audit (fail-open by default)
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.enable_audit_logs = bool(settings.observability.enable_audit_logs)
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None

        # 생성된 인덱스 캐시 (중복 생성 방지)
        self.created_indices = set()

        # DLQ 토픽
        self.dlq_topic = AppConfig.PROJECTION_DLQ_TOPIC

        # 재시도 설정
        max_retries = int(worker_cfg.max_retries)
        self.max_retries_config = max_retries
        # Legacy semantics: max_retries counted retries (not attempts). Our runtime uses registry attempt_count starting at 1.
        self.max_retries = max(1, max_retries + 1)
        self.backoff_base = 2
        self.backoff_max = 30

        # ProcessedEventRegistry handler base (actual handler is per-topic).
        self.handler = "projection_worker"

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
    def _normalize_localized_field(value: Any, *, default_lang: str) -> tuple[str, Dict[str, str]]:
        i18n_map = coerce_localized_text(value, default_lang=default_lang)
        text = select_localized_text(value, lang=default_lang)
        if not text and value is not None and not i18n_map:
            try:
                text = str(value).strip()
            except Exception:
                text = ""
        return text, i18n_map

    def _normalize_ontology_properties(
        self,
        properties: List[Any],
        *,
        default_lang: str,
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for prop in properties or []:
            if not isinstance(prop, dict):
                logger.warning(f"Skipping non-dict ontology property payload: {prop}")
                continue
            item = dict(prop)
            label_text, label_i18n = self._normalize_localized_field(
                prop.get("label"),
                default_lang=default_lang,
            )
            if label_text or "label" in item:
                item["label"] = label_text
            if label_i18n:
                item["label_i18n"] = label_i18n
            else:
                item.pop("label_i18n", None)

            description_text, description_i18n = self._normalize_localized_field(
                prop.get("description"),
                default_lang=default_lang,
            )
            if description_text or "description" in item:
                item["description"] = description_text
            if description_i18n:
                item["description_i18n"] = description_i18n
            else:
                item.pop("description_i18n", None)

            normalized.append(item)
        return normalized

    def _normalize_ontology_relationships(
        self,
        relationships: List[Any],
        *,
        default_lang: str,
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for rel in relationships or []:
            if not isinstance(rel, dict):
                logger.warning(f"Skipping non-dict ontology relationship payload: {rel}")
                continue
            item = dict(rel)
            label_text, label_i18n = self._normalize_localized_field(
                rel.get("label"),
                default_lang=default_lang,
            )
            if label_text or "label" in item:
                item["label"] = label_text
            if label_i18n:
                item["label_i18n"] = label_i18n
            else:
                item.pop("label_i18n", None)

            description_text, description_i18n = self._normalize_localized_field(
                rel.get("description"),
                default_lang=default_lang,
            )
            if description_text or "description" in item:
                item["description"] = description_text
            if description_i18n:
                item["description_i18n"] = description_i18n
            else:
                item.pop("description_i18n", None)

            inverse_label_text, inverse_label_i18n = self._normalize_localized_field(
                rel.get("inverse_label"),
                default_lang=default_lang,
            )
            if inverse_label_text or "inverse_label" in item:
                item["inverse_label"] = inverse_label_text
            if inverse_label_i18n:
                item["inverse_label_i18n"] = inverse_label_i18n
            else:
                item.pop("inverse_label_i18n", None)

            normalized.append(item)
        return normalized

    @staticmethod
    def _extract_envelope_metadata(event_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        metadata = event_data.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        command_id = metadata.get("command_id")
        trace_id = metadata.get("trace_id")
        correlation_id = metadata.get("correlation_id")
        service = metadata.get("service")
        ontology_ref, ontology_commit = split_ref_commit(metadata.get("ontology"))
        return {
            "command_id": str(command_id) if command_id else None,
            "trace_id": str(trace_id) if trace_id else None,
            "correlation_id": str(correlation_id) if correlation_id else None,
            "origin_service": str(service) if service else None,
            "ontology_ref": ontology_ref,
            "ontology_commit": ontology_commit,
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
        ontology_payload: Dict[str, str] = {}
        if meta.get("ontology_ref"):
            ontology_payload["ref"] = str(meta["ontology_ref"])
        if meta.get("ontology_commit"):
            ontology_payload["commit"] = str(meta["ontology_commit"])

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
                    "ontology_ref": meta.get("ontology_ref"),
                    "ontology_commit": meta.get("ontology_commit"),
                    "run_id": get_settings().observability.run_id,
                    "code_sha": get_settings().observability.code_sha,
                }
                if ontology_payload:
                    audit_metadata["ontology"] = ontology_payload
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
                        "ontology_ref": meta.get("ontology_ref"),
                        "ontology_commit": meta.get("ontology_commit"),
                        "ontology": ontology_payload or None,
                    },
                )
            except Exception as e:
                logger.debug(f"Lineage record failed (non-fatal): {e}")

    async def _consumer_call(self, func, *args, **kwargs):
        return await call_in_executor(self._consumer_executor, func, *args, **kwargs)

    async def _poll_message(self, *, timeout: float) -> Any:
        if not self.consumer:
            return None
        return await self._consumer_call(self.consumer.poll, timeout=timeout)
        
    async def initialize(self):
        """워커 초기화"""
        settings = get_settings()

        group_id = (AppConfig.PROJECTION_WORKER_GROUP or "projection-worker-group").strip()

        # Kafka Consumer (strong consistency: read_committed + rebalance-safe offsets)
        topics = [AppConfig.INSTANCE_EVENTS_TOPIC, AppConfig.ONTOLOGY_EVENTS_TOPIC, AppConfig.ACTION_EVENTS_TOPIC]
        self.consumer = await self._consumer_call(
            create_safe_consumer,
            group_id,
            topics,
            "projection-worker",
            max_poll_interval_ms=300000,
            session_timeout_ms=45000,
        )
        
        # Kafka Producer 설정 (실패 이벤트 발행용)
        self.producer = create_kafka_producer(
            bootstrap_servers=self.kafka_servers,
            client_id="projection-worker",
        )
        
        # Redis 연결 설정 (온톨로지 캐싱용)
        self.redis_service = create_redis_service(settings)
        await self.redis_service.connect()
        logger.info("Redis connection established")
        
        # Elasticsearch 연결 설정
        self.elasticsearch_service = create_elasticsearch_service(settings)
        await self.elasticsearch_service.connect()
        logger.info("Elasticsearch connection established")

        # lakeFS storage gateway (required for ActionApplied -> patchset reads). Best-effort on startup.
        try:
            self.lakefs_storage = create_lakefs_storage_service(settings)
            if self.lakefs_storage:
                logger.info("lakeFS storage configured for writeback patchset reads")
            else:
                logger.warning("lakeFS storage disabled (boto3 missing) - ActionApplied projection will be degraded")
        except Exception as e:
            logger.warning(f"lakeFS storage unavailable - ActionApplied projection will be degraded: {e}")
            self.lakefs_storage = None

        # Durable processed-events registry (idempotency + ordering guard)
        self.processed_event_registry = await create_processed_event_registry()
        self.processed = self.processed_event_registry
        logger.info("✅ ProcessedEventRegistry connected (Postgres)")

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
        
        logger.info(f"Subscribed to topics: {topics}")
        
        # Initialize OpenTelemetry
        self.tracing_service = get_tracing_service("projection-worker")
        self.metrics_collector = get_metrics_collector("projection-worker")
        self.tracing = self.tracing_service
        self.metrics = self.metrics_collector
        
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
            
    async def _ensure_index_exists(self, db_name: str, index_type: str = "instances", *, branch: str = "main"):
        """특정 데이터베이스의 인덱스가 존재하는지 확인하고 없으면 생성"""
        if index_type == "instances":
            index_name = get_instances_index_name(db_name, branch=branch)
            mapping = self.instances_mapping
        else:
            index_name = get_ontologies_index_name(db_name, branch=branch)
            mapping = self.ontologies_mapping
            
        # Fast path: if we *think* we've already created it, confirm it still exists.
        # ES indices can be deleted between runs (or reset in CI/local gate), so the cache must be resilient.
        if index_name in self.created_indices:
            try:
                if await self.elasticsearch_service.index_exists(index_name):
                    return index_name
            except Exception:
                # Fall through to recreate on any transient ES error.
                pass
            self.created_indices.discard(index_name)
            
        try:
            if not await self.elasticsearch_service.index_exists(index_name):
                # 설정 병합 (매핑 파일 설정 + 기본 설정)
                settings = mapping.get('settings', {}).copy()
                settings.update(get_default_index_settings())
                
                await self.elasticsearch_service.create_index(
                    index_name,
                    mappings=mapping['mappings'],
                    settings=settings
                )
                logger.info(f"Created index: {index_name} for database: {db_name}")

            # Ensure ontology stamp fields exist even for indices created before this feature.
            try:
                await self.elasticsearch_service.update_mapping(
                    index_name,
                    properties={
                        "ontology_ref": {"type": "keyword"},
                        "ontology_commit": {"type": "keyword"},
                        "lifecycle_id": {"type": "keyword"},
                        "patchset_commit_id": {"type": "keyword"},
                        "action_log_id": {"type": "keyword"},
                        "overlay_tombstone": {"type": "boolean"},
                        "conflict_status": {"type": "keyword"},
                        "base_token": {"type": "object", "enabled": True},
                    },
                )
            except Exception as e:
                logger.debug(f"Failed to update mapping for ontology stamps (index={index_name}): {e}")

            if index_type == "ontologies":
                try:
                    i18n_text_mapping = {
                        "type": "object",
                        "properties": {
                            "en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "ko": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        },
                    }
                    await self.elasticsearch_service.update_mapping(
                        index_name,
                        properties={
                            "label_i18n": i18n_text_mapping,
                            "description_i18n": i18n_text_mapping,
                            "properties": {
                                "type": "nested",
                                "properties": {
                                    "label_i18n": i18n_text_mapping,
                                    "description_i18n": i18n_text_mapping,
                                },
                            },
                            "relationships": {
                                "type": "nested",
                                "properties": {
                                    "label_i18n": i18n_text_mapping,
                                    "description_i18n": i18n_text_mapping,
                                    "inverse_label_i18n": i18n_text_mapping,
                                },
                            },
                        },
                    )
                except Exception as e:
                    logger.debug(f"Failed to update mapping for i18n labels (index={index_name}): {e}")
                
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
            
    async def run(self) -> None:
        """메인 실행 루프"""
        self.running = True
        logger.info("Projection Worker started")

        try:
            await self.run_loop(poll_timeout=1.0, idle_sleep=None)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self._shutdown()

    def _span_name(self, *, payload: EventEnvelope) -> str:  # type: ignore[override]
        return "projection_worker.process_event"

    def _registry_handler(self, *, msg: Any, payload: EventEnvelope) -> str:  # type: ignore[override]
        return f"{self.handler}:{msg.topic()}"

    def _registry_key(self, payload: EventEnvelope) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(
            event_id=str(payload.event_id),
            aggregate_id=str(payload.aggregate_id) if payload.aggregate_id else None,
            sequence_number=int(payload.sequence_number) if payload.sequence_number is not None else None,
        )

    def _is_retryable_error(self, exc: Exception, *, payload: EventEnvelope) -> bool:  # type: ignore[override]
        if self._is_transient_infra_error(exc):
            return True
        return super()._is_retryable_error(exc, payload=payload)

    def _max_retries_for_error(  # type: ignore[override]
        self,
        exc: Exception,
        *,
        payload: EventEnvelope,
        error: str,
        retryable: bool,
    ) -> int:
        if retryable and self._is_transient_infra_error(exc):
            return 1_000_000_000
        return int(self.max_retries)

    def _backoff_seconds_for_error(  # type: ignore[override]
        self,
        exc: Exception,
        *,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> int:
        backoff = min(max(1, int(attempt_count) * 2), 30)
        return backoff

    def _in_progress_sleep_seconds(self, *, claim: Any, payload: EventEnvelope) -> float:  # type: ignore[override]
        try:
            logger.info(
                "Lease in progress elsewhere; retrying later (event_id=%s attempt=%s)",
                payload.event_id,
                int(getattr(claim, "attempt_count", 0) or 0),
            )
        except Exception:
            pass
        return 2.0

    async def _on_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        if self._is_transient_infra_error(RuntimeError(error)):
            logger.warning(
                "Transient infra error; retrying without DLQ (event_id=%s attempt=%s backoff=%ss): %s",
                payload.event_id,
                attempt_count,
                int(backoff_s),
                error,
            )
            return

        max_failures = int(getattr(self, "max_retries_config", 0) or 0)
        denom = max_failures if max_failures > 0 else int(self.max_retries)
        logger.warning(
            "Projection event failed; will retry (event_id=%s attempt=%s/%s backoff=%ss): %s",
            payload.event_id,
            attempt_count,
            denom,
            int(backoff_s),
            error,
        )

    async def _on_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: EventEnvelope,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        logger.error(
            "Projection event max retries exceeded; sending to DLQ (event_id=%s attempt=%s)",
            payload.event_id,
            attempt_count,
        )

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:  # type: ignore[override]
        kafka_headers = None
        with suppress(Exception):
            kafka_headers = msg.headers()

        fallback_metadata = None
        if raw_payload:
            try:
                decoded = json.loads(raw_payload)
                if isinstance(decoded, dict) and isinstance(decoded.get("metadata"), dict):
                    fallback_metadata = decoded["metadata"]
            except Exception:
                fallback_metadata = None

        try:
            await self._publish_to_dlq(
                msg=msg,
                error=str(error),
                attempt_count=1,
                payload_text=raw_payload,
                kafka_headers=kafka_headers,
                fallback_metadata=fallback_metadata,
            )
        except Exception as dlq_err:
            logger.error("Failed to publish invalid projection payload to DLQ; retrying: %s", dlq_err, exc_info=True)
            raise

        logger.exception("Invalid projection payload; skipping: %s", error)

    async def _commit(self, msg: Any) -> None:  # type: ignore[override]
        if not self.consumer:
            return
        maybe_crash("projection_worker:before_commit", logger=logger)
        await self._consumer_call(self.consumer.commit_sync, msg)

    async def _seek(self, *, topic: str, partition: int, offset: int) -> None:  # type: ignore[override]
        if not self.consumer:
            return
        await self._consumer_call(self.consumer.seek, TopicPartition(topic, partition, offset))

    async def _publish_to_dlq(
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        payload_text: Optional[str],
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.producer:
            logger.error("DLQ producer not configured; dropping DLQ message: %s", error)
            return

        dlq_message = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "original_value": payload_text,
            "error": str(error),
            "attempt_count": int(attempt_count),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "worker": "projection-worker",
        }

        with attach_context_from_kafka(
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            service_name="projection-worker",
        ):
            headers = kafka_headers_from_current_context()
            with self.tracing_service.span(
                "projection_worker.dlq_produce",
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": self.dlq_topic,
                    "messaging.destination_kind": "topic",
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.offset": msg.offset(),
                },
            ):
                self.producer.produce(
                    self.dlq_topic,
                    key=f"{msg.topic()}:{msg.partition()}:{msg.offset()}",
                    value=json.dumps(dlq_message, ensure_ascii=False, default=str).encode("utf-8"),
                    headers=headers or None,
                )
                self.producer.flush()

        logger.info(
            "Message sent to DLQ (topic=%s partition=%s offset=%s)",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: EventEnvelope,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        kafka_headers = None
        with suppress(Exception):
            kafka_headers = msg.headers()

        await self._publish_to_dlq(
            msg=msg,
            error=error,
            attempt_count=attempt_count,
            payload_text=raw_payload,
            kafka_headers=kafka_headers,
            fallback_metadata=payload.metadata if isinstance(payload.metadata, dict) else None,
        )

    async def _process_payload(self, payload: EventEnvelope) -> None:  # type: ignore[override]
        maybe_crash("projection_worker:after_claim", logger=logger)

        event_data = payload.model_dump(mode="json")
        event_type = str(payload.event_type)
        logger.info("Processing event: %s", event_type)

        maybe_crash("projection_worker:before_side_effect", logger=logger)
        if event_type in {
            EventType.INSTANCE_CREATED.value,
            EventType.INSTANCE_UPDATED.value,
            EventType.INSTANCE_DELETED.value,
            EventType.INSTANCES_BULK_CREATED.value,
            EventType.INSTANCES_BULK_UPDATED.value,
            EventType.INSTANCES_BULK_DELETED.value,
        }:
            await self._handle_instance_event(event_data)
        elif event_type == EventType.ACTION_APPLIED.value:
            await self._handle_action_event(event_data)
        else:
            await self._handle_ontology_event(event_data)
        maybe_crash("projection_worker:after_side_effect", logger=logger)

        maybe_crash("projection_worker:before_mark_done", logger=logger)
            
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

    async def _handle_action_event(self, event_data: Dict[str, Any]):
        """Action writeback events -> overlay projection."""
        try:
            event_type = event_data.get("event_type")
            payload = event_data.get("data", {})
            event_id = event_data.get("event_id")

            if event_type == EventType.ACTION_APPLIED.value:
                await self._handle_action_applied(payload, event_id, event_data)
            else:
                logger.warning(f"Unknown action event type: {event_type}")
        except Exception as e:
            logger.error(f"Error handling action event: {e}")
            raise

    async def _handle_action_applied(
        self,
        action_data: Dict[str, Any],
        event_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        if not self.lakefs_storage:
            raise RuntimeError("lakeFS storage is not configured; cannot project ActionApplied patchsets")

        db_name = event_data.get("db_name") or (action_data.get("db_name") if isinstance(action_data, dict) else None)
        if not isinstance(db_name, str) or not db_name.strip():
            raise ValueError("db_name is required for action applied projection")
        db_name = db_name.strip()

        if not isinstance(action_data, dict):
            raise ValueError("action event data must be an object")

        action_log_id = action_data.get("action_log_id")
        patchset_commit_id = action_data.get("patchset_commit_id")
        overlay_branch = action_data.get("overlay_branch") or AppConfig.get_ontology_writeback_branch(db_name)
        writeback_target = action_data.get("writeback_target") if isinstance(action_data.get("writeback_target"), dict) else {}
        repo = str(writeback_target.get("repo") or AppConfig.ONTOLOGY_WRITEBACK_REPO).strip()

        if not action_log_id or not patchset_commit_id:
            raise ValueError("action_log_id and patchset_commit_id are required")
        action_log_id = str(action_log_id)
        patchset_commit_id = str(patchset_commit_id)
        overlay_branch = str(overlay_branch).strip() or AppConfig.get_ontology_writeback_branch(db_name)

        patchset = await self.lakefs_storage.load_json(
            bucket=repo,
            key=ref_key(patchset_commit_id, writeback_patchset_key(action_log_id)),
        )
        targets = patchset.get("targets") if isinstance(patchset, dict) else None
        if not isinstance(targets, list):
            targets = []

        incoming_seq = self._parse_sequence(event_data.get("sequence_number"))
        if incoming_seq is None:
            # Overlay writes must be externally versioned to preserve ordering.
            raise ValueError("ActionApplied requires sequence_number for overlay projection")

        overlay_index = await self._ensure_index_exists(db_name, "instances", branch=overlay_branch)
        base_index = await self._ensure_index_exists(db_name, "instances", branch="main")

        for target in targets:
            if not isinstance(target, dict):
                continue
            resource_rid = str(target.get("resource_rid") or "")
            class_id = strip_rid_revision(resource_rid)
            class_id = class_id.strip() or None
            instance_id = str(target.get("instance_id") or "").strip()
            lifecycle_id = str(target.get("lifecycle_id") or "lc-0").strip() or "lc-0"
            changes = None
            if isinstance(target.get("applied_changes"), dict):
                changes = target.get("applied_changes")
            elif isinstance(target.get("changes"), dict):
                changes = target.get("changes")
            else:
                changes = {}
            base_token = target.get("base_token") if isinstance(target.get("base_token"), dict) else {}
            conflict = target.get("conflict") if isinstance(target.get("conflict"), dict) else {}
            conflict_status = None
            if conflict:
                status_value = str(conflict.get("status") or "").strip().upper()
                policy_value = str(conflict.get("policy") or "").strip().upper()
                resolution_value = str(conflict.get("resolution") or "").strip().upper()
                if status_value:
                    conflict_status = ":".join([part for part in (status_value, policy_value, resolution_value) if part])

            if isinstance(target.get("applied_changes"), dict):
                # BASE_WINS etc: do not write overlay docs for no-op applied changes.
                is_noop = not bool(changes.get("delete")) and not (
                    (changes.get("set") or {})
                    or (changes.get("unset") or [])
                    or (changes.get("link_add") or [])
                    or (changes.get("link_remove") or [])
                )
                if is_noop:
                    continue

            if not instance_id:
                continue

            overlay_id = overlay_doc_id(instance_id=instance_id, lifecycle_id=lifecycle_id)
            overlay_doc = await self.elasticsearch_service.get_document(overlay_index, overlay_id)
            base_doc = None
            if not overlay_doc:
                base_doc = await self.elasticsearch_service.get_document(base_index, instance_id)

            effective = dict(overlay_doc or base_doc or {})
            data_payload = effective.get("data")
            if not isinstance(data_payload, dict):
                data_payload = {}
            effective["data"] = data_payload

            if bool(changes.get("delete")):
                effective = {
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "db_name": db_name,
                    "branch": overlay_branch,
                    "overlay_tombstone": True,
                    "lifecycle_id": lifecycle_id,
                    "base_token": base_token,
                    "patchset_commit_id": patchset_commit_id,
                    "action_log_id": action_log_id,
                    "conflict_status": conflict_status,
                    "event_id": event_id,
                    "event_sequence": incoming_seq,
                    "version": int(incoming_seq),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                set_ops = changes.get("set") if isinstance(changes.get("set"), dict) else {}
                unset_ops = changes.get("unset") if isinstance(changes.get("unset"), list) else []

                for k, v in (set_ops or {}).items():
                    if isinstance(k, str) and k:
                        data_payload[k] = v
                for k in unset_ops or []:
                    if isinstance(k, str) and k:
                        data_payload.pop(k, None)

                for op_key, add in (("link_add", True), ("link_remove", False)):
                    ops = changes.get(op_key)
                    if not isinstance(ops, list):
                        continue
                    for item in ops:
                        field = None
                        target = None
                        if isinstance(item, dict):
                            field = item.get("field") or item.get("predicate") or item.get("name")
                            target = item.get("value") or item.get("to") or item.get("target")
                            if (field is None or target is None) and len(item) == 1:
                                k, v = next(iter(item.items()))
                                field = k
                                target = v
                        elif isinstance(item, str):
                            raw = item.strip()
                            if ":" in raw:
                                field, target = raw.split(":", 1)
                        field_str = str(field or "").strip()
                        target_str = str(target or "").strip()
                        if not field_str or not target_str:
                            continue
                        existing = data_payload.get(field_str)
                        if isinstance(existing, list):
                            values = [str(v) for v in existing if v is not None]
                        elif existing is None:
                            values = []
                        else:
                            values = [str(existing)]
                        if add:
                            if target_str not in values:
                                values.append(target_str)
                        else:
                            values = [v for v in values if v != target_str]
                        data_payload[field_str] = values

                effective.update(
                    {
                        "instance_id": instance_id,
                        "class_id": class_id or effective.get("class_id"),
                        "db_name": db_name,
                        "branch": overlay_branch,
                        "overlay_tombstone": False,
                        "lifecycle_id": lifecycle_id,
                        "base_token": base_token,
                        "patchset_commit_id": patchset_commit_id,
                        "action_log_id": action_log_id,
                        "conflict_status": conflict_status,
                        "event_id": event_id,
                        "event_sequence": incoming_seq,
                        "version": int(incoming_seq),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            try:
                await self.elasticsearch_service.index_document(
                    overlay_index,
                    effective,
                    doc_id=overlay_id,
                    refresh=False,
                    version=incoming_seq,
                    version_type="external_gte",
                )
            except Exception as e:
                if self._is_es_version_conflict(e):
                    logger.info(
                        "Skipping stale ActionApplied overlay write via ES version conflict "
                        "(seq=%s, instance_id=%s, lifecycle_id=%s)",
                        incoming_seq,
                        instance_id,
                        lifecycle_id,
                    )
                    continue
                raise
            
    async def _handle_instance_created(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 생성 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance creation")
            branch = instance_data.get("branch") or "main"
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "instances", branch=branch)
            
            # 클래스 라벨 조회 (Redis 캐시 활용)
            class_label = await self._get_class_label(instance_data.get('class_id'), db_name, branch=branch)
            
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

            event_meta = event_data.get("metadata") if isinstance(event_data, dict) else None
            if not isinstance(event_meta, dict):
                event_meta = {}
            ontology_ref, ontology_commit = split_ref_commit(event_meta.get("ontology"))

            lifecycle_id = str(event_meta.get("command_id") or event_data.get("command_id") or "").strip()
            if not lifecycle_id:
                lifecycle_id = str(event_id).strip()

            doc = {
                'instance_id': instance_id,
                'class_id': instance_data.get('class_id'),
                'class_label': class_label,
                'properties': self._normalize_properties(instance_data.get('properties', [])),
                'data': instance_data,  # 원본 데이터 (enabled: false)
                'lifecycle_id': lifecycle_id,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'version': int(incoming_seq) if incoming_seq is not None else 1,
                'db_name': db_name,
                'branch': branch,
                'ontology_ref': ontology_ref,
                'ontology_commit': ontology_commit,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # instance_id를 문서 ID로 사용 (업데이트/삭제 정합성)
            try:
                await self.elasticsearch_service.index_document(
                    index_name,
                    doc,
                    doc_id=instance_id,
                    refresh=False,
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
            branch = instance_data.get("branch") or "main"
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "instances", branch=branch)
            
            # 클래스 라벨 조회
            class_label = await self._get_class_label(instance_data.get('class_id'), db_name, branch=branch)
            
            # 기존 문서 조회
            instance_id = instance_data.get('instance_id')
            if not instance_id:
                raise ValueError("instance_id is required for instance update")

            existing_doc = await self.elasticsearch_service.get_document(
                index_name,
                instance_id
            )

            lifecycle_id = ""
            if isinstance(existing_doc, dict):
                lifecycle_id = str(existing_doc.get("lifecycle_id") or "").strip()
            if not lifecycle_id:
                lifecycle_id = str(instance_data.get("lifecycle_id") or "").strip()
            lifecycle_id = lifecycle_id or "lc-0"

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

            event_meta = event_data.get("metadata") if isinstance(event_data, dict) else None
            if not isinstance(event_meta, dict):
                event_meta = {}
            ontology_ref, ontology_commit = split_ref_commit(event_meta.get("ontology"))
            
            # 업데이트 문서 구성
            doc = {
                'instance_id': instance_id,
                'class_id': instance_data.get('class_id'),
                'class_label': class_label,
                'properties': self._normalize_properties(instance_data.get('properties', [])),
                'data': instance_data,
                'lifecycle_id': lifecycle_id,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'version': version,
                'db_name': db_name,
                'branch': branch,
                'ontology_ref': ontology_ref,
                'ontology_commit': ontology_commit,
                'created_at': created_at or datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            try:
                if incoming_seq is not None:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=instance_id,
                        refresh=False,
                        version=incoming_seq,
                        version_type="external_gte",
                    )
                else:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=instance_id,
                        refresh=False,
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
            branch = instance_data.get("branch") or "main"
                
            # 인덱스 이름 결정
            index_name = await self._ensure_index_exists(db_name, "instances", branch=branch)
            instance_id = instance_data.get('instance_id')
            if not instance_id:
                raise ValueError("instance_id is required for instance deletion")

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))

            # Branch overlay semantics:
            # - main branch: physical delete (existing behavior)
            # - non-main branch: write a tombstone document so branch reads do not fall back to main
            if branch != "main":
                if incoming_seq is None:
                    logger.warning(
                        f"Refusing to tombstone instance without sequence_number "
                        f"(branch={branch}, instance_id={instance_id})"
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

                existing_doc = await self.elasticsearch_service.get_document(index_name, instance_id)
                if existing_doc and existing_doc.get("event_id") == event_id and existing_doc.get("deleted"):
                    logger.info(
                        f"Skipping duplicate instance tombstone event (event_id={event_id}, instance_id={instance_id})"
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
                        skip_reason="duplicate",
                        extra_metadata={"tombstone": True},
                    )
                    return

                event_meta = event_data.get("metadata") if isinstance(event_data, dict) else None
                if not isinstance(event_meta, dict):
                    event_meta = {}
                ontology_ref, ontology_commit = split_ref_commit(event_meta.get("ontology"))

                tombstone_doc = {
                    "instance_id": instance_id,
                    "class_id": instance_data.get("class_id"),
                    "db_name": db_name,
                    "branch": branch,
                    "ontology_ref": ontology_ref,
                    "ontology_commit": ontology_commit,
                    "deleted": True,
                    "deleted_at": datetime.now(timezone.utc).isoformat(),
                    "event_id": event_id,
                    "event_sequence": incoming_seq,
                    "event_timestamp": event_data.get("occurred_at") or event_data.get("timestamp"),
                    "version": int(incoming_seq),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

                try:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        tombstone_doc,
                        doc_id=instance_id,
                        refresh=False,
                        version=incoming_seq,
                        version_type="external_gte",
                    )
                except Exception as e:
                    if self._is_es_version_conflict(e):
                        logger.info(
                            f"Skipping stale instance tombstone event via ES version conflict "
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
                            extra_metadata={"tombstone": True},
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
                        extra_metadata={"tombstone": True},
                    )
                    raise

                logger.info(
                    f"Instance tombstoned in branch overlay (branch={branch}, instance_id={instance_id}, index={index_name})"
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
                    extra_metadata={"tombstone": True},
                )
                return

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
                    refresh=False,
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
            branch = ontology_data.get("branch") or "main"
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "ontologies", branch=branch)
            
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

            event_meta = event_data.get("metadata") if isinstance(event_data, dict) else None
            if not isinstance(event_meta, dict):
                event_meta = {}
            ontology_ref, ontology_commit = split_ref_commit(event_meta.get("ontology"))

            default_lang = get_default_language()
            label_text, label_i18n = self._normalize_localized_field(
                ontology_data.get('label'),
                default_lang=default_lang,
            )
            description_text, description_i18n = self._normalize_localized_field(
                ontology_data.get('description'),
                default_lang=default_lang,
            )
            properties = self._normalize_ontology_properties(
                ontology_data.get('properties', []),
                default_lang=default_lang,
            )
            relationships = self._normalize_ontology_relationships(
                ontology_data.get('relationships', []),
                default_lang=default_lang,
            )

            doc = {
                'class_id': class_id,
                'label': label_text,
                'description': description_text,
                'properties': properties,
                'relationships': relationships,
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': db_name,
                'branch': branch,
                'ontology_ref': ontology_ref,
                'ontology_commit': ontology_commit,
                'version': int(incoming_seq) if incoming_seq is not None else 1,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            if label_i18n:
                doc['label_i18n'] = label_i18n
            if description_i18n:
                doc['description_i18n'] = description_i18n
            
            # 인덱싱 (external version guard)
            try:
                await self.elasticsearch_service.index_document(
                    index_name,
                    doc,
                    doc_id=class_id,
                    refresh=False,
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
                label_text,
                db_name,
                branch=branch,
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
            branch = ontology_data.get("branch") or "main"
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "ontologies", branch=branch)

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

            event_meta = event_data.get("metadata") if isinstance(event_data, dict) else None
            if not isinstance(event_meta, dict):
                event_meta = {}
            ontology_ref, ontology_commit = split_ref_commit(event_meta.get("ontology"))
            
            # 업데이트 문서 구성
            default_lang = get_default_language()
            label_text, label_i18n = self._normalize_localized_field(
                ontology_data.get('label'),
                default_lang=default_lang,
            )
            description_text, description_i18n = self._normalize_localized_field(
                ontology_data.get('description'),
                default_lang=default_lang,
            )
            properties = self._normalize_ontology_properties(
                ontology_data.get('properties', []),
                default_lang=default_lang,
            )
            relationships = self._normalize_ontology_relationships(
                ontology_data.get('relationships', []),
                default_lang=default_lang,
            )
            doc = {
                'class_id': class_id,
                'label': label_text,
                'description': description_text,
                'properties': properties,
                'relationships': relationships,
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': db_name,
                'branch': branch,
                'ontology_ref': ontology_ref,
                'ontology_commit': ontology_commit,
                'version': version,
                'event_id': event_id,
                'event_sequence': incoming_seq,
                'event_timestamp': event_data.get('occurred_at') or event_data.get('timestamp'),
                'created_at': created_at or datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            if label_i18n:
                doc['label_i18n'] = label_i18n
            if description_i18n:
                doc['description_i18n'] = description_i18n
            
            try:
                if incoming_seq is not None:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=class_id,
                        refresh=False,
                        version=incoming_seq,
                        version_type="external_gte",
                    )
                else:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        doc,
                        doc_id=class_id,
                        refresh=False,
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
                label_text,
                db_name,
                branch=branch,
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
            branch = ontology_data.get("branch") or "main"
                
            # 인덱스 이름 결정
            index_name = await self._ensure_index_exists(db_name, "ontologies", branch=branch)
            class_id = ontology_data.get('class_id') or ontology_data.get('id')
            if not class_id:
                raise ValueError("class_id is required for ontology class deletion")

            incoming_seq = self._parse_sequence(event_data.get("sequence_number"))

            # Branch overlay semantics:
            # - main branch: physical delete (existing behavior)
            # - non-main branch: write a tombstone document so branch reads do not fall back to main
            if branch != "main":
                if incoming_seq is None:
                    logger.warning(
                        f"Refusing to tombstone ontology class without sequence_number "
                        f"(branch={branch}, class_id={class_id})"
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
                        extra_metadata={"tombstone": True},
                    )
                    return

                existing_doc = await self.elasticsearch_service.get_document(index_name, class_id)
                if existing_doc and existing_doc.get("event_id") == event_id and existing_doc.get("deleted"):
                    logger.info(
                        f"Skipping duplicate ontology tombstone event (event_id={event_id}, class_id={class_id})"
                    )
                    await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id, branch))
                    await self._record_es_side_effect(
                        event_id=str(event_id),
                        event_data=event_data,
                        db_name=db_name,
                        index_name=index_name,
                        doc_id=str(class_id),
                        operation="index",
                        status="success",
                        record_lineage=False,
                        skip_reason="duplicate",
                        extra_metadata={"tombstone": True},
                    )
                    return

                event_meta = event_data.get("metadata") if isinstance(event_data, dict) else None
                if not isinstance(event_meta, dict):
                    event_meta = {}
                ontology_ref, ontology_commit = split_ref_commit(event_meta.get("ontology"))

                tombstone_doc = {
                    "class_id": class_id,
                    "db_name": db_name,
                    "branch": branch,
                    "ontology_ref": ontology_ref,
                    "ontology_commit": ontology_commit,
                    "deleted": True,
                    "deleted_at": datetime.now(timezone.utc).isoformat(),
                    "event_id": event_id,
                    "event_sequence": incoming_seq,
                    "event_timestamp": event_data.get("occurred_at") or event_data.get("timestamp"),
                    "version": int(incoming_seq),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

                try:
                    await self.elasticsearch_service.index_document(
                        index_name,
                        tombstone_doc,
                        doc_id=class_id,
                        refresh=False,
                        version=incoming_seq,
                        version_type="external_gte",
                    )
                except Exception as e:
                    if self._is_es_version_conflict(e):
                        logger.info(
                            f"Skipping stale ontology tombstone event via ES version conflict "
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
                            extra_metadata={"tombstone": True},
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
                        extra_metadata={"tombstone": True},
                    )
                    raise

                await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id, branch))
                logger.info(
                    f"Ontology tombstoned in branch overlay (branch={branch}, class_id={class_id}, index={index_name})"
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
                    extra_metadata={"tombstone": True},
                )
                return

            if incoming_seq is None:
                existing_doc = await self.elasticsearch_service.get_document(index_name, class_id)
                if not existing_doc:
                    logger.info(
                        f"Ontology class already deleted (class_id={class_id}); treating delete as idempotent success"
                    )
                    await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id, branch))
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
                    await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id, branch))
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
                    refresh=False,
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
            await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id, branch))
            
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
                    settings=get_default_index_settings()
                )
            
            # 메타데이터 문서 인덱싱
            await self.elasticsearch_service.index_document(
                metadata_index,
                metadata_doc,
                doc_id=db_name,
                refresh=False
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
            
            # Related indices:
            # - base indices (main)
            # - branch overlay indices (`__br_*`)
            # - versioned rebuild indices (`_*` suffix)
            base = sanitize_index_name(db_name)
            patterns = [f"{base}_instances*", f"{base}_ontologies*"]

            indices_to_delete: List[str] = []
            for pattern in patterns:
                try:
                    result = await self.elasticsearch_service.client.indices.get(
                        index=pattern,
                        allow_no_indices=True,
                        ignore_unavailable=True,
                    )
                    if isinstance(result, dict):
                        indices_to_delete.extend(list(result.keys()))
                except Exception:
                    continue

            # Always include canonical base names (in case wildcards are restricted).
            indices_to_delete.extend(
                [
                    get_instances_index_name(db_name),
                    get_ontologies_index_name(db_name),
                ]
            )

            # Dedup while preserving order
            seen = set()
            indices_to_delete = [i for i in indices_to_delete if not (i in seen or seen.add(i))]

            for index_name in indices_to_delete:
                try:
                    if await self.elasticsearch_service.index_exists(index_name):
                        await self.elasticsearch_service.delete_index(index_name)
                        logger.info(f"Deleted index: {index_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete index {index_name} (continuing): {e}")
            
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
                    refresh=False
                )
            
            # 생성된 인덱스 캐시에서 제거
            for index_name in indices_to_delete:
                self.created_indices.discard(index_name)
            
            logger.info(f"Database deletion processed: {db_name}, indices cleaned up and metadata updated")
            
        except Exception as e:
            logger.error(f"Failed to handle database deleted: {e}")
            raise
            
    async def _get_class_label(self, class_id: str, db_name: str, *, branch: str = "main") -> Optional[str]:
        """
        Redis에서 클래스 라벨 조회 (Cache Stampede 방지)
        
        분산 락을 사용하여 동시에 여러 요청이 들어와도 
        Elasticsearch에는 한 번만 요청하도록 최적화합니다.
        """
        try:
            if not class_id or not db_name:
                return None
                
            cache_key = AppConfig.get_class_label_key(db_name, class_id, branch)
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
                        index_name = get_ontologies_index_name(db_name, branch=branch)
                        doc = await self.elasticsearch_service.get_document(index_name, class_id)

                        # Branch virtualization: fall back to main ontology index when the overlay
                        # does not override this class (copy-on-write).
                        if (not doc) and branch != "main":
                            base_index = get_ontologies_index_name(db_name, branch="main")
                            doc = await self.elasticsearch_service.get_document(base_index, class_id)
                        
                        if doc:
                            label = doc.get('label')
                            if isinstance(label, dict):
                                label = select_localized_text(label, lang=get_default_language())
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
            return await self._get_class_label_fallback(class_id, db_name, branch=branch)
            
        except Exception as e:
            logger.error(f"Failed to get class label for {class_id} in {db_name}: {e}")
            return None
    
    async def _get_class_label_fallback(
        self, class_id: str, db_name: str, *, branch: str = "main"
    ) -> Optional[str]:
        """
        락 획득 실패 시 fallback 조회 (성능보다 안정성 우선)
        """
        try:
            self.cache_metrics['fallback_queries'] += 1
            self.cache_metrics['elasticsearch_queries'] += 1
            
            index_name = get_ontologies_index_name(db_name, branch=branch)
            doc = await self.elasticsearch_service.get_document(index_name, class_id)
            if (not doc) and branch != "main":
                base_index = get_ontologies_index_name(db_name, branch="main")
                doc = await self.elasticsearch_service.get_document(base_index, class_id)
            
            if doc:
                label = doc.get('label')
                if isinstance(label, dict):
                    label = select_localized_text(label, lang=get_default_language())
                if label:
                    # 짧은 시간만 캐싱 (경합 상황이므로)
                    cache_key = AppConfig.get_class_label_key(db_name, class_id, branch)
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
            
    async def _cache_class_label(self, class_id: str, label: str, db_name: str, *, branch: str = "main"):
        """클래스 라벨을 Redis에 캐싱"""
        try:
            if not class_id or not label or not db_name:
                return
                
            cache_key = AppConfig.get_class_label_key(db_name, class_id, branch)
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

            
    async def _shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Projection Worker...")
        
        self.running = False
        
        if self.consumer:
            await self._consumer_call(self.consumer.close)
        self._consumer_executor.shutdown(wait=True, cancel_futures=True)
            
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
