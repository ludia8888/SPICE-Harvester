"""
Instance Worker — Direct ES Write (Foundry-aligned runtime)

Kafka message contract:
- Commands arrive as EventEnvelope JSON (metadata.kind == "command")
- Domain events are appended to the S3/MinIO Event Store and relayed to Kafka

Write path:
1. S3: Durable command log (source of truth for audit)
2. Elasticsearch: Direct instance index/update/delete (ephemeral, rebuildable)
3. Event Store: Domain events (INSTANCE_CREATED/UPDATED/DELETED)

Ontology schema resolved via OMS HTTP API.
"""

import asyncio
import inspect
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set, List, Union
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

from shared.config.app_config import AppConfig
from shared.config.instances_index_mapping import INSTANCE_INDEX_MAPPING
from shared.services.storage.redis_service import create_redis_service
from shared.services.core.command_status_service import (
    CommandStatusService as CommandStatusTracker,
    CommandStatus as CommandStatusEnum,
)
from shared.services.core.link_index_job_builder import build_link_index_objectify_job
from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.models.lineage_edge_types import (
    EDGE_EVENT_DELETED_ES_DOCUMENT,
    EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
    EDGE_EVENT_STORED_IN_OBJECT_STORE,
)
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.security.input_sanitizer import validate_branch_name, validate_instance_id
from shared.services.grpc.oms_gateway_client import OMSGrpcHttpCompatClient
from shared.services.kafka.dlq_publisher import DlqPublishSpec
from shared.services.kafka.processed_event_worker import (
    CommandEnvelopePayload,
    CommandParseError,
    FailureLogContext,
    RegistryKey,
    StrictCommandEnvelopeKafkaWorker,
    WorkerRuntimeConfig,
)
from shared.services.kafka.safe_consumer import SafeKafkaConsumer
from shared.services.kafka.retry_classifier import (
    INSTANCE_COMMAND_RETRY_PROFILE,
    classify_retryable_with_profile,
)
from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
)
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.core.worker_stores import WorkerObservability, initialize_worker_stores
from shared.services.core.write_path_contract import (
    emit_write_path_contract,
    followup_completed,
    followup_degraded,
    followup_skipped,
)
from shared.services.registries.lineage_store import LineageStore
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.errors.runtime_exception_policy import LineageRecordError, LineageUnavailableError
from shared.utils.chaos import maybe_crash
from shared.utils.ontology_version import normalize_ontology_version
from shared.utils.app_logger import configure_logging
from shared.utils.deterministic_ids import deterministic_uuid5_hex_prefix, deterministic_uuid5_str
from shared.utils.blank_utils import strip_to_none
from shared.utils.spice_event_ids import spice_event_id

from oms.services.event_store import EventStore
from shared.services.storage.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
from shared.config.search_config import get_instances_index_name, get_default_index_settings
from shared.services.core.relationship_extractor import extract_relationships as shared_extract_relationships

_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)


@dataclass
class _InstanceCommandPayload(CommandEnvelopePayload):
    pass


class _InstanceCommandParseError(CommandParseError):
    pass


class StrictInstanceWorker(StrictCommandEnvelopeKafkaWorker[_InstanceCommandPayload, None]):
    """
    STRICT Lightweight Instance Worker
    Graph는 관계와 참조만, 데이터는 ES/S3에만

    Command input is consumed exclusively as EventEnvelope JSON.
    """
    parse_error_enable_dlq = True
    parse_error_publish_failure_message = "Failed to publish invalid instance payload to DLQ: %s"
    parse_error_invalid_payload_message = "Invalid instance payload; skipping: %s"
    parse_error_raise_on_publish_failure = False
    command_payload_cls = _InstanceCommandPayload
    command_parse_error_cls = _InstanceCommandParseError

    def _emit_write_path_contract(
        self,
        *,
        authoritative_write: str,
        followups: List[Dict[str, Any]],
        level: str = "info",
    ) -> Dict[str, Any]:
        return emit_write_path_contract(
            logger,
            authoritative_write=authoritative_write,
            followups=followups,
            level=level,
            message_prefix="Instance worker write path contract",
        )

    def __init__(self):
        settings = get_settings()
        worker_cfg = settings.workers.instance

        self._bootstrap_worker_runtime(
            config=WorkerRuntimeConfig(
                service_name="instance-worker",
                handler="instance_worker",
                kafka_servers=settings.database.kafka_servers,
                dlq_topic=AppConfig.INSTANCE_COMMANDS_DLQ_TOPIC,
                dlq_flush_timeout_seconds=float(worker_cfg.dlq_flush_timeout_seconds),
                max_retry_attempts=int(worker_cfg.max_retry_attempts),
                backoff_base=1,
                backoff_max=60,
            ),
            tracing=get_tracing_service("instance-worker"),
            metrics=get_metrics_collector("instance-worker"),
        )

        self.enable_event_sourcing = bool(settings.event_sourcing.enable_event_sourcing)
        self.producer = None
        self._dlq_spec = DlqPublishSpec(
            dlq_topic=self.dlq_topic,
            service_name=self.service_name,
            span_name="instance_worker.dlq_produce",
            metric_event_name="INSTANCE_COMMAND_DLQ",
            flush_timeout_seconds=self.dlq_flush_timeout_seconds,
        )
        self.untyped_ref_max_retry_attempts = int(worker_cfg.untyped_ref_max_retry_attempts)
        self.untyped_ref_backoff_max_seconds = float(worker_cfg.untyped_ref_backoff_max_seconds)
        self.redis_client = None
        self.command_status_service: Optional[CommandStatusTracker] = None
        self.s3_client = None
        self.elasticsearch_service: Optional[ElasticsearchService] = None
        self.oms_http: Optional[OMSGrpcHttpCompatClient] = None
        self._created_es_indices: set = set()
        self.instance_bucket = AppConfig.INSTANCE_BUCKET

        # Durable idempotency (Postgres)
        self.enable_processed_event_registry = bool(settings.event_sourcing.enable_processed_event_registry)
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.event_store: Optional[EventStore] = None

        # First-class provenance/audit (lineage fail-closed by default)
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.lineage_required = bool(settings.observability.lineage_required_effective and self.enable_lineage)
        self.enable_audit_logs = bool(settings.observability.enable_audit_logs)
        self.allow_pk_generation = bool(worker_cfg.allow_pk_generation)
        self.strict_relationship_schema = bool(worker_cfg.relationship_strict)
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None
        self.observability = WorkerObservability(
            lineage_store=None,
            audit_store=None,
            logger=logger,
            lineage_required=self.lineage_required,
        )
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.run_id = settings.observability.run_id
        self.code_sha = settings.observability.code_sha
        
        # Lightweight graph principle: only business concepts in graph
        # No system fields or storage details

    @staticmethod
    def _is_ingest_metadata(metadata: Any) -> bool:
        if not isinstance(metadata, dict):
            return False
        return str(metadata.get("kind") or "").strip().lower() == "ingest"

    @classmethod
    def _writeback_guard_blocks(cls, command: Dict[str, Any]) -> bool:
        if not AppConfig.WRITEBACK_ENFORCE:
            return False
        if not isinstance(command, dict):
            return False
        command_type = str(command.get("command_type") or "").strip()
        if command_type not in {
            "CREATE_INSTANCE",
            "UPDATE_INSTANCE",
            "DELETE_INSTANCE",
            "BULK_CREATE_INSTANCES",
            "BULK_UPDATE_INSTANCES",
        }:
            return False
        class_id = str(command.get("class_id") or "").strip()
        if not class_id:
            return False
        if not AppConfig.is_writeback_enabled_object_type(class_id):
            return False
        return not cls._is_ingest_metadata(command.get("metadata"))
        
    async def initialize(self):
        """Initialize all connections"""
        logger.info("Initializing STRICT Instance Worker...")

        settings = get_settings()
        
        group_id = (AppConfig.INSTANCE_WORKER_GROUP or "instance-worker-group").strip()
        self._initialize_safe_consumer_runtime(
            group_id=group_id,
            topics=[AppConfig.INSTANCE_COMMANDS_TOPIC],
            service_name="instance-worker",
            thread_name_prefix="instance-worker-kafka",
            max_poll_interval_ms=600_000,   # 10min (default 5min) — bulk-create can be slow
            session_timeout_ms=120_000,     # 2min (default 45s)
        )
        logger.info(f"Using consumer group: {group_id}")
        
        # Kafka Producer for DLQ (best-effort)
        self.producer = create_kafka_dlq_producer(
            bootstrap_servers=self.kafka_servers,
            client_id="strict-instance-worker-producer",
        )
        self.dlq_producer = self.producer
        
        # Redis (optional - don't fail if not available)
        try:
            self.redis_service = create_redis_service(settings)
            await self.redis_service.connect()
            self.redis_client = self.redis_service.client
            self.command_status_service = CommandStatusTracker(self.redis_service)
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis connection failed, continuing without Redis: {e}")
            self.redis_service = None
            self.redis_client = None
            self.command_status_service = None
        
        # S3/MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url=settings.storage.minio_endpoint_url,
            aws_access_key_id=settings.storage.minio_access_key,
            aws_secret_access_key=settings.storage.minio_secret_key,
            region_name='us-east-1'
        )
        
        # Ensure bucket exists
        try:
            await self._s3_call(self.s3_client.head_bucket, Bucket=self.instance_bucket)
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code", "")
            if str(code) not in {"404", "NoSuchBucket", "NotFound"}:
                logger.error("S3 head_bucket failed (bucket=%s code=%s)", self.instance_bucket, code, exc_info=True)
                raise
            try:
                await self._s3_call(self.s3_client.create_bucket, Bucket=self.instance_bucket)
            except ClientError as create_err:
                create_code = (create_err.response.get("Error") or {}).get("Code", "")
                if str(create_code) not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    logger.error(
                        "S3 create_bucket failed (bucket=%s code=%s)",
                        self.instance_bucket,
                        create_code,
                        exc_info=True,
                    )
                    raise
        
        # Elasticsearch (direct instance writes — Phase 2)
        self.elasticsearch_service = create_elasticsearch_service(settings)
        es_connect_error: Optional[Exception] = None
        for attempt in range(1, 11):
            try:
                await self.elasticsearch_service.connect()
                es_connect_error = None
                break
            except Exception as exc:
                es_connect_error = exc
                msg = str(exc or "").lower()
                retryable = (
                    isinstance(exc, (asyncio.TimeoutError, TimeoutError))
                    or self._is_elasticsearch_outage_error_message(msg)
                    or "cannot connect to host" in msg
                    or "connectionerror" in msg
                    or "connection refused" in msg
                )
                if (not retryable) or attempt >= 10:
                    raise
                backoff_s = min(30, int(2 ** max(attempt - 1, 0)))
                logger.warning(
                    "Elasticsearch not ready during startup (attempt %s/10); retrying in %ss: %s",
                    attempt,
                    backoff_s,
                    exc,
                )
                await asyncio.sleep(backoff_s)
        if es_connect_error is not None:
            raise RuntimeError("Failed to connect to Elasticsearch during worker startup") from es_connect_error
        logger.info("Elasticsearch connected for direct instance writes")

        # OMS gRPC-backed client (ontology schema resolution)
        self.oms_http = OMSGrpcHttpCompatClient()

        stores = None
        stores_init_error: Optional[Exception] = None
        for attempt in range(1, 6):
            try:
                stores = await initialize_worker_stores(
                    enable_lineage=self.enable_lineage,
                    enable_audit_logs=self.enable_audit_logs,
                    logger=logger,
                )
                break
            except (asyncio.TimeoutError, TimeoutError) as exc:
                stores_init_error = exc
                if attempt >= 5:
                    raise
                backoff_s = min(30, int(2 ** max(attempt - 1, 0)))
                logger.warning(
                    "initialize_worker_stores timed out (attempt %s/5); retrying in %ss",
                    attempt,
                    backoff_s,
                )
                await asyncio.sleep(backoff_s)

        if stores is None:
            raise RuntimeError("initialize_worker_stores failed after retries") from stores_init_error

        self.processed_event_registry = stores.processed
        self.processed = stores.processed
        self.lineage_store = stores.lineage_store
        self.audit_store = stores.audit_store
        self.observability = WorkerObservability(
            lineage_store=self.lineage_store,
            audit_store=self.audit_store,
            logger=logger,
            lineage_required=stores.lineage_required,
        )

        # Dataset registry for edit tracking (best-effort)
        try:
            self.dataset_registry = DatasetRegistry()
            await self.dataset_registry.initialize()
            logger.info("✅ DatasetRegistry connected (instance edits tracking)")
        except Exception as e:
            logger.warning(f"⚠️ DatasetRegistry unavailable (edits tracking disabled): {e}")
            self.dataset_registry = None

        # Objectify registry for direct link reindex enqueue (no BFF v1 dependency).
        try:
            self.objectify_registry = ObjectifyRegistry()
            await self.objectify_registry.initialize()
            logger.info("✅ ObjectifyRegistry connected (link reindex enqueue)")
        except Exception as e:
            logger.warning(f"⚠️ ObjectifyRegistry unavailable (link reindex enqueue disabled): {e}")
            self.objectify_registry = None

        # Event Sourcing is now mandatory for write-side correctness.
        # Deprecated "direct Kafka publish" paths are removed because they break SSoT.
        if not self.enable_event_sourcing:
            raise RuntimeError(
                "ENABLE_EVENT_SOURCING=false is no longer supported. "
                "Enable Event Sourcing and provide a reachable Event Store (MinIO/S3)."
            )

        self.event_store = EventStore()
        self.event_store.attach_observability_stores(
            lineage_store=self.lineage_store,
            audit_store=self.audit_store,
        )
        await self.event_store.connect()
        logger.info("✅ Event Store connected (domain events will be appended to S3/MinIO)")
        
        logger.info("✅ STRICT Instance Worker initialized")

    async def _ensure_instances_index(self, *, db_name: str, branch: str) -> str:
        """Ensure the ES instances index exists (create if needed, update mapping if exists)."""
        index_name = get_instances_index_name(db_name, branch=branch)
        if index_name in self._created_es_indices:
            try:
                if await self.elasticsearch_service.index_exists(index_name):
                    return index_name
            except Exception as exc:
                logger.warning(
                    "Failed to verify cached Elasticsearch index %s, forcing recreate: %s",
                    index_name,
                    exc,
                    exc_info=True,
                )
            self._created_es_indices.discard(index_name)

        if not await self.elasticsearch_service.index_exists(index_name):
            await self.elasticsearch_service.create_index(
                index=index_name,
                mappings=INSTANCE_INDEX_MAPPING,
                settings=get_default_index_settings(),
            )

        # Always reconcile mapping after create attempt.
        # `create_index()` can race and return "already exists" after an earlier
        # `index_exists()` check, in which case we still must enforce canonical
        # mapping to avoid transient type-conflict failures.
        await self.elasticsearch_service.update_mapping(
            index=index_name,
            properties=INSTANCE_INDEX_MAPPING["properties"],
        )
        logger.info("Ensured instances index + mapping: %s", index_name)

        self._created_es_indices.add(index_name)
        return index_name

    async def _fetch_class_schema_via_oms(
        self, db_name: str, class_id: str, branch: str = "main"
    ) -> Optional[Dict[str, Any]]:
        """Fetch ontology schema from OMS HTTP API."""
        if not self.oms_http:
            return None
        try:
            resp = await self.oms_http.get_ontology_typed(
                db_name=db_name,
                class_id=class_id,
                branch=branch,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.debug("OMS ontology fetch returned %d for %s/%s", resp.status_code, db_name, class_id)
            return None
        except Exception as e:
            logger.debug("OMS ontology fetch failed for %s/%s: %s", db_name, class_id, e)
            return None

    async def _resolve_ontology_version_via_oms(
        self, *, db_name: str, branch: str
    ) -> Dict[str, str]:
        """Resolve ontology version stamp in Foundry-style ref form."""
        from shared.utils.ontology_version import build_ontology_version
        _ = db_name
        return build_ontology_version(branch=branch, commit=None)

    @staticmethod
    def _build_es_document(
        *,
        db_name: str,
        branch: str,
        class_id: str,
        instance_id: str,
        payload: Dict[str, Any],
        ontology_version: Optional[Dict[str, str]],
        event_sequence: int,
        relationships: Optional[Dict[str, Any]] = None,
        now_iso: str,
        created_at_override: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build ES document in canonical format matching objectify_worker write_paths._build_document."""
        from typing import Mapping

        lifecycle_seed = f"{db_name}:{class_id}:{instance_id}"
        lifecycle_id = f"lc-{deterministic_uuid5_hex_prefix(lifecycle_seed, length=12)}"
        event_id = deterministic_uuid5_str(
            f"instance_crud:{command_id or 'nocmd'}:{instance_id}:{event_sequence}"
        )

        data_payload = dict(payload)
        data_payload.setdefault("instance_id", instance_id)
        data_payload.setdefault("class_id", class_id)
        data_payload.setdefault("db_name", db_name)
        data_payload.setdefault("branch", branch)

        properties = data_payload.get("properties")
        if not isinstance(properties, list):
            properties = []

        ontology_ref = None
        ontology_commit = None
        if isinstance(ontology_version, Mapping):
            ontology_ref = ontology_version.get("ref")
            ontology_commit = ontology_version.get("commit")

        return {
            "instance_id": instance_id,
            "class_id": class_id,
            "class_label": class_id,
            "properties": properties,
            "data": data_payload,
            "relationships": relationships or {},
            "backing_dataset": None,
            "lifecycle_id": lifecycle_id,
            "event_id": event_id,
            "event_sequence": int(event_sequence),
            "event_timestamp": now_iso,
            "version": int(event_sequence),
            "db_name": db_name,
            "branch": branch,
            "ontology_ref": str(ontology_ref).strip() if ontology_ref else None,
            "ontology_commit": str(ontology_commit).strip() if ontology_commit else None,
            "created_at": created_at_override or now_iso,
            "updated_at": now_iso,
        }

    async def _s3_call(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    async def _s3_read_body(self, body) -> bytes:
        return await asyncio.to_thread(body.read)

    def _extract_payload_from_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Unwrap a command from the canonical EventEnvelope message.

        Contract: commands arrive as EventEnvelope JSON with metadata.kind == "command".
        """
        if not isinstance(message, dict):
            raise ValueError("Kafka command message must be a JSON object")

        try:
            envelope = EventEnvelope.model_validate(message)
        except Exception as e:
            raise ValueError(f"Invalid EventEnvelope: {e}") from e

        expected_kind = str(getattr(self, "expected_envelope_kind", "") or "command").strip()
        kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
        if expected_kind and kind != expected_kind:
            raise ValueError(f"Unexpected envelope kind for command topic: {kind}")

        return self._command_from_envelope(envelope)

    async def extract_payload_from_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return self._extract_payload_from_message(message)

    async def _stamp_ontology_version(
        self,
        *,
        command: Dict[str, Any],
        db_name: str,
        branch: str,
    ) -> Dict[str, str]:
        """
        Ensure commands carry an ontology ref/commit stamp for reproducibility.

        If the caller already provided partial ontology metadata (ref/commit), we
        merge in any missing fields from the authoritative resolver.
        """

        ontology_version = await self._resolve_ontology_version_via_oms(
            db_name=db_name, branch=branch,
        )
        command_meta = command.get("metadata")
        if not isinstance(command_meta, dict):
            command_meta = {}
            command["metadata"] = command_meta

        existing_ontology = normalize_ontology_version(command_meta.get("ontology"))
        if existing_ontology:
            merged = dict(existing_ontology)
            if "ref" not in merged and ontology_version.get("ref"):
                merged["ref"] = ontology_version["ref"]
            if "commit" not in merged and ontology_version.get("commit"):
                merged["commit"] = ontology_version["commit"]
            command_meta["ontology"] = merged
            return merged

        command_meta["ontology"] = dict(ontology_version)
        return dict(ontology_version)
    
    def get_primary_key_value(
        self,
        class_id: str,
        payload: Dict[str, Any],
        *,
        allow_generate: bool = True,
    ) -> str:
        """
        Extract primary key value dynamically based on class naming convention
        Pattern: {class_name.lower()}_id
        """
        # Standard pattern: class_name_id (e.g., product_id, client_id, order_id)
        expected_key = f"{class_id.lower()}_id"
        
        if expected_key in payload and payload.get(expected_key) is not None and str(payload.get(expected_key)).strip():
            return str(payload[expected_key])
        
        # Fallback: Look for any field ending with _id
        for key, value in payload.items():
            if key.endswith('_id') and value is not None and str(value).strip():
                logger.info(f"Using fallback primary key: {key} = {value}")
                return str(value)
        
        # Last resort: Generate a unique ID
        if not allow_generate:
            raise ValueError(f"Primary key '{expected_key}' is required for {class_id}")
        generated_id = f"{class_id.lower()}_{uuid4().hex[:8]}"
        logger.warning(f"No primary key found for {class_id}, generated: {generated_id}")
        return generated_id

    @staticmethod
    def _is_objectify_command(command: Dict[str, Any]) -> bool:
        meta = command.get("metadata")
        if not isinstance(meta, dict):
            return False
        for key in ("objectify_job_id", "mapping_spec_id", "mapping_spec_version"):
            if meta.get(key):
                return True
        return False
    
    async def extract_relationships(
        self,
        db_name: str,
        class_id: str,
        payload: Dict[str, Any],
        *,
        branch: str = "main",
        allow_pattern_fallback: bool = True,
        strict_schema: bool = False,
    ) -> Dict[str, Union[str, List[str]]]:
        """
        Extract ONLY relationship fields from payload via shared library.
        Ontology schema fetched from OMS HTTP API.
        """
        branch = validate_branch_name(branch)
        ontology_data = await self._fetch_class_schema_via_oms(db_name, class_id, branch)

        relationships = shared_extract_relationships(
            payload,
            ontology_data=ontology_data,
            allow_pattern_fallback=allow_pattern_fallback,
        )

        if strict_schema and ontology_data:
            known_fields: Set[str] = set(relationships.keys())
            # Identify known relationship fields from ontology
            if isinstance(ontology_data, dict):
                for rel in ontology_data.get("relationships") or []:
                    if isinstance(rel, dict):
                        name = rel.get("predicate") or rel.get("name")
                        if name:
                            known_fields.add(str(name))

            unknown_fields: List[str] = []
            for key, value in payload.items():
                if key in known_fields:
                    continue
                if isinstance(value, str) and "/" in value:
                    unknown_fields.append(str(key))
                elif isinstance(value, list) and value and all(isinstance(i, str) and "/" in i for i in value):
                    unknown_fields.append(str(key))
            if unknown_fields:
                raise ValueError(f"Unknown relationship fields: {sorted(set(unknown_fields))}")

        return relationships

    async def _apply_create_instance_side_effects(
        self,
        *,
        command_id: str,
        db_name: str,
        class_id: str,
        branch: str,
        payload: Dict[str, Any],
        instance_id: str,
        command_log: Dict[str, Any],
        ontology_version: Dict[str, str],
        created_by: str,
        allow_pattern_fallback: bool = True,
    ) -> Dict[str, Any]:
        """
        Apply the create-instance side-effects without touching command status.

        Used by BULK_CREATE_INSTANCES to create many instances under a single command_id
        while keeping command status semantics correct.
        """
        if not self.s3_client:
            raise RuntimeError("S3 client not initialized")
        if not self.elasticsearch_service:
            raise RuntimeError("Elasticsearch service not initialized")
        if not self.event_store:
            raise RuntimeError("Event Store not initialized (ENABLE_EVENT_SOURCING requires Event Store)")

        # 1) Save FULL data to S3 (instance command log bucket)
        s3_path = f"{db_name}/{branch}/{class_id}/{instance_id}/{command_id}.json"
        s3_data = {
            "command": command_log,
            "payload": payload,
            "instance_id": instance_id,
            "class_id": class_id,
            "branch": branch,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        put_resp = await self._s3_call(
            self.s3_client.put_object,
            Bucket=self.instance_bucket,
            Key=s3_path,
            Body=json.dumps(s3_data, indent=2),
            ContentType="application/json",
            Metadata={
                "command_id": str(command_id),
                "instance_id": instance_id,
                "class_id": class_id,
            },
        )

        etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
        version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
        await self.observability.record_link(
            from_node_id=LineageStore.node_event(str(command_id)),
            to_node_id=LineageStore.node_artifact("s3", self.instance_bucket, s3_path),
            edge_type=EDGE_EVENT_STORED_IN_OBJECT_STORE,
            occurred_at=datetime.now(timezone.utc),
            db_name=db_name,
            to_label=f"s3://{self.instance_bucket}/{s3_path}",
            edge_metadata={
                "bucket": self.instance_bucket,
                "key": s3_path,
                "purpose": "instance_command_log",
                "db_name": db_name,
                "etag": etag,
                "version_id": version_id,
                "ontology": ontology_version,
            },
        )
        await self.observability.audit_log(
            partition_key=f"db:{db_name}",
            actor="instance_worker",
            action="INSTANCE_S3_WRITE",
            status="success",
            resource_type="s3_object",
            resource_id=f"s3://{self.instance_bucket}/{s3_path}",
            event_id=str(command_id),
            command_id=str(command_id),
            metadata={
                "class_id": class_id,
                "instance_id": instance_id,
                "bucket": self.instance_bucket,
                "key": s3_path,
                "etag": etag,
                "version_id": version_id,
                "ontology": ontology_version,
            },
        )

        # 2) Extract relationships and persist the authoritative domain event first.
        relationships = await self.extract_relationships(
            db_name,
            class_id,
            payload,
            branch=branch,
            allow_pattern_fallback=allow_pattern_fallback,
            strict_schema=self.strict_relationship_schema,
        )
        event_payload = {
            "db_name": db_name,
            "branch": branch,
            "class_id": class_id,
            "instance_id": instance_id,
            **payload,
        }
        aggregate_id = f"{db_name}:{branch}:{class_id}:{instance_id}"

        domain_event_id = (
            spice_event_id(command_id=str(command_id), event_type="INSTANCE_CREATED", aggregate_id=aggregate_id)
            if command_id
            else str(uuid4())
        )

        envelope = EventEnvelope(
            event_id=domain_event_id,
            event_type="INSTANCE_CREATED",
            aggregate_type="Instance",
            aggregate_id=aggregate_id,
            occurred_at=datetime.now(timezone.utc),
            actor=created_by or "system",
            data=event_payload,
            metadata={
                "kind": "domain",
                "kafka_topic": AppConfig.INSTANCE_EVENTS_TOPIC,
                "service": "instance_worker",
                "command_id": command_id,
                "ontology": ontology_version,
                "run_id": self.run_id,
                "code_sha": self.code_sha,
            },
        )
        await self.event_store.append_event(envelope)

        # 3) Index the document with the authoritative event sequence.
        now_iso = datetime.now(timezone.utc).isoformat()
        event_sequence = int(envelope.sequence_number or 0)
        index_name = await self._ensure_instances_index(db_name=db_name, branch=branch)

        es_doc = self._build_es_document(
            db_name=db_name,
            branch=branch,
            class_id=class_id,
            instance_id=instance_id,
            payload=payload,
            ontology_version=ontology_version,
            event_sequence=event_sequence,
            relationships=relationships,
            now_iso=now_iso,
            command_id=str(command_id),
        )

        try:
            maybe_crash("instance_worker:before_es_index", logger=logger)
            await self.elasticsearch_service.index_document(
                index=index_name,
                document=es_doc,
                doc_id=instance_id,
            )
            maybe_crash("instance_worker:after_es_index", logger=logger)

            await self.observability.record_link(
                from_node_id=LineageStore.node_event(str(command_id)),
                to_node_id=LineageStore.node_artifact("es", db_name, branch, instance_id),
                edge_type=EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
                occurred_at=datetime.now(timezone.utc),
                db_name=db_name,
                to_label=f"es:{db_name}:{branch}:{instance_id}",
                edge_metadata={
                    "db_name": db_name,
                    "branch": branch,
                    "index_name": index_name,
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "ontology": ontology_version,
                    "sequence_number": event_sequence,
                },
            )
            await self.observability.audit_log(
                partition_key=f"db:{db_name}",
                actor="instance_worker",
                action="INSTANCE_ES_WRITE",
                status="success",
                resource_type="elasticsearch_document",
                resource_id=f"es:{index_name}/{instance_id}",
                event_id=str(command_id),
                command_id=str(command_id),
                metadata={
                    "class_id": class_id,
                    "branch": branch,
                    "instance_id": instance_id,
                    "index_name": index_name,
                    "ontology": ontology_version,
                    "sequence_number": event_sequence,
                },
            )
        except Exception as e:
            await self.observability.audit_log(
                partition_key=f"db:{db_name}",
                actor="instance_worker",
                action="INSTANCE_ES_WRITE",
                status="failure",
                resource_type="elasticsearch_document",
                resource_id=f"es:{index_name}/{instance_id}",
                event_id=str(command_id),
                command_id=str(command_id),
                metadata={
                    "class_id": class_id,
                    "branch": branch,
                    "instance_id": instance_id,
                    "index_name": index_name,
                    "ontology": ontology_version,
                    "sequence_number": event_sequence,
                },
                error=str(e),
            )
            self._emit_write_path_contract(
                authoritative_write="instance_create_event",
                followups=[
                    followup_degraded(
                        "instance_es_materialization",
                        error=str(e),
                        details={"index": index_name, "instance_id": instance_id},
                    )
                ],
                level="warning",
            )
            raise

        self._emit_write_path_contract(
            authoritative_write="instance_create_event",
            followups=[
                followup_completed(
                    "instance_es_materialization",
                    details={"index": index_name, "instance_id": instance_id},
                ),
                followup_completed(
                    "instance_es_audit",
                    details={"command_id": str(command_id) if command_id else None},
                ),
            ],
        )
        return {
            "instance_id": instance_id,
            "es_index": index_name,
            "s3_path": s3_path,
            "aggregate_id": aggregate_id,
            "domain_event_id": domain_event_id,
        }
        
    async def process_create_instance(self, command: Dict[str, Any]) -> None:
        """Process CREATE_INSTANCE command - strict lightweight mode."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = str(command.get("command_id") or "").strip()
        branch = validate_branch_name(command.get("branch") or "main")
        payload = command.get("payload", {}) or {}
        is_objectify = self._is_objectify_command(command)
        relationship_sync_status = followup_skipped(
            "relationship_object_link_sync",
            details={"reason": "objectify_command" if is_objectify else "not_attempted"},
        )

        if not command_id:
            raise ValueError("command_id is required")

        # Set command status early (202 already returned to user).
        await self.set_command_status(command_id, "processing")

        try:
            if not isinstance(payload, dict):
                raise ValueError("CREATE_INSTANCE payload must be an object")
            if not db_name:
                raise ValueError("db_name is required")
            if not class_id:
                raise ValueError("class_id is required")

            provided_instance_id = command.get("instance_id")
            if provided_instance_id:
                instance_id = str(provided_instance_id)
                validate_instance_id(instance_id)

                expected_key = f"{class_id.lower()}_id"
                if expected_key in payload and payload.get(expected_key) and str(payload.get(expected_key)) != instance_id:
                    raise ValueError(
                        f"instance_id mismatch: command.instance_id={instance_id} "
                        f"payload[{expected_key}]={payload.get(expected_key)}"
                    )

                expected_aggregate_id = f"{db_name}:{branch}:{class_id}:{instance_id}"
                aggregate_id_raw = command.get("aggregate_id")
                if aggregate_id_raw and str(aggregate_id_raw) != expected_aggregate_id:
                    raise ValueError(
                        f"aggregate_id mismatch: command.aggregate_id={aggregate_id_raw} expected={expected_aggregate_id}"
                    )
            else:
                instance_id = self.get_primary_key_value(
                    class_id,
                    payload,
                    allow_generate=self.allow_pk_generation and not is_objectify,
                )
                validate_instance_id(instance_id)

            command["instance_id"] = instance_id

            # Stamp semantic contract version (ontology ref/commit) for reproducibility.
            ontology_version = await self._stamp_ontology_version(
                command=command,
                db_name=db_name,
                branch=branch,
            )
            created_by = str(command.get("created_by") or "system")

            logger.info(f"🔷 STRICT: Creating {class_id}/{instance_id}")

            result = await self._apply_create_instance_side_effects(
                command_id=command_id,
                db_name=db_name,
                class_id=class_id,
                branch=branch,
                payload=payload,
                instance_id=instance_id,
                command_log=command,
                ontology_version=ontology_version,
                created_by=created_by,
                allow_pattern_fallback=not is_objectify,
            )

            s3_path = str(result.get("s3_path") or "")
            await self.set_command_status(
                command_id,
                "completed",
                {
                    "instance_id": instance_id,
                    "es_doc_id": instance_id,
                    "s3_uri": f"s3://{self.instance_bucket}/{s3_path}",
                },
            )

            await self._record_instance_edit(
                db_name=db_name,
                class_id=class_id,
                instance_id=instance_id,
                edit_type="create",
                fields=sorted(payload.keys()),
                metadata={
                    "command_id": command_id,
                    "branch": branch,
                    "actor": created_by,
                    "ontology": ontology_version,
                    "objectify": bool(is_objectify),
                },
            )

            if not is_objectify:
                try:
                    await self._apply_relationship_object_link_edits(
                        db_name=db_name,
                        branch=branch,
                        class_id=class_id,
                        instance_id=instance_id,
                        current_payload=payload,
                    )
                except Exception as exc:
                    logger.debug("Relationship object link sync failed: %s", exc, exc_info=True)

            logger.info("✅ STRICT: Instance created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create instance: {e}")
            raise

    async def process_bulk_create_instances(self, command: Dict[str, Any]) -> None:
        """Process BULK_CREATE_INSTANCES command (idempotent per event_id; no sequence-guard)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        branch = validate_branch_name(command.get("branch") or "main")
        is_objectify = self._is_objectify_command(command)
        payload = command.get("payload", {}) or {}
        command_meta = command.get("metadata")
        if not isinstance(command_meta, dict):
            command_meta = {}
        is_objectify = self._is_objectify_command(command)

        await self.set_command_status(command_id, "processing")

        try:
            if not db_name:
                raise ValueError("db_name is required")
            if not class_id:
                raise ValueError("class_id is required")
            if not isinstance(payload, dict):
                raise ValueError("BULK_CREATE_INSTANCES payload must be an object")

            raw_instances = payload.get("instances")
            if not isinstance(raw_instances, list):
                raise ValueError("BULK_CREATE_INSTANCES payload.instances must be a list")

            if not raw_instances:
                await self.set_command_status(
                    command_id,
                    "completed",
                    {"created": 0, "total": 0, "note": "No instances provided"},
                )
                return

            # Stamp semantic contract version (ontology ref/commit) once for the whole bulk op.
            ontology_version = await self._stamp_ontology_version(
                command=command,
                db_name=db_name,
                branch=branch,
            )

            created_by = command.get("created_by") or "system"
            expected_key = f"{class_id.lower()}_id"

            total = len(raw_instances)
            created_count = 0
            sample_instance_ids: List[str] = []

            def _coerce_pk_fields(value: Any) -> List[str]:
                if not value:
                    return []
                if isinstance(value, str):
                    field = value.strip()
                    return [field] if field else []
                if isinstance(value, list):
                    fields: List[str] = []
                    for item in value:
                        if isinstance(item, str) and item.strip():
                            fields.append(item.strip())
                    return fields
                return []

            objectify_pk_fields = _coerce_pk_fields(command_meta.get("objectify_pk_fields"))
            if not objectify_pk_fields:
                # Backward/alternate key support (best-effort).
                objectify_pk_fields = _coerce_pk_fields(command_meta.get("objectify_instance_id_field"))

            for idx, inst in enumerate(raw_instances):
                if not isinstance(inst, dict):
                    raise ValueError(f"instances[{idx}] must be an object")

                inst_payload = dict(inst)

                # Prefer explicit primary-key fields. Fall back to deterministic per-row ID so retries don't duplicate.
                candidate = inst_payload.get(expected_key)
                instance_id_source_key: Optional[str] = expected_key if candidate else None
                if candidate:
                    instance_id = str(candidate)
                else:
                    instance_id = None
                    for key, value in inst_payload.items():
                        if not key.endswith("_id"):
                            continue
                        if value is None:
                            continue
                        value_str = str(value).strip()
                        if not value_str:
                            continue
                        instance_id = value_str
                        instance_id_source_key = key
                        break

                    if not instance_id and is_objectify:
                        # Objectify contract: derive stable instance ID from declared PK fields.
                        pk_fields = objectify_pk_fields
                        if not pk_fields:
                            raise ValueError(f"Primary key is required for objectify bulk create ({class_id})")
                        values: List[str] = []
                        for field in pk_fields:
                            raw = inst_payload.get(field)
                            if raw is None:
                                raise ValueError(
                                    f"Primary key is required for objectify bulk create ({class_id}): missing '{field}'"
                                )
                            raw_str = str(raw).strip()
                            if not raw_str:
                                raise ValueError(
                                    f"Primary key is required for objectify bulk create ({class_id}): blank '{field}'"
                                )
                            values.append(raw_str)

                        proposed = values[0] if len(values) == 1 else ":".join(values)
                        try:
                            validate_instance_id(proposed)
                        except (TypeError, ValueError):
                            suffix = deterministic_uuid5_hex_prefix(
                                f"objectify:{class_id}:{'|'.join(values)}",
                                length=12,
                            )
                            proposed = f"{class_id.lower()}_{suffix}"
                        instance_id = proposed
                        instance_id_source_key = None

                    if not instance_id:
                        if is_objectify:
                            raise ValueError(f"Primary key is required for objectify bulk create ({class_id})")
                        suffix = deterministic_uuid5_hex_prefix(f"bulk:{command_id}:{idx}", length=12)
                        instance_id = f"{class_id.lower()}_{suffix}"
                        inst_payload[expected_key] = instance_id

                validate_instance_id(instance_id)
                # Avoid injecting `{class}_id` into payloads that don't declare it (schema check would fail),
                # while still preserving the conventional id field when it's explicitly present.
                if expected_key in inst_payload:
                    current = inst_payload.get(expected_key)
                    if current is None or (isinstance(current, str) and not current.strip()):
                        inst_payload[expected_key] = instance_id
                elif instance_id_source_key == expected_key:
                    inst_payload[expected_key] = instance_id

                command_log = {
                    "command_id": str(command_id),
                    "command_type": "BULK_CREATE_INSTANCES",
                    "db_name": db_name,
                    "class_id": class_id,
                    "branch": branch,
                    "bulk_index": idx,
                    "bulk_total": total,
                    "created_by": created_by,
                    "metadata": command_meta,
                }

                await self._apply_create_instance_side_effects(
                    command_id=str(command_id),
                    db_name=db_name,
                    class_id=class_id,
                    branch=branch,
                    payload=inst_payload,
                    instance_id=instance_id,
                    command_log=command_log,
                    ontology_version=ontology_version,
                    created_by=created_by,
                    allow_pattern_fallback=not is_objectify,
                )

                created_count += 1
                if len(sample_instance_ids) < 5:
                    sample_instance_ids.append(instance_id)

            await self.set_command_status(
                command_id,
                "completed",
                {
                    "created": created_count,
                    "total": total,
                    "sample_instance_ids": sample_instance_ids,
                },
            )

        except Exception as e:
            logger.error(f"❌ Failed to bulk create instances: {e}")
            raise

    async def process_bulk_update_instances(self, command: Dict[str, Any]) -> None:
        """Process BULK_UPDATE_INSTANCES command (updates multiple instances)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        branch = validate_branch_name(command.get("branch") or "main")
        payload = command.get("payload", {}) or {}
        command_meta = command.get("metadata")
        if not isinstance(command_meta, dict):
            command_meta = {}

        await self.set_command_status(command_id, "processing")

        try:
            if not db_name:
                raise ValueError("db_name is required")
            if not class_id:
                raise ValueError("class_id is required")
            if not isinstance(payload, dict):
                raise ValueError("BULK_UPDATE_INSTANCES payload must be an object")

            raw_instances = payload.get("instances")
            if not isinstance(raw_instances, list):
                raise ValueError("BULK_UPDATE_INSTANCES payload.instances must be a list")

            if not raw_instances:
                await self.set_command_status(
                    command_id,
                    "completed",
                    {"updated": 0, "total": 0, "note": "No instances provided"},
                )
                return

            ontology_version = await self._stamp_ontology_version(
                command=command,
                db_name=db_name,
                branch=branch,
            )

            created_by = command.get("created_by") or "system"
            total = len(raw_instances)
            updated_count = 0
            sample_instance_ids: List[str] = []

            for idx, inst in enumerate(raw_instances):
                if not isinstance(inst, dict):
                    raise ValueError(f"instances[{idx}] must be an object")
                instance_id = inst.get("instance_id")
                if not instance_id:
                    raise ValueError(f"instances[{idx}].instance_id is required")
                instance_id = str(instance_id)
                validate_instance_id(instance_id)
                data = inst.get("data")
                if not isinstance(data, dict):
                    raise ValueError(f"instances[{idx}].data must be an object")

                child_command_id = deterministic_uuid5_str(f"bulk-update:{command_id}:{instance_id}:{idx}")
                update_command = {
                    "command_id": child_command_id,
                    "command_type": "UPDATE_INSTANCE",
                    "db_name": db_name,
                    "class_id": class_id,
                    "instance_id": instance_id,
                    "branch": branch,
                    "payload": data,
                    "metadata": {
                        **command_meta,
                        "bulk_parent_id": str(command_id),
                        "bulk_index": idx,
                        "bulk_total": total,
                        "created_by": created_by,
                        "ontology": ontology_version,
                    },
                    "created_by": created_by,
                }
                await self.process_update_instance(update_command, skip_status=True)
                updated_count += 1
                if len(sample_instance_ids) < 5:
                    sample_instance_ids.append(instance_id)

            await self.set_command_status(
                command_id,
                "completed",
                {
                    "updated": updated_count,
                    "total": total,
                    "sample_instance_ids": sample_instance_ids,
                },
            )
        except Exception as e:
            logger.error(f"❌ Failed to bulk update instances: {e}")
            raise

    async def process_update_instance(self, command: Dict[str, Any], *, skip_status: bool = False) -> None:
        """Process UPDATE_INSTANCE command (idempotent + ordered via registry claim)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        branch = validate_branch_name(command.get("branch") or "main")
        payload = command.get("payload", {}) or {}
        is_objectify = self._is_objectify_command(command)

        # Set command status early (202 already returned to user).
        if not skip_status:
            await self.set_command_status(command_id, "processing")

        if not db_name:
            raise ValueError("db_name is required")
        if not class_id:
            raise ValueError("class_id is required")

        # Stamp semantic contract version (ontology ref/commit) for reproducibility.
        ontology_version = await self._stamp_ontology_version(
            command=command,
            db_name=db_name,
            branch=branch,
        )

        instance_id = command.get("instance_id")
        if not instance_id:
            raise ValueError("instance_id is required for UPDATE_INSTANCE")
        instance_id = str(instance_id)
        validate_instance_id(instance_id)

        if not isinstance(payload, dict):
            raise ValueError("UPDATE_INSTANCE payload must be an object")

        expected_aggregate_id = f"{db_name}:{branch}:{class_id}:{instance_id}"
        if command.get("aggregate_id") and command.get("aggregate_id") != expected_aggregate_id:
            raise ValueError(
                f"aggregate_id mismatch: command.aggregate_id={command.get('aggregate_id')} "
                f"expected={expected_aggregate_id}"
            )

        primary_key_field = f"{class_id.lower()}_id"
        if (
            primary_key_field in payload
            and payload.get(primary_key_field) is not None
            and str(payload.get(primary_key_field)) != instance_id
        ):
            raise ValueError(
                f"instance_id mismatch: command.instance_id={instance_id} "
                f"payload[{primary_key_field}]={payload.get(primary_key_field)}"
            )

        aggregate_id = expected_aggregate_id

        # Rebuild "previous payload" from the Event Store so updates remain patch-based
        # without relying on Elasticsearch as a state store.
        #
        # Determinism requirement (idempotency): on retries of the same command_id,
        # we must compute the same merged payload, otherwise append_event(event_id=uuid5(command_id,...))
        # will detect a mismatch and raise.
        previous_payload: Dict[str, Any] = {}
        last_domain_was_delete = False
        has_existing_state = False
        event_store_error: Optional[Exception] = None

        # Version boundary: prefer expected_seq (OCC contract). Fallback to command sequence_number-1.
        to_version: Optional[int] = None
        raw_expected = command.get("expected_seq")
        if raw_expected is not None:
            try:
                to_version = int(raw_expected)
            except Exception as e:
                raise ValueError("expected_seq must be an integer") from e
            if to_version < 0:
                raise ValueError("expected_seq must be >= 0")
        else:
            raw_seq = command.get("sequence_number")
            if raw_seq is not None:
                try:
                    to_version = int(raw_seq) - 1
                except (TypeError, ValueError):
                    to_version = None

        if self.enable_event_sourcing and self.event_store:
            try:
                events = await self.event_store.get_events(
                    aggregate_type="Instance",
                    aggregate_id=aggregate_id,
                    to_version=to_version,
                )
                for ev in events:
                    kind = ev.metadata.get("kind") if isinstance(ev.metadata, dict) else None
                    if kind != "domain":
                        continue
                    if ev.event_type in {"INSTANCE_CREATED", "INSTANCE_UPDATED"} and isinstance(ev.data, dict):
                        previous_payload = {
                            k: v
                            for k, v in ev.data.items()
                            if k not in {"db_name", "branch", "class_id", "instance_id"}
                        }
                        last_domain_was_delete = False
                        has_existing_state = True
                    elif ev.event_type == "INSTANCE_DELETED":
                        previous_payload = {}
                        last_domain_was_delete = True
                        has_existing_state = True
            except Exception as e:
                event_store_error = e
                logger.warning(
                    f"Failed to rebuild previous state from Event Store (aggregate_id={aggregate_id}): {e}"
                )

        # Branch virtualization: for the *first* write on a non-base branch, the branch stream has no prior domain
        # state. For patch updates, we must rebuild the baseline payload from the base branch stream (typically `main`)
        # at the caller-provided expected_seq boundary.
        base_branch = get_settings().branch_virtualization.base_branch
        try:
            base_branch = validate_branch_name(base_branch)
        except (TypeError, ValueError):
            base_branch = "main"

        if (
            (not has_existing_state)
            and branch != base_branch
            and self.enable_event_sourcing
            and self.event_store
        ):
            if to_version is None:
                raise ValueError("expected_seq is required for branch patch updates")

            base_aggregate_id = f"{db_name}:{base_branch}:{class_id}:{instance_id}"
            try:
                base_events = await self.event_store.get_events(
                    aggregate_type="Instance",
                    aggregate_id=base_aggregate_id,
                    to_version=to_version,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Unable to rebuild baseline payload from base branch stream (base_aggregate_id={base_aggregate_id})"
                ) from e

            base_payload: Dict[str, Any] = {}
            base_deleted = False
            base_has_state = False
            for ev in base_events:
                kind = ev.metadata.get("kind") if isinstance(ev.metadata, dict) else None
                if kind != "domain":
                    continue
                if ev.event_type in {"INSTANCE_CREATED", "INSTANCE_UPDATED"} and isinstance(ev.data, dict):
                    base_payload = {
                        k: v
                        for k, v in ev.data.items()
                        if k not in {"db_name", "branch", "class_id", "instance_id"}
                    }
                    base_deleted = False
                    base_has_state = True
                elif ev.event_type == "INSTANCE_DELETED":
                    base_payload = {}
                    base_deleted = True
                    base_has_state = True

            if base_deleted:
                raise ValueError(f"Cannot update deleted instance (base_aggregate_id={base_aggregate_id})")
            if base_has_state:
                previous_payload = dict(base_payload)
                has_existing_state = True

        if last_domain_was_delete:
            raise ValueError(f"Cannot update deleted instance (aggregate_id={aggregate_id})")

        # Fallback (best-effort): use latest S3 snapshot if Event Store lookup failed or found no domain state.
        if (not has_existing_state) and self.s3_client:
            try:
                prefixes = [f"{db_name}/{branch}/{class_id}/{instance_id}/"]
                # Backward compatibility: older snapshots stored without branch segment for main.
                if branch == "main":
                    prefixes.append(f"{db_name}/{class_id}/{instance_id}/")

                objs = []
                for prefix in prefixes:
                    resp = await self._s3_call(
                        self.s3_client.list_objects_v2,
                        Bucket=self.instance_bucket,
                        Prefix=prefix,
                    )
                    objs.extend(list((resp or {}).get("Contents") or []))
                objs.sort(key=lambda o: o.get("LastModified") or datetime.fromtimestamp(0, tz=timezone.utc))
                snapshot_key = (objs[-1].get("Key") if objs else None)
                if snapshot_key:
                    obj = await self._s3_call(
                        self.s3_client.get_object,
                        Bucket=self.instance_bucket,
                        Key=snapshot_key,
                    )
                    body = obj.get("Body")
                    if body is None:
                        raise RuntimeError(f"S3 snapshot missing body for key={snapshot_key}")
                    raw = await self._s3_read_body(body)
                    doc = json.loads(raw.decode("utf-8"))
                    if isinstance(doc, dict):
                        if doc.get("deleted_at") or (
                            isinstance(obj.get("Metadata"), dict) and obj["Metadata"].get("tombstone") == "true"
                        ):
                            raise ValueError(f"Cannot update deleted instance (snapshot={snapshot_key})")
                        snap_payload = doc.get("payload")
                        if isinstance(snap_payload, dict):
                            previous_payload = dict(snap_payload)
                            has_existing_state = True
            except Exception as e:
                # Only fail hard if we also couldn't consult the Event Store; otherwise snapshot fallback is optional.
                if event_store_error is not None:
                    raise RuntimeError(
                        f"Unable to rebuild previous payload for patch update (aggregate_id={aggregate_id}): {e}"
                    ) from e
                logger.debug(f"Snapshot fallback failed (non-fatal): {e}")

        if not has_existing_state:
            raise ValueError(f"Cannot update missing instance (aggregate_id={aggregate_id})")

        merged_payload = {**previous_payload, **payload}

        # Save snapshot to S3 (optional durable blob store).
        s3_path = f"{db_name}/{branch}/{class_id}/{instance_id}/{command_id}.json"
        s3_data = {
            "command": command,
            "payload": merged_payload,
            "instance_id": instance_id,
            "class_id": class_id,
            "branch": branch,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            put_resp = await self._s3_call(
                self.s3_client.put_object,
                Bucket=self.instance_bucket,
                Key=s3_path,
                Body=json.dumps(s3_data, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "command_id": str(command_id) if command_id else "",
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "branch": branch,
                },
            )
            if command_id:
                etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                await self.observability.record_link(
                    from_node_id=LineageStore.node_event(str(command_id)),
                    to_node_id=LineageStore.node_artifact("s3", self.instance_bucket, s3_path),
                    edge_type=EDGE_EVENT_STORED_IN_OBJECT_STORE,
                    occurred_at=datetime.now(timezone.utc),
                    db_name=db_name,
                    to_label=f"s3://{self.instance_bucket}/{s3_path}",
                    edge_metadata={
                        "bucket": self.instance_bucket,
                        "key": s3_path,
                        "purpose": "instance_update_snapshot",
                        "db_name": db_name,
                        "etag": etag,
                        "version_id": version_id,
                        "ontology": ontology_version,
                    },
                )
                await self.observability.audit_log(
                    partition_key=f"db:{db_name}",
                    actor="instance_worker",
                    action="INSTANCE_S3_WRITE",
                    status="success",
                    resource_type="s3_object",
                    resource_id=f"s3://{self.instance_bucket}/{s3_path}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "bucket": self.instance_bucket,
                        "key": s3_path,
                        "etag": etag,
                        "version_id": version_id,
                        "ontology": ontology_version,
                    },
                )
        except Exception as e:
            if isinstance(e, (LineageRecordError, LineageUnavailableError)):
                raise
            # Blob snapshot is best-effort; continue (Event Store is SSoT for events).
            logger.warning(f"Failed to store update snapshot to S3 (continuing): {e}")
            if command_id:
                await self.observability.audit_log(
                    partition_key=f"db:{db_name}",
                    actor="instance_worker",
                    action="INSTANCE_S3_WRITE",
                    status="failure",
                    resource_type="s3_object",
                    resource_id=f"s3://{self.instance_bucket}/{s3_path}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "bucket": self.instance_bucket,
                        "key": s3_path,
                        "ontology": ontology_version,
                    },
                    error=str(e),
                )

        # Resolve relationships and persist the source-of-truth event before the direct ES write.
        relationships = await self.extract_relationships(
            db_name,
            class_id,
            merged_payload,
            branch=branch,
            allow_pattern_fallback=not is_objectify,
            strict_schema=self.strict_relationship_schema,
        )

        # Publish domain event (SSoT: Event Store)
        domain_event_id = (
            spice_event_id(command_id=str(command_id), event_type="INSTANCE_UPDATED", aggregate_id=aggregate_id)
            if command_id
            else str(uuid4())
        )
        envelope = EventEnvelope(
            event_id=domain_event_id,
            event_type="INSTANCE_UPDATED",
            aggregate_type="Instance",
            aggregate_id=aggregate_id,
            occurred_at=datetime.now(timezone.utc),
            actor=command.get("created_by") or "system",
            data={
                "db_name": db_name,
                "branch": branch,
                "class_id": class_id,
                "instance_id": instance_id,
                **merged_payload,
            },
            metadata={
                "kind": "domain",
                "kafka_topic": AppConfig.INSTANCE_EVENTS_TOPIC,
                "service": "instance_worker",
                "command_id": command_id,
                "ontology": ontology_version,
                "run_id": self.run_id,
                "code_sha": self.code_sha,
            },
        )

        if not self.event_store:
            raise RuntimeError("Event Store not initialized (ENABLE_EVENT_SOURCING requires Event Store)")
        await self.event_store.append_event(envelope)
        logger.info(f"  ✅ Stored INSTANCE_UPDATED in Event Store (seq={envelope.sequence_number})")

        event_sequence = int(envelope.sequence_number or 0)
        now_iso = datetime.now(timezone.utc).isoformat()
        index_name = await self._ensure_instances_index(db_name=db_name, branch=branch)

        # Preserve created_at from existing document
        created_at_override = None
        try:
            existing_doc = await self.elasticsearch_service.get_document(index=index_name, doc_id=instance_id)
            if existing_doc and isinstance(existing_doc, dict):
                source = existing_doc.get("_source") or existing_doc
                created_at_override = source.get("created_at")
        except Exception as exc:
            logger.warning(
                "Failed to fetch existing document for created_at override (index=%s, id=%s): %s",
                index_name,
                instance_id,
                exc,
                exc_info=True,
            )

        es_doc = self._build_es_document(
            db_name=db_name,
            branch=branch,
            class_id=class_id,
            instance_id=instance_id,
            payload=merged_payload,
            ontology_version=ontology_version,
            event_sequence=event_sequence,
            relationships=relationships,
            now_iso=now_iso,
            created_at_override=created_at_override,
            command_id=str(command_id) if command_id else None,
        )

        try:
            maybe_crash("instance_worker:before_es_index", logger=logger)
            await self.elasticsearch_service.index_document(
                index=index_name,
                document=es_doc,
                doc_id=instance_id,
            )
            maybe_crash("instance_worker:after_es_index", logger=logger)
            logger.info("  Updated instance in Elasticsearch: %s", instance_id)
            if command_id:
                await self.observability.record_link(
                    from_node_id=LineageStore.node_event(str(command_id)),
                    to_node_id=LineageStore.node_artifact("es", db_name, branch, instance_id),
                    edge_type=EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
                    occurred_at=datetime.now(timezone.utc),
                    db_name=db_name,
                    to_label=f"es:{db_name}:{branch}:{instance_id}",
                    edge_metadata={
                        "db_name": db_name,
                        "branch": branch,
                        "index_name": index_name,
                        "instance_id": instance_id,
                        "class_id": class_id,
                        "ontology": ontology_version,
                        "sequence_number": event_sequence,
                    },
                )
                await self.observability.audit_log(
                    partition_key=f"db:{db_name}",
                    actor="instance_worker",
                    action="INSTANCE_ES_WRITE",
                    status="success",
                    resource_type="elasticsearch_document",
                    resource_id=f"es:{index_name}/{instance_id}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "branch": branch,
                        "instance_id": instance_id,
                        "index_name": index_name,
                        "ontology": ontology_version,
                        "sequence_number": event_sequence,
                    },
                )
        except Exception as e:
            logger.warning("Could not update ES document: %s", e)
            if command_id:
                await self.observability.audit_log(
                    partition_key=f"db:{db_name}",
                    actor="instance_worker",
                    action="INSTANCE_ES_WRITE",
                    status="failure",
                    resource_type="elasticsearch_document",
                    resource_id=f"es:{index_name}/{instance_id}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "branch": branch,
                        "instance_id": instance_id,
                        "index_name": index_name,
                        "ontology": ontology_version,
                        "sequence_number": event_sequence,
                    },
                    error=str(e),
                )
            self._emit_write_path_contract(
                authoritative_write="instance_update_event",
                followups=[
                    followup_degraded(
                        "instance_es_materialization",
                        error=str(e),
                        details={"index": index_name, "instance_id": instance_id},
                    )
                ],
                level="warning",
            )
            raise

        if not skip_status:
            await self.set_command_status(
                command_id,
                "completed",
                {"instance_id": instance_id, "s3_uri": f"s3://{self.instance_bucket}/{s3_path}"},
            )

        await self._record_instance_edit(
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            edit_type="update",
            fields=sorted(payload.keys()),
            metadata={
                "command_id": command_id,
                "branch": branch,
                "actor": command.get("created_by") or "system",
                "ontology": ontology_version,
            },
        )

        if not is_objectify:
            try:
                await self._apply_relationship_object_link_edits(
                    db_name=db_name,
                    branch=branch,
                    class_id=class_id,
                    instance_id=instance_id,
                    current_payload=merged_payload,
                    previous_payload=previous_payload,
                )
                relationship_sync_status = followup_completed(
                    "relationship_object_link_sync",
                    details={"instance_id": instance_id},
                )
            except Exception as exc:
                logger.debug("Relationship object link sync failed: %s", exc, exc_info=True)
                relationship_sync_status = followup_degraded(
                    "relationship_object_link_sync",
                    error=str(exc),
                    details={"instance_id": instance_id},
                )

        self._emit_write_path_contract(
            authoritative_write="instance_update_event",
            followups=[
                followup_completed(
                    "instance_es_materialization",
                    details={"index": index_name, "instance_id": instance_id},
                ),
                followup_completed(
                    "instance_status_update",
                    details={"command_id": str(command_id)},
                )
                if not skip_status
                else followup_skipped(
                    "instance_status_update",
                    details={"reason": "skip_status"},
                ),
                followup_completed(
                    "instance_edit_log",
                    details={"instance_id": instance_id},
                ),
                relationship_sync_status,
            ],
        )

    async def process_delete_instance(self, command: Dict[str, Any]) -> None:
        """Process DELETE_INSTANCE command (idempotent delete)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        branch = validate_branch_name(command.get("branch") or "main")
        is_objectify = self._is_objectify_command(command)

        await self.set_command_status(command_id, "processing")

        if not db_name:
            raise ValueError("db_name is required")
        if not class_id:
            raise ValueError("class_id is required")

        # Stamp semantic contract version (ontology ref/commit) for reproducibility.
        ontology_version = await self._stamp_ontology_version(
            command=command,
            db_name=db_name,
            branch=branch,
        )

        instance_id = command.get("instance_id")
        if not instance_id:
            raise ValueError("instance_id is required for DELETE_INSTANCE")
        instance_id = str(instance_id)
        validate_instance_id(instance_id)

        expected_aggregate_id = f"{db_name}:{branch}:{class_id}:{instance_id}"
        if command.get("aggregate_id") and command.get("aggregate_id") != expected_aggregate_id:
            raise ValueError(
                f"aggregate_id mismatch: command.aggregate_id={command.get('aggregate_id')} "
                f"expected={expected_aggregate_id}"
            )

        aggregate_id = expected_aggregate_id
        domain_event_id = (
            spice_event_id(command_id=str(command_id), event_type="INSTANCE_DELETED", aggregate_id=aggregate_id)
            if command_id
            else str(uuid4())
        )
        envelope = EventEnvelope(
            event_id=domain_event_id,
            event_type="INSTANCE_DELETED",
            aggregate_type="Instance",
            aggregate_id=aggregate_id,
            occurred_at=datetime.now(timezone.utc),
            actor=command.get("created_by") or "system",
            data={"db_name": db_name, "branch": branch, "class_id": class_id, "instance_id": instance_id},
            metadata={
                "kind": "domain",
                "kafka_topic": AppConfig.INSTANCE_EVENTS_TOPIC,
                "service": "instance_worker",
                "command_id": command_id,
                "ontology": ontology_version,
                "objectify": bool(is_objectify),
                "run_id": self.run_id,
                "code_sha": self.code_sha,
            },
        )

        if not self.event_store:
            raise RuntimeError("Event Store not initialized (ENABLE_EVENT_SOURCING requires Event Store)")
        await self.event_store.append_event(envelope)
        logger.info(f"  ✅ Stored INSTANCE_DELETED in Event Store (seq={envelope.sequence_number})")

        index_name = await self._ensure_instances_index(db_name=db_name, branch=branch)
        es_deleted = False
        es_already_missing = False
        es_error: Optional[str] = None
        try:
            maybe_crash("instance_worker:before_es_delete", logger=logger)
            await self.elasticsearch_service.delete_document(index=index_name, doc_id=instance_id)
            maybe_crash("instance_worker:after_es_delete", logger=logger)
            logger.info("  Deleted instance from Elasticsearch: %s", instance_id)
            es_deleted = True
        except Exception as e:
            msg = str(e).lower()
            if "not found" in msg or "404" in msg or "notfounderror" in msg:
                logger.info("  ES document already deleted (idempotent): %s", instance_id)
                es_deleted = True
                es_already_missing = True
            else:
                logger.warning("Could not delete ES document: %s", e)
                es_error = str(e)

        if not es_deleted:
            self._emit_write_path_contract(
                authoritative_write="instance_delete_event",
                followups=[
                    followup_degraded(
                        "instance_es_delete",
                        error=es_error or "delete_failed",
                        details={"index": index_name, "instance_id": instance_id},
                    )
                ],
                level="warning",
            )
            raise RuntimeError(f"Failed to delete ES document {instance_id}: {es_error or 'unknown'}")

        if command_id and es_deleted:
            await self.observability.record_link(
                from_node_id=LineageStore.node_event(str(command_id)),
                to_node_id=LineageStore.node_artifact("es", db_name, branch, instance_id),
                edge_type=EDGE_EVENT_DELETED_ES_DOCUMENT,
                occurred_at=datetime.now(timezone.utc),
                db_name=db_name,
                to_label=f"es:{db_name}:{branch}:{instance_id}",
                edge_metadata={
                    "db_name": db_name,
                    "branch": branch,
                    "index_name": index_name,
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "ontology": ontology_version,
                    "sequence_number": int(envelope.sequence_number or 0),
                },
            )

        if command_id:
            await self.observability.audit_log(
                partition_key=f"db:{db_name}",
                actor="instance_worker",
                action="INSTANCE_ES_DELETE",
                status="success" if es_deleted else "failure",
                resource_type="elasticsearch_document",
                resource_id=f"es:{index_name}/{instance_id}",
                event_id=str(command_id),
                command_id=str(command_id),
                metadata={
                    "class_id": class_id,
                    "branch": branch,
                    "instance_id": instance_id,
                    "index_name": index_name,
                    "already_missing": es_already_missing,
                    "ontology": ontology_version,
                    "sequence_number": int(envelope.sequence_number or 0),
                },
                error=None if es_deleted else (es_error or "delete_failed"),
            )

        # Tombstone snapshot (best-effort)
        s3_path = f"{db_name}/{branch}/{class_id}/{instance_id}/{command_id}.json"
        s3_written = False
        tombstone_resp: Optional[Dict[str, Any]] = None
        try:
            tombstone_resp = await self._s3_call(
                self.s3_client.put_object,
                Bucket=self.instance_bucket,
                Key=s3_path,
                Body=json.dumps(
                    {
                        "command": command,
                        "instance_id": instance_id,
                        "class_id": class_id,
                        "branch": branch,
                        "deleted_at": datetime.now(timezone.utc).isoformat(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ).encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "command_id": str(command_id) if command_id else "",
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "branch": branch,
                    "tombstone": "true",
                },
            )
            s3_written = True
        except Exception as e:
            logger.warning(f"Failed to store delete tombstone to S3 (continuing): {e}")
            if command_id:
                await self.observability.audit_log(
                    partition_key=f"db:{db_name}",
                    actor="instance_worker",
                    action="INSTANCE_S3_TOMBSTONE_WRITE",
                    status="failure",
                    resource_type="s3_object",
                    resource_id=f"s3://{self.instance_bucket}/{s3_path}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "bucket": self.instance_bucket,
                        "key": s3_path,
                        "ontology": ontology_version,
                        "sequence_number": int(envelope.sequence_number or 0),
                    },
                    error=str(e),
                )
        self._emit_write_path_contract(
            authoritative_write="instance_delete_event",
            followups=[
                followup_completed(
                    "instance_es_delete",
                    details={
                        "index": index_name,
                        "instance_id": instance_id,
                        "already_missing": es_already_missing,
                    },
                ),
                followup_completed(
                    "instance_delete_audit",
                    details={"command_id": str(command_id) if command_id else None},
                ),
                followup_completed(
                    "instance_s3_tombstone",
                    details={"key": s3_path},
                )
                if s3_written
                else followup_degraded(
                    "instance_s3_tombstone",
                    error="tombstone_write_failed",
                    details={"key": s3_path},
                ),
            ],
        )

        if command_id and s3_written:
            etag = tombstone_resp.get("ETag") if isinstance(tombstone_resp, dict) else None
            version_id = tombstone_resp.get("VersionId") if isinstance(tombstone_resp, dict) else None
            await self.observability.record_link(
                from_node_id=LineageStore.node_event(str(command_id)),
                to_node_id=LineageStore.node_artifact("s3", self.instance_bucket, s3_path),
                edge_type=EDGE_EVENT_STORED_IN_OBJECT_STORE,
                occurred_at=datetime.now(timezone.utc),
                db_name=db_name,
                to_label=f"s3://{self.instance_bucket}/{s3_path}",
                edge_metadata={
                    "bucket": self.instance_bucket,
                    "key": s3_path,
                    "purpose": "instance_delete_tombstone",
                    "db_name": db_name,
                    "etag": etag,
                    "version_id": version_id,
                    "ontology": ontology_version,
                    "sequence_number": int(envelope.sequence_number or 0),
                },
            )
            await self.observability.audit_log(
                partition_key=f"db:{db_name}",
                actor="instance_worker",
                action="INSTANCE_S3_TOMBSTONE_WRITE",
                status="success",
                resource_type="s3_object",
                resource_id=f"s3://{self.instance_bucket}/{s3_path}",
                event_id=str(command_id),
                command_id=str(command_id),
                metadata={
                    "class_id": class_id,
                    "instance_id": instance_id,
                    "bucket": self.instance_bucket,
                    "key": s3_path,
                    "etag": etag,
                    "version_id": version_id,
                    "ontology": ontology_version,
                    "sequence_number": int(envelope.sequence_number or 0),
                },
            )

        await self.set_command_status(command_id, "completed", {"instance_id": instance_id})

        await self._record_instance_edit(
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            edit_type="delete",
            fields=[],
            metadata={
                "command_id": command_id,
                "branch": branch,
                "actor": command.get("created_by") or "system",
                "ontology": ontology_version,
            },
        )

        if not is_objectify:
            try:
                previous_payload = await self._resolve_instance_payload(
                    db_name=db_name,
                    branch=branch,
                    class_id=class_id,
                    instance_id=instance_id,
                )
                await self._apply_relationship_object_link_edits(
                    db_name=db_name,
                    branch=branch,
                    class_id=class_id,
                    instance_id=instance_id,
                    current_payload=None,
                    previous_payload=previous_payload,
                )
            except Exception as exc:
                logger.debug("Relationship object delete sync failed: %s", exc, exc_info=True)
            
    async def _record_instance_edit(
        self,
        *,
        db_name: str,
        class_id: str,
        instance_id: str,
        edit_type: str,
        fields: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.dataset_registry:
            return
        try:
            await self.dataset_registry.record_instance_edit(
                db_name=db_name,
                class_id=class_id,
                instance_id=instance_id,
                edit_type=str(edit_type or "").strip() or "update",
                fields=fields,
                metadata=metadata,
            )
        except Exception as exc:
            logger.debug("Instance edit tracking failed (non-fatal): %s", exc, exc_info=True)

    async def _resolve_instance_payload(
        self,
        *,
        db_name: str,
        branch: str,
        class_id: str,
        instance_id: str,
    ) -> Optional[Dict[str, Any]]:
        aggregate_id = f"{db_name}:{branch}:{class_id}:{instance_id}"
        payload: Optional[Dict[str, Any]] = None
        tombstoned = False
        if self.event_store:
            try:
                events = await self.event_store.get_events(
                    aggregate_type="Instance",
                    aggregate_id=aggregate_id,
                )
                for ev in events:
                    if ev.event_type == "INSTANCE_DELETED":
                        tombstoned = True
                        payload = None
                        continue
                    if ev.event_type in {"INSTANCE_CREATED", "INSTANCE_UPDATED"} and isinstance(ev.data, dict):
                        if tombstoned:
                            continue
                        payload = {
                            k: v
                            for k, v in ev.data.items()
                            if k not in {"db_name", "branch", "class_id", "instance_id"}
                        }
            except Exception as exc:
                logger.debug("Failed to resolve instance payload from event store: %s", exc, exc_info=True)

        if tombstoned:
            return None

        if payload is not None:
            return payload

        if self.s3_client:
            prefixes = [f"{db_name}/{branch}/{class_id}/{instance_id}/"]
            if branch == "main":
                prefixes.append(f"{db_name}/{class_id}/{instance_id}/")
            try:
                objs = []
                for prefix in prefixes:
                    resp = await self._s3_call(
                        self.s3_client.list_objects_v2,
                        Bucket=self.instance_bucket,
                        Prefix=prefix,
                    )
                    objs.extend(list((resp or {}).get("Contents") or []))
                objs.sort(key=lambda o: o.get("LastModified") or datetime.fromtimestamp(0, tz=timezone.utc))
                for obj in reversed(objs):
                    key = obj.get("Key")
                    if not key:
                        continue
                    resp = await self._s3_call(
                        self.s3_client.get_object,
                        Bucket=self.instance_bucket,
                        Key=key,
                    )
                    body = resp.get("Body")
                    if body is None:
                        continue
                    raw = await self._s3_read_body(body)
                    doc = json.loads(raw.decode("utf-8"))
                    if isinstance(doc, dict):
                        if doc.get("deleted_at") or (
                            isinstance(resp.get("Metadata"), dict) and resp["Metadata"].get("tombstone") == "true"
                        ):
                            continue
                        payload_doc = doc.get("payload")
                        if isinstance(payload_doc, dict):
                            return payload_doc
            except Exception as exc:
                logger.debug("Failed to resolve instance payload from S3: %s", exc, exc_info=True)

        return None

    async def _enqueue_link_reindex(self, *, db_name: str, link_type_id: str) -> None:
        if not self.dataset_registry or not self.objectify_registry:
            return
        try:
            build_result = await build_link_index_objectify_job(
                db_name=db_name,
                link_type_id=link_type_id,
                dataset_registry=self.dataset_registry,
                objectify_registry=self.objectify_registry,
            )
            if build_result.existing_job_id:
                logger.debug(
                    "Link reindex skipped: dedupe hit for link_type=%s existing_job_id=%s",
                    link_type_id,
                    build_result.existing_job_id,
                )
                return
            if not build_result.job:
                if build_result.skip_reason:
                    logger.debug(
                        "Link reindex skipped: %s for link_type=%s",
                        build_result.skip_reason,
                        link_type_id,
                    )
                return

            await self.objectify_registry.enqueue_objectify_job(job=build_result.job)
        except Exception as exc:
            logger.warning("Failed to enqueue link reindex for %s: %s", link_type_id, exc)

    async def _apply_relationship_object_link_edits(
        self,
        *,
        db_name: str,
        branch: str,
        class_id: str,
        instance_id: str,
        current_payload: Optional[Dict[str, Any]],
        previous_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.dataset_registry or not current_payload and not previous_payload:
            return
        try:
            specs = await self.dataset_registry.list_relationship_specs_by_relationship_object_type(
                db_name=db_name,
                relationship_object_type=class_id,
                status="ACTIVE",
                limit=1000,
            )
        except Exception as exc:
            logger.debug("Failed to list relationship specs for %s: %s", class_id, exc, exc_info=True)
            return
        if not specs:
            return
        reindex_targets: Set[str] = set()
        for spec in specs:
            spec_payload = spec.spec or {}
            source_key = str(spec_payload.get("source_key_column") or "").strip()
            target_key = str(spec_payload.get("target_key_column") or "").strip()
            if not source_key or not target_key:
                continue

            prev_source = None
            prev_target = None
            if isinstance(previous_payload, dict):
                prev_source = strip_to_none(previous_payload.get(source_key))
                prev_target = strip_to_none(previous_payload.get(target_key))
            curr_source = None
            curr_target = None
            if isinstance(current_payload, dict):
                curr_source = strip_to_none(current_payload.get(source_key))
                curr_target = strip_to_none(current_payload.get(target_key))

            edits: List[Dict[str, Any]] = []
            if prev_source and prev_target and (prev_source != curr_source or prev_target != curr_target):
                edits.append({"source": prev_source, "target": prev_target, "edit_type": "REMOVE"})
            if curr_source and curr_target and (prev_source != curr_source or prev_target != curr_target):
                edits.append({"source": curr_source, "target": curr_target, "edit_type": "ADD"})

            for edit in edits:
                try:
                    await self.dataset_registry.record_link_edit(
                        db_name=db_name,
                        link_type_id=spec.link_type_id,
                        branch=branch,
                        source_object_type=spec.source_object_type,
                        target_object_type=spec.target_object_type,
                        predicate=spec.predicate,
                        source_instance_id=str(edit["source"]),
                        target_instance_id=str(edit["target"]),
                        edit_type=edit["edit_type"],
                        status="ACTIVE",
                        metadata={
                            "reason": "relationship_object_sync",
                            "relationship_object_id": instance_id,
                            "relationship_object_type": class_id,
                        },
                    )
                    reindex_targets.add(spec.link_type_id)
                except Exception as exc:
                    logger.debug("Failed to record link edit: %s", exc, exc_info=True)
        for link_type_id in reindex_targets:
            await self._enqueue_link_reindex(db_name=db_name, link_type_id=link_type_id)

    async def set_command_status(self, command_id: str, status: str, result: Dict = None):
        """Set command status using CommandStatusService (preserves history + pubsub)."""
        if not command_id or not self.command_status_service:
            return

        status_norm = (status or "").strip().lower()
        try:
            # If OMS couldn't create initial status (Redis blip), create a minimal entry here.
            try:
                existing = await self.command_status_service.redis.get_command_status(str(command_id))
                if not existing:
                    await self.command_status_service.create_command_status(
                        command_id=str(command_id),
                        command_type="UNKNOWN",
                        aggregate_id=str(command_id),
                        payload={},
                        user_id=None,
                    )
            except Exception as status_err:
                logger.debug(
                    f"Failed to create minimal command status entry (continuing): {status_err}", exc_info=True
                )

            if status_norm == "processing":
                await self.command_status_service.start_processing(
                    command_id=str(command_id),
                    worker_id=f"instance-worker-{os.getpid()}",
                )
                return

            if status_norm == "retrying":
                error = None
                if isinstance(result, dict):
                    error = result.get("error")
                await self.command_status_service.update_status(
                    command_id=str(command_id),
                    status=CommandStatusEnum.RETRYING,
                    message="Retrying command after transient failure",
                    error=error or (str(result) if result else None),
                )
                return

            if status_norm in {"completed", "done"}:
                await self.command_status_service.complete_command(
                    command_id=str(command_id),
                    result=result or {},
                    message="Command completed",
                )
                return

            if status_norm == "failed":
                error = None
                if isinstance(result, dict):
                    error = result.get("error")
                await self.command_status_service.fail_command(
                    command_id=str(command_id),
                    error=error or (str(result) if result else "unknown error"),
                )
                return

            # Fallback: store as FAILED with details for observability.
            await self.command_status_service.fail_command(
                command_id=str(command_id),
                error=f"Unhandled status update: {status} ({result})",
            )
        except Exception as e:
            logger.warning(f"Failed to update command status for {command_id}: {e}")

    @staticmethod
    def _is_retryable_error_impl(exc: Exception) -> bool:
        msg = str(exc).lower()
        # ES transient failures (connection reset, timeout, 429 too-many-requests).
        if "connectionerror" in msg or "timeout" in msg or "429" in msg:
            return True
        # ES 409 version conflict can be transient under concurrent upserts.
        if "409" in msg and "conflict" in msg:
            return True
        return classify_retryable_with_profile(exc, INSTANCE_COMMAND_RETRY_PROFILE)

    @staticmethod
    def _is_elasticsearch_outage_error_message(message: str) -> bool:
        msg = str(message or "").lower()
        if not msg:
            return False
        if "unavailableshardsexception" in msg:
            return True
        if "elasticsearch" in msg and (
            "connectionerror" in msg
            or "cannot connect to host" in msg
            or "connection refused" in msg
            or "service unavailable" in msg
            or "503" in msg
        ):
            return True
        return False

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
        await self._publish_standard_dlq_record(
            producer=self.dlq_producer,
            msg=msg,
            worker=self.service_name,
            dlq_spec=self._dlq_spec,
            error=error,
            attempt_count=int(attempt_count),
            stage=stage,
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            tracing=self.tracing,
            metrics=self.metrics,
        )

    def _registry_key(self, payload: _InstanceCommandPayload) -> RegistryKey:  # type: ignore[override]
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
        except (TypeError, ValueError):
            seq_int = None

        command_type = command.get("command_type")
        command_type = str(getattr(command_type, "value", command_type) or "").strip()
        if command_type.startswith("BULK_"):
            seq_int = None
        if self._writeback_guard_blocks(command):
            seq_int = None

        return RegistryKey(event_id=registry_event_id, aggregate_id=aggregate_id, sequence_number=seq_int)

    async def _process_payload(self, payload: _InstanceCommandPayload) -> None:  # type: ignore[override]
        command = payload.command if isinstance(payload.command, dict) else {}
        command_type_raw = command.get("command_type")
        command_type = str(getattr(command_type_raw, "value", command_type_raw) or "").strip()
        command_id_raw = command.get("command_id")
        command_id = str(command_id_raw or "").strip() or None

        maybe_crash("instance_worker:after_claim", logger=logger)

        if self.redis_client and command_id:
            try:
                status_key = AppConfig.get_command_status_key(str(command_id))
                status_raw = await self.redis_client.get(status_key)
                if status_raw:
                    status_data = json.loads(status_raw)
                    if str(status_data.get("status", "")).upper() == "COMPLETED":
                        logger.info("Skipping already completed command %s", command_id)
                        return
            except Exception as exc:
                logger.warning("Failed to read command status for %s: %s", command_id, exc)

        if self._writeback_guard_blocks(command):
            await self.set_command_status(
                str(command_id or ""),
                "failed",
                {
                    "error": "writeback_enforced",
                    "message": (
                        "Direct CRUD instance writes are ingestion-only for writeback-enabled object types. "
                        "Use Actions for operational edits."
                    ),
                    "class_id": command.get("class_id"),
                },
            )
            return

        if command_type == "CREATE_INSTANCE":
            await self.process_create_instance(command)
        elif command_type == "BULK_CREATE_INSTANCES":
            await self.process_bulk_create_instances(command)
        elif command_type == "BULK_UPDATE_INSTANCES":
            await self.process_bulk_update_instances(command)
        elif command_type == "UPDATE_INSTANCE":
            await self.process_update_instance(command)
        elif command_type == "DELETE_INSTANCE":
            await self.process_delete_instance(command)
        else:
            raise ValueError(f"Unknown command type: {command_type}")

        maybe_crash("instance_worker:before_mark_done", logger=logger)

    def _span_name(self, *, payload: _InstanceCommandPayload) -> str:  # type: ignore[override]
        return "instance_worker.process_command"

    def _span_attributes(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: _InstanceCommandPayload,
        registry_key: RegistryKey,
    ) -> Dict[str, Any]:
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        command = payload.command if isinstance(payload.command, dict) else {}
        command_type_raw = command.get("command_type")
        command_type = str(getattr(command_type_raw, "value", command_type_raw) or "").strip()
        command_id = command.get("command_id")
        attrs.update(
            {
                "command.type": command_type or None,
                "command.id": str(command_id) if command_id is not None else None,
                "db.name": command.get("db_name"),
                "instance.class_id": command.get("class_id"),
                "instance.instance_id": command.get("instance_id"),
            }
        )
        return attrs

    def _metric_event_name(self, *, payload: _InstanceCommandPayload) -> Optional[str]:  # type: ignore[override]
        command = payload.command if isinstance(payload.command, dict) else {}
        command_type_raw = command.get("command_type")
        command_type = str(getattr(command_type_raw, "value", command_type_raw) or "").strip()
        return command_type or None

    @staticmethod
    def _is_retryable_error(  # type: ignore[override]
        exc: Exception, *, payload: Optional[_InstanceCommandPayload] = None
    ) -> bool:
        return StrictInstanceWorker._is_retryable_error_impl(exc)

    def _max_retries_for_error(  # type: ignore[override]
        self, exc: Exception, *, payload: _InstanceCommandPayload, error: str, retryable: bool
    ) -> int:
        msg = str(error or exc).lower()
        if "references_untyped_object" in msg or "no_unique_type_for_document" in msg:
            return max(int(self.max_retries), int(self.untyped_ref_max_retry_attempts))
        # ES node restart/rolling outage can exceed the default retry window.
        # Keep the retry window wide enough to bridge restart periods.
        if self._is_elasticsearch_outage_error_message(msg):
            return max(int(self.max_retries), 20)
        return int(self.max_retries)

    def _backoff_seconds_for_error(  # type: ignore[override]
        self,
        exc: Exception,
        *,
        payload: _InstanceCommandPayload,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> int:
        msg = str(error or exc).lower()
        cap = int(self.backoff_max)
        if "references_untyped_object" in msg or "no_unique_type_for_document" in msg:
            cap = max(1, int(self.untyped_ref_backoff_max_seconds))
        elif self._is_elasticsearch_outage_error_message(msg):
            # Avoid long head-of-line blocking during transient ES outages.
            cap = min(cap, 8)
        return min(cap, int(2 ** max(attempt_count - 1, 0)))

    async def _after_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: _InstanceCommandPayload,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        command = payload.command if isinstance(payload.command, dict) else {}
        command_id = str(command.get("command_id") or "").strip()
        if command_id:
            max_attempts = self._max_retries_for_error(Exception(error), payload=payload, error=error, retryable=retryable)
            try:
                await self.set_command_status(
                    command_id,
                    "retrying",
                    {"error": error, "attempt": attempt_count, "max_attempts": int(max_attempts)},
                )
            except Exception as exc:
                logger.warning(
                    "Failed to mark command as retrying (command_id=%s): %s",
                    command_id,
                    exc,
                    exc_info=True,
                )

    def _retry_log_context(  # type: ignore[override]
        self,
        *,
        payload: _InstanceCommandPayload,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> FailureLogContext:
        _ = payload, retryable
        return FailureLogContext(
            message="Retrying instance command in %ss (attempt %s): %s",
            args=(int(backoff_s), attempt_count, error),
        )

    async def _after_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: _InstanceCommandPayload,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        command = payload.command if isinstance(payload.command, dict) else {}
        command_id = str(command.get("command_id") or "").strip()
        if command_id:
            try:
                await self.set_command_status(command_id, "failed", {"error": error})
            except Exception as exc:
                logger.warning(
                    "Failed to mark command as failed (command_id=%s): %s",
                    command_id,
                    exc,
                    exc_info=True,
                )

    def _terminal_failure_log_context(  # type: ignore[override]
        self,
        *,
        payload: _InstanceCommandPayload,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> FailureLogContext:
        _ = payload
        return FailureLogContext(
            message="Instance command failed after %s attempts (retryable=%s): %s",
            args=(attempt_count, retryable, error),
        )

    async def run(self):
        """Main processing loop"""
        self.running = True
        logger.info("🚀 STRICT Instance Worker started (topic=%s)", AppConfig.INSTANCE_COMMANDS_TOPIC)
        await self.run_loop(
            poll_timeout=1.0,
            idle_sleep=None,
            unexpected_error_sleep=1.0,
            seek_on_error=False,
            post_seek_sleep=0.0,
        )
                
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down STRICT Instance Worker...")
        self.running = False

        async def _safe_close(component_name: str, obj: Any, *method_names: str) -> None:
            if obj is None:
                return
            for method_name in method_names:
                method = getattr(obj, method_name, None)
                if not callable(method):
                    continue
                try:
                    maybe_awaitable = method()
                    if inspect.isawaitable(maybe_awaitable):
                        await maybe_awaitable
                except Exception as exc:
                    logger.warning(
                        "Failed to close %s via %s: %s",
                        component_name,
                        method_name,
                        exc,
                        exc_info=True,
                    )
                return
        
        await self._close_consumer_runtime()
        await close_kafka_producer(
            producer=self.producer,
            timeout_s=5.0,
            warning_logger=logger,
            warning_message="Kafka producer flush failed during shutdown: %s",
        )
        self.producer = None
        self.dlq_producer = None
        await _safe_close("redis_client", self.redis_client, "aclose", "close")
        await _safe_close("elasticsearch_service", self.elasticsearch_service, "disconnect", "aclose", "close")
        await _safe_close("oms_http", self.oms_http, "aclose", "close")
        await _safe_close("processed_event_registry", self.processed_event_registry, "close")
        await _safe_close("dataset_registry", self.dataset_registry, "close")
        await _safe_close("objectify_registry", self.objectify_registry, "close")
            

async def main():
    """Main entry point"""
    worker = StrictInstanceWorker()
    
    try:
        await worker.initialize()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await worker.shutdown()
        

if __name__ == "__main__":
    asyncio.run(main())
