"""
STRICT Lightweight Instance Worker
경량 그래프 원칙을 100% 준수하는 구현

Kafka message contract:
- Commands arrive as EventEnvelope JSON (metadata.kind == "command")
- Domain events are appended to the S3/MinIO Event Store and relayed to Kafka

LIGHTWEIGHT GRAPH RULES:
1. Graph stores ONLY: @id, @type, primary_key + relationships
2. NO domain fields in graph (no name, price, description, etc.)
3. ALL domain data goes to ES and S3 only
4. Relationships are @id → @id references only
5. ES stores terminus_id for Federation lookup
"""

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set, List, Union
from uuid import uuid4

from confluent_kafka import Producer, TopicPartition
import boto3
import httpx
from botocore.exceptions import ClientError

from shared.config.app_config import AppConfig
from shared.services.storage.redis_service import create_redis_service
from shared.services.core.command_status_service import (
    CommandStatusService as CommandStatusTracker,
    CommandStatus as CommandStatusEnum,
)
from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import (
    attach_context_from_kafka,
    kafka_headers_from_current_context,
)
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.security.auth_utils import BFF_TOKEN_ENV_KEYS, get_expected_token
from shared.security.input_sanitizer import validate_branch_name, validate_class_id, validate_instance_id
from shared.services.kafka.processed_event_worker import RegistryKey, StrictHeartbeatKafkaWorker
from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
)
from shared.services.kafka.safe_consumer import SafeKafkaConsumer, create_safe_consumer
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.registries.lineage_store import LineageStore
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.chaos import maybe_crash
from shared.utils.executor_utils import call_in_executor
from shared.utils.ontology_version import normalize_ontology_version, resolve_ontology_version
from shared.utils.app_logger import configure_logging
from shared.utils.deterministic_ids import deterministic_uuid5_hex_prefix, deterministic_uuid5_str
from shared.utils.spice_event_ids import spice_event_id

# ULTRA CRITICAL: Import TerminusDB service
from oms.services.async_terminus import AsyncTerminusService
from oms.services.event_store import EventStore
from shared.models.config import ConnectionConfig

_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _InstanceCommandPayload:
    command: Dict[str, Any]
    envelope_metadata: Optional[Dict[str, Any]] = None


class _InstanceCommandParseError(ValueError):
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


class StrictInstanceWorker(StrictHeartbeatKafkaWorker[_InstanceCommandPayload, None]):
    """
    STRICT Lightweight Instance Worker
    Graph는 관계와 참조만, 데이터는 ES/S3에만

    Command input is consumed exclusively as EventEnvelope JSON.
    """
    
    def __init__(self):
        settings = get_settings()
        worker_cfg = settings.workers.instance

        self.running = False
        self.kafka_servers = settings.database.kafka_servers
        self.enable_event_sourcing = bool(settings.event_sourcing.enable_event_sourcing)
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.producer = None
        self.dlq_producer: Optional[Producer] = None
        self.dlq_topic = AppConfig.INSTANCE_COMMANDS_DLQ_TOPIC
        self.dlq_flush_timeout_seconds = float(worker_cfg.dlq_flush_timeout_seconds)
        self.max_retry_attempts = int(worker_cfg.max_retry_attempts)
        self.service_name = "instance-worker"
        self.handler = "instance_worker"
        self.max_retries = int(self.max_retry_attempts)
        self.backoff_base = 1
        self.backoff_max = 60
        self.untyped_ref_max_retry_attempts = int(worker_cfg.untyped_ref_max_retry_attempts)
        self.untyped_ref_backoff_max_seconds = float(worker_cfg.untyped_ref_backoff_max_seconds)
        self.redis_client = None
        self.command_status_service: Optional[CommandStatusTracker] = None
        self.s3_client = None
        self.terminus_service = None
        self.instance_bucket = AppConfig.INSTANCE_BUCKET

        # Durable idempotency (Postgres)
        self.enable_processed_event_registry = bool(settings.event_sourcing.enable_processed_event_registry)
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.event_store: Optional[EventStore] = None

        # First-class provenance/audit (fail-open by default)
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.enable_audit_logs = bool(settings.observability.enable_audit_logs)
        self.allow_pk_generation = bool(worker_cfg.allow_pk_generation)
        self.strict_relationship_schema = bool(worker_cfg.relationship_strict)
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.bff_http: Optional[httpx.AsyncClient] = None
        self._consumer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="instance-worker-kafka")
        self.tracing = get_tracing_service(self.service_name)
        self.metrics = get_metrics_collector(self.service_name)
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
        
        # Kafka Consumer (strong consistency: read_committed + rebalance-safe offsets)
        group_id = (AppConfig.INSTANCE_WORKER_GROUP or "instance-worker-group").strip()
        self.consumer = await self._consumer_call(
            create_safe_consumer,
            group_id,
            [AppConfig.INSTANCE_COMMANDS_TOPIC],
            "instance-worker",
            max_poll_interval_ms=300000,
            session_timeout_ms=45000,
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
        
        # TerminusDB
        connection_info = ConnectionConfig(
            server_url=settings.database.terminus_url.rstrip("/"),
            user=settings.database.terminus_user,
            account=settings.database.terminus_account,
            key=settings.database.terminus_password,
        )
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()

        # Durable processed-events registry (idempotency + ordering guard)
        self.processed_event_registry = await create_processed_event_registry()
        self.processed = self.processed_event_registry
        logger.info("✅ ProcessedEventRegistry connected (Postgres)")

        # Dataset registry for edit tracking (best-effort)
        try:
            self.dataset_registry = DatasetRegistry()
            await self.dataset_registry.initialize()
            logger.info("✅ DatasetRegistry connected (instance edits tracking)")
        except Exception as e:
            logger.warning(f"⚠️ DatasetRegistry unavailable (edits tracking disabled): {e}")
            self.dataset_registry = None

        token = get_expected_token(BFF_TOKEN_ENV_KEYS)
        headers: Dict[str, str] = {}
        if token:
            headers["X-Admin-Token"] = token
        else:
            logger.warning("No BFF auth token configured; link reindex calls may fail.")
        self.bff_http = httpx.AsyncClient(
            base_url=settings.services.bff_base_url,
            timeout=60.0,
            headers=headers,
        )

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
        
        logger.info("✅ STRICT Instance Worker initialized")

    async def _s3_call(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    async def _s3_read_body(self, body) -> bytes:
        return await asyncio.to_thread(body.read)

    async def _consumer_call(self, func, *args, **kwargs):
        return await call_in_executor(self._consumer_executor, func, *args, **kwargs)

    async def _poll_message(self, *, timeout: float) -> Any:
        if not self.consumer:
            return None
        return await self._consumer_call(self.consumer.poll, timeout=timeout)
        
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

        kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
        if kind != "command":
            raise ValueError(f"Unexpected envelope kind for command topic: {kind}")

        command = dict(envelope.data or {})
        if command.get("aggregate_id") and command.get("aggregate_id") != envelope.aggregate_id:
            raise ValueError(
                f"aggregate_id mismatch: command.aggregate_id={command.get('aggregate_id')} "
                f"envelope.aggregate_id={envelope.aggregate_id}"
            )
        command["aggregate_id"] = envelope.aggregate_id
        command.setdefault("event_id", envelope.event_id)
        command.setdefault("sequence_number", envelope.sequence_number)
        return command

    async def extract_payload_from_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return self._extract_payload_from_message(message)
    
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
        Extract ONLY relationship fields from payload
        Returns: {field_name: "<TargetClass>/<instance_id>" | ["<TargetClass>/<instance_id>", ...]}
        """
        relationships: Dict[str, Union[str, List[str]]] = {}
        known_relationship_fields: Set[str] = set()

        def _normalize_ref(ref: Any) -> str:
            if not isinstance(ref, str):
                raise ValueError(f"Relationship ref must be a string, got {type(ref).__name__}")

            candidate = ref.strip()
            if "/" not in candidate:
                raise ValueError(
                    f"Invalid relationship ref '{ref}'. Expected '<TargetClass>/<instance_id>'"
                )

            target_class, target_instance_id = candidate.split("/", 1)
            validate_class_id(target_class)
            validate_instance_id(target_instance_id)
            return f"{target_class}/{target_instance_id}"

        def _expects_many(cardinality: Optional[Any]) -> Optional[bool]:
            if cardinality is None:
                return None
            try:
                card = str(cardinality).strip()
            except Exception:
                return None
            if not card:
                return None
            return card.endswith(":n") or card.endswith(":m")

        try:
            branch = validate_branch_name(branch)
            # Get ontology to identify relationship fields + cardinality
            ontology = await self.terminus_service.get_ontology(db_name, class_id, branch=branch)
            
            if ontology:
                # OntologyResponse (pydantic) shape
                rel_list = getattr(ontology, "relationships", None)
                if isinstance(rel_list, list):
                    for rel in rel_list:
                        field_name = None
                        if isinstance(rel, dict):
                            field_name = rel.get("predicate") or rel.get("name")
                            cardinality = rel.get("cardinality")
                        else:
                            field_name = getattr(rel, "predicate", None) or getattr(rel, "name", None)
                            cardinality = getattr(rel, "cardinality", None)

                        if field_name:
                            known_relationship_fields.add(str(field_name))
                        if not field_name or field_name not in payload:
                            continue

                        value = payload[field_name]
                        wants_many = _expects_many(cardinality)

                        if wants_many is True:
                            items = value if isinstance(value, list) else [value]
                            normalized: List[str] = []
                            for item in items:
                                normalized.append(_normalize_ref(item))
                            # Deduplicate while preserving order (Set semantics)
                            seen: Set[str] = set()
                            unique = [v for v in normalized if not (v in seen or seen.add(v))]
                            relationships[str(field_name)] = unique
                            logger.info(f"  📎 Found relationship (many): {field_name} → {unique}")
                        else:
                            if isinstance(value, list):
                                if len(value) == 0:
                                    continue
                                if len(value) != 1:
                                    raise ValueError(
                                        f"Relationship '{field_name}' expects a single ref (cardinality={cardinality}), "
                                        f"got {len(value)} items"
                                    )
                                value = value[0]

                            normalized = _normalize_ref(value)
                            relationships[str(field_name)] = normalized
                            logger.info(f"  📎 Found relationship: {field_name} → {normalized}")

                # Dict (OMS-like) shape
                elif isinstance(ontology, dict):
                    # Check for relationships in OMS format (has 'relationships' key)
                    if "relationships" in ontology and isinstance(ontology.get("relationships"), list):
                        for rel in ontology["relationships"]:
                            if not isinstance(rel, dict):
                                continue
                            field_name = rel.get("predicate") or rel.get("name")
                            if field_name:
                                known_relationship_fields.add(str(field_name))
                            if not field_name or field_name not in payload:
                                continue
                            cardinality = rel.get("cardinality")
                            value = payload[field_name]
                            wants_many = _expects_many(cardinality)

                            if wants_many is True:
                                items = value if isinstance(value, list) else [value]
                                normalized: List[str] = []
                                for item in items:
                                    normalized.append(_normalize_ref(item))
                                seen: Set[str] = set()
                                unique = [v for v in normalized if not (v in seen or seen.add(v))]
                                relationships[str(field_name)] = unique
                                logger.info(f"  📎 Found relationship (many): {field_name} → {unique}")
                            else:
                                if isinstance(value, list):
                                    if len(value) == 0:
                                        continue
                                    if len(value) != 1:
                                        raise ValueError(
                                            f"Relationship '{field_name}' expects a single ref (cardinality={cardinality}), "
                                            f"got {len(value)} items"
                                        )
                                    value = value[0]

                                normalized = _normalize_ref(value)
                                relationships[str(field_name)] = normalized
                                logger.info(f"  📎 Found relationship: {field_name} → {normalized}")

                    # TerminusDB schema format (relationships as properties with @class)
                    else:
                        for key, value_def in ontology.items():
                            if isinstance(value_def, dict) and "@class" in value_def:
                                known_relationship_fields.add(str(key))
                            if isinstance(value_def, dict) and "@class" in value_def and key in payload:
                                value = payload[key]
                                wants_many = True if str(value_def.get("@type") or "").strip() == "Set" else None

                                if wants_many is True:
                                    items = value if isinstance(value, list) else [value]
                                    normalized: List[str] = []
                                    for item in items:
                                        normalized.append(_normalize_ref(item))
                                    seen: Set[str] = set()
                                    unique = [v for v in normalized if not (v in seen or seen.add(v))]
                                    relationships[str(key)] = unique
                                    logger.info(f"  📎 Found relationship (many): {key} → {unique}")
                                else:
                                    if isinstance(value, list):
                                        if len(value) == 0:
                                            continue
                                        if len(value) != 1:
                                            raise ValueError(
                                                f"Relationship '{key}' expects a single ref, got {len(value)} items"
                                            )
                                        value = value[0]

                                    normalized = _normalize_ref(value)
                                    relationships[str(key)] = normalized
                                    logger.info(f"  📎 Found relationship: {key} → {normalized}")
                            
        except Exception as e:
            logger.warning(f"Could not get ontology for relationship extraction: {e}")
            
        if strict_schema:
            unknown_fields: List[str] = []

            def _looks_like_ref(value: Any) -> bool:
                if isinstance(value, str):
                    return "/" in value
                if isinstance(value, list) and value:
                    return all(isinstance(item, str) and "/" in item for item in value)
                return False

            for key, value in payload.items():
                if key in known_relationship_fields or key in relationships:
                    continue
                if _looks_like_ref(value):
                    unknown_fields.append(str(key))
            if unknown_fields:
                raise ValueError(f"Unknown relationship fields: {sorted(set(unknown_fields))}")

        if not allow_pattern_fallback:
            return relationships

        # FALLBACK: Always check for common relationship patterns
        # This ensures relationships work even without schema
        for key, value in payload.items():
            if key in relationships:
                continue

            # Looks like an @id reference (string)
            if isinstance(value, str) and "/" in value:
                if any(pattern in key for pattern in ["_by", "_to", "_ref", "contains", "linked"]):
                    relationships[key] = _normalize_ref(value)
                    logger.info(f"  📎 Found relationship pattern: {key} → {relationships[key]}")
                continue

            # Looks like an @id reference (list)
            if isinstance(value, list) and value:
                if any(pattern in key for pattern in ["_by", "_to", "_ref", "contains", "linked"]):
                    normalized: List[str] = [_normalize_ref(item) for item in value]
                    seen: Set[str] = set()
                    unique = [v for v in normalized if not (v in seen or seen.add(v))]
                    relationships[key] = unique
                    logger.info(f"  📎 Found relationship pattern (many): {key} → {unique}")
                        
        return relationships

    async def extract_required_properties(
        self, db_name: str, class_id: str, *, branch: str = "main"
    ) -> List[str]:
        """
        Extract required property names from the class schema.

        We store a lightweight node in TerminusDB for graph traversal, but TerminusDB
        enforces schema-required fields. Include required scalar fields so inserts
        do not fail schema checks.
        """
        branch = validate_branch_name(branch)
        try:
            ontology = await self.terminus_service.get_ontology(db_name, class_id, branch=branch)
        except Exception as e:
            # Without schema we cannot safely satisfy required-field constraints.
            # Treat as retryable by failing fast (outer loop will apply backoff/retry).
            raise RuntimeError(
                f"Ontology fetch failed for required-field extraction ({db_name}:{branch}:{class_id}): {e}"
            ) from e

        if not ontology:
            # Covers: temporary Terminus unavailability, schema not yet applied, or missing class.
            # Failing fast prevents us from writing a schema-invalid document that then becomes a
            # non-retryable 400 and wedges the command permanently.
            raise RuntimeError(f"Ontology unavailable for required-field extraction ({db_name}:{branch}:{class_id})")

        required: List[str] = []

        # OntologyResponse (pydantic) shape
        props = getattr(ontology, "properties", None)
        if isinstance(props, list):
            for prop in props:
                name = getattr(prop, "name", None) or (prop.get("name") if isinstance(prop, dict) else None)
                is_required = (
                    bool(getattr(prop, "required", False))
                    if not isinstance(prop, dict)
                    else bool(prop.get("required"))
                )
                if name and is_required:
                    required.append(str(name))
            return required

        # Dict (OMS-like) shape
        if isinstance(ontology, dict):
            for prop in ontology.get("properties") or []:
                if not isinstance(prop, dict):
                    continue
                if prop.get("required") and prop.get("name"):
                    required.append(str(prop["name"]))

        return required

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
        if not self.terminus_service:
            raise RuntimeError("Terminus service not initialized")

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

        if self.lineage_store:
            try:
                etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                await self.lineage_store.record_link(
                    from_node_id=self.lineage_store.node_event(str(command_id)),
                    to_node_id=self.lineage_store.node_artifact("s3", self.instance_bucket, s3_path),
                    edge_type="event_wrote_s3_object",
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
            except Exception as e:
                logger.debug(f"Lineage record failed (non-fatal): {e}")

        if self.audit_store:
            try:
                etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                await self.audit_store.log(
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
                logger.debug(f"Audit record failed (non-fatal): {e}")

        # 2) Extract ONLY relationships for graph
        relationships = await self.extract_relationships(
            db_name,
            class_id,
            payload,
            branch=branch,
            allow_pattern_fallback=allow_pattern_fallback,
            strict_schema=self.strict_relationship_schema,
        )

        # 3) Store lightweight node in TerminusDB for graph traversal
        primary_key_value = instance_id
        terminus_id = f"{class_id}/{primary_key_value}"

        graph_node = {
            "@id": terminus_id,
            "@type": class_id,
        }

        # Do NOT blindly inject `{class}_id` into graph documents: user-defined schemas may use
        # a different primary-key field (e.g., `call_id`) or even a non-`*_id` field (e.g., `phone`).
        # Injecting unknown properties causes Terminus schema check failures.
        primary_key_field = f"{class_id.lower()}_id"
        if primary_key_field in payload:
            current = payload.get(primary_key_field)
            if current is None or (isinstance(current, str) and not current.strip()):
                graph_node[primary_key_field] = primary_key_value
            else:
                graph_node[primary_key_field] = current
        else:
            for key, value in payload.items():
                if not key.endswith("_id") or value is None:
                    continue
                value_str = str(value).strip()
                if not value_str:
                    continue
                graph_node[key] = value
                break

        for rel_field, rel_target in relationships.items():
            # Relationship fields must be *references* (not embedded documents).
            # TerminusDB link properties accept the "<Class>/<id>" string form; using
            # {"@id": "..."} can be interpreted as an inline doc and trigger schema
            # validation on the target type.
            if isinstance(rel_target, str):
                graph_node[rel_field] = rel_target
            elif isinstance(rel_target, list):
                graph_node[rel_field] = [item for item in rel_target if isinstance(item, str) and item.strip()]
            else:
                graph_node[rel_field] = rel_target

        for field in await self.extract_required_properties(db_name, class_id, branch=branch):
            if field in graph_node:
                continue
            if field in payload:
                graph_node[field] = payload[field]

        try:
            maybe_crash("instance_worker:before_terminus", logger=logger)
            await self.terminus_service.create_instance(
                db_name,
                class_id,
                graph_node,
                branch=branch,
            )
            maybe_crash("instance_worker:after_terminus", logger=logger)

            if self.lineage_store:
                try:
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, terminus_id),
                        edge_type="event_wrote_terminus_document",
                        occurred_at=datetime.now(timezone.utc),
                        db_name=db_name,
                        to_label=f"terminus:{db_name}:{branch}:{terminus_id}",
                        edge_metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "terminus_id": terminus_id,
                            "class_id": class_id,
                            "ontology": ontology_version,
                        },
                    )
                except Exception as e:
                    logger.debug(f"Lineage record failed (non-fatal): {e}")

            if self.audit_store:
                try:
                    await self.audit_store.log(
                        partition_key=f"db:{db_name}",
                        actor="instance_worker",
                        action="INSTANCE_TERMINUS_WRITE",
                        status="success",
                        resource_type="terminus_document",
                        resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={
                            "class_id": class_id,
                            "branch": branch,
                            "instance_id": instance_id,
                            "terminus_id": terminus_id,
                            "ontology": ontology_version,
                        },
                    )
                except Exception as e:
                    logger.debug(f"Audit record failed (non-fatal): {e}")
        except Exception as e:
            existing = None
            try:
                existing = await self.terminus_service.document_service.get_document(
                    db_name, terminus_id, graph_type="instance", branch=branch
                )
            except Exception:
                existing = None

            if existing:
                logger.info(f"✅ TerminusDB node already exists (idempotent create): {terminus_id}")
                await self.terminus_service.update_instance(
                    db_name,
                    class_id,
                    terminus_id,
                    graph_node,
                    branch=branch,
                )
            else:
                if self.audit_store:
                    try:
                        await self.audit_store.log(
                            partition_key=f"db:{db_name}",
                            actor="instance_worker",
                            action="INSTANCE_TERMINUS_WRITE",
                            status="failure",
                            resource_type="terminus_document",
                            resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                            event_id=str(command_id),
                            command_id=str(command_id),
                            metadata={
                                "class_id": class_id,
                                "branch": branch,
                                "instance_id": instance_id,
                                "terminus_id": terminus_id,
                                "ontology": ontology_version,
                            },
                            error=str(e),
                        )
                    except Exception as audit_err:
                        logger.debug(
                            f"Audit record failed while handling Terminus write failure (non-fatal): {audit_err}",
                            exc_info=True,
                        )
                raise

        # 4) Store/publish domain event (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka)
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

        if not self.event_store:
            raise RuntimeError("Event Store not initialized (ENABLE_EVENT_SOURCING requires Event Store)")
        await self.event_store.append_event(envelope)

        return {
            "instance_id": instance_id,
            "terminus_id": terminus_id,
            "s3_path": s3_path,
            "aggregate_id": aggregate_id,
            "domain_event_id": domain_event_id,
        }
        
    async def process_create_instance(self, command: Dict[str, Any]):
        """Process CREATE_INSTANCE command - strict lightweight mode."""
        db_name = command.get('db_name')
        class_id = command.get('class_id')
        command_id = command.get('command_id')
        branch = validate_branch_name(command.get("branch") or "main")
        payload = command.get('payload', {}) or {}
        is_objectify = self._is_objectify_command(command)

        # Set command status early (202 already returned to user).
        await self.set_command_status(command_id, 'processing')

        try:
            if not isinstance(payload, dict):
                raise ValueError("CREATE_INSTANCE payload must be an object")

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
                if command.get("aggregate_id") and command.get("aggregate_id") != expected_aggregate_id:
                    raise ValueError(
                        f"aggregate_id mismatch: command.aggregate_id={command.get('aggregate_id')} "
                        f"expected={expected_aggregate_id}"
                    )
            else:
                # Extract primary key value dynamically
                instance_id = self.get_primary_key_value(
                    class_id, payload, allow_generate=self.allow_pk_generation and not is_objectify
                )
                validate_instance_id(instance_id)

            # Ensure downstream uses the resolved instance_id consistently
            command["instance_id"] = instance_id
            primary_key_value = instance_id

            if not db_name:
                raise ValueError("db_name is required")
            if not class_id:
                raise ValueError("class_id is required")

            # Stamp semantic contract version (ontology ref/commit) for reproducibility.
            ontology_version = await resolve_ontology_version(
                self.terminus_service, db_name=db_name, branch=branch, logger=logger
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
                ontology_version = merged
            else:
                command_meta["ontology"] = dict(ontology_version)

            logger.info(f"🔷 STRICT: Creating {class_id}/{instance_id}")

            # 1. Save FULL data to S3 (Event Store)
            s3_path = f"{db_name}/{branch}/{class_id}/{instance_id}/{command_id}.json"
            s3_data = {
                'command': command,
                'payload': payload,
                'instance_id': instance_id,
                'class_id': class_id,
                'branch': branch,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            put_resp = await self._s3_call(
                self.s3_client.put_object,
                Bucket=self.instance_bucket,
                Key=s3_path,
                Body=json.dumps(s3_data, indent=2),
                ContentType='application/json',
                Metadata={
                    'command_id': command_id,
                    'instance_id': instance_id,
                    'class_id': class_id
                }
            )
            logger.info(f"  ✅ Saved to S3: {s3_path}")

            # Provenance/Audit: command event -> instance command object
            if self.lineage_store:
                try:
                    etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                    version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("s3", self.instance_bucket, s3_path),
                        edge_type="event_wrote_s3_object",
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
                except Exception as e:
                    logger.debug(f"Lineage record failed (non-fatal): {e}")

            if self.audit_store:
                try:
                    etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                    version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                    await self.audit_store.log(
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
                    logger.debug(f"Audit record failed (non-fatal): {e}")
            
            # 2. Extract ONLY relationships for graph
            relationships = await self.extract_relationships(
                db_name,
                class_id,
                payload,
                branch=branch,
                allow_pattern_fallback=not is_objectify,
                strict_schema=self.strict_relationship_schema,
            )
            
            # 3. Create PURE lightweight node for TerminusDB (NO system fields!)
            graph_node = {
                "@id": f"{class_id}/{primary_key_value}",
                "@type": class_id,
            }
            
            # Add the primary key field dynamically
            primary_key_field = f"{class_id.lower()}_id"
            graph_node[primary_key_field] = payload.get(primary_key_field, primary_key_value)
            
            # Add ONLY relationships (no domain fields!)
            for rel_field, rel_target in relationships.items():
                graph_node[rel_field] = rel_target

            # Include required scalar properties to satisfy TerminusDB schema checks.
            for field in await self.extract_required_properties(db_name, class_id, branch=branch):
                if field in graph_node:
                    continue
                if field in payload:
                    graph_node[field] = payload[field]
                
            # Lightweight graph principle: NO domain attributes in TerminusDB!
            # TerminusDB stores ONLY lightweight nodes (IDs + relationships)
            # All domain data goes to Elasticsearch
            
            # Stable graph identifier used across TerminusDB + ES federation.
            terminus_id = f"{class_id}/{primary_key_value}"

            # Store lightweight node in TerminusDB for graph traversal
            try:
                # Create instance using TerminusDB service
                # This will store ONLY the lightweight node with relationships
                # Note: The instance_id is already part of graph_node["@id"]
                maybe_crash("instance_worker:before_terminus", logger=logger)
                await self.terminus_service.create_instance(
                    db_name,
                    class_id,
                    graph_node,  # Contains only @id, @type, relationships
                    branch=branch,
                )
                maybe_crash("instance_worker:after_terminus", logger=logger)
                logger.info("  ✅ Stored lightweight node in TerminusDB")
                logger.info(f"  📊 Relationships: {list(relationships.keys())}")

                if self.lineage_store:
                    try:
                        await self.lineage_store.record_link(
                            from_node_id=self.lineage_store.node_event(str(command_id)),
                            to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, terminus_id),
                            edge_type="event_wrote_terminus_document",
                            occurred_at=datetime.now(timezone.utc),
                            db_name=db_name,
                            to_label=f"terminus:{db_name}:{branch}:{terminus_id}",
                            edge_metadata={
                                "db_name": db_name,
                                "branch": branch,
                                "terminus_id": terminus_id,
                                "class_id": class_id,
                                "ontology": ontology_version,
                            },
                        )
                    except Exception as e:
                        logger.debug(f"Lineage record failed (non-fatal): {e}")

                if self.audit_store:
                    try:
                        await self.audit_store.log(
                            partition_key=f"db:{db_name}",
                            actor="instance_worker",
                            action="INSTANCE_TERMINUS_WRITE",
                            status="success",
                            resource_type="terminus_document",
                            resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                            event_id=str(command_id),
                            command_id=str(command_id),
                            metadata={
                                "class_id": class_id,
                                "branch": branch,
                                "instance_id": instance_id,
                                "terminus_id": terminus_id,
                                "ontology": ontology_version,
                            },
                        )
                    except Exception as e:
                        logger.debug(f"Audit record failed (non-fatal): {e}")
            except Exception as e:
                existing = None
                try:
                    existing = await self.terminus_service.document_service.get_document(
                        db_name, terminus_id, graph_type="instance", branch=branch
                    )
                except Exception:
                    existing = None

                if existing:
                    logger.info(
                        f"  ✅ TerminusDB node already exists (idempotent create): {terminus_id}"
                    )
                else:
                    logger.warning(f"  ⚠️ Could not store in TerminusDB: {e}")
                    if self.audit_store:
                        try:
                            await self.audit_store.log(
                                partition_key=f"db:{db_name}",
                                actor="instance_worker",
                                action="INSTANCE_TERMINUS_WRITE",
                                status="failure",
                                resource_type="terminus_document",
                                resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                                event_id=str(command_id),
                                command_id=str(command_id),
                                metadata={
                                    "class_id": class_id,
                                    "branch": branch,
                                    "instance_id": instance_id,
                                    "terminus_id": terminus_id,
                                    "ontology": ontology_version,
                                },
                                error=str(e),
                            )
                        except Exception as audit_err:
                            logger.debug(
                                f"Audit record failed while handling Terminus write failure (non-fatal): {audit_err}",
                                exc_info=True,
                            )
                    # TerminusDB is the graph authority; if we cannot write it, we must retry.
                    raise
            
            # 4. Store/publish domain event (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka)
            
            # NOTE: Elasticsearch indexing is handled by projection_worker (single writer) for schema consistency.
            event_payload = {
                "db_name": db_name,
                "branch": branch,
                "class_id": class_id,
                "instance_id": instance_id,
                **payload,  # Include full payload in event
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
                actor=command.get("created_by") or "system",
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

            if not self.event_store:
                raise RuntimeError("Event Store not initialized (ENABLE_EVENT_SOURCING requires Event Store)")
            await self.event_store.append_event(envelope)
            logger.info(f"  ✅ Stored INSTANCE_CREATED in Event Store (seq={envelope.sequence_number})")
            
            # Set success status
            await self.set_command_status(command_id, 'completed', {
                'instance_id': instance_id,
                'es_doc_id': instance_id,
                's3_uri': f"s3://{self.instance_bucket}/{s3_path}"
            })

            await self._record_instance_edit(
                db_name=db_name,
                class_id=class_id,
                instance_id=instance_id,
                edit_type="create",
                fields=sorted(payload.keys()),
                metadata={
                    "command_id": command_id,
                    "branch": branch,
                    "actor": command.get("created_by") or "system",
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
            await self.set_command_status(command_id, 'failed', {'error': str(e)})
            raise

    async def process_bulk_create_instances(self, command: Dict[str, Any]) -> None:
        """Process BULK_CREATE_INSTANCES command (idempotent per event_id; no sequence-guard)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        branch = validate_branch_name(command.get("branch") or "main")
        is_objectify = self._is_objectify_command(command)
        payload = command.get("payload", {}) or {}
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
            ontology_version = await resolve_ontology_version(
                self.terminus_service, db_name=db_name, branch=branch, logger=logger
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
                ontology_version = merged
            else:
                command_meta["ontology"] = dict(ontology_version)

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
                        except Exception:
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
            await self.set_command_status(command_id, "failed", {"error": str(e)})
            raise

    async def process_bulk_update_instances(self, command: Dict[str, Any]) -> None:
        """Process BULK_UPDATE_INSTANCES command (updates multiple instances)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        branch = validate_branch_name(command.get("branch") or "main")
        payload = command.get("payload", {}) or {}

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

            ontology_version = await resolve_ontology_version(
                self.terminus_service, db_name=db_name, branch=branch, logger=logger
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
                ontology_version = merged
            else:
                command_meta["ontology"] = dict(ontology_version)

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
            await self.set_command_status(command_id, "failed", {"error": str(e)})
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
        ontology_version = await resolve_ontology_version(
            self.terminus_service, db_name=db_name, branch=branch, logger=logger
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
            ontology_version = merged
        else:
            command_meta["ontology"] = dict(ontology_version)

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
                except Exception:
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
        except Exception:
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
            if command_id and self.lineage_store:
                try:
                    etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                    version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("s3", self.instance_bucket, s3_path),
                        edge_type="event_wrote_s3_object",
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
                except Exception as e:
                    logger.debug(f"Lineage record failed (non-fatal): {e}")
            if command_id and self.audit_store:
                try:
                    etag = put_resp.get("ETag") if isinstance(put_resp, dict) else None
                    version_id = put_resp.get("VersionId") if isinstance(put_resp, dict) else None
                    await self.audit_store.log(
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
                    logger.debug(f"Audit record failed (non-fatal): {e}")
        except Exception as e:
            # Blob snapshot is best-effort; continue (Event Store is SSoT for events).
            logger.warning(f"Failed to store update snapshot to S3 (continuing): {e}")
            if command_id and self.audit_store:
                try:
                    await self.audit_store.log(
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
                except Exception as audit_err:
                    logger.debug(f"Audit record failed (non-fatal): {audit_err}", exc_info=True)

        # Update lightweight node (relationships + required scalar fields)
        relationships = await self.extract_relationships(
            db_name,
            class_id,
            merged_payload,
            branch=branch,
            allow_pattern_fallback=not is_objectify,
            strict_schema=self.strict_relationship_schema,
        )
        terminus_id = f"{class_id}/{instance_id}"
        graph_node: Dict[str, Any] = {
            "@id": terminus_id,
            "@type": class_id,
            primary_key_field: merged_payload.get(primary_key_field, instance_id),
        }
        for rel_field, rel_target in relationships.items():
            graph_node[rel_field] = rel_target
        for field in await self.extract_required_properties(db_name, class_id, branch=branch):
            if field in graph_node:
                continue
            if field in merged_payload:
                graph_node[field] = merged_payload[field]

        try:
            maybe_crash("instance_worker:before_terminus", logger=logger)
            await self.terminus_service.update_instance(db_name, class_id, terminus_id, graph_node, branch=branch)
            maybe_crash("instance_worker:after_terminus", logger=logger)
            logger.info("  ✅ Updated lightweight node in TerminusDB")
            if command_id and self.lineage_store:
                try:
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, terminus_id),
                        edge_type="event_wrote_terminus_document",
                        occurred_at=datetime.now(timezone.utc),
                        db_name=db_name,
                        to_label=f"terminus:{db_name}:{branch}:{terminus_id}",
                        edge_metadata={
                            "db_name": db_name,
                            "branch": branch,
                            "terminus_id": terminus_id,
                            "class_id": class_id,
                            "ontology": ontology_version,
                        },
                    )
                except Exception as e:
                    logger.debug(f"Lineage record failed (non-fatal): {e}")
            if command_id and self.audit_store:
                try:
                    await self.audit_store.log(
                        partition_key=f"db:{db_name}",
                        actor="instance_worker",
                        action="INSTANCE_TERMINUS_WRITE",
                        status="success",
                        resource_type="terminus_document",
                        resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={
                            "class_id": class_id,
                            "branch": branch,
                            "instance_id": instance_id,
                            "terminus_id": terminus_id,
                            "ontology": ontology_version,
                        },
                    )
                except Exception as e:
                    logger.debug(f"Audit record failed (non-fatal): {e}")
        except Exception as e:
            logger.warning(f"  ⚠️ Could not update TerminusDB node: {e}")
            if command_id and self.audit_store:
                try:
                    await self.audit_store.log(
                        partition_key=f"db:{db_name}",
                        actor="instance_worker",
                        action="INSTANCE_TERMINUS_WRITE",
                        status="failure",
                        resource_type="terminus_document",
                        resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={
                            "class_id": class_id,
                            "branch": branch,
                            "instance_id": instance_id,
                            "terminus_id": terminus_id,
                            "ontology": ontology_version,
                        },
                        error=str(e),
                    )
                except Exception as audit_err:
                    logger.debug(f"Audit record failed (non-fatal): {audit_err}", exc_info=True)
            # TerminusDB is the graph authority; retry until it is updated.
            raise

        # Elasticsearch indexing is handled by projection_worker (single writer) for schema consistency.

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
            except Exception as exc:
                logger.debug("Relationship object link sync failed: %s", exc, exc_info=True)

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
        ontology_version = await resolve_ontology_version(
            self.terminus_service, db_name=db_name, branch=branch, logger=logger
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
            ontology_version = merged
        else:
            command_meta["ontology"] = dict(ontology_version)

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

        terminus_id = f"{class_id}/{instance_id}"
        terminus_deleted = False
        terminus_already_missing = False
        terminus_error: Optional[str] = None
        try:
            maybe_crash("instance_worker:before_terminus", logger=logger)
            await self.terminus_service.delete_instance(db_name, class_id, terminus_id, branch=branch)
            maybe_crash("instance_worker:after_terminus", logger=logger)
            logger.info("  ✅ Deleted lightweight node from TerminusDB")
            terminus_deleted = True
        except Exception as e:
            msg = str(e).lower()
            if "not found" in msg or "404" in msg:
                logger.info("  ✅ TerminusDB node already deleted (idempotent)")
                terminus_deleted = True
                terminus_already_missing = True
            else:
                logger.warning(f"  ⚠️ Could not delete TerminusDB node: {e}")
                terminus_error = str(e)

        if not terminus_deleted:
            # TerminusDB is the graph authority; never succeed the command if delete could not be applied.
            raise RuntimeError(f"Failed to delete TerminusDB node {terminus_id}: {terminus_error or 'unknown'}")

        if command_id and terminus_deleted and self.lineage_store:
            try:
                await self.lineage_store.record_link(
                    from_node_id=self.lineage_store.node_event(str(command_id)),
                    to_node_id=self.lineage_store.node_artifact("terminus", db_name, branch, terminus_id),
                    edge_type="event_deleted_terminus_document",
                    occurred_at=datetime.now(timezone.utc),
                    db_name=db_name,
                    to_label=f"terminus:{db_name}:{branch}:{terminus_id}",
                    edge_metadata={
                        "db_name": db_name,
                        "branch": branch,
                        "terminus_id": terminus_id,
                        "class_id": class_id,
                        "ontology": ontology_version,
                    },
                )
            except Exception as e:
                logger.debug(f"Lineage record failed (non-fatal): {e}")

        if command_id and self.audit_store:
            try:
                await self.audit_store.log(
                    partition_key=f"db:{db_name}",
                    actor="instance_worker",
                    action="INSTANCE_TERMINUS_DELETE",
                    status="success" if terminus_deleted else "failure",
                    resource_type="terminus_document",
                    resource_id=f"terminus:{db_name}:{branch}:{terminus_id}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "branch": branch,
                        "instance_id": instance_id,
                        "terminus_id": terminus_id,
                        "already_missing": terminus_already_missing,
                        "ontology": ontology_version,
                    },
                    error=None if terminus_deleted else (terminus_error or "delete_failed"),
                )
            except Exception as e:
                logger.debug(f"Audit record failed (non-fatal): {e}")

        # Elasticsearch deletion is handled by projection_worker (single writer) for schema consistency.

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
            if command_id and self.audit_store:
                try:
                    await self.audit_store.log(
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
                        },
                        error=str(e),
                    )
                except Exception as audit_err:
                    logger.debug(f"Audit record failed (non-fatal): {audit_err}", exc_info=True)

        if command_id and s3_written and self.lineage_store:
            try:
                etag = tombstone_resp.get("ETag") if isinstance(tombstone_resp, dict) else None
                version_id = tombstone_resp.get("VersionId") if isinstance(tombstone_resp, dict) else None
                await self.lineage_store.record_link(
                    from_node_id=self.lineage_store.node_event(str(command_id)),
                    to_node_id=self.lineage_store.node_artifact("s3", self.instance_bucket, s3_path),
                    edge_type="event_wrote_s3_object",
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
                    },
                )
            except Exception as e:
                logger.debug(f"Lineage record failed (non-fatal): {e}")

        if command_id and s3_written and self.audit_store:
            try:
                etag = tombstone_resp.get("ETag") if isinstance(tombstone_resp, dict) else None
                version_id = tombstone_resp.get("VersionId") if isinstance(tombstone_resp, dict) else None
                await self.audit_store.log(
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
                    },
                )
            except Exception as e:
                logger.debug(f"Audit record failed (non-fatal): {e}")

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
        if self.event_store:
            try:
                events = await self.event_store.get_events(
                    aggregate_type="Instance",
                    aggregate_id=aggregate_id,
                )
                for ev in events:
                    if ev.event_type in {"INSTANCE_CREATED", "INSTANCE_UPDATED"} and isinstance(ev.data, dict):
                        payload = {
                            k: v
                            for k, v in ev.data.items()
                            if k not in {"db_name", "branch", "class_id", "instance_id"}
                        }
            except Exception as exc:
                logger.debug("Failed to resolve instance payload from event store: %s", exc, exc_info=True)

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
        if not self.bff_http:
            return
        try:
            resp = await self.bff_http.post(
                f"/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex"
            )
            resp.raise_for_status()
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

            def _normalize_edit_value(value: Any) -> Optional[str]:
                if value is None:
                    return None
                text = str(value).strip()
                return text or None

            prev_source = None
            prev_target = None
            if isinstance(previous_payload, dict):
                prev_source = _normalize_edit_value(previous_payload.get(source_key))
                prev_target = _normalize_edit_value(previous_payload.get(target_key))
            curr_source = None
            curr_target = None
            if isinstance(current_payload, dict):
                curr_source = _normalize_edit_value(current_payload.get(source_key))
                curr_target = _normalize_edit_value(current_payload.get(target_key))

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
        # TerminusDB schema failures can be transient under at-least-once + cross-aggregate reordering.
        # Example: Product references Customer that hasn't been created yet.
        if "references_untyped_object" in msg or "no_unique_type_for_document" in msg:
            return True
        non_retryable_markers = [
            "aggregate_id mismatch",
            "instance_id mismatch",
            "payload must be",
            "db_name is required",
            "class_id is required",
            "instance_id is required",
            "unknown command type",
            "security violation",
            "invalid",
            "bad request",
        ]
        # Default: Terminus 400s are treated as non-retryable unless we explicitly
        # recognize them as transient above.
        if "api error: 400" in msg:
            return False
        return not any(marker in msg for marker in non_retryable_markers)

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
            "worker": "instance-worker",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if payload_obj is not None:
            dlq_message["parsed_payload"] = payload_obj

        key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}".encode("utf-8")
        value = json.dumps(dlq_message, ensure_ascii=False, default=str).encode("utf-8")

        with attach_context_from_kafka(
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata if isinstance(fallback_metadata, dict) else None,
            service_name="instance-worker",
        ):
            headers = kafka_headers_from_current_context()
            with self.tracing.span(
                "instance_worker.dlq_produce",
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
            self.metrics.record_event("INSTANCE_COMMAND_DLQ", action="published")
        except Exception:
            pass

    # --- ProcessedEventKafkaWorker hooks ---
    def _parse_payload(self, payload: Any) -> _InstanceCommandPayload:  # type: ignore[override]
        if not isinstance(payload, (bytes, bytearray)):
            raise _InstanceCommandParseError(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=TypeError("Kafka payload must be bytes"),
            )
        try:
            raw_text = payload.decode("utf-8")
        except Exception as exc:
            raise _InstanceCommandParseError(
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
            raise _InstanceCommandParseError(
                stage="parse_json",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=exc,
            ) from exc

        fallback_metadata = raw_message.get("metadata") if isinstance(raw_message, dict) else None
        fallback_metadata = fallback_metadata if isinstance(fallback_metadata, dict) else None

        try:
            command = self._extract_payload_from_message(raw_message if isinstance(raw_message, dict) else {})
        except Exception as exc:
            payload_obj = raw_message if isinstance(raw_message, dict) else None
            raise _InstanceCommandParseError(
                stage="validate_envelope",
                payload_text=raw_text,
                payload_obj=payload_obj,
                fallback_metadata=fallback_metadata,
                cause=exc,
            ) from exc

        return _InstanceCommandPayload(command=command, envelope_metadata=fallback_metadata)

    def _fallback_metadata(self, payload: _InstanceCommandPayload) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        return payload.envelope_metadata

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
        except Exception:
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
        return min(cap, int(2 ** max(attempt_count - 1, 0)))

    async def _commit(self, msg: Any) -> None:  # type: ignore[override]
        if not self.consumer:
            return
        await self._consumer_call(self.consumer.commit, msg, asynchronous=False)

    async def _seek(self, *, topic: str, partition: int, offset: int) -> None:  # type: ignore[override]
        if not self.consumer:
            return
        await self._consumer_call(self.consumer.seek, TopicPartition(topic, partition, offset))

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:  # type: ignore[override]
        stage = "parse"
        payload_text = raw_payload
        payload_obj = None
        fallback_metadata = None
        cause = error

        if isinstance(error, _InstanceCommandParseError):
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
        except Exception:
            logger.exception("Failed to publish invalid instance payload to DLQ")
        logger.exception("Invalid instance payload; skipping: %s", cause)

    async def _on_retry_scheduled(  # type: ignore[override]
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
            with suppress(Exception):
                await self.set_command_status(
                    command_id,
                    "retrying",
                    {"error": error, "attempt": attempt_count, "max_attempts": int(max_attempts)},
                )
        logger.warning(
            "Retrying instance command in %ss (attempt %s): %s",
            int(backoff_s),
            attempt_count,
            error,
        )

    async def _on_terminal_failure(  # type: ignore[override]
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
            with suppress(Exception):
                await self.set_command_status(command_id, "failed", {"error": error})
        logger.error(
            "Instance command failed after %s attempts (retryable=%s): %s",
            attempt_count,
            retryable,
            error,
        )

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        payload: Optional[_InstanceCommandPayload] = None,
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

        if payload_obj is None and payload is not None and isinstance(payload.command, dict):
            payload_obj = payload.command

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
        
        if self.consumer:
            await self._consumer_call(self.consumer.close)
        if self.producer:
            self.producer.flush()
        if self.redis_client:
            await self.redis_client.close()
        if self.terminus_service:
            await self.terminus_service.close()
        if self.processed_event_registry:
            await self.processed_event_registry.close()
        if self.dataset_registry:
            await self.dataset_registry.close()
        if self.bff_http:
            await self.bff_http.aclose()
        self._consumer_executor.shutdown(wait=True, cancel_futures=True)
            

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
