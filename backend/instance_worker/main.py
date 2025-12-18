"""
STRICT Palantir-style Instance Worker
경량 그래프 원칙을 100% 준수하는 구현

Kafka message contract:
- Commands arrive as EventEnvelope JSON (metadata.kind == "command")
- Domain events are appended to the S3/MinIO Event Store and relayed to Kafka

PALANTIR RULES:
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
from contextlib import suppress
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set, List
from uuid import uuid4, uuid5, NAMESPACE_URL

from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition
import redis
import boto3

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig
from shared.services.redis_service import create_redis_service
from shared.services.command_status_service import (
    CommandStatusService as CommandStatusTracker,
    CommandStatus as CommandStatusEnum,
)
from shared.config.settings import ApplicationSettings
from shared.models.event_envelope import EventEnvelope
from shared.security.input_sanitizer import validate_instance_id
from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry
from shared.services.lineage_store import LineageStore
from shared.services.audit_log_store import AuditLogStore
from shared.utils.chaos import maybe_crash

# ULTRA CRITICAL: Import TerminusDB service
from oms.services.async_terminus import AsyncTerminusService
from oms.services.event_store import EventStore
from shared.models.config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrictPalantirInstanceWorker:
    """
    STRICT Palantir-style Instance Worker
    Graph는 관계와 참조만, 데이터는 ES/S3에만

    Command input is consumed exclusively as EventEnvelope JSON.
    """
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.enable_event_sourcing = os.getenv("ENABLE_EVENT_SOURCING", "true").lower() == "true"
        self.consumer = None
        self.producer = None
        self.redis_client = None
        self.command_status_service: Optional[CommandStatusTracker] = None
        self.s3_client = None
        self.terminus_service = None
        self.instance_bucket = AppConfig.INSTANCE_BUCKET

        # Durable idempotency (Postgres)
        self.enable_processed_event_registry = (
            os.getenv("ENABLE_PROCESSED_EVENT_REGISTRY", "true").lower() == "true"
        )
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.event_store: Optional[EventStore] = None

        # First-class provenance/audit (fail-open by default)
        self.enable_lineage = os.getenv("ENABLE_LINEAGE", "true").strip().lower() in {"1", "true", "yes", "on"}
        self.enable_audit_logs = os.getenv("ENABLE_AUDIT_LOGS", "true").strip().lower() in {"1", "true", "yes", "on"}
        self.lineage_store: Optional[LineageStore] = None
        self.audit_store: Optional[AuditLogStore] = None
        
        # PALANTIR PRINCIPLE: Only business concepts in graph
        # No system fields or storage details
        
    async def initialize(self):
        """Initialize all connections"""
        logger.info("Initializing STRICT Palantir Instance Worker...")
        
        # Kafka Consumer - stable consumer group (at-least-once with manual commit)
        group_id = os.getenv("INSTANCE_WORKER_GROUP", "instance-worker-group")
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Read from beginning
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 45000,  # 45 seconds
        })
        logger.info(f"Using consumer group: {group_id}")
        
        # Kafka Producer for events
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'strict-palantir-instance-worker-producer',
        })
        
        # Redis (optional - don't fail if not available)
        settings = ApplicationSettings()
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
            endpoint_url=ServiceConfig.get_minio_endpoint(),
            aws_access_key_id=ServiceConfig.get_minio_access_key(),
            aws_secret_access_key=ServiceConfig.get_minio_secret_key(),
            region_name='us-east-1'
        )
        
        # Ensure bucket exists
        try:
            self.s3_client.head_bucket(Bucket=self.instance_bucket)
        except:
            self.s3_client.create_bucket(Bucket=self.instance_bucket)
        
        # TerminusDB
        connection_info = ConnectionConfig(
            server_url=ServiceConfig.get_terminus_url(),
            user=os.getenv('TERMINUS_USER', 'admin'),
            account=os.getenv('TERMINUS_ACCOUNT', 'admin'),
            key=os.getenv('TERMINUS_KEY', 'admin')
        )
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()

        # Durable processed-events registry (idempotency + ordering guard)
        if self.enable_processed_event_registry:
            self.processed_event_registry = ProcessedEventRegistry()
            await self.processed_event_registry.connect()
            logger.info("✅ ProcessedEventRegistry connected (Postgres)")
        else:
            logger.warning("⚠️ ProcessedEventRegistry disabled (duplicates may re-apply side-effects)")

        if self.enable_event_sourcing:
            try:
                self.event_store = EventStore()
                await self.event_store.connect()
                logger.info("✅ Event Store connected (domain events will be appended to S3/MinIO)")
            except Exception as e:
                logger.warning(f"⚠️ Event Store connection failed, falling back to direct Kafka publish: {e}")
                self.event_store = None

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
        
        # Subscribe to Kafka topic
        self.consumer.subscribe([AppConfig.INSTANCE_COMMANDS_TOPIC])
        
        logger.info("✅ STRICT Palantir Instance Worker initialized")
        
    async def extract_payload_from_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
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
    
    def get_primary_key_value(self, class_id: str, payload: Dict[str, Any]) -> str:
        """
        Extract primary key value dynamically based on class naming convention
        Pattern: {class_name.lower()}_id
        """
        # Standard pattern: class_name_id (e.g., product_id, client_id, order_id)
        expected_key = f"{class_id.lower()}_id"
        
        if expected_key in payload:
            return str(payload[expected_key])
        
        # Fallback: Look for any field ending with _id
        for key, value in payload.items():
            if key.endswith('_id') and value:
                logger.info(f"Using fallback primary key: {key} = {value}")
                return str(value)
        
        # Last resort: Generate a unique ID
        generated_id = f"{class_id.lower()}_{uuid4().hex[:8]}"
        logger.warning(f"No primary key found for {class_id}, generated: {generated_id}")
        return generated_id
    
    async def extract_relationships(self, db_name: str, class_id: str, payload: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract ONLY relationship fields from payload
        Returns: {field_name: target_id} for @id references only
        """
        relationships = {}
        
        try:
            # Get ontology to identify relationship fields
            ontology = await self.terminus_service.get_ontology(db_name, class_id)
            
            if ontology:
                # OntologyResponse (pydantic) shape
                rel_list = getattr(ontology, "relationships", None)
                if isinstance(rel_list, list):
                    for rel in rel_list:
                        field_name = None
                        if isinstance(rel, dict):
                            field_name = rel.get("predicate") or rel.get("name")
                        else:
                            field_name = getattr(rel, "predicate", None) or getattr(rel, "name", None)

                        if not field_name or field_name not in payload:
                            continue

                        value = payload[field_name]
                        if isinstance(value, str) and "/" in value:
                            relationships[str(field_name)] = value
                            logger.info(f"  📎 Found relationship: {field_name} → {value}")

                # Dict (OMS-like) shape
                elif isinstance(ontology, dict):
                    # Check for relationships in OMS format (has 'relationships' key)
                    if "relationships" in ontology and isinstance(ontology.get("relationships"), list):
                        for rel in ontology["relationships"]:
                            if not isinstance(rel, dict):
                                continue
                            field_name = rel.get("predicate") or rel.get("name")
                            if not field_name or field_name not in payload:
                                continue
                            value = payload[field_name]
                            if isinstance(value, str) and "/" in value:
                                relationships[str(field_name)] = value
                                logger.info(f"  📎 Found relationship: {field_name} → {value}")

                    # TerminusDB schema format (relationships as properties with @class)
                    else:
                        for key, value_def in ontology.items():
                            if isinstance(value_def, dict) and "@class" in value_def and key in payload:
                                value = payload[key]
                                if isinstance(value, str) and "/" in value:
                                    relationships[str(key)] = value
                                    logger.info(f"  📎 Found relationship: {key} → {value}")
                            
        except Exception as e:
            logger.warning(f"Could not get ontology for relationship extraction: {e}")
            
        # FALLBACK: Always check for common relationship patterns
        # This ensures relationships work even without schema
        for key, value in payload.items():
            if isinstance(value, str) and '/' in value:
                # Looks like an @id reference
                if any(pattern in key for pattern in ['_by', '_to', '_ref', 'contains', 'linked']):
                    if key not in relationships:  # Don't duplicate
                        relationships[key] = value
                        logger.info(f"  📎 Found relationship pattern: {key} → {value}")
                        
        return relationships

    async def extract_required_properties(self, db_name: str, class_id: str) -> List[str]:
        """
        Extract required property names from the class schema.

        We store a lightweight node in TerminusDB for graph traversal, but TerminusDB
        enforces schema-required fields. Include required scalar fields so inserts
        do not fail schema checks.
        """
        try:
            ontology = await self.terminus_service.get_ontology(db_name, class_id)
        except Exception as e:
            # Without schema we cannot safely satisfy required-field constraints.
            # Treat as retryable by failing fast (outer loop will apply backoff/retry).
            raise RuntimeError(f"Ontology fetch failed for required-field extraction ({db_name}:{class_id}): {e}") from e

        if not ontology:
            # Covers: temporary Terminus unavailability, schema not yet applied, or missing class.
            # Failing fast prevents us from writing a schema-invalid document that then becomes a
            # non-retryable 400 and wedges the command permanently.
            raise RuntimeError(f"Ontology unavailable for required-field extraction ({db_name}:{class_id})")

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
        
    async def process_create_instance(self, command: Dict[str, Any]):
        """Process CREATE_INSTANCE command - STRICT Palantir style"""
        db_name = command.get('db_name')
        class_id = command.get('class_id')
        command_id = command.get('command_id')
        payload = command.get('payload', {}) or {}

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

                expected_aggregate_id = f"{db_name}:{class_id}:{instance_id}"
                if command.get("aggregate_id") and command.get("aggregate_id") != expected_aggregate_id:
                    raise ValueError(
                        f"aggregate_id mismatch: command.aggregate_id={command.get('aggregate_id')} "
                        f"expected={expected_aggregate_id}"
                    )
            else:
                # Extract primary key value dynamically
                instance_id = self.get_primary_key_value(class_id, payload)
                validate_instance_id(instance_id)

            # Ensure downstream uses the resolved instance_id consistently
            command["instance_id"] = instance_id
            primary_key_value = instance_id

            logger.info(f"🔷 STRICT Palantir: Creating {class_id}/{instance_id}")

            # 1. Save FULL data to S3 (Event Store)
            s3_path = f"{db_name}/{class_id}/{instance_id}/{command_id}.json"
            s3_data = {
                'command': command,
                'payload': payload,
                'instance_id': instance_id,
                'class_id': class_id,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            put_resp = self.s3_client.put_object(
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
                        },
                    )
                except Exception as e:
                    logger.debug(f"Audit record failed (non-fatal): {e}")
            
            # 2. Extract ONLY relationships for graph
            relationships = await self.extract_relationships(db_name, class_id, payload)
            
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
            for field in await self.extract_required_properties(db_name, class_id):
                if field in graph_node:
                    continue
                if field in payload:
                    graph_node[field] = payload[field]
                
            # PALANTIR PRINCIPLE: NO domain attributes in TerminusDB!
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
                )
                maybe_crash("instance_worker:after_terminus", logger=logger)
                logger.info("  ✅ Stored lightweight node in TerminusDB")
                logger.info(f"  📊 Relationships: {list(relationships.keys())}")

                if self.lineage_store:
                    try:
                        await self.lineage_store.record_link(
                            from_node_id=self.lineage_store.node_event(str(command_id)),
                            to_node_id=self.lineage_store.node_artifact("terminus", db_name, terminus_id),
                            edge_type="event_wrote_terminus_document",
                            occurred_at=datetime.now(timezone.utc),
                            db_name=db_name,
                            to_label=f"terminus:{db_name}:{terminus_id}",
                            edge_metadata={"db_name": db_name, "terminus_id": terminus_id, "class_id": class_id},
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
                            resource_id=f"terminus:{db_name}:{terminus_id}",
                            event_id=str(command_id),
                            command_id=str(command_id),
                            metadata={"class_id": class_id, "instance_id": instance_id, "terminus_id": terminus_id},
                        )
                    except Exception as e:
                        logger.debug(f"Audit record failed (non-fatal): {e}")
            except Exception as e:
                existing = None
                try:
                    existing = await self.terminus_service.document_service.get_document(
                        db_name, terminus_id, graph_type="instance"
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
                                resource_id=f"terminus:{db_name}:{terminus_id}",
                                event_id=str(command_id),
                                command_id=str(command_id),
                                metadata={"class_id": class_id, "instance_id": instance_id, "terminus_id": terminus_id},
                                error=str(e),
                            )
                        except Exception:
                            pass
                    # TerminusDB is the graph authority; if we cannot write it, we must retry.
                    raise
            
            # 4. Store/publish domain event (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka)
            
            # NOTE: Elasticsearch indexing is handled by projection_worker (single writer) for schema consistency.
            event_payload = {
                "db_name": db_name,
                "class_id": class_id,
                "instance_id": instance_id,
                **payload,  # Include full payload in event
            }
            aggregate_id = f"{db_name}:{class_id}:{instance_id}"

            domain_event_id = (
                str(uuid5(NAMESPACE_URL, f"spice:{command_id}:INSTANCE_CREATED:{aggregate_id}"))
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
                    "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
                    "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
                },
            )

            if self.enable_event_sourcing and self.event_store:
                await self.event_store.append_event(envelope)
                logger.info(
                    f"  ✅ Stored INSTANCE_CREATED in Event Store (seq={envelope.sequence_number})"
                )
            else:
                # Fallback: direct Kafka publish (still use canonical EventEnvelope format).
                self.producer.produce(
                    AppConfig.INSTANCE_EVENTS_TOPIC,
                    key=aggregate_id,
                    value=envelope.as_json(),
                )
                self.producer.flush()
                logger.info("  ✅ Published INSTANCE_CREATED event (fallback; not persisted in Event Store)")
            
            # Set success status
            await self.set_command_status(command_id, 'completed', {
                'instance_id': instance_id,
                'es_doc_id': instance_id,
                's3_uri': f"s3://{self.instance_bucket}/{s3_path}"
            })
            
            logger.info(f"✅ STRICT Palantir: Instance created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create instance: {e}")
            await self.set_command_status(command_id, 'failed', {'error': str(e)})
            raise

    async def process_update_instance(self, command: Dict[str, Any]) -> None:
        """Process UPDATE_INSTANCE command (idempotent + ordered via registry claim)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")
        payload = command.get("payload", {}) or {}

        # Set command status early (202 already returned to user).
        await self.set_command_status(command_id, "processing")

        if not db_name:
            raise ValueError("db_name is required")
        if not class_id:
            raise ValueError("class_id is required")

        instance_id = command.get("instance_id")
        if not instance_id:
            raise ValueError("instance_id is required for UPDATE_INSTANCE")
        instance_id = str(instance_id)
        validate_instance_id(instance_id)

        if not isinstance(payload, dict):
            raise ValueError("UPDATE_INSTANCE payload must be an object")

        expected_aggregate_id = f"{db_name}:{class_id}:{instance_id}"
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
                            if k not in {"db_name", "class_id", "instance_id"}
                        }
                        last_domain_was_delete = False
                    elif ev.event_type == "INSTANCE_DELETED":
                        previous_payload = {}
                        last_domain_was_delete = True
            except Exception as e:
                event_store_error = e
                logger.warning(
                    f"Failed to rebuild previous state from Event Store (aggregate_id={aggregate_id}): {e}"
                )

        if last_domain_was_delete:
            raise ValueError(f"Cannot update deleted instance (aggregate_id={aggregate_id})")

        # Fallback (best-effort): use latest S3 snapshot if Event Store lookup failed or found no domain state.
        if (not previous_payload) and self.s3_client:
            try:
                prefix = f"{db_name}/{class_id}/{instance_id}/"
                resp = self.s3_client.list_objects_v2(Bucket=self.instance_bucket, Prefix=prefix)
                objs = list((resp or {}).get("Contents") or [])
                objs.sort(key=lambda o: o.get("LastModified") or datetime.fromtimestamp(0, tz=timezone.utc))
                snapshot_key = (objs[-1].get("Key") if objs else None)
                if snapshot_key:
                    obj = self.s3_client.get_object(Bucket=self.instance_bucket, Key=snapshot_key)
                    raw = obj.get("Body").read()
                    doc = json.loads(raw.decode("utf-8"))
                    if isinstance(doc, dict):
                        if doc.get("deleted_at") or (
                            isinstance(obj.get("Metadata"), dict) and obj["Metadata"].get("tombstone") == "true"
                        ):
                            raise ValueError(f"Cannot update deleted instance (snapshot={snapshot_key})")
                        snap_payload = doc.get("payload")
                        if isinstance(snap_payload, dict):
                            previous_payload = dict(snap_payload)
            except Exception as e:
                # Only fail hard if we also couldn't consult the Event Store; otherwise snapshot fallback is optional.
                if event_store_error is not None:
                    raise RuntimeError(
                        f"Unable to rebuild previous payload for patch update (aggregate_id={aggregate_id}): {e}"
                    ) from e
                logger.debug(f"Snapshot fallback failed (non-fatal): {e}")

        merged_payload = {**previous_payload, **payload}

        # Save snapshot to S3 (optional durable blob store).
        s3_path = f"{db_name}/{class_id}/{instance_id}/{command_id}.json"
        s3_data = {
            "command": command,
            "payload": merged_payload,
            "instance_id": instance_id,
            "class_id": class_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            put_resp = self.s3_client.put_object(
                Bucket=self.instance_bucket,
                Key=s3_path,
                Body=json.dumps(s3_data, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "command_id": str(command_id) if command_id else "",
                    "instance_id": instance_id,
                    "class_id": class_id,
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
                        metadata={"class_id": class_id, "instance_id": instance_id, "bucket": self.instance_bucket, "key": s3_path},
                        error=str(e),
                    )
                except Exception:
                    pass

        # Update lightweight node (relationships + required scalar fields)
        relationships = await self.extract_relationships(db_name, class_id, merged_payload)
        terminus_id = f"{class_id}/{instance_id}"
        graph_node: Dict[str, Any] = {
            "@id": terminus_id,
            "@type": class_id,
            primary_key_field: merged_payload.get(primary_key_field, instance_id),
        }
        for rel_field, rel_target in relationships.items():
            graph_node[rel_field] = rel_target
        for field in await self.extract_required_properties(db_name, class_id):
            if field in graph_node:
                continue
            if field in merged_payload:
                graph_node[field] = merged_payload[field]

        try:
            maybe_crash("instance_worker:before_terminus", logger=logger)
            await self.terminus_service.update_instance(db_name, class_id, terminus_id, graph_node)
            maybe_crash("instance_worker:after_terminus", logger=logger)
            logger.info("  ✅ Updated lightweight node in TerminusDB")
            if command_id and self.lineage_store:
                try:
                    await self.lineage_store.record_link(
                        from_node_id=self.lineage_store.node_event(str(command_id)),
                        to_node_id=self.lineage_store.node_artifact("terminus", db_name, terminus_id),
                        edge_type="event_wrote_terminus_document",
                        occurred_at=datetime.now(timezone.utc),
                        db_name=db_name,
                        to_label=f"terminus:{db_name}:{terminus_id}",
                        edge_metadata={"db_name": db_name, "terminus_id": terminus_id, "class_id": class_id},
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
                        resource_id=f"terminus:{db_name}:{terminus_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={"class_id": class_id, "instance_id": instance_id, "terminus_id": terminus_id},
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
                        resource_id=f"terminus:{db_name}:{terminus_id}",
                        event_id=str(command_id),
                        command_id=str(command_id),
                        metadata={"class_id": class_id, "instance_id": instance_id, "terminus_id": terminus_id},
                        error=str(e),
                    )
                except Exception:
                    pass
            # TerminusDB is the graph authority; retry until it is updated.
            raise

        # Elasticsearch indexing is handled by projection_worker (single writer) for schema consistency.

        # Publish domain event (SSoT: Event Store)
        domain_event_id = (
            str(uuid5(NAMESPACE_URL, f"spice:{command_id}:INSTANCE_UPDATED:{aggregate_id}"))
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
                "class_id": class_id,
                "instance_id": instance_id,
                **merged_payload,
            },
            metadata={
                "kind": "domain",
                "kafka_topic": AppConfig.INSTANCE_EVENTS_TOPIC,
                "service": "instance_worker",
                "command_id": command_id,
                "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
                "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
            },
        )

        if self.enable_event_sourcing and self.event_store:
            await self.event_store.append_event(envelope)
            logger.info(f"  ✅ Stored INSTANCE_UPDATED in Event Store (seq={envelope.sequence_number})")
        else:
            self.producer.produce(
                AppConfig.INSTANCE_EVENTS_TOPIC,
                key=aggregate_id,
                value=envelope.as_json(),
            )
            self.producer.flush()
            logger.info("  ✅ Published INSTANCE_UPDATED event (fallback; not persisted in Event Store)")

        await self.set_command_status(
            command_id,
            "completed",
            {"instance_id": instance_id, "s3_uri": f"s3://{self.instance_bucket}/{s3_path}"},
        )

    async def process_delete_instance(self, command: Dict[str, Any]) -> None:
        """Process DELETE_INSTANCE command (idempotent delete)."""
        db_name = command.get("db_name")
        class_id = command.get("class_id")
        command_id = command.get("command_id")

        await self.set_command_status(command_id, "processing")

        if not db_name:
            raise ValueError("db_name is required")
        if not class_id:
            raise ValueError("class_id is required")

        instance_id = command.get("instance_id")
        if not instance_id:
            raise ValueError("instance_id is required for DELETE_INSTANCE")
        instance_id = str(instance_id)
        validate_instance_id(instance_id)

        expected_aggregate_id = f"{db_name}:{class_id}:{instance_id}"
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
            await self.terminus_service.delete_instance(db_name, class_id, terminus_id)
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
                    to_node_id=self.lineage_store.node_artifact("terminus", db_name, terminus_id),
                    edge_type="event_deleted_terminus_document",
                    occurred_at=datetime.now(timezone.utc),
                    db_name=db_name,
                    to_label=f"terminus:{db_name}:{terminus_id}",
                    edge_metadata={"db_name": db_name, "terminus_id": terminus_id, "class_id": class_id},
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
                    resource_id=f"terminus:{db_name}:{terminus_id}",
                    event_id=str(command_id),
                    command_id=str(command_id),
                    metadata={
                        "class_id": class_id,
                        "instance_id": instance_id,
                        "terminus_id": terminus_id,
                        "already_missing": terminus_already_missing,
                    },
                    error=None if terminus_deleted else (terminus_error or "delete_failed"),
                )
            except Exception as e:
                logger.debug(f"Audit record failed (non-fatal): {e}")

        # Elasticsearch deletion is handled by projection_worker (single writer) for schema consistency.

        # Tombstone snapshot (best-effort)
        s3_path = f"{db_name}/{class_id}/{instance_id}/{command_id}.json"
        s3_written = False
        tombstone_resp: Optional[Dict[str, Any]] = None
        try:
            tombstone_resp = self.s3_client.put_object(
                Bucket=self.instance_bucket,
                Key=s3_path,
                Body=json.dumps(
                    {
                        "command": command,
                        "instance_id": instance_id,
                        "class_id": class_id,
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
                        metadata={"class_id": class_id, "instance_id": instance_id, "bucket": self.instance_bucket, "key": s3_path},
                        error=str(e),
                    )
                except Exception:
                    pass

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
                    },
                )
            except Exception as e:
                logger.debug(f"Audit record failed (non-fatal): {e}")

        aggregate_id = expected_aggregate_id
        domain_event_id = (
            str(uuid5(NAMESPACE_URL, f"spice:{command_id}:INSTANCE_DELETED:{aggregate_id}"))
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
            data={"db_name": db_name, "class_id": class_id, "instance_id": instance_id},
            metadata={
                "kind": "domain",
                "kafka_topic": AppConfig.INSTANCE_EVENTS_TOPIC,
                "service": "instance_worker",
                "command_id": command_id,
                "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("RUN_ID") or os.getenv("EXECUTION_ID"),
                "code_sha": os.getenv("CODE_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA"),
            },
        )

        if self.enable_event_sourcing and self.event_store:
            await self.event_store.append_event(envelope)
            logger.info(f"  ✅ Stored INSTANCE_DELETED in Event Store (seq={envelope.sequence_number})")
        else:
            self.producer.produce(
                AppConfig.INSTANCE_EVENTS_TOPIC,
                key=aggregate_id,
                value=envelope.as_json(),
            )
            self.producer.flush()
            logger.info("  ✅ Published INSTANCE_DELETED event (fallback; not persisted in Event Store)")

        await self.set_command_status(command_id, "completed", {"instance_id": instance_id})
            
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
            except Exception:
                pass

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

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        if not self.processed_event_registry:
            return
        interval = int(os.getenv("PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS", "30"))
        while True:
            await asyncio.sleep(interval)
            ok = await self.processed_event_registry.heartbeat(handler=handler, event_id=event_id)
            if not ok:
                return

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        # TerminusDB schema failures can be transient under at-least-once + cross-aggregate reordering.
        # Example: Product references Customer that hasn't been created yet.
        if "references_untyped_object" in msg:
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
        
    async def run(self):
        """Main processing loop"""
        self.running = True
        logger.info("🚀 STRICT Palantir Instance Worker started")
        logger.info(f"  Subscribed to topic: {AppConfig.INSTANCE_COMMANDS_TOPIC}")
        
        poll_count = 0
        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            poll_count += 1
            
            if poll_count % 10 == 0:
                logger.info(f"  Polled {poll_count} times, no messages yet...")
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                    
            registry_event_id = None
            registry_aggregate_id = None
            registry_sequence = None
            registry_claimed = False
            registry_attempt_count = 1
            command: Dict[str, Any] = {}
            command_id = None
            heartbeat_task = None

            try:
                logger.info("📨 Received message from Kafka!")
                try:
                    raw_message = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Kafka message JSON: {e}")
                    # Poison pill: commit to avoid infinite retry loop.
                    self.consumer.commit(msg, asynchronous=False)
                    continue

                # Extract the actual command payload (canonical EventEnvelope only)
                try:
                    command = await self.extract_payload_from_message(raw_message)
                except Exception as e:
                    logger.error(f"Invalid command envelope; committing offset to avoid poison pill: {e}")
                    self.consumer.commit(msg, asynchronous=False)
                    continue

                command_type = command.get("command_type")

                # Idempotency guard: if this command is already completed, skip processing.
                command_id = command.get("command_id")
                if self.redis_client and command_id:
                    try:
                        status_key = AppConfig.get_command_status_key(str(command_id))
                        status_raw = await self.redis_client.get(status_key)
                        if status_raw:
                            status_data = json.loads(status_raw)
                            if str(status_data.get("status", "")).upper() == "COMPLETED":
                                logger.info(f"Skipping already completed command {command_id}")
                                self.consumer.commit(msg, asynchronous=False)
                                continue
                    except Exception as e:
                        logger.warning(f"Failed to read command status for {command_id}: {e}")

                # Durable idempotency + ordering guard (Postgres)
                registry_event_id = command.get("event_id") or command.get("command_id")
                registry_aggregate_id = command.get("aggregate_id")
                registry_sequence = command.get("sequence_number")
                if self.processed_event_registry and registry_event_id:
                    claim = await self.processed_event_registry.claim(
                        handler="instance_worker",
                        event_id=str(registry_event_id),
                        aggregate_id=str(registry_aggregate_id) if registry_aggregate_id else None,
                        sequence_number=int(registry_sequence) if registry_sequence is not None else None,
                    )
                    registry_attempt_count = int(claim.attempt_count or 1)
                    if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                        logger.info(
                            f"Skipping {claim.decision.value} command event_id={registry_event_id} "
                            f"(aggregate_id={registry_aggregate_id}, seq={registry_sequence})"
                        )
                        self.consumer.commit(msg, asynchronous=False)
                        continue
                    if claim.decision == ClaimDecision.IN_PROGRESS:
                        logger.info(f"Command {registry_event_id} is in progress elsewhere; retrying later")
                        await asyncio.sleep(2)
                        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                        continue
                    registry_claimed = True
                    maybe_crash("instance_worker:after_claim", logger=logger)

                if registry_claimed and self.processed_event_registry and registry_event_id:
                    heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(handler="instance_worker", event_id=str(registry_event_id))
                    )

                logger.info(f"Processing command: {command_type}")
                logger.info(f"  Database: {command.get('db_name')}")
                logger.info(f"  Class: {command.get('class_id')}")

                if command_type == "CREATE_INSTANCE":
                    await self.process_create_instance(command)
                elif command_type == "UPDATE_INSTANCE":
                    await self.process_update_instance(command)
                elif command_type == "DELETE_INSTANCE":
                    await self.process_delete_instance(command)
                else:
                    raise ValueError(f"Unknown command type: {command_type}")

                if registry_claimed and self.processed_event_registry and registry_event_id:
                    maybe_crash("instance_worker:before_mark_done", logger=logger)
                    await self.processed_event_registry.mark_done(
                        handler="instance_worker",
                        event_id=str(registry_event_id),
                        aggregate_id=str(registry_aggregate_id) if registry_aggregate_id else None,
                        sequence_number=int(registry_sequence) if registry_sequence is not None else None,
                    )

                self.consumer.commit(msg, asynchronous=False)

            except Exception as e:
                logger.error(f"Error processing command: {e}")
                if registry_claimed and self.processed_event_registry and registry_event_id:
                    try:
                        await self.processed_event_registry.mark_failed(
                            handler="instance_worker",
                            event_id=str(registry_event_id),
                            error=str(e),
                        )
                    except Exception as reg_err:
                        logger.warning(f"Failed to mark event failed in registry: {reg_err}")

                retryable = self._is_retryable_error(e)
                attempt_count = int(registry_attempt_count or 1)
                max_attempts = int(os.getenv("INSTANCE_WORKER_MAX_RETRY_ATTEMPTS", "5"))

                if retryable and attempt_count < max_attempts:
                    try:
                        await self.set_command_status(
                            command_id,
                            "retrying",
                            {"error": str(e), "attempt": attempt_count, "max_attempts": max_attempts},
                        )
                    except Exception:
                        pass
                    backoff_s = min(2 ** max(attempt_count - 1, 0), 60)
                    logger.warning(
                        f"Retrying command event_id={registry_event_id} in {backoff_s}s "
                        f"(attempt {attempt_count}/{max_attempts})"
                    )
                    await asyncio.sleep(backoff_s)
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    continue

                try:
                    await self.set_command_status(command_id, "failed", {"error": str(e)})
                except Exception:
                    pass

                logger.error(
                    f"Skipping failed command event_id={registry_event_id} after {attempt_count} attempts "
                    f"(retryable={retryable}); committing offset to avoid poison pill"
                )
                self.consumer.commit(message=msg, asynchronous=False)
                continue

            finally:
                if heartbeat_task:
                    heartbeat_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await heartbeat_task
                
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down STRICT Palantir Instance Worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
        if self.redis_client:
            await self.redis_client.close()
        if self.terminus_service:
            await self.terminus_service.close()
        if self.processed_event_registry:
            await self.processed_event_registry.close()
            

async def main():
    """Main entry point"""
    worker = StrictPalantirInstanceWorker()
    
    try:
        await worker.initialize()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await worker.shutdown()
        

if __name__ == "__main__":
    asyncio.run(main())
