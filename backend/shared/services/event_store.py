"""
🔥 THINK ULTRA! Shared Event Store (S3/MinIO)

This is the Single Source of Truth (SSoT) for all events.

Aligned with Palantir Foundry architecture:
- S3/MinIO = Immutable event log (SSoT)
- TerminusDB = Graph relationships  
- Elasticsearch = Search indexes
- PostgreSQL = Processed-event registry + relational features

This module lives in `shared` so every service (OMS/BFF/workers) can read/write
the canonical SSoT without importing other service packages.
"""

import json
from datetime import datetime, timezone
from typing import List, AsyncIterator, Optional, Dict, Any, AsyncGenerator
from datetime import timedelta
import os
import hashlib

import aioboto3
from botocore.exceptions import ClientError

from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.aggregate_sequence_allocator import AggregateSequenceAllocator
from shared.utils.ontology_version import extract_ontology_version
import logging

logger = logging.getLogger(__name__)

try:
    from shared.services.lineage_store import LineageStore
    from shared.services.audit_log_store import AuditLogStore
except Exception:  # pragma: no cover
    LineageStore = None  # type: ignore
    AuditLogStore = None  # type: ignore


class EventStore:
    """
    The REAL Event Store using S3/MinIO as Single Source of Truth.
    
    This is the authoritative, immutable event log that serves as the
    foundation for Event Sourcing.
    
    Event Storage Structure:
    /events/{year}/{month}/{day}/{aggregate_type}/{aggregate_id}/{event_id}.json
    
    Index Structure:
    /indexes/by-aggregate/{aggregate_type}/{aggregate_id}/{sequence}_{event_id}.json
    /indexes/by-date/{year}/{month}/{day}/{timestamp_ms}_{event_id}.json
    """
    
    def __init__(self):
        self.endpoint_url = ServiceConfig.get_minio_endpoint()
        self.access_key = ServiceConfig.get_minio_access_key()
        self.secret_key = ServiceConfig.get_minio_secret_key()
        # Keep consistent with EventPublisher (message_relay)
        self.bucket_name = os.getenv("EVENT_STORE_BUCKET", "spice-event-store")  # The SSoT bucket
        self.session = None
        self.s3_client = None
        self._sequence_allocator: Optional[AggregateSequenceAllocator] = None
        self._sequence_mode = os.getenv("EVENT_STORE_SEQUENCE_ALLOCATOR_MODE", "postgres").strip().lower()
        self._lineage_enabled = os.getenv("ENABLE_LINEAGE", "true").strip().lower() in {"1", "true", "yes", "on"}
        self._audit_enabled = os.getenv("ENABLE_AUDIT_LOGS", "true").strip().lower() in {"1", "true", "yes", "on"}
        self._lineage_store: Optional[Any] = None
        self._audit_store: Optional[Any] = None
        
    async def connect(self):
        """Initialize S3/MinIO connection"""
        try:
            self.session = aioboto3.Session()
            
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Ensure bucket exists
                try:
                    await s3.head_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ Event Store bucket '{self.bucket_name}' exists")
                except ClientError:
                    await s3.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ Created Event Store bucket '{self.bucket_name}'")
                    
                    # Set bucket versioning for immutability
                    await s3.put_bucket_versioning(
                        Bucket=self.bucket_name,
                        VersioningConfiguration={'Status': 'Enabled'}
                    )
                    logger.info("✅ Enabled versioning for immutability")

            # Best-effort: initialize lineage/audit stores (Postgres-backed).
            await self._initialize_lineage_and_audit()
                    
        except Exception as e:
            logger.error(f"Failed to connect to S3/MinIO Event Store: {e}")
            raise

    async def _initialize_lineage_and_audit(self) -> None:
        if self._lineage_enabled and LineageStore:
            try:
                self._lineage_store = LineageStore()
                await self._lineage_store.initialize()
                logger.info("✅ LineageStore connected (Postgres)")
            except Exception as e:
                logger.warning(f"⚠️ LineageStore unavailable (continuing without lineage): {e}")
                self._lineage_store = None

        if self._audit_enabled and AuditLogStore:
            try:
                self._audit_store = AuditLogStore()
                await self._audit_store.initialize()
                logger.info("✅ AuditLogStore connected (Postgres)")
            except Exception as e:
                logger.warning(f"⚠️ AuditLogStore unavailable (continuing without audit logs): {e}")
                self._audit_store = None

    @staticmethod
    def _partition_key_for_envelope(envelope: EventEnvelope) -> str:
        if isinstance(envelope.data, dict):
            db_name = envelope.data.get("db_name")
            if isinstance(db_name, str) and db_name:
                return f"db:{db_name}"
        if envelope.aggregate_type and envelope.aggregate_id:
            return f"agg:{envelope.aggregate_type}:{envelope.aggregate_id}"
        return "global"

    async def _record_lineage_and_audit(
        self,
        envelope: EventEnvelope,
        *,
        s3_key: Optional[str],
        audit_action: str = "EVENT_APPENDED",
    ) -> None:
        # Lineage graph
        if self._lineage_store and self._lineage_enabled:
            try:
                await self._lineage_store.record_event_envelope(
                    envelope,
                    s3_bucket=self.bucket_name,
                    s3_key=s3_key,
                )
            except Exception as e:
                logger.debug(f"LineageStore record failed (non-fatal): {e}")
                # Best-effort: enqueue for eventual backfill
                try:
                    await self._lineage_store.enqueue_backfill(
                        envelope=envelope,
                        s3_bucket=self.bucket_name,
                        s3_key=s3_key,
                        error=str(e),
                    )
                except Exception as enqueue_err:
                    logger.debug(f"Lineage backfill enqueue failed (non-fatal): {enqueue_err}")

        # Audit logs
        if self._audit_store and self._audit_enabled:
            try:
                partition_key = self._partition_key_for_envelope(envelope)
                service = envelope.metadata.get("service") if isinstance(envelope.metadata, dict) else None
                kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
                kafka_topic = envelope.metadata.get("kafka_topic") if isinstance(envelope.metadata, dict) else None
                command_id = envelope.metadata.get("command_id") if isinstance(envelope.metadata, dict) else None
                trace_id = envelope.metadata.get("trace_id") if isinstance(envelope.metadata, dict) else None
                run_id = envelope.metadata.get("run_id") if isinstance(envelope.metadata, dict) else None
                code_sha = envelope.metadata.get("code_sha") if isinstance(envelope.metadata, dict) else None
                correlation_id = (
                    envelope.metadata.get("correlation_id") if isinstance(envelope.metadata, dict) else None
                )
                ontology = extract_ontology_version(envelope_metadata=envelope.metadata, envelope_data=envelope.data)
                await self._audit_store.log(
                    partition_key=partition_key,
                    actor=service,
                    action=audit_action,
                    status="success",
                    resource_type="object_store",
                    resource_id=f"s3://{self.bucket_name}/{s3_key}" if s3_key else None,
                    event_id=str(envelope.event_id),
                    command_id=str(command_id) if command_id else None,
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    metadata={
                        "event_type": envelope.event_type,
                        "aggregate_type": envelope.aggregate_type,
                        "aggregate_id": envelope.aggregate_id,
                        "sequence_number": envelope.sequence_number,
                        "kind": kind,
                        "kafka_topic": kafka_topic,
                        "run_id": str(run_id) if run_id else None,
                        "code_sha": str(code_sha) if code_sha else None,
                        "schema_version": envelope.schema_version,
                        "ontology": ontology,
                    },
                    occurred_at=envelope.occurred_at,
                )
            except Exception as e:
                logger.debug(f"AuditLogStore record failed (non-fatal): {e}")

    async def _record_audit_failure(self, envelope: EventEnvelope, *, error: str) -> None:
        if not (self._audit_store and self._audit_enabled):
            return
        partition_key = self._partition_key_for_envelope(envelope)
        service = envelope.metadata.get("service") if isinstance(envelope.metadata, dict) else None
        kind = envelope.metadata.get("kind") if isinstance(envelope.metadata, dict) else None
        run_id = envelope.metadata.get("run_id") if isinstance(envelope.metadata, dict) else None
        code_sha = envelope.metadata.get("code_sha") if isinstance(envelope.metadata, dict) else None
        ontology = extract_ontology_version(envelope_metadata=envelope.metadata, envelope_data=envelope.data)
        await self._audit_store.log(
            partition_key=partition_key,
            actor=service,
            action="EVENT_APPEND_FAILED",
            status="failure",
            resource_type="object_store",
            resource_id=f"s3://{self.bucket_name}/events/.../{envelope.event_id}.json",
            event_id=str(envelope.event_id),
            command_id=(envelope.metadata.get("command_id") if isinstance(envelope.metadata, dict) else None),
            trace_id=(envelope.metadata.get("trace_id") if isinstance(envelope.metadata, dict) else None),
            correlation_id=(
                envelope.metadata.get("correlation_id") if isinstance(envelope.metadata, dict) else None
            ),
            metadata={
                "event_type": envelope.event_type,
                "aggregate_type": envelope.aggregate_type,
                "aggregate_id": envelope.aggregate_id,
                "sequence_number": envelope.sequence_number,
                "kind": kind,
                "run_id": str(run_id) if run_id else None,
                "code_sha": str(code_sha) if code_sha else None,
                "schema_version": envelope.schema_version,
                "ontology": ontology,
            },
            error=error,
            occurred_at=datetime.now(timezone.utc),
        )

    async def _get_sequence_allocator(self) -> AggregateSequenceAllocator:
        if self._sequence_allocator:
            return self._sequence_allocator

        allocator = AggregateSequenceAllocator(
            schema=os.getenv("EVENT_STORE_SEQUENCE_SCHEMA", "spice_event_registry"),
            handler_prefix=os.getenv("EVENT_STORE_SEQUENCE_HANDLER_PREFIX", "write_side"),
        )
        await allocator.connect()
        self._sequence_allocator = allocator
        return allocator

    async def _ensure_sequence_number(self, event: EventEnvelope) -> None:
        """
        Ensure `event.sequence_number` is set using an atomic write-side allocator.

        Default mode is Postgres (`EVENT_STORE_SEQUENCE_ALLOCATOR_MODE=postgres`).
        Emergency/legacy modes:
        - `legacy`/`s3`: best-effort `get_aggregate_version()+1` (non-atomic, not recommended)
        - `off`: require caller to provide `sequence_number`
        """
        if not event.aggregate_type or not event.aggregate_id:
            raise ValueError("aggregate_type and aggregate_id are required to allocate sequence_number")

        mode = (self._sequence_mode or "postgres").strip().lower()
        if mode in {"off", "disabled", "none"}:
            if event.sequence_number is None:
                raise RuntimeError(
                    "sequence_number is required when EVENT_STORE_SEQUENCE_ALLOCATOR_MODE is disabled"
                )
            return

        if mode in {"legacy", "s3", "best_effort"}:
            if event.sequence_number is not None:
                return
            current = await self.get_aggregate_version(event.aggregate_type, event.aggregate_id)
            event.sequence_number = int(current) + 1
            return

        if event.sequence_number is not None:
            logger.warning(
                "sequence_number provided in postgres allocator mode; overwriting with atomic reservation "
                f"(event_id={event.event_id}, aggregate={event.aggregate_type}/{event.aggregate_id})"
            )

        allocator = await self._get_sequence_allocator()
        handler = allocator.handler_for(event.aggregate_type)

        async def _seed_last_sequence_for_event() -> int:
            """
            Compute the seed last_sequence for a new aggregate stream.

            Branch virtualization note:
            - We scope aggregate_id by branch (e.g. db:branch:...).
            - When a non-main branch is created from main, the branch inherits the current state
              even though it has no branch-specific events yet.
            - For the first write on that branch, we must seed from the base branch's last sequence
              so `expected_seq` reflects the inherited state (OCC correctness).
            """
            seed = await self.get_aggregate_version(event.aggregate_type, event.aggregate_id)
            if seed:
                return int(seed)

            if not isinstance(event.data, dict):
                return int(seed or 0)
            branch = event.data.get("branch")
            if not isinstance(branch, str) or not branch or branch == "main":
                return int(seed or 0)
            db_name = event.data.get("db_name")
            if not isinstance(db_name, str) or not db_name:
                return int(seed or 0)

            base_branch = os.getenv("BRANCH_VIRTUALIZATION_BASE_BRANCH", "main").strip() or "main"

            base_aggregate_id: Optional[str] = None
            if event.aggregate_type == "Instance":
                class_id = event.data.get("class_id")
                instance_id = event.data.get("instance_id")
                if isinstance(class_id, str) and class_id and isinstance(instance_id, str) and instance_id:
                    base_aggregate_id = f"{db_name}:{base_branch}:{class_id}:{instance_id}"
            elif event.aggregate_type == "OntologyClass":
                class_id = event.data.get("class_id")
                if not class_id:
                    payload = event.data.get("payload")
                    if isinstance(payload, dict):
                        class_id = payload.get("class_id")
                if isinstance(class_id, str) and class_id:
                    base_aggregate_id = f"{db_name}:{base_branch}:{class_id}"

            if not base_aggregate_id:
                return int(seed or 0)

            try:
                return int(await self.get_aggregate_version(event.aggregate_type, base_aggregate_id) or 0)
            except Exception:
                return int(seed or 0)

        expected_seq = None
        if isinstance(event.metadata, dict) and event.metadata.get("kind") == "command":
            raw_expected = event.data.get("expected_seq") if isinstance(event.data, dict) else None
            if raw_expected is not None:
                try:
                    expected_seq = int(raw_expected)
                except Exception as e:
                    raise ValueError("expected_seq must be an integer") from e
                if expected_seq < 0:
                    raise ValueError("expected_seq must be >= 0")

        if expected_seq is not None:
            reserved = await allocator.try_reserve_next_sequence_if_expected(
                handler=handler,
                aggregate_id=event.aggregate_id,
                expected_last_sequence=int(expected_seq),
            )
            if reserved is not None:
                event.sequence_number = reserved
                return

            # First encounter / allocator reset: seed from current S3 max so seq never restarts at 1 for existing streams.
            seed = await _seed_last_sequence_for_event()
            event.sequence_number = await allocator.reserve_next_sequence_if_expected(
                handler=handler,
                aggregate_id=event.aggregate_id,
                seed_last_sequence=int(seed),
                expected_last_sequence=int(expected_seq),
            )
            return

        reserved = await allocator.try_reserve_next_sequence(handler=handler, aggregate_id=event.aggregate_id)
        if reserved is not None:
            event.sequence_number = reserved
            return

        # First encounter / allocator reset: seed from current S3 max so seq never restarts at 1 for existing streams.
        seed = await _seed_last_sequence_for_event()
        event.sequence_number = await allocator.reserve_next_sequence(
            handler=handler,
            aggregate_id=event.aggregate_id,
            seed_last_sequence=int(seed),
        )
    
    async def append_event(self, event: EventEnvelope) -> str:
        """
        Append an immutable event to S3/MinIO.
        This is the ONLY place where events are authoritatively stored.
        
        Returns:
            event_id: The unique identifier of the stored event
        """
        if not self.session:
            # Allow lazy usage in workers/tests
            self.session = aioboto3.Session()

        # Build S3 key path (legacy layout). NOTE: append idempotency MUST NOT rely
        # on occurred_at, because retries/replays may re-materialize the same
        # event_id with a different timestamp. We therefore also maintain a
        # stable by-event-id index (indexes/by-event-id/{event_id}.json).
        dt = (
            event.occurred_at
            if event.occurred_at.tzinfo
            else event.occurred_at.replace(tzinfo=timezone.utc)
        ).astimezone(timezone.utc)
        key = (
            f"events/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
            f"{event.aggregate_type}/{event.aggregate_id}/{event.event_id}.json"
        )
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # 0) Global idempotency guard: by-event-id index (stable across occurred_at changes).
                existing_key = await self._get_existing_key_by_event_id(s3, str(event.event_id))
                if existing_key:
                    existing_event = await self._read_event_object(s3, existing_key)
                    self._enforce_idempotency_contract(existing_event, event, source="by-event-id")

                    # If the existing event is missing kafka_topic (older writers),
                    # allow index routing to be fixed by the incoming envelope.
                    if (
                        isinstance(existing_event.metadata, dict)
                        and isinstance(event.metadata, dict)
                        and not existing_event.metadata.get("kafka_topic")
                        and event.metadata.get("kafka_topic")
                    ):
                        existing_event.metadata["kafka_topic"] = event.metadata.get("kafka_topic")

                    try:
                        await self._update_indexes(existing_event, existing_key)
                    except Exception as e:
                        logger.warning(f"Failed to update indexes for existing event {existing_event.event_id}: {e}")

                    logger.info(
                        f"✅ Event already exists in S3/MinIO (idempotent append): {existing_event.event_id} "
                        f"[{existing_event.event_type}] for {existing_event.aggregate_type}/{existing_event.aggregate_id}"
                    )
                    await self._record_lineage_and_audit(
                        existing_event,
                        s3_key=existing_key,
                        audit_action="EVENT_APPEND_SKIPPED_DUPLICATE",
                    )
                    return existing_event.event_id

                # 0.1) Back-compat idempotency guard: find by aggregate index if by-event-id is missing.
                existing_key = await self._get_existing_key_by_aggregate_index(
                    s3,
                    aggregate_type=event.aggregate_type,
                    aggregate_id=event.aggregate_id,
                    event_id=str(event.event_id),
                )
                if existing_key:
                    existing_event = await self._read_event_object(s3, existing_key)
                    self._enforce_idempotency_contract(existing_event, event, source="by-aggregate-index")
                    if (
                        isinstance(existing_event.metadata, dict)
                        and isinstance(event.metadata, dict)
                        and not existing_event.metadata.get("kafka_topic")
                        and event.metadata.get("kafka_topic")
                    ):
                        existing_event.metadata["kafka_topic"] = event.metadata.get("kafka_topic")

                    try:
                        await self._update_indexes(existing_event, existing_key)
                    except Exception as e:
                        logger.warning(f"Failed to update indexes for existing event {existing_event.event_id}: {e}")

                    logger.info(
                        f"✅ Event already exists in S3/MinIO (idempotent append via aggregate index): {existing_event.event_id} "
                        f"[{existing_event.event_type}] for {existing_event.aggregate_type}/{existing_event.aggregate_id}"
                    )
                    await self._record_lineage_and_audit(
                        existing_event,
                        s3_key=existing_key,
                        audit_action="EVENT_APPEND_SKIPPED_DUPLICATE",
                    )
                    return existing_event.event_id

                # Idempotency: if the event already exists (same event_id => same key),
                # treat append as a no-op and ensure indexes exist.
                try:
                    await s3.head_object(Bucket=self.bucket_name, Key=key)
                    existing_event = await self._read_event_object(s3, key)
                    self._enforce_idempotency_contract(existing_event, event, source="legacy-key")

                    # If the existing event is missing kafka_topic (older writers),
                    # allow index routing to be fixed by the incoming envelope.
                    if (
                        isinstance(existing_event.metadata, dict)
                        and isinstance(event.metadata, dict)
                        and not existing_event.metadata.get("kafka_topic")
                        and event.metadata.get("kafka_topic")
                    ):
                        existing_event.metadata["kafka_topic"] = event.metadata.get("kafka_topic")

                    try:
                        await self._update_indexes(existing_event, key)
                    except Exception as e:
                        logger.warning(f"Failed to update indexes for existing event {existing_event.event_id}: {e}")

                    logger.info(
                        f"✅ Event already exists in S3/MinIO (idempotent append): {existing_event.event_id} "
                        f"[{existing_event.event_type}] for {existing_event.aggregate_type}/{existing_event.aggregate_id}"
                    )
                    await self._record_lineage_and_audit(
                        existing_event,
                        s3_key=key,
                        audit_action="EVENT_APPEND_SKIPPED_DUPLICATE",
                    )
                    return existing_event.event_id
                except ClientError as e:
                    code = (e.response.get("Error") or {}).get("Code", "")
                    if str(code) not in {"404", "NoSuchKey", "NotFound"}:
                        raise

                # Store the immutable event
                await self._ensure_sequence_number(event)
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=event.model_dump_json().encode('utf-8'),
                    ContentType='application/json',
                    Metadata={
                        'event-type': event.event_type,
                        'aggregate-type': event.aggregate_type,
                        'aggregate-id': event.aggregate_id,
                        'sequence-number': str(event.sequence_number or ""),
                        'timestamp': dt.isoformat(),
                        'schema-version': event.schema_version,
                    }
                )
                
                logger.info(
                    f"✅ Event stored in S3/MinIO (SSoT): {event.event_id} "
                    f"[{event.event_type}] for {event.aggregate_type}/{event.aggregate_id}"
                )
                
                # Update indexes (derived data). Never fail the append on index errors.
                try:
                    await self._update_indexes(event, key)
                except Exception as e:
                    logger.warning(f"Failed to update indexes for {event.event_id}: {e}")

                await self._record_lineage_and_audit(event, s3_key=key)
                return event.event_id
                
        except Exception as e:
            logger.error(f"Failed to store event in S3/MinIO: {e}")
            try:
                await self._record_audit_failure(event, error=str(e))
            except Exception:
                pass
            raise

    def _enforce_idempotency_contract(self, existing: EventEnvelope, incoming: EventEnvelope, *, source: str) -> None:
        """
        Detect event_id reuse with conflicting contents.

        Why:
        - Deterministic event_id is only safe if event_id is never reused for a different payload.
        - Without this, "same id, different content" can silently no-op (bad observability).

        Only enforced for command envelopes (metadata.kind == "command"), because domain events may be
        re-materialized during retries with non-deterministic timestamps, while still being safe to no-op.
        """
        if not self._is_command_envelope(incoming):
            return

        existing_norm = self._normalize_for_idempotency_compare(existing)
        incoming_norm = self._normalize_for_idempotency_compare(incoming)
        if existing_norm == incoming_norm:
            return

        existing_hash = self._stable_hash(existing_norm)
        incoming_hash = self._stable_hash(incoming_norm)

        mode = os.getenv("EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE", "error").strip().lower()
        message = (
            f"event_id reuse with different command payload (source={source}) "
            f"event_id={incoming.event_id} existing_sha256={existing_hash} incoming_sha256={incoming_hash}"
        )

        if mode in {"warn", "warning", "log"}:
            logger.error(message)
            return

        raise RuntimeError(message)

    @staticmethod
    def _is_command_envelope(env: EventEnvelope) -> bool:
        if not isinstance(env.metadata, dict):
            return False
        return env.metadata.get("kind") == "command"

    @staticmethod
    def _normalize_for_idempotency_compare(env: EventEnvelope) -> Dict[str, Any]:
        doc = env.model_dump(mode="json")
        # sequence_number may be computed from a moving "current version" on retries; ignore for idempotency compare.
        doc.pop("sequence_number", None)
        metadata = doc.get("metadata")
        if isinstance(metadata, dict):
            # kafka_topic may be backfilled for older writers; ignore for idempotency compare.
            metadata.pop("kafka_topic", None)
        return doc

    @staticmethod
    def _stable_hash(payload: Dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    async def _get_existing_key_by_event_id(self, s3, event_id: str) -> Optional[str]:
        index_key = f"indexes/by-event-id/{event_id}.json"
        try:
            idx_obj = await s3.get_object(Bucket=self.bucket_name, Key=index_key)
            idx_data = json.loads((await idx_obj["Body"].read()).decode("utf-8"))
            s3_key = idx_data.get("s3_key")
            return str(s3_key) if s3_key else None
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code", "")
            if str(code) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    async def _get_existing_key_by_aggregate_index(
        self,
        s3,
        *,
        aggregate_type: str,
        aggregate_id: str,
        event_id: str,
    ) -> Optional[str]:
        prefix = f"indexes/by-aggregate/{aggregate_type}/{aggregate_id}/"
        paginator = s3.get_paginator("list_objects_v2")
        suffix = f"_{event_id}.json"
        async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key") or ""
                if not key.endswith(suffix):
                    continue
                idx_obj = await s3.get_object(Bucket=self.bucket_name, Key=key)
                idx_data = json.loads((await idx_obj["Body"].read()).decode("utf-8"))
                s3_key = idx_data.get("s3_key")
                return str(s3_key) if s3_key else None
        return None

    async def _read_event_object(self, s3, key: str) -> EventEnvelope:
        obj = await s3.get_object(Bucket=self.bucket_name, Key=key)
        raw = await obj["Body"].read()
        return EventEnvelope.model_validate_json(raw)

    async def get_event_object_key(self, *, event_id: str) -> Optional[str]:
        """
        Resolve an event_id to its S3 object key using the by-event-id index.

        This is useful for backfill jobs and operational tooling.
        """
        if not self.session:
            self.session = aioboto3.Session()

        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            return await self._get_existing_key_by_event_id(s3, str(event_id))

    async def read_event_by_key(self, *, key: str) -> EventEnvelope:
        """Read an event envelope from S3/MinIO by object key."""
        if not self.session:
            self.session = aioboto3.Session()

        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            return await self._read_event_object(s3, key)
    
    async def get_events(
        self, 
        aggregate_type: str, 
        aggregate_id: str,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[EventEnvelope]:
        """
        Retrieve all events for an aggregate from S3/MinIO.
        This reads from the Single Source of Truth.
        """
        events = []
        
        try:
            if not self.session:
                self.session = aioboto3.Session()

            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Fast path: use aggregate index entries (one object per event).
                index_prefix = f"indexes/by-aggregate/{aggregate_type}/{aggregate_id}/"
                paginator = s3.get_paginator('list_objects_v2')

                index_keys: List[str] = []
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=index_prefix):
                    for obj in page.get("Contents", []):
                        index_keys.append(obj["Key"])

                if index_keys:
                    index_keys.sort()
                    for idx_key in index_keys:
                        filename = idx_key.rsplit("/", 1)[-1]
                        seq_str = filename.split("_", 1)[0]
                        seq = int(seq_str) if seq_str.isdigit() else 0

                        if from_version is not None and seq < from_version:
                            continue
                        if to_version is not None and seq > to_version:
                            continue

                        idx_obj = await s3.get_object(Bucket=self.bucket_name, Key=idx_key)
                        idx_data = json.loads((await idx_obj["Body"].read()).decode("utf-8"))
                        s3_key = idx_data.get("s3_key")
                        if not s3_key:
                            continue

                        ev_obj = await s3.get_object(Bucket=self.bucket_name, Key=s3_key)
                        ev_data = await ev_obj["Body"].read()
                        events.append(EventEnvelope.model_validate_json(ev_data))

                    # Preserve ordering by sequence (or 0 if missing).
                    events.sort(key=lambda e: e.sequence_number or 0)
                    events = self._dedup_events(events)

                    logger.info(
                        f"Retrieved {len(events)} events for "
                        f"{aggregate_type}/{aggregate_id} from S3/MinIO (index)"
                    )
                    return events

                # Slow fallback: scan events/ and filter by path substring.
                prefix = "events/"
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if f"/{aggregate_type}/{aggregate_id}/" not in key:
                            continue
                        response = await s3.get_object(Bucket=self.bucket_name, Key=key)
                        event_data = await response["Body"].read()
                        event = EventEnvelope.model_validate_json(event_data)
                        if from_version is not None and (event.sequence_number or 0) < from_version:
                            continue
                        if to_version is not None and (event.sequence_number or 0) > to_version:
                            continue
                        events.append(event)

                events.sort(key=lambda e: e.sequence_number or 0)
                events = self._dedup_events(events)
                
                logger.info(
                    f"Retrieved {len(events)} events for "
                    f"{aggregate_type}/{aggregate_id} from S3/MinIO"
                )
                
                return events
                
        except Exception as e:
            logger.error(f"Failed to retrieve events from S3/MinIO: {e}")
            raise

    @staticmethod
    def _dedup_events(events: List[EventEnvelope]) -> List[EventEnvelope]:
        """Best-effort dedup to hide historical duplicates during migration."""
        seen: set[str] = set()
        deduped: List[EventEnvelope] = []
        for event in events:
            key = EventStore._dedup_key(event)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(event)
        return deduped

    @staticmethod
    def _dedup_key(event: EventEnvelope) -> str:
        command_id: Optional[str] = None
        if isinstance(event.metadata, dict):
            command_id = event.metadata.get("command_id") or event.metadata.get("commandId")
        if not command_id and isinstance(event.data, dict):
            command_id = event.data.get("command_id") or event.data.get("commandId")

        if command_id:
            return f"cmd:{command_id}:{event.event_type}:{event.aggregate_type}:{event.aggregate_id}"
        return f"id:{event.event_id}"
    
    async def replay_events(
        self,
        from_timestamp: datetime,
        to_timestamp: Optional[datetime] = None,
        event_types: Optional[List[str]] = None
    ) -> AsyncIterator[EventEnvelope]:
        """
        Replay events from S3/MinIO for a time range.
        This is used for system recovery, debugging, or building new projections.
        """
        try:
            if not self.session:
                self.session = aioboto3.Session()

            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Fast path: replay using by-date index entries.
                start = from_timestamp.astimezone(timezone.utc) if from_timestamp.tzinfo else from_timestamp.replace(tzinfo=timezone.utc)
                end_dt = to_timestamp or datetime.now(timezone.utc)
                end = end_dt.astimezone(timezone.utc) if end_dt.tzinfo else end_dt.replace(tzinfo=timezone.utc)

                paginator = s3.get_paginator("list_objects_v2")
                any_index = False

                day = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
                last_day = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)

                while day <= last_day:
                    day_prefix = f"indexes/by-date/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
                    day_keys: List[str] = []
                    async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=day_prefix):
                        for obj in page.get("Contents", []):
                            day_keys.append(obj["Key"])

                    if day_keys:
                        any_index = True
                        day_keys.sort()

                        for idx_key in day_keys:
                            filename = idx_key.rsplit("/", 1)[-1]
                            ts_str = filename.split("_", 1)[0]
                            if not ts_str.isdigit():
                                continue

                            ts_ms = int(ts_str)
                            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

                            if ts < start:
                                continue
                            if ts > end:
                                continue

                            idx_obj = await s3.get_object(Bucket=self.bucket_name, Key=idx_key)
                            idx_data = json.loads((await idx_obj["Body"].read()).decode("utf-8"))

                            if event_types:
                                idx_type = idx_data.get("event_type")
                                if idx_type not in event_types:
                                    continue

                            s3_key = idx_data.get("s3_key")
                            if not s3_key:
                                continue

                            ev_obj = await s3.get_object(Bucket=self.bucket_name, Key=s3_key)
                            ev_data = await ev_obj["Body"].read()
                            yield EventEnvelope.model_validate_json(ev_data)

                    day += timedelta(days=1)

                if any_index:
                    return

                # Slow fallback (no indexes): scan by year prefix like legacy code.
                prefix = f"events/{start.year:04d}/"
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        response = await s3.get_object(Bucket=self.bucket_name, Key=key)
                        event_data = await response["Body"].read()
                        event = EventEnvelope.model_validate_json(event_data)
                        if event.occurred_at < start:
                            continue
                        if event.occurred_at > end:
                            continue
                        if event_types and event.event_type not in event_types:
                            continue
                        yield event
                        
        except Exception as e:
            logger.error(f"Failed to replay events from S3/MinIO: {e}")
            raise
    
    async def get_aggregate_version(
        self,
        aggregate_type: str,
        aggregate_id: str
    ) -> int:
        """
        Get the current version of an aggregate (max sequence_number).
        """
        if not self.session:
            self.session = aioboto3.Session()

        index_prefix = f"indexes/by-aggregate/{aggregate_type}/{aggregate_id}/"
        max_seq = 0
        found = False

        try:
            async with self.session.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False,
            ) as s3:
                paginator = s3.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=index_prefix):
                    for obj in page.get("Contents", []):
                        found = True
                        key = obj.get("Key") or ""
                        filename = key.rsplit("/", 1)[-1]
                        seq_str = filename.split("_", 1)[0]
                        if seq_str.isdigit():
                            seq = int(seq_str)
                            if seq > max_seq:
                                max_seq = seq

            if found:
                return max_seq
        except Exception as e:
            logger.warning(f"Failed to read aggregate index for {aggregate_type}/{aggregate_id}: {e}")

        # Fallback: legacy streams without indexes (slow).
        events = await self.get_events(aggregate_type, aggregate_id)
        return max((e.sequence_number or 0) for e in events) if events else 0
    
    async def _update_indexes(self, event: EventEnvelope, key: str):
        """
        Update various indexes for efficient querying.
        Indexes are derived data, not the source of truth.
        """
        if not self.session:
            self.session = aioboto3.Session()

        dt = (
            event.occurred_at
            if event.occurred_at.tzinfo
            else event.occurred_at.replace(tzinfo=timezone.utc)
        ).astimezone(timezone.utc)
        ts_ms = int(dt.timestamp() * 1000)
        seq = event.sequence_number if event.sequence_number is not None else 0

        index_entry = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": event.aggregate_id,
            "sequence_number": event.sequence_number,
            "occurred_at": dt.isoformat(),
            "s3_key": key,
            "schema_version": event.schema_version,
            "kafka_topic": event.metadata.get("kafka_topic") if isinstance(event.metadata, dict) else None,
        }

        aggregate_index_key = (
            f"indexes/by-aggregate/{event.aggregate_type}/{event.aggregate_id}/"
            f"{seq:020d}_{event.event_id}.json"
        )

        date_index_key = (
            f"indexes/by-date/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
            f"{ts_ms:013d}_{event.event_id}.json"
        )

        event_id_index_key = f"indexes/by-event-id/{event.event_id}.json"

        try:
            async with self.session.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False,
            ) as s3:
                body = json.dumps(index_entry).encode("utf-8")

                # Independent objects -> concurrency-safe, no read/modify/write races.
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=aggregate_index_key,
                    Body=body,
                    ContentType="application/json",
                )
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=date_index_key,
                    Body=body,
                    ContentType="application/json",
                )
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=event_id_index_key,
                    Body=body,
                    ContentType="application/json",
                )

                logger.debug(
                    f"Updated indexes for {event.event_id}: {aggregate_index_key} / {date_index_key}"
                )
        except Exception as e:
            logger.warning(f"Failed to update indexes: {e}")
            # Index update failures don't fail the event append
    
    async def get_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a snapshot of an aggregate at a specific version.
        Snapshots are optional optimizations, not the source of truth.
        """
        snapshot_key = (
            f"snapshots/{aggregate_type}/{aggregate_id}/"
            f"v{version or 'latest'}.json"
        )
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                response = await s3.get_object(
                    Bucket=self.bucket_name,
                    Key=snapshot_key
                )
                
                snapshot_data = await response['Body'].read()
                return json.loads(snapshot_data)
                
        except ClientError:
            return None
    
    async def save_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: str,
        version: int,
        state: Dict[str, Any]
    ):
        """
        Save a snapshot for performance optimization.
        Remember: Events are the truth, snapshots are just caches.
        """
        snapshot_key = f"snapshots/{aggregate_type}/{aggregate_id}/v{version}.json"
        latest_key = f"snapshots/{aggregate_type}/{aggregate_id}/vlatest.json"
        
        snapshot = {
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "version": version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "state": state
        }
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Save versioned snapshot
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=snapshot_key,
                    Body=json.dumps(snapshot).encode('utf-8'),
                    ContentType='application/json'
                )
                
                # Update latest snapshot
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=latest_key,
                    Body=json.dumps(snapshot).encode('utf-8'),
                    ContentType='application/json'
                )
                
                logger.info(
                    f"Saved snapshot for {aggregate_type}/{aggregate_id} "
                    f"at version {version}"
                )
                
        except Exception as e:
            logger.warning(f"Failed to save snapshot: {e}")
            # Snapshot failures don't affect the system


# Global instance
event_store = EventStore()


async def get_event_store() -> EventStore:
    """Dependency to get the Event Store instance"""
    return event_store
