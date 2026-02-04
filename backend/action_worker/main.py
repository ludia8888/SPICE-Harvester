"""
Action Worker Service

Consumes ActionCommand envelopes from Kafka and executes Action-only writeback:
- Computes a server-side patchset (intent-only inputs)
- Writes the patchset to lakeFS (commit-addressed)
- Updates ActionLog state machine (Postgres)
- Emits ActionApplied (SSoT Event Store -> Kafka via message relay)
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Producer, TopicPartition

from shared.services.kafka.safe_consumer import SafeKafkaConsumer

from oms.services.async_terminus import AsyncTerminusService
from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.enterprise_catalog import is_external_code, resolve_enterprise_error
from shared.errors.error_types import ErrorCode
from shared.models.event_envelope import EventEnvelope
from shared.models.events import ActionAppliedEvent
from shared.observability.context_propagation import attach_context_from_kafka, kafka_headers_from_current_context
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.security.database_access import DOMAIN_MODEL_ROLES, get_database_access_role
from shared.services.registries.action_log_registry import ActionLogRegistry, ActionLogStatus
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.event_store import event_store
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service, LakeFSStorageService
from shared.services.kafka.processed_event_worker import HeartbeatOptions, ProcessedEventKafkaWorker, RegistryKey
from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
    validate_registry_enabled,
    validate_lease_settings,
)
from shared.services.storage.storage_service import StorageService, create_storage_service
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.action_input_schema import (
    ActionInputSchemaError,
    ActionInputValidationError,
    validate_action_input,
)
from shared.utils.action_audit_policy import audit_action_log_result
from shared.utils.access_policy import apply_access_policy
from shared.utils.principal_policy import build_principal_tags, policy_allows
from shared.utils.resource_rid import format_resource_rid, parse_metadata_rev, strip_rid_revision
from shared.utils.writeback_conflicts import (
    compute_base_token,
    compute_observed_base,
    detect_overlap_fields,
    detect_overlap_links,
    parse_conflict_policy,
    resolve_applied_changes,
)
from shared.utils.action_template_engine import (
    ActionImplementationError,
    compile_template_v1,
    compile_template_v1_change_shape,
)
from shared.utils.safe_bool_expression import BoolExpressionError, safe_eval_bool_expression
from shared.utils.submission_criteria_diagnostics import infer_submission_criteria_failure_reason
from shared.utils.writeback_governance import extract_backing_dataset_id, policies_aligned
from shared.utils.writeback_paths import (
    queue_entry_key,
    ref_key,
    writeback_patchset_key,
    writeback_patchset_metadata_key,
)
from shared.utils.writeback_lifecycle import derive_lifecycle_id
from shared.utils.action_writeback import action_applied_event_id, is_noop_changes, safe_str
from shared.utils.app_logger import configure_logging
from shared.utils.time_utils import utcnow

_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)


@dataclass
class _ActionCommandPayload:
    envelope: EventEnvelope
    raw_text: Optional[str]
    envelope_metadata: Optional[Dict[str, Any]] = None
    stage: str = "execute_action"


class _ActionCommandParseError(ValueError):
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


class _ActionRejected(Exception):
    """Used to short-circuit retries when the ActionLog is already finalized with a rejection result."""


class ActionWorker(ProcessedEventKafkaWorker[_ActionCommandPayload, None]):
    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.action

        self.running = False
        self.service_name = "action-worker"
        self.handler = "action_worker"
        self.tracing = get_tracing_service("action-worker")
        self.metrics = get_metrics_collector("action-worker")
        self.kafka_servers = settings.database.kafka_servers
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.dlq_topic = AppConfig.ACTION_COMMANDS_DLQ_TOPIC
        self.dlq_flush_timeout_seconds = float(cfg.dlq_flush_timeout_seconds)
        self.max_retry_attempts = int(cfg.max_retry_attempts)
        self.max_retries = int(self.max_retry_attempts)
        self.backoff_base = 1
        self.backoff_max = 60

        self.enable_processed_event_registry = settings.event_sourcing.enable_processed_event_registry
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None

        self.action_logs = ActionLogRegistry()
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.lakefs_client: Optional[LakeFSClient] = None
        self.lakefs_storage: Optional[LakeFSStorageService] = None
        self.base_storage: Optional[StorageService] = None
        self.terminus: Optional[AsyncTerminusService] = None

    async def initialize(self) -> None:
        validate_registry_enabled()
        validate_lease_settings()

        group_id = (AppConfig.ACTION_WORKER_GROUP or "action-worker-group").strip()
        topic = AppConfig.ACTION_COMMANDS_TOPIC
        # Use SafeKafkaConsumer for strong consistency guarantees
        self.consumer = SafeKafkaConsumer(
            group_id=group_id,
            topics=[topic],
            service_name="action-worker",
            max_poll_interval_ms=300000,
            session_timeout_ms=45000,
            on_revoke=self._on_partitions_revoked,
            on_assign=self._on_partitions_assigned,
        )
        self._rebalance_in_progress = False
        logger.info("ActionWorker subscribed to topic=%s group=%s", topic, group_id)

        settings = get_settings()
        cfg = settings.workers.action
        service_name = settings.observability.service_name or "action-worker-dlq"
        self.dlq_producer = Producer(
            {
                "bootstrap.servers": self.kafka_servers,
                "client.id": service_name,
                "acks": "all",
                "retries": int(cfg.dlq_retries),
                "retry.backoff.ms": 250,
                "linger.ms": 10,
                "compression.type": "snappy",
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
            }
        )

        self.processed_event_registry = ProcessedEventRegistry()
        await self.processed_event_registry.connect()
        self.processed = self.processed_event_registry

        await event_store.connect()
        await self.action_logs.connect()
        if AppConfig.WRITEBACK_ENFORCE_GOVERNANCE:
            self.dataset_registry = DatasetRegistry()
            await self.dataset_registry.connect()

        settings = get_settings()
        self.lakefs_client = LakeFSClient()
        self.lakefs_storage = create_lakefs_storage_service(settings)
        if not self.lakefs_storage:
            raise RuntimeError("LakeFSStorageService unavailable (boto3 missing?)")

        self.base_storage = create_storage_service(settings)
        if not self.base_storage:
            raise RuntimeError("StorageService unavailable (boto3 missing?)")

        # TerminusDB client (for loading action definitions).
        from shared.models.config import ConnectionConfig

        connection_info = ConnectionConfig(
            server_url=settings.database.terminus_url.rstrip("/"),
            user=settings.database.terminus_user,
            account=settings.database.terminus_account,
            key=settings.database.terminus_password,
        )
        self.terminus = AsyncTerminusService(connection_info)

    def _on_partitions_revoked(self, partitions: list) -> None:
        """Handle partition revocation during rebalance."""
        self._rebalance_in_progress = True
        logger.info(
            "Action worker partitions revoked: %s",
            [(p.topic, p.partition) for p in partitions],
        )

    def _on_partitions_assigned(self, partitions: list) -> None:
        """Handle partition assignment during rebalance."""
        self._rebalance_in_progress = False
        logger.info(
            "Action worker partitions assigned: %s",
            [(p.topic, p.partition) for p in partitions],
        )

    async def shutdown(self) -> None:
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.dlq_producer:
            try:
                await asyncio.to_thread(self.dlq_producer.flush, self.dlq_flush_timeout_seconds)
            except Exception as exc:
                logger.warning("DLQ producer flush failed during shutdown: %s", exc, exc_info=True)
        if self.processed_event_registry:
            await self.processed_event_registry.close()
        await self.action_logs.close()
        if self.dataset_registry:
            await self.dataset_registry.close()
        if self.terminus:
            await self.terminus.close()

    async def _poll(self, timeout: float) -> Any:
        if not self.consumer:
            return None
        return await asyncio.to_thread(self.consumer.poll, timeout)

    async def _commit(self, msg: Any) -> None:
        if not self.consumer:
            return
        await asyncio.to_thread(self.consumer.commit_sync, msg)

    def _heartbeat_options(self) -> HeartbeatOptions:  # type: ignore[override]
        return HeartbeatOptions(
            stop_when_false=True,
            continue_on_exception=False,
        )

    def _parse_payload(self, payload: Any) -> _ActionCommandPayload:  # type: ignore[override]
        if not isinstance(payload, (bytes, bytearray)):
            raise _ActionCommandParseError(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                fallback_metadata=None,
                cause=TypeError("Kafka payload must be bytes"),
            )
        try:
            raw_text = payload.decode("utf-8")
        except Exception as exc:
            raise _ActionCommandParseError(
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
            raise _ActionCommandParseError(
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
            raise _ActionCommandParseError(
                stage="parse_envelope",
                payload_text=raw_text,
                payload_obj=payload_obj,
                fallback_metadata=fallback_metadata,
                cause=exc,
            ) from exc

        return _ActionCommandPayload(
            envelope=envelope,
            raw_text=raw_text,
            envelope_metadata=envelope.metadata if isinstance(envelope.metadata, dict) else fallback_metadata,
        )

    def _fallback_metadata(self, payload: _ActionCommandPayload) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        return payload.envelope_metadata

    def _registry_key(self, payload: _ActionCommandPayload) -> RegistryKey:  # type: ignore[override]
        envelope = payload.envelope
        event_id = str(envelope.event_id or "").strip()
        if not event_id:
            raise ValueError("event_id is required")
        aggregate_id = str(envelope.aggregate_id).strip() if envelope.aggregate_id is not None else None
        if not aggregate_id:
            aggregate_id = None
        sequence_number = int(envelope.sequence_number) if envelope.sequence_number is not None else None
        return RegistryKey(event_id=event_id, aggregate_id=aggregate_id, sequence_number=sequence_number)

    async def _process_payload(self, payload: _ActionCommandPayload) -> None:  # type: ignore[override]
        envelope = payload.envelope
        meta = envelope.metadata if isinstance(envelope.metadata, dict) else {}
        if meta.get("kind") != "command":
            logger.info(
                "Skipping non-command envelope event_id=%s type=%s",
                envelope.event_id,
                envelope.event_type,
            )
            return

        command_data = envelope.data if isinstance(envelope.data, dict) else {}
        command_type = command_data.get("command_type")
        command_type = getattr(command_type, "value", command_type)
        if str(command_type) != "EXECUTE_ACTION":
            logger.info("Skipping non-action command_type=%s event_id=%s", command_type, envelope.event_id)
            return

        payload.stage = "validate_command"
        action_log_id = safe_str(command_data.get("action_log_id"))
        db_name = safe_str(command_data.get("db_name"))
        if not action_log_id or not db_name:
            raise ValueError("action_log_id and db_name are required on ActionCommand")

        payload.stage = "execute_action"
        try:
            await self._execute_action(
                db_name=db_name,
                action_log_id=action_log_id,
                command=command_data,
                envelope=envelope,
            )
        except _ActionRejected:
            return

    def _span_name(self, *, payload: _ActionCommandPayload) -> str:  # type: ignore[override]
        return "action_worker.process_message"

    def _span_attributes(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: _ActionCommandPayload,
        registry_key: RegistryKey,
    ) -> Dict[str, Any]:
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        envelope = payload.envelope
        command_data = envelope.data if isinstance(envelope.data, dict) else {}
        attrs.update(
            {
                "messaging.operation": "process",
                "event.type": str(envelope.event_type or ""),
                "event.aggregate_type": str(envelope.aggregate_type or ""),
                "action_log_id": safe_str(command_data.get("action_log_id")) or None,
                "db.name": safe_str(command_data.get("db_name")) or None,
            }
        )
        return attrs

    def _metric_event_name(self, *, payload: _ActionCommandPayload) -> Optional[str]:  # type: ignore[override]
        return str(payload.envelope.event_type or "").strip() or None

    async def _seek(self, *, topic: str, partition: int, offset: int) -> None:  # type: ignore[override]
        if not self.consumer:
            return
        await asyncio.to_thread(self.consumer.seek, TopicPartition(topic, partition, offset))

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:  # type: ignore[override]
        stage = "parse"
        payload_text = raw_payload
        payload_obj = None
        fallback_metadata = None
        cause = error

        if isinstance(error, _ActionCommandParseError):
            stage = error.stage
            payload_text = error.payload_text if error.payload_text is not None else raw_payload
            payload_obj = error.payload_obj
            fallback_metadata = error.fallback_metadata
            cause = error.cause

        kafka_headers = None
        with suppress(Exception):
            kafka_headers = msg.headers()

        try:
            await self._send_to_dlq(
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
            logger.error("Failed to publish invalid action payload to DLQ; retrying: %s", dlq_err, exc_info=True)
            raise

        logger.exception("Invalid action payload; skipping: %s", cause)

    async def _on_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: _ActionCommandPayload,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        logger.warning(
            "Retrying action command in %ss (attempt %s): %s",
            int(backoff_s),
            attempt_count,
            error,
        )

    async def _on_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: _ActionCommandPayload,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        logger.error(
            "Action command failed after %s attempts (retryable=%s): %s",
            attempt_count,
            retryable,
            error,
        )

    @staticmethod
    def _is_retryable_error(exc: BaseException, *, payload: Optional[_ActionCommandPayload] = None) -> bool:
        if isinstance(exc, _ActionRejected):
            return False
        if isinstance(exc, PermissionError):
            return False
        if isinstance(exc, ValueError):
            return False
        msg = str(exc).lower()
        non_retryable_markers = [
            "permission denied",
            "validation",
            "schema",
            "bad request",
            "invalid",
            "missing",
        ]
        if any(marker in msg for marker in non_retryable_markers):
            return False
        retryable_markers = [
            "timeout",
            "temporarily",
            "unavailable",
            "connection",
            "reset",
            "broken pipe",
            "429",
            "rate limit",
            "too many requests",
            "500",
            "502",
            "503",
            "504",
        ]
        if any(marker in msg for marker in retryable_markers):
            return True
        # Default to retryable: unknown failures are more likely to be infra/transient.
        return True

    async def _publish_to_dlq(
        self,
        *,
        msg: Any,
        stage: str,
        error: str,
        attempt_count: int,
        payload_text: Optional[str],
        payload_obj: Optional[dict[str, Any]],
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.dlq_producer:
            raise RuntimeError("DLQ producer not configured")

        try:
            original_value = payload_text if payload_text is not None else msg.value().decode("utf-8", errors="replace")
        except Exception:
            original_value = "<unavailable>"

        dlq_message: dict[str, Any] = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "original_timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "original_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
            "original_value": original_value,
            "stage": stage,
            "error": (error or "").strip()[:4000],
            "attempt_count": int(attempt_count),
            "worker": "action-worker",
            "timestamp": utcnow().isoformat(),
        }
        if payload_obj is not None:
            dlq_message["parsed_payload"] = payload_obj

        key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}".encode("utf-8")
        value = json.dumps(dlq_message, ensure_ascii=False, default=str).encode("utf-8")

        with attach_context_from_kafka(
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata if isinstance(fallback_metadata, dict) else None,
            service_name=self.service_name,
        ):
            headers = kafka_headers_from_current_context()
            with self.tracing.span(
                "action_worker.dlq_produce",
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
            self.metrics.record_event("ACTION_COMMAND_DLQ", action="published")
        except Exception:
            pass

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        error: str,
        attempt_count: int,
        payload: Optional[_ActionCommandPayload] = None,
        raw_payload: Optional[str] = None,
        stage: str = "execute_action",
        payload_text: Optional[str] = None,
        payload_obj: Optional[dict[str, Any]] = None,
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if kafka_headers is None:
            with suppress(Exception):
                kafka_headers = msg.headers()

        stage_final = str(stage or "").strip() or "execute_action"
        if payload is not None and stage_final == "execute_action":
            stage_final = str(payload.stage or stage_final).strip() or stage_final

        if payload_text is None:
            payload_text = raw_payload

        if payload_obj is None and payload is not None:
            payload_obj = payload.envelope.model_dump(mode="json")

        if fallback_metadata is None and payload is not None:
            fallback_metadata = payload.envelope_metadata

        await self._publish_to_dlq(
            msg=msg,
            stage=stage_final,
            error=error,
            attempt_count=int(attempt_count),
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
        )

    async def run(self) -> None:
        self.running = True
        await self.run_loop(poll_timeout=1.0, idle_sleep=None)

    async def _enforce_permission(
        self,
        *,
        db_name: str,
        submitted_by: Optional[str],
        submitted_by_type: str = "user",
        action_spec: Dict[str, Any],
    ) -> Optional[str]:
        # Minimal P0: enforce DB role + action permission_policy (best-effort).
        actor = (submitted_by or "").strip()
        if not actor or actor == "system":
            return None

        actor_type = str(submitted_by_type or "user").strip().lower() or "user"
        role = await get_database_access_role(db_name=db_name, principal_type=actor_type, principal_id=actor)
        if role not in DOMAIN_MODEL_ROLES:
            raise PermissionError("Permission denied")

        policy = action_spec.get("permission_policy")
        tags = build_principal_tags(principal_type=actor_type, principal_id=actor, role=role)
        if not policy_allows(policy=policy, principal_tags=tags):
            raise PermissionError("Permission denied")
        return role

    async def _check_writeback_dataset_acl_alignment(
        self,
        *,
        db_name: str,
        submitted_by: str,
        submitted_by_type: str,
        actor_role: Optional[str],
        ontology_commit_id: str,
        resources: OntologyResourceService,
        class_ids: set[str],
    ) -> Optional[Dict[str, Any]]:
        if not AppConfig.WRITEBACK_ENFORCE_GOVERNANCE:
            return None
        if not class_ids:
            return None
        if not self.dataset_registry:
            return {
                "error": "writeback_governance_unavailable",
                "message": "DatasetRegistry not initialized (WRITEBACK_ENFORCE_GOVERNANCE=true)",
            }

        scope = AppConfig.WRITEBACK_DATASET_ACL_SCOPE
        writeback_dataset_id = AppConfig.ONTOLOGY_WRITEBACK_DATASET_ID
        if not writeback_dataset_id:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "ONTOLOGY_WRITEBACK_DATASET_ID is required when WRITEBACK_ENFORCE_GOVERNANCE=true",
                "scope": scope,
            }

        try:
            writeback_dataset = await self.dataset_registry.get_dataset(dataset_id=writeback_dataset_id)
        except Exception as exc:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Failed to load writeback dataset",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
                "detail": str(exc),
            }
        if not writeback_dataset:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Writeback dataset not found",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
            }
        if writeback_dataset.db_name != db_name:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Writeback dataset db_name mismatch",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
                "writeback_db_name": writeback_dataset.db_name,
                "expected_db_name": db_name,
            }

        try:
            writeback_acl = await self.dataset_registry.get_access_policy(
                db_name=db_name,
                scope=scope,
                subject_type="dataset",
                subject_id=writeback_dataset_id,
            )
        except Exception as exc:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Failed to load writeback dataset ACL policy",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
                "detail": str(exc),
            }
        if not writeback_acl:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Writeback dataset ACL policy missing",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
            }
        if not isinstance(writeback_acl.policy, dict) or not writeback_acl.policy:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Writeback dataset ACL policy is empty",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
            }

        actor_id = str(submitted_by or "").strip()
        if actor_id and actor_id != "system":
            tags = build_principal_tags(
                principal_type=submitted_by_type,
                principal_id=actor_id,
                role=actor_role,
            )
            if not policy_allows(policy=writeback_acl.policy, principal_tags=tags):
                return {
                    "error": "writeback_acl_denied",
                    "message": "Actor is not allowed by writeback dataset ACL",
                    "scope": scope,
                    "writeback_dataset_id": writeback_dataset_id,
                    "submitted_by": actor_id,
                    "role": actor_role,
                }

        for class_id in sorted({cid for cid in class_ids if cid}):
            try:
                object_resource = await resources.get_resource(
                    db_name,
                    branch=ontology_commit_id,
                    resource_type="object_type",
                    resource_id=class_id,
                )
            except Exception as exc:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Failed to load object_type resource for writeback governance checks",
                    "scope": scope,
                    "class_id": class_id,
                    "detail": str(exc),
                }
            obj_spec = object_resource.get("spec") if isinstance(object_resource, dict) else None
            backing_dataset_id = extract_backing_dataset_id(obj_spec)
            if not backing_dataset_id:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "object_type.spec.backing_source.dataset_id is required for writeback governance checks",
                    "scope": scope,
                    "class_id": class_id,
                }

            try:
                backing_dataset = await self.dataset_registry.get_dataset(dataset_id=backing_dataset_id)
            except Exception as exc:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Failed to load backing dataset for writeback governance checks",
                    "scope": scope,
                    "class_id": class_id,
                    "backing_dataset_id": backing_dataset_id,
                    "detail": str(exc),
                }
            if not backing_dataset:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Backing dataset not found",
                    "scope": scope,
                    "class_id": class_id,
                    "backing_dataset_id": backing_dataset_id,
                }
            if backing_dataset.db_name != db_name:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Backing dataset db_name mismatch",
                    "scope": scope,
                    "class_id": class_id,
                    "backing_dataset_id": backing_dataset_id,
                    "backing_db_name": backing_dataset.db_name,
                    "expected_db_name": db_name,
                }

            try:
                backing_acl = await self.dataset_registry.get_access_policy(
                    db_name=db_name,
                    scope=scope,
                    subject_type="dataset",
                    subject_id=backing_dataset_id,
                )
            except Exception as exc:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Failed to load backing dataset ACL policy",
                    "scope": scope,
                    "class_id": class_id,
                    "backing_dataset_id": backing_dataset_id,
                    "detail": str(exc),
                }
            if not backing_acl:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Backing dataset ACL policy missing",
                    "scope": scope,
                    "class_id": class_id,
                    "backing_dataset_id": backing_dataset_id,
                }
            if not isinstance(backing_acl.policy, dict) or not backing_acl.policy:
                return {
                    "error": "writeback_acl_unverifiable",
                    "message": "Backing dataset ACL policy is empty",
                    "scope": scope,
                    "class_id": class_id,
                    "backing_dataset_id": backing_dataset_id,
                }

            if not policies_aligned(backing_acl.policy, writeback_acl.policy):
                return {
                    "error": "writeback_acl_misaligned",
                    "message": "Writeback dataset ACL must match backing dataset ACL",
                    "scope": scope,
                    "class_id": class_id,
                    "writeback_dataset_id": writeback_dataset_id,
                    "backing_dataset_id": backing_dataset_id,
                }

        return None

    async def _execute_action(
        self,
        *,
        db_name: str,
        action_log_id: str,
        command: Dict[str, Any],
        envelope: EventEnvelope,
    ) -> None:
        if not self.lakefs_client or not self.lakefs_storage or not self.base_storage or not self.terminus:
            raise RuntimeError("ActionWorker not initialized")

        log_rec = await self.action_logs.get_log(action_log_id=action_log_id)
        if not log_rec:
            raise RuntimeError(f"ActionLog not found (action_log_id={action_log_id})")

        if log_rec.status in {ActionLogStatus.SUCCEEDED.value, ActionLogStatus.FAILED.value}:
            return

        submitted_by = safe_str(log_rec.submitted_by)
        if not submitted_by:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result={"error": "submitted_by_required", "message": "submitted_by is required for action execution"},
            )
            raise _ActionRejected("submitted_by_required")

        # Load action definition at the deployed commit.
        action_type_id = log_rec.action_type_id
        ontology_commit_id = log_rec.ontology_commit_id or safe_str(command.get("ontology_commit_id"))
        if not ontology_commit_id:
            raise RuntimeError("ontology_commit_id is required for action execution")
        resources = OntologyResourceService(self.terminus)
        action_resource = await resources.get_resource(
            db_name,
            branch=ontology_commit_id,
            resource_type="action_type",
            resource_id=action_type_id,
        )
        spec = action_resource.get("spec") if isinstance(action_resource, dict) else None
        if not isinstance(spec, dict):
            spec = {}
        action_meta = action_resource.get("metadata") if isinstance(action_resource, dict) else None
        action_type_rid = format_resource_rid(
            resource_type="action_type",
            resource_id=action_type_id,
            rev=parse_metadata_rev(action_meta),
        )

        submitted_by_type = str((log_rec.metadata or {}).get("user_type") or "user").strip().lower() or "user"
        actor_role = await self._enforce_permission(
            db_name=db_name,
            submitted_by=log_rec.submitted_by,
            submitted_by_type=submitted_by_type,
            action_spec=spec,
        )
        audit_policy = spec.get("audit_policy")

        def _audit_result(payload: Dict[str, Any]) -> Dict[str, Any]:
            enriched = dict(payload or {})
            error_key = str(enriched.get("error") or "").strip()
            if error_key and "enterprise" not in enriched:
                enterprise = None
                if is_external_code(error_key):
                    enterprise = resolve_enterprise_error(
                        service_name="action-worker",
                        code=None,
                        category=None,
                        status_code=400,
                        external_code=error_key,
                    ).to_dict()
                else:
                    with suppress(Exception):
                        enterprise = resolve_enterprise_error(
                            service_name="action-worker",
                            code=ErrorCode(error_key),
                            category=None,
                            status_code=400,
                            external_code=None,
                        ).to_dict()
                if enterprise is not None:
                    enriched["enterprise"] = enterprise
            return audit_action_log_result(enriched, audit_policy=audit_policy)

        writeback_target = log_rec.writeback_target or {
            "repo": AppConfig.ONTOLOGY_WRITEBACK_REPO,
            "branch": AppConfig.get_ontology_writeback_branch(db_name),
        }
        repo = str(writeback_target.get("repo") or "").strip()
        branch = str(writeback_target.get("branch") or "").strip()
        if not repo or not branch:
            raise RuntimeError("writeback_target.repo and writeback_target.branch are required")

        patchset_commit_id = log_rec.writeback_commit_id
        if log_rec.status == ActionLogStatus.PENDING.value:
            try:
                # Compute patchset from intent-only input.
                raw_payload = command.get("payload") if isinstance(command, dict) else None
                if not isinstance(raw_payload, dict):
                    raw_payload = dict(log_rec.input or {})
                input_payload = dict(raw_payload or {})
                try:
                    input_payload = validate_action_input(input_schema=spec.get("input_schema"), payload=input_payload)
                except ActionInputValidationError as exc:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                            "error": "action_input_invalid",
                            "message": str(exc),
                            }
                        ),
                    )
                    raise _ActionRejected("action_input_invalid") from exc
                except ActionInputSchemaError as exc:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                            "error": "action_type_input_schema_invalid",
                            "message": str(exc),
                            }
                        ),
                    )
                    raise _ActionRejected("action_type_input_schema_invalid") from exc

                implementation = spec.get("implementation")
                try:
                    compiled_shape = compile_template_v1_change_shape(
                        implementation,
                        input_payload=input_payload,
                    )
                except ActionImplementationError as exc:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                            "error": "action_implementation_invalid",
                            "message": str(exc),
                            }
                        ),
                    )
                    raise _ActionRejected("action_implementation_invalid") from exc

                if not compiled_shape:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                            "error": "action_no_targets",
                            "message": "template_v1 resolved to zero targets",
                            }
                        ),
                    )
                    raise _ActionRejected("action_no_targets")
                base_branch = safe_str(command.get("base_branch") or "main") or "main"

                governance_error = await self._check_writeback_dataset_acl_alignment(
                    db_name=db_name,
                    submitted_by=submitted_by,
                    submitted_by_type=submitted_by_type,
                    actor_role=actor_role,
                    ontology_commit_id=ontology_commit_id,
                    resources=resources,
                    class_ids={t.class_id for t in compiled_shape if t.class_id},
                )
                if governance_error:
                    await self.action_logs.mark_failed(action_log_id=action_log_id, result=_audit_result(governance_error))
                    raise _ActionRejected("writeback_governance_rejected")

                submission_snapshot = log_rec.metadata.get("__writeback_submission")
                submission_targets: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
                if isinstance(submission_snapshot, dict):
                    for item in submission_snapshot.get("targets") or []:
                        if not isinstance(item, dict):
                            continue
                        sid_class = safe_str(item.get("class_id"))
                        sid_instance = safe_str(item.get("instance_id"))
                        sid_lifecycle = safe_str(item.get("lifecycle_id") or "lc-0") or "lc-0"
                        if not sid_class or not sid_instance:
                            continue
                        submission_targets[(sid_class, sid_instance, sid_lifecycle)] = item

                action_conflict_policy = parse_conflict_policy(spec.get("conflict_policy"))
                object_type_meta_cache: Dict[str, Dict[str, Any]] = {}

                async def _get_object_type_meta(class_id: str) -> Dict[str, Any]:
                    cached = object_type_meta_cache.get(class_id)
                    if isinstance(cached, dict):
                        return cached

                    meta: Dict[str, Any] = {"conflict_policy": None, "rev": 1}
                    try:
                        object_resource = await resources.get_resource(
                            db_name,
                            branch=ontology_commit_id,
                            resource_type="object_type",
                            resource_id=class_id,
                        )
                        obj_spec = object_resource.get("spec") if isinstance(object_resource, dict) else None
                        if isinstance(obj_spec, dict):
                            meta["conflict_policy"] = parse_conflict_policy(obj_spec.get("conflict_policy"))
                        obj_meta = object_resource.get("metadata") if isinstance(object_resource, dict) else None
                        meta["rev"] = parse_metadata_rev(obj_meta)
                    except Exception:
                        pass

                    object_type_meta_cache[class_id] = meta
                    return meta

                def _is_public_identifier(value: Any) -> bool:
                    text = str(value or "").strip()
                    return bool(text) and text.isidentifier() and not text.startswith("_")

                loaded_targets: List[Dict[str, Any]] = []
                target_docs: Dict[Tuple[str, str], Dict[str, Any]] = {}
                for item in compiled_shape:
                    class_id = safe_str(item.class_id)
                    instance_id = safe_str(item.instance_id)
                    if not class_id or not instance_id:
                        raise ValueError("each compiled target requires class_id and instance_id")

                    prefix = f"{db_name}/{base_branch}/{class_id}/{instance_id}/"
                    command_files = await self.base_storage.list_command_files(
                        bucket=AppConfig.INSTANCE_BUCKET,
                        prefix=prefix,
                    )
                    base_state = await self.base_storage.replay_instance_state(
                        bucket=AppConfig.INSTANCE_BUCKET,
                        command_files=command_files,
                    )
                    if not base_state:
                        raise RuntimeError(f"Base instance state not found (prefix={prefix})")

                    target_docs[(class_id, instance_id)] = base_state

                user_ctx: Dict[str, Any] = {"id": submitted_by, "role": actor_role, "is_system": submitted_by == "system"}
                try:
                    compiled_targets = compile_template_v1(
                        implementation,
                        input_payload=input_payload,
                        user=user_ctx,
                        target_docs=target_docs,
                        now=utcnow(),
                    )
                except ActionImplementationError as exc:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                            "error": "action_implementation_compile_error",
                            "message": str(exc),
                            }
                        ),
                    )
                    raise _ActionRejected("action_implementation_compile_error") from exc

                changes_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {
                    (t.class_id, t.instance_id): dict(t.changes or {}) for t in compiled_targets
                }

                for item in compiled_shape:
                    class_id = safe_str(item.class_id)
                    instance_id = safe_str(item.instance_id)
                    base_state = target_docs.get((class_id, instance_id))
                    if not isinstance(base_state, dict) or not base_state:
                        raise RuntimeError("base_state missing for compiled target")
                    changes = changes_by_key.get((class_id, instance_id))
                    if not isinstance(changes, dict):
                        raise RuntimeError("compiled changes missing for target")

                    lifecycle_id = derive_lifecycle_id(base_state)
                    obj_meta = await _get_object_type_meta(class_id)
                    object_type_rid = format_resource_rid(
                        resource_type="object_type",
                        resource_id=class_id,
                        rev=obj_meta.get("rev") if isinstance(obj_meta, dict) else None,
                    )

                    submission_key = (class_id, instance_id, lifecycle_id)
                    submission_item = submission_targets.get(submission_key)
                    observed_base = None
                    base_token = None
                    if isinstance(submission_item, dict):
                        candidate_observed = submission_item.get("observed_base")
                        candidate_token = submission_item.get("base_token")
                        if isinstance(candidate_observed, dict):
                            observed_base = candidate_observed
                        if isinstance(candidate_token, dict):
                            base_token = candidate_token

                    if not isinstance(observed_base, dict):
                        observed_base = compute_observed_base(base=base_state, changes=changes)
                    if not isinstance(base_token, dict):
                        base_token = compute_base_token(
                            db_name=db_name,
                            class_id=class_id,
                            instance_id=instance_id,
                            lifecycle_id=lifecycle_id,
                            base_doc=base_state,
                            object_type_version_id=object_type_rid,
                        )
                    else:
                        base_token["object_type_version_id"] = object_type_rid

                    loaded_targets.append(
                        {
                            "resource_rid": object_type_rid,
                            "class_id": class_id,
                            "instance_id": instance_id,
                            "lifecycle_id": lifecycle_id,
                            "base_state": base_state,
                            "changes": changes,
                            "observed_base": observed_base,
                            "base_token": base_token,
                        }
                    )

                if AppConfig.WRITEBACK_ENFORCE_GOVERNANCE and self.dataset_registry:
                    denied: List[Dict[str, Any]] = []
                    for loaded in loaded_targets:
                        class_id = safe_str(loaded.get("class_id"))
                        instance_id = safe_str(loaded.get("instance_id"))
                        base_state = loaded.get("base_state") if isinstance(loaded.get("base_state"), dict) else None
                        if not class_id or not instance_id or not base_state:
                            continue
                        try:
                            access_policy = await self.dataset_registry.get_access_policy(
                                db_name=db_name,
                                scope="data_access",
                                subject_type="object_type",
                                subject_id=class_id,
                            )
                        except Exception:
                            access_policy = None
                        if not access_policy or not isinstance(access_policy.policy, dict) or not access_policy.policy:
                            continue
                        filtered, _info = apply_access_policy([base_state], policy=access_policy.policy)
                        if not filtered:
                            denied.append({"class_id": class_id, "instance_id": instance_id})

                    if denied:
                        await self.action_logs.mark_failed(
                            action_log_id=action_log_id,
                            result=_audit_result(
                                {
                                "error": "data_access_denied",
                                "message": "Actor cannot access one or more target rows under data_access policy",
                                "denied": denied,
                                }
                            ),
                        )
                        raise _ActionRejected("data_access_denied")

                submission_criteria = str(spec.get("submission_criteria") or "").strip()
                if submission_criteria:
                    if not submitted_by:
                        await self.action_logs.mark_failed(
                            action_log_id=action_log_id,
                            result=_audit_result(
                                {
                                "error": "submission_criteria_missing_user",
                                "message": "submitted_by is required to evaluate submission_criteria",
                                "submission_criteria": submission_criteria,
                                "targets": [
                                    {
                                        "class_id": t.get("class_id"),
                                        "instance_id": t.get("instance_id"),
                                        "lifecycle_id": t.get("lifecycle_id"),
                                    }
                                    for t in loaded_targets
                                ],
                                }
                            ),
                        )
                        raise _ActionRejected("submission_criteria_missing_user")

                    target_docs = [t.get("base_state") for t in loaded_targets]
                    target_meta = [
                        {
                            "class_id": t.get("class_id"),
                            "instance_id": t.get("instance_id"),
                            "lifecycle_id": t.get("lifecycle_id"),
                        }
                        for t in loaded_targets
                    ]
                    criteria_vars: Dict[str, Any] = {
                        "user": {"id": submitted_by, "role": actor_role, "is_system": submitted_by == "system"},
                        "input": input_payload,
                        "targets": target_docs,
                        "target": target_docs[0] if len(target_docs) == 1 else None,
                        "db_name": db_name,
                        "base_branch": base_branch,
                        "targets_meta": target_meta,
                    }

                    for key, value in input_payload.items():
                        if not _is_public_identifier(key) or key in criteria_vars:
                            continue
                        if isinstance(value, dict):
                            ref_class = safe_str(value.get("class_id"))
                            ref_instance = safe_str(value.get("instance_id"))
                            if ref_class and ref_instance:
                                match = next(
                                    (
                                        t.get("base_state")
                                        for t in loaded_targets
                                        if t.get("class_id") == ref_class and t.get("instance_id") == ref_instance
                                    ),
                                    None,
                                )
                                if match is not None:
                                    criteria_vars[key] = match
                                    continue
                        criteria_vars[key] = value

                    counts = Counter(
                        [
                            str(t.get("class_id") or "").strip().lower()
                            for t in loaded_targets
                            if str(t.get("class_id") or "").strip()
                        ]
                    )
                    for t in loaded_targets:
                        class_id = str(t.get("class_id") or "").strip()
                        class_var = class_id.lower()
                        if not class_id or not _is_public_identifier(class_var) or class_var in criteria_vars:
                            continue
                        if counts.get(class_var) == 1:
                            criteria_vars[class_var] = t.get("base_state")

                    try:
                        criteria_ok = safe_eval_bool_expression(submission_criteria, variables=criteria_vars)
                    except BoolExpressionError as exc:
                        await self.action_logs.mark_failed(
                            action_log_id=action_log_id,
                            result=_audit_result(
                                {
                                "error": "submission_criteria_error",
                                "message": str(exc),
                                "submission_criteria": submission_criteria,
                                "targets": target_meta,
                                }
                            ),
                        )
                        raise _ActionRejected("submission_criteria_error") from exc

                    if not criteria_ok:
                        failure_info = infer_submission_criteria_failure_reason(submission_criteria)
                        await self.action_logs.mark_failed(
                            action_log_id=action_log_id,
                            result=_audit_result(
                                {
                                "error": "submission_criteria_failed",
                                "message": "submission_criteria evaluated to false",
                                "reason": failure_info.get("reason"),
                                "reasons": failure_info.get("reasons"),
                                "criteria_identifiers": failure_info.get("identifiers"),
                                "actor_role": actor_role,
                                "submission_criteria": submission_criteria,
                                "targets": target_meta,
                                }
                            ),
                        )
                        raise _ActionRejected("submission_criteria_failed")

                validation_rules = spec.get("validation_rules")
                if validation_rules is not None and not isinstance(validation_rules, list):
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                                "error": "validation_rules_invalid",
                                "message": "validation_rules must be a list",
                            }
                        ),
                    )
                    raise _ActionRejected("validation_rules_invalid")

                if isinstance(validation_rules, list) and validation_rules:
                    target_docs = [t.get("base_state") for t in loaded_targets]
                    target_meta = [
                        {
                            "class_id": t.get("class_id"),
                            "instance_id": t.get("instance_id"),
                            "lifecycle_id": t.get("lifecycle_id"),
                        }
                        for t in loaded_targets
                    ]
                    base_vars: Dict[str, Any] = {
                        "user": {"id": submitted_by, "role": actor_role, "is_system": submitted_by == "system"},
                        "input": input_payload,
                        "targets": target_docs,
                        "target": target_docs[0] if len(target_docs) == 1 else None,
                        "db_name": db_name,
                        "base_branch": base_branch,
                        "targets_meta": target_meta,
                    }

                    for rule_idx, rule in enumerate(validation_rules):
                        if not isinstance(rule, dict):
                            await self.action_logs.mark_failed(
                                action_log_id=action_log_id,
                                result=_audit_result(
                                    {
                                        "error": "validation_rule_invalid",
                                        "message": "validation_rules entries must be objects",
                                        "rule_index": rule_idx,
                                    }
                                ),
                            )
                            raise _ActionRejected("validation_rule_invalid")

                        rule_type = str(rule.get("type") or "").strip().lower()
                        if rule_type != "assert":
                            await self.action_logs.mark_failed(
                                action_log_id=action_log_id,
                                result=_audit_result(
                                    {
                                        "error": "validation_rule_invalid",
                                        "message": "only validation_rules.type=assert is supported in P0",
                                        "rule_index": rule_idx,
                                    }
                                ),
                            )
                            raise _ActionRejected("validation_rule_invalid")

                        scope = str(rule.get("scope") or "each_target").strip().lower()
                        expr = str(rule.get("expr") or rule.get("expression") or "").strip()
                        msg = str(rule.get("message") or "").strip() or None
                        if not expr:
                            await self.action_logs.mark_failed(
                                action_log_id=action_log_id,
                                result=_audit_result(
                                    {
                                        "error": "validation_rule_invalid",
                                        "message": "validation_rules.assert requires expr",
                                        "rule_index": rule_idx,
                                    }
                                ),
                            )
                            raise _ActionRejected("validation_rule_invalid")

                        if scope == "action":
                            try:
                                ok = safe_eval_bool_expression(expr, variables=base_vars)
                            except BoolExpressionError as exc:
                                await self.action_logs.mark_failed(
                                    action_log_id=action_log_id,
                                    result=_audit_result(
                                        {
                                            "error": "validation_rule_error",
                                            "message": str(exc),
                                            "rule_index": rule_idx,
                                            "scope": scope,
                                            "expr": expr,
                                        }
                                    ),
                                )
                                raise _ActionRejected("validation_rule_error") from exc
                            if not ok:
                                await self.action_logs.mark_failed(
                                    action_log_id=action_log_id,
                                    result=_audit_result(
                                        {
                                            "error": "validation_rule_failed",
                                            "message": msg or "validation rule evaluated to false",
                                            "rule_index": rule_idx,
                                            "scope": scope,
                                            "expr": expr,
                                        }
                                    ),
                                )
                                raise _ActionRejected("validation_rule_failed")
                            continue

                        if scope == "each_target":
                            for target_idx, target_doc in enumerate(target_docs):
                                each_vars = dict(base_vars)
                                each_vars["target"] = target_doc
                                try:
                                    ok = safe_eval_bool_expression(expr, variables=each_vars)
                                except BoolExpressionError as exc:
                                    await self.action_logs.mark_failed(
                                        action_log_id=action_log_id,
                                        result=_audit_result(
                                            {
                                                "error": "validation_rule_error",
                                                "message": str(exc),
                                                "rule_index": rule_idx,
                                                "scope": scope,
                                                "expr": expr,
                                                "target": target_meta[target_idx] if target_idx < len(target_meta) else None,
                                            }
                                        ),
                                    )
                                    raise _ActionRejected("validation_rule_error") from exc
                                if not ok:
                                    await self.action_logs.mark_failed(
                                        action_log_id=action_log_id,
                                        result=_audit_result(
                                            {
                                                "error": "validation_rule_failed",
                                                "message": msg or "validation rule evaluated to false",
                                                "rule_index": rule_idx,
                                                "scope": scope,
                                                "expr": expr,
                                                "target": target_meta[target_idx] if target_idx < len(target_meta) else None,
                                            }
                                        ),
                                    )
                                    raise _ActionRejected("validation_rule_failed")
                            continue

                        await self.action_logs.mark_failed(
                            action_log_id=action_log_id,
                            result=_audit_result(
                                {
                                    "error": "validation_rule_invalid",
                                    "message": "validation_rules.assert scope must be action or each_target",
                                    "rule_index": rule_idx,
                                    "scope": scope,
                                }
                            ),
                        )
                        raise _ActionRejected("validation_rule_invalid")

                targets: List[Dict[str, Any]] = []
                conflicts: List[Dict[str, Any]] = []
                policies_used: set[str] = set()
                for loaded in loaded_targets:
                    class_id = safe_str(loaded.get("class_id"))
                    instance_id = safe_str(loaded.get("instance_id"))
                    lifecycle_id = safe_str(loaded.get("lifecycle_id") or "lc-0") or "lc-0"
                    resource_rid = safe_str(loaded.get("resource_rid")) or f"object_type:{class_id}@1"
                    base_state = loaded.get("base_state") if isinstance(loaded.get("base_state"), dict) else None
                    changes = loaded.get("changes") if isinstance(loaded.get("changes"), dict) else None
                    observed_base = loaded.get("observed_base") if isinstance(loaded.get("observed_base"), dict) else None
                    base_token = loaded.get("base_token") if isinstance(loaded.get("base_token"), dict) else None

                    if not class_id or not instance_id or not base_state or not changes or not observed_base or not base_token:
                        raise RuntimeError("target state missing after load")

                    conflict_fields = detect_overlap_fields(observed_base=observed_base, current_base=base_state)
                    conflict_links = detect_overlap_links(
                        observed_base=observed_base,
                        current_base=base_state,
                        changes=changes,
                    )

                    obj_meta = await _get_object_type_meta(class_id)
                    conflict_policy = action_conflict_policy or (
                        obj_meta.get("conflict_policy") if isinstance(obj_meta, dict) else None
                    ) or "FAIL"
                    policies_used.add(conflict_policy)
                    applied_changes, resolution = resolve_applied_changes(
                        conflict_policy=conflict_policy,
                        changes=changes,
                        conflict_fields=conflict_fields,
                        conflict_links=conflict_links,
                    )
                    has_conflict = bool(conflict_fields) or bool(conflict_links)

                    if has_conflict:
                        conflicts.append(
                            {
                                "class_id": class_id,
                                "instance_id": instance_id,
                                "lifecycle_id": lifecycle_id,
                                "fields": conflict_fields,
                                "links": conflict_links,
                                "policy": conflict_policy,
                                "resolution": resolution,
                            }
                        )

                    targets.append(
                        {
                            "resource_rid": resource_rid,
                            "instance_id": instance_id,
                            "lifecycle_id": lifecycle_id,
                            "base_token": base_token,
                            "observed_base": observed_base,
                            "changes": changes,
                            "applied_changes": applied_changes,
                            "conflict": {
                                "status": "OVERLAP" if has_conflict else "NONE",
                                "fields": conflict_fields,
                                "links": conflict_links,
                                "policy": conflict_policy,
                                "resolution": resolution,
                            },
                        }
                    )

                should_reject = any(str(c.get("resolution") or "").strip().upper() == "REJECTED" for c in conflicts)
                if should_reject:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=_audit_result(
                            {
                                "error": "conflict_detected",
                                "conflict_policy": action_conflict_policy,
                                "conflict_policies_used": sorted(policies_used),
                                "conflicts": conflicts,
                                "attempted_changes": targets,
                            }
                        ),
                    )
                    raise _ActionRejected("conflict_detected")

                patchset = {
                    "action_log_id": action_log_id,
                    "action_type_rid": action_type_rid,
                    "ontology_commit_id": ontology_commit_id,
                    "targets": targets,
                    "metadata": {
                        "submitted_by": log_rec.submitted_by,
                        "submitted_at": (log_rec.submitted_at or utcnow()).isoformat(),
                        "correlation_id": log_rec.correlation_id,
                        "conflict_policy": action_conflict_policy,
                        "conflict_policies_used": sorted(policies_used),
                    },
                }
                metadata_doc = {
                    "action_log_id": action_log_id,
                    "db_name": db_name,
                    "action_type_id": action_type_id,
                    "ontology_commit_id": ontology_commit_id,
                    "created_at": utcnow().isoformat(),
                    "patchset_sha256": sha256_canonical_json_prefixed(patchset),
                }

                patchset_commit_id = await self._write_patchset_commit(
                    repository=repo,
                    branch=branch,
                    action_log_id=action_log_id,
                    patchset=patchset,
                    metadata_doc=metadata_doc,
                )
                await self.action_logs.mark_commit_written(
                    action_log_id=action_log_id,
                    writeback_commit_id=patchset_commit_id,
                    result=_audit_result(
                        {
                            "attempted_changes": patchset.get("targets", []),
                            "applied_changes": [
                                t for t in patchset.get("targets", [])
                                if isinstance(t, dict) and not is_noop_changes(t.get("applied_changes", t.get("changes")))
                            ],
                            "conflict_policy": action_conflict_policy,
                            "conflict_policies_used": sorted(policies_used),
                            "conflicts": conflicts,
                        }
                    ),
                )
            except _ActionRejected:
                raise
            except Exception as exc:
                await self.action_logs.mark_failed(
                    action_log_id=action_log_id,
                    result=_audit_result({"error": str(exc)}),
                )
                raise

        if not patchset_commit_id:
            # COMMIT_WRITTEN should always have it.
            log_rec = await self.action_logs.get_log(action_log_id=action_log_id)
            patchset_commit_id = log_rec.writeback_commit_id if log_rec else None
        if not patchset_commit_id:
            raise RuntimeError("writeback_commit_id missing after commit")

        # Emit ActionApplied if needed.
        log_rec = await self.action_logs.get_log(action_log_id=action_log_id)
        if not log_rec:
            raise RuntimeError("ActionLog missing after commit")

        if log_rec.action_applied_event_id:
            action_applied_seq = log_rec.action_applied_seq
            action_applied_event_id = log_rec.action_applied_event_id
        else:
            overlay_branch = safe_str(command.get("overlay_branch") or branch) or branch
            evt = ActionAppliedEvent(
                event_id=action_applied_event_id(action_log_id),
                db_name=db_name,
                action_log_id=action_log_id,
                patchset_commit_id=patchset_commit_id,
                writeback_target=writeback_target,
                overlay_branch=overlay_branch,
                data={
                    "db_name": db_name,
                    "action_log_id": action_log_id,
                    "patchset_commit_id": patchset_commit_id,
                    "writeback_target": writeback_target,
                    "overlay_branch": overlay_branch,
                },
                metadata={
                    "correlation_id": log_rec.correlation_id,
                    "ontology": {"ref": f"branch:{safe_str(command.get('base_branch') or 'main')}", "commit": ontology_commit_id},
                },
                occurred_at=utcnow(),
                occurred_by=log_rec.submitted_by,
            )
            action_env = EventEnvelope.from_base_event(
                evt,
                kafka_topic=AppConfig.ACTION_EVENTS_TOPIC,
                metadata={"service": "action_worker", "mode": "action_writeback"},
            )
            await event_store.append_event(action_env)
            action_applied_seq = action_env.sequence_number
            action_applied_event_id = str(action_env.event_id)

            await self.action_logs.mark_event_emitted(
                action_log_id=action_log_id,
                action_applied_event_id=action_applied_event_id,
                action_applied_seq=action_applied_seq,
            )

        # Append per-object queue entries (best-effort; idempotent by key).
        if action_applied_seq is not None:
            await self._append_queue_entries(
                repository=repo,
                branch=branch,
                patchset_commit_id=patchset_commit_id,
                action_log_id=action_log_id,
                action_applied_seq=int(action_applied_seq),
            )

        await self.action_logs.mark_succeeded(
            action_log_id=action_log_id,
            result=_audit_result(
                {
                    "writeback_commit_id": patchset_commit_id,
                    "action_applied_event_id": action_applied_event_id,
                    "action_applied_seq": action_applied_seq,
                }
            ),
        )

    async def _ensure_branch(self, *, repository: str, branch: str) -> None:
        if not self.lakefs_client:
            raise RuntimeError("lakefs_client not initialized")
        try:
            await self.lakefs_client.create_branch(repository=repository, name=branch, source="main")
        except LakeFSConflictError:
            return

    async def _write_patchset_commit(
        self,
        *,
        repository: str,
        branch: str,
        action_log_id: str,
        patchset: Dict[str, Any],
        metadata_doc: Dict[str, Any],
    ) -> str:
        if not self.lakefs_client or not self.lakefs_storage:
            raise RuntimeError("lakefs services not initialized")

        await self._ensure_branch(repository=repository, branch=branch)

        staging_branch = AppConfig.sanitize_lakefs_branch_id(f"{branch}__staging__{action_log_id}")
        try:
            await self.lakefs_client.create_branch(repository=repository, name=staging_branch, source=branch)
        except LakeFSConflictError:
            pass

        patchset_key = ref_key(staging_branch, writeback_patchset_key(action_log_id))
        meta_key = ref_key(staging_branch, writeback_patchset_metadata_key(action_log_id))
        await self.lakefs_storage.save_json(bucket=repository, key=patchset_key, data=patchset)
        await self.lakefs_storage.save_json(bucket=repository, key=meta_key, data=metadata_doc)

        commit_id = await self.lakefs_client.commit(
            repository=repository,
            branch=staging_branch,
            message=f"Action patchset {action_log_id}",
            metadata={
                "action_log_id": action_log_id,
                "kind": "writeback_patchset",
            },
        )
        await self.lakefs_client.merge(
            repository=repository,
            source_ref=staging_branch,
            destination_branch=branch,
            message=f"Merge action patchset {action_log_id}",
            metadata={"action_log_id": action_log_id, "kind": "writeback_patchset_merge"},
            allow_empty=True,
        )
        with suppress(Exception):
            await self.lakefs_client.delete_branch(repository=repository, name=staging_branch)
        return commit_id

    async def _append_queue_entries(
        self,
        *,
        repository: str,
        branch: str,
        patchset_commit_id: str,
        action_log_id: str,
        action_applied_seq: int,
    ) -> None:
        if not self.lakefs_client or not self.lakefs_storage:
            raise RuntimeError("lakefs services not initialized")

        staging_branch = AppConfig.sanitize_lakefs_branch_id(f"{branch}__queue__{action_applied_seq}_{action_log_id}")
        # Load patchset to derive per-object queue keys.
        patchset = await self.lakefs_storage.load_json(
            bucket=repository,
            key=ref_key(patchset_commit_id, writeback_patchset_key(action_log_id)),
        )
        targets = patchset.get("targets") if isinstance(patchset, dict) else None
        if not isinstance(targets, list):
            targets = []

        submitted_at = None
        meta = patchset.get("metadata") if isinstance(patchset, dict) else None
        if isinstance(meta, dict):
            submitted_at = meta.get("submitted_at")

        entries: list[tuple[str, dict[str, Any]]] = []
        for t in targets:
            if not isinstance(t, dict):
                continue
            applied_changes = t.get("applied_changes")
            if isinstance(applied_changes, dict) and is_noop_changes(applied_changes):
                continue
            resource_rid = safe_str(t.get("resource_rid"))
            instance_id = safe_str(t.get("instance_id"))
            lifecycle_id = safe_str(t.get("lifecycle_id") or "lc-0") or "lc-0"
            base_token = t.get("base_token") if isinstance(t.get("base_token"), dict) else {}
            object_type = strip_rid_revision(resource_rid) or "object"

            payload = {
                "action_log_id": action_log_id,
                "patchset_commit_id": patchset_commit_id,
                "action_applied_seq": int(action_applied_seq),
                "resource_rid": resource_rid,
                "instance_id": instance_id,
                "lifecycle_id": lifecycle_id,
                "base_token": base_token,
                "submitted_at": submitted_at,
            }
            key = ref_key(
                staging_branch,
                queue_entry_key(
                    object_type=object_type,
                    instance_id=instance_id,
                    lifecycle_id=lifecycle_id,
                    action_applied_seq=int(action_applied_seq),
                    action_log_id=action_log_id,
                ),
            )
            entries.append((key, payload))

        # Nothing to write (e.g., conflict_policy=BASE_WINS skipped all targets).
        # Avoid failing the action on an empty commit/merge; cleanup any stale staging branch.
        if not entries:
            with suppress(Exception):
                await self.lakefs_client.delete_branch(repository=repository, name=staging_branch)
            return

        try:
            await self.lakefs_client.create_branch(repository=repository, name=staging_branch, source=branch)
        except LakeFSConflictError:
            pass

        for key, payload in entries:
            await self.lakefs_storage.save_json(bucket=repository, key=key, data=payload)

        try:
            await self.lakefs_client.commit(
                repository=repository,
                branch=staging_branch,
                message=f"Writeback queue entries {action_applied_seq}_{action_log_id}",
                metadata={"kind": "writeback_queue_entries", "action_log_id": action_log_id},
            )
        except LakeFSError as exc:
            # Predicate failures can happen if the same queue entries were already committed.
            if "predicate failed" not in str(exc).lower():
                raise

        await self.lakefs_client.merge(
            repository=repository,
            source_ref=staging_branch,
            destination_branch=branch,
            message=f"Merge writeback queue entries {action_applied_seq}_{action_log_id}",
            metadata={"kind": "writeback_queue_entries_merge", "action_log_id": action_log_id},
            allow_empty=True,
        )
        with suppress(Exception):
            await self.lakefs_client.delete_branch(repository=repository, name=staging_branch)


async def main() -> None:
    worker = ActionWorker()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    try:
        await worker.initialize()
        task = asyncio.create_task(worker.run())
        await stop_event.wait()
        worker.running = False
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
