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
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from action_worker.governance import check_writeback_dataset_acl_alignment
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer

from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.enterprise_catalog import is_external_code, resolve_enterprise_error
from shared.errors.error_types import ErrorCode
from shared.models.event_envelope import EventEnvelope
from shared.models.events import ActionAppliedEvent
from shared.models.commands import ActionCommand
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.security.database_access import DATA_ENGINEER_ROLES, DOMAIN_MODEL_ROLES, inspect_database_access
from shared.services.kafka.dlq_publisher import DlqPublishSpec
from shared.services.kafka.retry_classifier import (
    ACTION_COMMAND_RETRY_PROFILE,
    classify_retryable_with_profile,
)
from shared.services.registries.action_log_registry import ActionLogRecord, ActionLogRegistry, ActionLogStatus
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.event_store import event_store
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.storage.lakefs_branch_utils import ensure_lakefs_branch
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service, LakeFSStorageService
from shared.services.kafka.processed_event_worker import (
    CommandEnvelopePayload,
    CommandParseError,
    FailureLogContext,
    RegistryKey,
    StrictCommandEnvelopeKafkaWorker,
    WorkerRuntimeConfig,
)
from shared.services.kafka.safe_consumer import SafeKafkaConsumer
from shared.services.core.object_type_meta_resolver import build_object_type_meta_resolver
from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
)
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.storage.storage_service import StorageService, create_storage_service
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.action_input_schema import (
    ActionInputSchemaError,
    ActionInputValidationError,
    validate_action_input,
)
from shared.utils.action_audit_policy import audit_action_log_result
from shared.utils.action_data_access import evaluate_action_target_data_access
from shared.utils.action_runtime_contracts import (
    extract_required_action_interfaces,
    load_action_target_runtime_contract,
)
from shared.utils.action_permission_profile import (
    ActionPermissionProfile,
    ActionPermissionProfileError,
    PERMISSION_MODEL_ONTOLOGY_ROLES,
    requires_action_data_access_enforcement,
    resolve_action_permission_profile,
)
from shared.utils.principal_policy import build_principal_tags, policy_allows
from shared.utils.worker_runner import run_worker_until_stopped
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
    compile_action_change_shape,
    compile_action_implementation,
)
from shared.utils.safe_bool_expression import BoolExpressionError, safe_eval_bool_expression
from shared.utils.submission_criteria_diagnostics import infer_submission_criteria_failure_reason
from shared.utils.writeback_paths import (
    queue_entry_key,
    ref_key,
    writeback_patchset_key,
    writeback_patchset_metadata_key,
)
from shared.utils.writeback_lifecycle import derive_lifecycle_id
from shared.utils.action_writeback import action_applied_event_id as generate_action_applied_event_id, is_noop_changes, safe_str
from shared.utils.app_logger import configure_logging
from shared.utils.time_utils import utcnow

_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)


@dataclass
class _ActionCommandPayload(CommandEnvelopePayload):
    stage: str = "execute_action"


class _ActionCommandParseError(CommandParseError):
    pass


class _ActionRejected(Exception):
    """Used to short-circuit retries when the ActionLog is already finalized with a rejection result."""


class ActionWorker(StrictCommandEnvelopeKafkaWorker[_ActionCommandPayload, None]):
    parse_error_enable_dlq = True
    parse_error_publish_failure_message = "Failed to publish invalid action payload to DLQ; retrying: %s"
    parse_error_invalid_payload_message = "Invalid action payload; skipping: %s"
    parse_error_raise_on_publish_failure = True
    command_payload_cls = _ActionCommandPayload
    command_parse_error_cls = _ActionCommandParseError
    expected_envelope_kind = None
    command_payload_stage = "execute_action"

    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.action

        self._bootstrap_worker_runtime(
            config=WorkerRuntimeConfig(
                service_name="action-worker",
                handler="action_worker",
                kafka_servers=settings.database.kafka_servers,
                dlq_topic=AppConfig.ACTION_COMMANDS_DLQ_TOPIC,
                dlq_flush_timeout_seconds=float(cfg.dlq_flush_timeout_seconds),
                max_retry_attempts=int(cfg.max_retry_attempts),
                backoff_base=1,
                backoff_max=60,
            ),
            tracing=get_tracing_service("action-worker"),
            metrics=get_metrics_collector("action-worker"),
        )
        self._dlq_spec = DlqPublishSpec(
            dlq_topic=self.dlq_topic,
            service_name=self.service_name,
            span_name="action_worker.dlq_produce",
            metric_event_name="ACTION_COMMAND_DLQ",
            flush_timeout_seconds=self.dlq_flush_timeout_seconds,
        )

        self.enable_processed_event_registry = settings.event_sourcing.enable_processed_event_registry
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.consumer: Optional[SafeKafkaConsumer] = None

        self.action_logs = ActionLogRegistry()
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.lakefs_client: Optional[LakeFSClient] = None
        self.lakefs_storage: Optional[LakeFSStorageService] = None
        self.base_storage: Optional[StorageService] = None
        self.ontology_resources: Optional[OntologyResourceService] = None

    async def initialize(self) -> None:
        self.processed_event_registry = await create_processed_event_registry()
        self.processed = self.processed_event_registry

        group_id = (AppConfig.ACTION_WORKER_GROUP or "action-worker-group").strip()
        topic = AppConfig.ACTION_COMMANDS_TOPIC
        self._initialize_safe_consumer_runtime(
            group_id=group_id,
            topics=[topic],
            service_name="action-worker",
            thread_name_prefix="action-worker-kafka",
        )
        logger.info("ActionWorker subscribed to topic=%s group=%s", topic, group_id)

        settings = get_settings()
        cfg = settings.workers.action
        service_name = settings.observability.service_name or "action-worker-dlq"
        self.dlq_producer = create_kafka_dlq_producer(
            bootstrap_servers=self.kafka_servers,
            client_id=service_name,
            retries=int(cfg.dlq_retries),
            retry_backoff_ms=250,
            linger_ms=10,
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
        )

        await event_store.connect()
        await self.action_logs.connect()
        if AppConfig.WRITEBACK_ENFORCE_GOVERNANCE or AppConfig.WRITEBACK_ENFORCE_ACTION_DATA_ACCESS:
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

        self.ontology_resources = OntologyResourceService()

    async def shutdown(self) -> None:
        self.running = False
        await self._close_consumer_runtime()
        await close_kafka_producer(
            producer=self.dlq_producer,
            timeout_s=self.dlq_flush_timeout_seconds,
            warning_logger=logger,
            warning_message="DLQ producer flush failed during shutdown: %s",
        )
        self.dlq_producer = None
        if self.processed_event_registry:
            await self.processed_event_registry.close()
        await self.action_logs.close()
        if self.dataset_registry:
            await self.dataset_registry.close()

    async def _process_payload(self, payload: _ActionCommandPayload) -> None:  # type: ignore[override]
        envelope = payload.envelope
        if envelope is None:
            raise ValueError("event envelope is required")
        meta = envelope.metadata if isinstance(envelope.metadata, dict) else {}
        if meta.get("kind") != "command":
            logger.info(
                "Skipping non-command envelope event_id=%s type=%s",
                envelope.event_id,
                envelope.event_type,
            )
            return

        command_data = payload.command if isinstance(payload.command, dict) else {}
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
        except _ActionRejected as exc:
            logger.info(
                "Action execution rejected (action_log_id=%s db_name=%s reason=%s)",
                action_log_id,
                db_name,
                exc,
            )
        finally:
            try:
                await self._trigger_dependent_actions(
                    db_name=db_name,
                    parent_action_log_id=action_log_id,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to evaluate dependent actions for parent=%s: %s",
                    action_log_id,
                    exc,
                    exc_info=True,
                )

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
        if envelope is None:
            raise ValueError("event envelope is required")
        command_data = payload.command if isinstance(payload.command, dict) else {}
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
        envelope = payload.envelope
        if envelope is None:
            return None
        return str(envelope.event_type or "").strip() or None

    def _retry_log_context(  # type: ignore[override]
        self,
        *,
        payload: _ActionCommandPayload,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> FailureLogContext:
        _ = payload, retryable
        return FailureLogContext(
            message="Retrying action command in %ss (attempt %s): %s",
            args=(int(backoff_s), attempt_count, error),
        )

    def _terminal_failure_log_context(  # type: ignore[override]
        self,
        *,
        payload: _ActionCommandPayload,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> FailureLogContext:
        _ = payload
        return FailureLogContext(
            message="Action command failed after %s attempts (retryable=%s): %s",
            args=(attempt_count, retryable, error),
        )

    @staticmethod
    def _is_retryable_error(exc: BaseException, *, payload: Optional[_ActionCommandPayload] = None) -> bool:
        if isinstance(exc, _ActionRejected):
            return False
        if isinstance(exc, PermissionError):
            return False
        if isinstance(exc, ValueError):
            return False
        return classify_retryable_with_profile(exc, ACTION_COMMAND_RETRY_PROFILE)

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
        await self._send_standard_dlq_record(
            msg=msg,
            error=error,
            attempt_count=int(attempt_count),
            stage=stage,
            default_stage="execute_action",
            raw_payload=raw_payload,
            payload_text=payload_text,
            payload_obj=payload_obj,
            kafka_headers=kafka_headers,
            fallback_metadata=fallback_metadata,
            publisher=self._publish_to_dlq,
            inferred_payload_obj=payload.envelope.model_dump(mode="json") if payload is not None else None,
            inferred_metadata=payload.envelope_metadata if payload is not None else None,
            inferred_stage=payload.stage if payload is not None else None,
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
    ) -> tuple[Optional[str], ActionPermissionProfile]:
        permission_profile = resolve_action_permission_profile(action_spec)
        actor = (submitted_by or "").strip()
        if not actor or actor == "system":
            return None, permission_profile

        actor_type = str(submitted_by_type or "user").strip().lower() or "user"
        inspection = await inspect_database_access(
            db_name=db_name,
            principal_type=actor_type,
            principal_id=actor,
        )
        if inspection.is_unavailable:
            raise RuntimeError("Database access registry unavailable")
        role = inspection.role
        if permission_profile.permission_model == PERMISSION_MODEL_ONTOLOGY_ROLES:
            if role not in DOMAIN_MODEL_ROLES:
                raise PermissionError("Permission denied")
        else:
            if role not in DATA_ENGINEER_ROLES:
                raise PermissionError("Permission denied")

        if permission_profile.permission_model == PERMISSION_MODEL_ONTOLOGY_ROLES:
            policy = action_spec.get("permission_policy")
            tags = build_principal_tags(principal_type=actor_type, principal_id=actor, role=role)
            if not policy_allows(policy=policy, principal_tags=tags):
                raise PermissionError("Permission denied")

        if permission_profile.edits_beyond_actions and role not in DATA_ENGINEER_ROLES:
            raise PermissionError("Permission denied")
        return role, permission_profile

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
        return await check_writeback_dataset_acl_alignment(
            dataset_registry=self.dataset_registry,
            db_name=db_name,
            submitted_by=submitted_by,
            submitted_by_type=submitted_by_type,
            actor_role=actor_role,
            ontology_commit_id=ontology_commit_id,
            resources=resources,
            class_ids=class_ids,
        )

    async def _execute_action(
        self,
        *,
        db_name: str,
        action_log_id: str,
        command: Dict[str, Any],
        envelope: EventEnvelope,
    ) -> None:
        if not self.lakefs_client or not self.lakefs_storage or not self.base_storage or not self.ontology_resources:
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
        resources = self.ontology_resources
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
        try:
            actor_role, permission_profile = await self._enforce_permission(
                db_name=db_name,
                submitted_by=log_rec.submitted_by,
                submitted_by_type=submitted_by_type,
                action_spec=spec,
            )
        except ActionPermissionProfileError as exc:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result={
                    "error": "action_permission_profile_invalid",
                    "message": str(exc),
                    "field": exc.field,
                },
            )
            raise _ActionRejected("action_permission_profile_invalid") from exc

        writeback_target = log_rec.writeback_target or {
            "repo": AppConfig.ONTOLOGY_WRITEBACK_REPO,
            "branch": AppConfig.get_ontology_writeback_branch(db_name),
        }
        repo = str(writeback_target.get("repo") or "").strip()
        branch = str(writeback_target.get("branch") or "").strip()
        if not repo or not branch:
            raise RuntimeError("writeback_target.repo and writeback_target.branch are required")

        audit_policy = spec.get("audit_policy")
        patchset_commit_id = log_rec.writeback_commit_id
        if log_rec.status == ActionLogStatus.PENDING.value:
            patchset_commit_id = await self._execute_action_pending(
                db_name=db_name,
                action_log_id=action_log_id,
                command=command,
                log_rec=log_rec,
                spec=spec,
                resources=resources,
                action_type_id=action_type_id,
                action_type_rid=action_type_rid,
                ontology_commit_id=ontology_commit_id,
                submitted_by=submitted_by,
                submitted_by_type=submitted_by_type,
                actor_role=actor_role,
                permission_profile=permission_profile,
                audit_policy=audit_policy,
                repo=repo,
                branch=branch,
            )

        if not patchset_commit_id:
            # COMMIT_WRITTEN should always have it.
            log_rec = await self.action_logs.get_log(action_log_id=action_log_id)
            patchset_commit_id = log_rec.writeback_commit_id if log_rec else None
        if not patchset_commit_id:
            raise RuntimeError("writeback_commit_id missing after commit")

        action_applied_seq, action_applied_event_id = await self._emit_action_applied_if_needed(
            db_name=db_name,
            action_log_id=action_log_id,
            command=command,
            writeback_target=writeback_target,
            patchset_commit_id=patchset_commit_id,
            ontology_commit_id=ontology_commit_id,
            branch=branch,
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

        await self._mark_action_succeeded(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            patchset_commit_id=patchset_commit_id,
            action_applied_event_id=action_applied_event_id,
            action_applied_seq=action_applied_seq,
        )

    def _audit_result(self, *, audit_policy: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
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
                try:
                    enterprise = resolve_enterprise_error(
                        service_name="action-worker",
                        code=ErrorCode(error_key),
                        category=None,
                        status_code=400,
                        external_code=None,
                    ).to_dict()
                except ValueError as exc:
                    logger.warning("Failed to resolve enterprise error for key=%s: %s", error_key, exc, exc_info=True)
            if enterprise is not None:
                enriched["enterprise"] = enterprise
        return audit_action_log_result(enriched, audit_policy=audit_policy)

    async def _prepare_pending_input(
        self,
        *,
        action_log_id: str,
        command: Dict[str, Any],
        log_rec: ActionLogRecord,
        spec: Dict[str, Any],
        audit_policy: Any,
    ) -> tuple[Dict[str, Any], Any, List[Any], str]:
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
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "action_input_invalid",
                        "message": str(exc),
                    },
                ),
            )
            raise _ActionRejected("action_input_invalid") from exc
        except ActionInputSchemaError as exc:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "action_type_input_schema_invalid",
                        "message": str(exc),
                    },
                ),
            )
            raise _ActionRejected("action_type_input_schema_invalid") from exc

        implementation = spec.get("implementation")
        try:
            compiled_shape = compile_action_change_shape(
                implementation,
                input_payload=input_payload,
            )
        except ActionImplementationError as exc:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "action_implementation_invalid",
                        "message": str(exc),
                    },
                ),
            )
            raise _ActionRejected("action_implementation_invalid") from exc

        if not compiled_shape:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "action_no_targets",
                        "message": "implementation resolved to zero targets",
                    },
                ),
            )
            raise _ActionRejected("action_no_targets")

        base_branch = safe_str(command.get("base_branch") or "main") or "main"
        return input_payload, implementation, compiled_shape, base_branch

    @staticmethod
    def _is_public_identifier(value: Any) -> bool:
        text = str(value or "").strip()
        return bool(text) and text.isidentifier() and not text.startswith("_")

    async def _execute_action_pending(
        self,
        *,
        db_name: str,
        action_log_id: str,
        command: Dict[str, Any],
        log_rec: ActionLogRecord,
        spec: Dict[str, Any],
        resources: OntologyResourceService,
        action_type_id: str,
        action_type_rid: str,
        ontology_commit_id: str,
        submitted_by: str,
        submitted_by_type: str,
        actor_role: Optional[str],
        permission_profile: ActionPermissionProfile,
        audit_policy: Any,
        repo: str,
        branch: str,
    ) -> str:
        try:
            input_payload, implementation, compiled_shape, base_branch = await self._prepare_pending_input(
                action_log_id=action_log_id,
                command=command,
                log_rec=log_rec,
                spec=spec,
                audit_policy=audit_policy,
            )

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
                await self.action_logs.mark_failed(
                    action_log_id=action_log_id,
                    result=self._audit_result(audit_policy=audit_policy, payload=governance_error),
                )
                raise _ActionRejected("writeback_governance_rejected")

            action_conflict_policy = parse_conflict_policy(spec.get("conflict_policy"))
            get_object_type_meta = build_object_type_meta_resolver(
                resources=resources,
                db_name=db_name,
                branch=ontology_commit_id,
            )
            submission_targets = self._extract_submission_targets(log_rec=log_rec)
            loaded_targets = await self._build_pending_loaded_targets(
                db_name=db_name,
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                resources=resources,
                ontology_commit_id=ontology_commit_id,
                spec=spec,
                compiled_shape=compiled_shape,
                implementation=implementation,
                input_payload=input_payload,
                submitted_by=submitted_by,
                actor_role=actor_role,
                base_branch=base_branch,
                get_object_type_meta=get_object_type_meta,
                submission_targets=submission_targets,
            )
            await self._enforce_pending_target_access(
                db_name=db_name,
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                permission_profile=permission_profile,
                submitted_by=submitted_by,
                submitted_by_type=submitted_by_type,
                actor_role=actor_role,
                loaded_targets=loaded_targets,
            )

            await self._enforce_submission_and_validation_rules(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                spec=spec,
                submitted_by=submitted_by,
                actor_role=actor_role,
                input_payload=input_payload,
                loaded_targets=loaded_targets,
                db_name=db_name,
                base_branch=base_branch,
            )

            return await self._build_and_commit_pending_patchset(
                db_name=db_name,
                action_log_id=action_log_id,
                action_type_id=action_type_id,
                action_type_rid=action_type_rid,
                ontology_commit_id=ontology_commit_id,
                action_conflict_policy=action_conflict_policy,
                get_object_type_meta=get_object_type_meta,
                loaded_targets=loaded_targets,
                log_rec=log_rec,
                audit_policy=audit_policy,
                repo=repo,
                branch=branch,
            )
        except _ActionRejected:
            raise
        except Exception as exc:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(audit_policy=audit_policy, payload={"error": str(exc)}),
            )
            raise

    def _extract_submission_targets(self, *, log_rec: ActionLogRecord) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
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
        return submission_targets

    async def _build_pending_loaded_targets(
        self,
        *,
        db_name: str,
        action_log_id: str,
        audit_policy: Any,
        resources: OntologyResourceService,
        ontology_commit_id: str,
        spec: Dict[str, Any],
        compiled_shape: List[Any],
        implementation: Any,
        input_payload: Dict[str, Any],
        submitted_by: str,
        actor_role: Optional[str],
        base_branch: str,
        get_object_type_meta: Any,
        submission_targets: Dict[Tuple[str, str, str], Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        required_interfaces = set(extract_required_action_interfaces(spec))
        class_interface_refs: Dict[str, List[str]] = {}
        class_field_types: Dict[str, Dict[str, str]] = {}

        async def _ensure_class_contract(class_id: str) -> None:
            if class_id in class_interface_refs:
                return
            contract = await load_action_target_runtime_contract(
                db_name=db_name,
                class_id=class_id,
                branch=ontology_commit_id,
                resources=resources,
            )
            if contract is None:
                raise RuntimeError(f"Target class not found at ontology commit (class_id={class_id})")
            class_interface_refs[class_id] = contract.interfaces
            class_field_types[class_id] = contract.field_types

        loaded_targets: List[Dict[str, Any]] = []
        target_docs: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for item in compiled_shape:
            class_id = safe_str(item.class_id)
            instance_id = safe_str(item.instance_id)
            if not class_id or not instance_id:
                raise ValueError("each compiled target requires class_id and instance_id")
            await _ensure_class_contract(class_id)

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
            compiled_targets = compile_action_implementation(
                implementation,
                input_payload=input_payload,
                user=user_ctx,
                target_docs=target_docs,
                now=utcnow(),
            )
        except ActionImplementationError as exc:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "action_implementation_compile_error",
                        "message": str(exc),
                    },
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
            obj_meta = await get_object_type_meta(class_id)
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
                    "field_types": class_field_types.get(class_id, {}),
                    "observed_base": observed_base,
                    "base_token": base_token,
                }
            )

        if required_interfaces:
            for tgt in loaded_targets:
                class_id = safe_str(tgt.get("class_id"))
                implemented = set(class_interface_refs.get(class_id, []))
                missing = sorted(required_interfaces - implemented)
                if missing:
                    await self.action_logs.mark_failed(
                        action_log_id=action_log_id,
                        result=self._audit_result(
                            audit_policy=audit_policy,
                            payload={
                                "error": "action_interface_not_implemented",
                                "message": "Action target class does not satisfy required interfaces",
                                "class_id": class_id,
                                "required_interfaces": sorted(required_interfaces),
                                "implemented_interfaces": sorted(implemented),
                                "missing_interfaces": missing,
                            },
                        ),
                    )
                    raise _ActionRejected("action_interface_not_implemented")

        return loaded_targets

    async def _enforce_pending_target_access(
        self,
        *,
        db_name: str,
        action_log_id: str,
        audit_policy: Any,
        permission_profile: ActionPermissionProfile,
        submitted_by: str,
        submitted_by_type: str,
        actor_role: Optional[str],
        loaded_targets: List[Dict[str, Any]],
    ) -> None:
        enforce_data_access = requires_action_data_access_enforcement(
            profile=permission_profile,
            global_enforcement=AppConfig.WRITEBACK_ENFORCE_ACTION_DATA_ACCESS,
        )
        principal_tags = None
        if submitted_by and submitted_by != "system":
            principal_tags = build_principal_tags(
                principal_type=submitted_by_type,
                principal_id=submitted_by,
                role=actor_role,
            )
        enforce_edit_access = bool(principal_tags)
        if not (enforce_data_access or enforce_edit_access):
            return

        if self.dataset_registry is None:
            self.dataset_registry = DatasetRegistry()
            await self.dataset_registry.connect()
        if not self.dataset_registry:
            error_key = "data_access_unverifiable" if enforce_data_access else "edit_access_unverifiable"
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": error_key,
                        "message": "DatasetRegistry not initialized for action target access checks",
                    },
                ),
            )
            raise _ActionRejected(error_key)

        access_report = await evaluate_action_target_data_access(
            dataset_registry=self.dataset_registry,
            db_name=db_name,
            targets=loaded_targets,
            enforce_data_access_policy=enforce_data_access,
            principal_tags=principal_tags,
            enforce_object_edit_policy=enforce_edit_access,
            enforce_attachment_edit_policy=enforce_edit_access,
            enforce_object_set_edit_policy=enforce_edit_access,
            fail_on_missing_edit_policy=False,
        )
        if enforce_data_access and access_report.unverifiable:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "data_access_unverifiable",
                        "message": "Unable to verify one or more target rows under data_access policy",
                        "unverifiable": access_report.unverifiable,
                    },
                ),
            )
            raise _ActionRejected("data_access_unverifiable")
        if enforce_data_access and access_report.denied:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "data_access_denied",
                        "message": "Actor cannot access one or more target rows under data_access policy",
                        "denied": access_report.denied,
                    },
                ),
            )
            raise _ActionRejected("data_access_denied")
        if enforce_edit_access and access_report.edit_unverifiable:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "edit_access_unverifiable",
                        "message": "Unable to verify one or more target edit permissions",
                        "unverifiable": access_report.edit_unverifiable,
                    },
                ),
            )
            raise _ActionRejected("edit_access_unverifiable")
        if enforce_edit_access and access_report.edit_denied:
            await self.action_logs.mark_failed(
                action_log_id=action_log_id,
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "edit_access_denied",
                        "message": "Actor cannot edit one or more target object types/fields",
                        "denied": access_report.edit_denied,
                    },
                ),
            )
            raise _ActionRejected("edit_access_denied")

    async def _reject_action(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        error_key: str,
        message: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        payload: Dict[str, Any] = {"error": error_key}
        if message is not None:
            payload["message"] = message
        if extra:
            payload.update(extra)
        await self.action_logs.mark_failed(
            action_log_id=action_log_id,
            result=self._audit_result(audit_policy=audit_policy, payload=payload),
        )
        if cause is None:
            raise _ActionRejected(error_key)
        raise _ActionRejected(error_key) from cause

    @staticmethod
    def _build_target_meta(loaded_targets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "class_id": target.get("class_id"),
                "instance_id": target.get("instance_id"),
                "lifecycle_id": target.get("lifecycle_id"),
            }
            for target in loaded_targets
        ]

    @staticmethod
    def _build_rule_base_vars(
        *,
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        target_docs_list: List[Any],
        target_meta: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
    ) -> Dict[str, Any]:
        return {
            "user": {"id": submitted_by, "role": actor_role, "is_system": submitted_by == "system"},
            "input": input_payload,
            "targets": target_docs_list,
            "target": target_docs_list[0] if len(target_docs_list) == 1 else None,
            "db_name": db_name,
            "base_branch": base_branch,
            "targets_meta": target_meta,
        }

    @staticmethod
    def _resolve_input_ref_target(
        *,
        value: Any,
        loaded_targets: List[Dict[str, Any]],
    ) -> Any:
        if not isinstance(value, dict):
            return None
        ref_class = safe_str(value.get("class_id"))
        ref_instance = safe_str(value.get("instance_id"))
        if not ref_class or not ref_instance:
            return None
        return next(
            (
                target.get("base_state")
                for target in loaded_targets
                if target.get("class_id") == ref_class and target.get("instance_id") == ref_instance
            ),
            None,
        )

    def _inject_submission_input_vars(
        self,
        *,
        criteria_vars: Dict[str, Any],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
    ) -> None:
        for key, value in input_payload.items():
            if not self._is_public_identifier(key) or key in criteria_vars:
                continue
            match = self._resolve_input_ref_target(value=value, loaded_targets=loaded_targets)
            criteria_vars[key] = value if match is None else match

    def _inject_submission_class_vars(
        self,
        *,
        criteria_vars: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
    ) -> None:
        counts = Counter(
            [
                str(target.get("class_id") or "").strip().lower()
                for target in loaded_targets
                if str(target.get("class_id") or "").strip()
            ]
        )
        for target in loaded_targets:
            class_id = str(target.get("class_id") or "").strip()
            class_var = class_id.lower()
            if not class_id or not self._is_public_identifier(class_var) or class_var in criteria_vars:
                continue
            if counts.get(class_var) == 1:
                criteria_vars[class_var] = target.get("base_state")

    def _build_submission_criteria_vars(
        self,
        *,
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
        target_meta: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        target_docs_list = [target.get("base_state") for target in loaded_targets]
        criteria_vars = self._build_rule_base_vars(
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            target_docs_list=target_docs_list,
            target_meta=target_meta,
            db_name=db_name,
            base_branch=base_branch,
        )
        self._inject_submission_input_vars(
            criteria_vars=criteria_vars,
            input_payload=input_payload,
            loaded_targets=loaded_targets,
        )
        self._inject_submission_class_vars(criteria_vars=criteria_vars, loaded_targets=loaded_targets)
        return criteria_vars

    async def _ensure_submission_actor_for_criteria(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        submission_criteria: str,
        submitted_by: str,
        target_meta: List[Dict[str, Any]],
    ) -> None:
        if submitted_by:
            return
        await self._reject_action(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            error_key="submission_criteria_missing_user",
            message="submitted_by is required to evaluate submission_criteria",
            extra={"submission_criteria": submission_criteria, "targets": target_meta},
        )

    async def _evaluate_submission_criteria(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        submission_criteria: str,
        criteria_vars: Dict[str, Any],
        target_meta: List[Dict[str, Any]],
    ) -> bool:
        try:
            return safe_eval_bool_expression(submission_criteria, variables=criteria_vars)
        except BoolExpressionError as exc:
            await self._reject_action(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                error_key="submission_criteria_error",
                message=str(exc),
                extra={"submission_criteria": submission_criteria, "targets": target_meta},
                cause=exc,
            )

    async def _reject_submission_criteria_failed(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        submission_criteria: str,
        actor_role: Optional[str],
        target_meta: List[Dict[str, Any]],
    ) -> None:
        failure_info = infer_submission_criteria_failure_reason(submission_criteria)
        await self._reject_action(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            error_key="submission_criteria_failed",
            message="submission_criteria evaluated to false",
            extra={
                "reason": failure_info.get("reason"),
                "reasons": failure_info.get("reasons"),
                "criteria_identifiers": failure_info.get("identifiers"),
                "actor_role": actor_role,
                "submission_criteria": submission_criteria,
                "targets": target_meta,
            },
        )

    async def _prepare_submission_criteria_context(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        submission_criteria: str,
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        target_meta = self._build_target_meta(loaded_targets)
        await self._ensure_submission_actor_for_criteria(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            submission_criteria=submission_criteria,
            submitted_by=submitted_by,
            target_meta=target_meta,
        )
        criteria_vars = self._build_submission_criteria_vars(
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            loaded_targets=loaded_targets,
            db_name=db_name,
            base_branch=base_branch,
            target_meta=target_meta,
        )
        return target_meta, criteria_vars

    async def _enforce_submission_criteria(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        submission_criteria: str,
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
    ) -> None:
        if not submission_criteria:
            return

        target_meta, criteria_vars = await self._prepare_submission_criteria_context(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            submission_criteria=submission_criteria,
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            loaded_targets=loaded_targets,
            db_name=db_name,
            base_branch=base_branch,
        )

        if await self._evaluate_submission_criteria(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            submission_criteria=submission_criteria,
            criteria_vars=criteria_vars,
            target_meta=target_meta,
        ):
            return

        await self._reject_submission_criteria_failed(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            submission_criteria=submission_criteria,
            actor_role=actor_role,
            target_meta=target_meta,
        )

    async def _reject_validation_rule(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        error_key: str,
        message: str,
        rule_index: int,
        scope: Optional[str] = None,
        expr: Optional[str] = None,
        target: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        extra: Dict[str, Any] = {"rule_index": rule_index}
        if scope is not None:
            extra["scope"] = scope
        if expr is not None:
            extra["expr"] = expr
        if target is not None:
            extra["target"] = target
        await self._reject_action(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            error_key=error_key,
            message=message,
            extra=extra,
            cause=cause,
        )

    async def _parse_validation_rule(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        rule: Any,
        rule_idx: int,
    ) -> Tuple[str, str, Optional[str]]:
        if not isinstance(rule, dict):
            await self._reject_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                error_key="validation_rule_invalid",
                message="validation_rules entries must be objects",
                rule_index=rule_idx,
            )
        rule_type = str(rule.get("type") or "").strip().lower()
        if rule_type != "assert":
            await self._reject_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                error_key="validation_rule_invalid",
                message="only validation_rules.type=assert is supported in P0",
                rule_index=rule_idx,
            )
        scope = str(rule.get("scope") or "each_target").strip().lower()
        expr = str(rule.get("expr") or rule.get("expression") or "").strip()
        message = str(rule.get("message") or "").strip() or None
        if not expr:
            await self._reject_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                error_key="validation_rule_invalid",
                message="validation_rules.assert requires expr",
                rule_index=rule_idx,
            )
        return scope, expr, message

    async def _enforce_action_scope_validation_rule(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        base_vars: Dict[str, Any],
        rule_idx: int,
        scope: str,
        expr: str,
        message: Optional[str],
    ) -> None:
        try:
            ok = safe_eval_bool_expression(expr, variables=base_vars)
        except BoolExpressionError as exc:
            await self._reject_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                error_key="validation_rule_error",
                message=str(exc),
                rule_index=rule_idx,
                scope=scope,
                expr=expr,
                cause=exc,
            )
        if ok:
            return
        await self._reject_validation_rule(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            error_key="validation_rule_failed",
            message=message or "validation rule evaluated to false",
            rule_index=rule_idx,
            scope=scope,
            expr=expr,
        )

    async def _enforce_each_target_validation_rule(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        base_vars: Dict[str, Any],
        target_docs_list: List[Any],
        target_meta: List[Dict[str, Any]],
        rule_idx: int,
        scope: str,
        expr: str,
        message: Optional[str],
    ) -> None:
        for target_idx, target_doc in enumerate(target_docs_list):
            each_vars = dict(base_vars)
            each_vars["target"] = target_doc
            target_info = target_meta[target_idx] if target_idx < len(target_meta) else None
            try:
                ok = safe_eval_bool_expression(expr, variables=each_vars)
            except BoolExpressionError as exc:
                await self._reject_validation_rule(
                    action_log_id=action_log_id,
                    audit_policy=audit_policy,
                    error_key="validation_rule_error",
                    message=str(exc),
                    rule_index=rule_idx,
                    scope=scope,
                    expr=expr,
                    target=target_info,
                    cause=exc,
                )
            if ok:
                continue
            await self._reject_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                error_key="validation_rule_failed",
                message=message or "validation rule evaluated to false",
                rule_index=rule_idx,
                scope=scope,
                expr=expr,
                target=target_info,
            )

    async def _normalize_validation_rules(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        validation_rules: Any,
    ) -> List[Any]:
        if validation_rules is None:
            return []
        if isinstance(validation_rules, list):
            return validation_rules
        await self._reject_action(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            error_key="validation_rules_invalid",
            message="validation_rules must be a list",
        )

    def _build_validation_rule_context(
        self,
        *,
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
    ) -> Tuple[List[Any], List[Dict[str, Any]], Dict[str, Any]]:
        target_docs_list = [target.get("base_state") for target in loaded_targets]
        target_meta = self._build_target_meta(loaded_targets)
        base_vars = self._build_rule_base_vars(
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            target_docs_list=target_docs_list,
            target_meta=target_meta,
            db_name=db_name,
            base_branch=base_branch,
        )
        return target_docs_list, target_meta, base_vars

    async def _enforce_single_validation_rule(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        rule: Any,
        rule_idx: int,
        base_vars: Dict[str, Any],
        target_docs_list: List[Any],
        target_meta: List[Dict[str, Any]],
    ) -> None:
        scope, expr, message = await self._parse_validation_rule(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            rule=rule,
            rule_idx=rule_idx,
        )
        if scope == "action":
            await self._enforce_action_scope_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                base_vars=base_vars,
                rule_idx=rule_idx,
                scope=scope,
                expr=expr,
                message=message,
            )
            return
        if scope == "each_target":
            await self._enforce_each_target_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                base_vars=base_vars,
                target_docs_list=target_docs_list,
                target_meta=target_meta,
                rule_idx=rule_idx,
                scope=scope,
                expr=expr,
                message=message,
            )
            return
        await self._reject_validation_rule(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            error_key="validation_rule_invalid",
            message="validation_rules.assert scope must be action or each_target",
            rule_index=rule_idx,
            scope=scope,
        )

    async def _enforce_validation_rules(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        validation_rules: Any,
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
    ) -> None:
        rules = await self._normalize_validation_rules(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            validation_rules=validation_rules,
        )
        if not rules:
            return

        target_docs_list, target_meta, base_vars = self._build_validation_rule_context(
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            loaded_targets=loaded_targets,
            db_name=db_name,
            base_branch=base_branch,
        )

        for rule_idx, rule in enumerate(rules):
            await self._enforce_single_validation_rule(
                action_log_id=action_log_id,
                audit_policy=audit_policy,
                rule=rule,
                rule_idx=rule_idx,
                base_vars=base_vars,
                target_docs_list=target_docs_list,
                target_meta=target_meta,
            )

    async def _enforce_submission_and_validation_rules(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        spec: Dict[str, Any],
        submitted_by: str,
        actor_role: Optional[str],
        input_payload: Dict[str, Any],
        loaded_targets: List[Dict[str, Any]],
        db_name: str,
        base_branch: str,
    ) -> None:
        submission_criteria = str(spec.get("submission_criteria") or "").strip()
        await self._enforce_submission_criteria(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            submission_criteria=submission_criteria,
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            loaded_targets=loaded_targets,
            db_name=db_name,
            base_branch=base_branch,
        )

        await self._enforce_validation_rules(
            action_log_id=action_log_id,
            audit_policy=audit_policy,
            validation_rules=spec.get("validation_rules"),
            submitted_by=submitted_by,
            actor_role=actor_role,
            input_payload=input_payload,
            loaded_targets=loaded_targets,
            db_name=db_name,
            base_branch=base_branch,
        )

    async def _build_and_commit_pending_patchset(
        self,
        *,
        db_name: str,
        action_log_id: str,
        action_type_id: str,
        action_type_rid: str,
        ontology_commit_id: str,
        action_conflict_policy: Optional[str],
        get_object_type_meta: Any,
        loaded_targets: List[Dict[str, Any]],
        log_rec: ActionLogRecord,
        audit_policy: Any,
        repo: str,
        branch: str,
    ) -> str:
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

            obj_meta = await get_object_type_meta(class_id)
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
                result=self._audit_result(
                    audit_policy=audit_policy,
                    payload={
                        "error": "conflict_detected",
                        "conflict_policy": action_conflict_policy,
                        "conflict_policies_used": sorted(policies_used),
                        "conflicts": conflicts,
                        "attempted_changes": targets,
                    },
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
            result=self._audit_result(
                audit_policy=audit_policy,
                payload={
                    "attempted_changes": patchset.get("targets", []),
                    "applied_changes": [
                        t for t in patchset.get("targets", [])
                        if isinstance(t, dict) and not is_noop_changes(t.get("applied_changes", t.get("changes")))
                    ],
                    "conflict_policy": action_conflict_policy,
                    "conflict_policies_used": sorted(policies_used),
                    "conflicts": conflicts,
                },
            ),
        )
        return patchset_commit_id

    async def _emit_action_applied_if_needed(
        self,
        *,
        db_name: str,
        action_log_id: str,
        command: Dict[str, Any],
        writeback_target: Dict[str, Any],
        patchset_commit_id: str,
        ontology_commit_id: str,
        branch: str,
    ) -> tuple[Optional[int], Optional[str]]:
        # Emit ActionApplied if needed.
        log_rec = await self.action_logs.get_log(action_log_id=action_log_id)
        if not log_rec:
            raise RuntimeError("ActionLog missing after commit")

        if log_rec.action_applied_event_id:
            return log_rec.action_applied_seq, log_rec.action_applied_event_id

        overlay_branch = safe_str(command.get("overlay_branch") or branch) or branch
        evt = ActionAppliedEvent(
            event_id=generate_action_applied_event_id(action_log_id),
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
        return action_applied_seq, action_applied_event_id

    async def _mark_action_succeeded(
        self,
        *,
        action_log_id: str,
        audit_policy: Any,
        patchset_commit_id: str,
        action_applied_event_id: Optional[str],
        action_applied_seq: Optional[int],
    ) -> None:
        await self.action_logs.mark_succeeded(
            action_log_id=action_log_id,
            result=self._audit_result(
                audit_policy=audit_policy,
                payload={
                    "writeback_commit_id": patchset_commit_id,
                    "action_applied_event_id": action_applied_event_id,
                    "action_applied_seq": action_applied_seq,
                },
            ),
        )

    @staticmethod
    def _dependency_is_satisfied(*, trigger_on: str, parent_status: Optional[str]) -> tuple[bool, bool]:
        """
        Returns (satisfied, impossible).

        - satisfied=True means dependency condition is met.
        - impossible=True means parent reached terminal state that can never satisfy this dependency.
        """
        trigger = str(trigger_on or "").strip().upper() or "SUCCEEDED"
        status_value = str(parent_status or "").strip().upper()
        terminal = {
            ActionLogStatus.SUCCEEDED.value,
            ActionLogStatus.FAILED.value,
        }
        if trigger == "SUCCEEDED":
            if status_value == ActionLogStatus.SUCCEEDED.value:
                return True, False
            if status_value in terminal:
                return False, True
            return False, False
        if trigger == "FAILED":
            if status_value == ActionLogStatus.FAILED.value:
                return True, False
            if status_value in terminal:
                return False, True
            return False, False
        if trigger == "COMPLETED":
            if status_value in terminal:
                return True, False
            return False, False
        return False, True

    async def _emit_deferred_action_command(
        self,
        *,
        log_rec: ActionLogRecord,
        triggered_by_action_log_id: str,
    ) -> None:
        base_ctx = (
            log_rec.metadata.get("__submit_context")
            if isinstance(log_rec.metadata, dict)
            else None
        )
        base_branch = safe_str(base_ctx.get("base_branch")) if isinstance(base_ctx, dict) else ""
        if not base_branch:
            base_branch = "main"
        overlay_branch = safe_str(base_ctx.get("overlay_branch")) if isinstance(base_ctx, dict) else ""
        if not overlay_branch:
            overlay_branch = safe_str(
                (log_rec.writeback_target or {}).get("branch") or AppConfig.get_ontology_writeback_branch(log_rec.db_name)
            )

        ontology_commit_id = safe_str(log_rec.ontology_commit_id)
        if not ontology_commit_id:
            raise RuntimeError("Deferred action log missing ontology_commit_id")

        action_log_uuid = UUID(str(log_rec.action_log_id))
        submitted_at = log_rec.submitted_at
        submitted_at_iso = submitted_at.isoformat() if hasattr(submitted_at, "isoformat") else utcnow().isoformat()
        payload = log_rec.input if isinstance(log_rec.input, dict) else {}
        if isinstance(log_rec.metadata, dict):
            batch_payload = log_rec.metadata.get("__batch_payload")
            if isinstance(batch_payload, dict):
                payload = batch_payload
        command = ActionCommand(
            command_id=action_log_uuid,
            db_name=log_rec.db_name,
            action_log_id=action_log_uuid,
            action_type_id=log_rec.action_type_id,
            ontology_commit_id=ontology_commit_id,
            base_branch=base_branch,
            overlay_branch=overlay_branch,
            correlation_id=log_rec.correlation_id,
            payload=dict(payload or {}),
            metadata={
                **(log_rec.metadata or {}),
                "correlation_id": log_rec.correlation_id,
                "submitted_at": submitted_at_iso,
                "ontology": {"ref": f"branch:{base_branch}", "commit": ontology_commit_id},
                "dependency_triggered_by": triggered_by_action_log_id,
            },
        )

        envelope = EventEnvelope.from_command(
            command,
            actor=log_rec.submitted_by,
            kafka_topic=AppConfig.ACTION_COMMANDS_TOPIC,
            metadata={"service": "action_worker", "mode": "action_writeback_dependency"},
        )
        await event_store.append_event(envelope)

    async def _trigger_dependent_actions(
        self,
        *,
        db_name: str,
        parent_action_log_id: str,
    ) -> None:
        parent = await self.action_logs.get_log(action_log_id=parent_action_log_id)
        if not parent:
            return
        parent_status = str(parent.status or "").strip().upper()
        if parent_status not in {ActionLogStatus.SUCCEEDED.value, ActionLogStatus.FAILED.value}:
            return

        child_ids = await self.action_logs.list_dependent_children(parent_action_log_id=parent_action_log_id)
        if not child_ids:
            return

        for child_id in child_ids:
            child = await self.action_logs.get_log(action_log_id=child_id)
            if not child:
                continue
            if str(child.db_name or "").strip() != str(db_name or "").strip():
                continue
            child_status = str(child.status or "").strip().upper()
            if child_status in {ActionLogStatus.SUCCEEDED.value, ActionLogStatus.FAILED.value}:
                continue

            dependencies = await self.action_logs.list_dependency_status_for_child(child_action_log_id=child_id)
            if not dependencies:
                continue

            waiting = False
            impossible_reasons: List[Dict[str, Any]] = []
            for dep in dependencies:
                satisfied, impossible = self._dependency_is_satisfied(
                    trigger_on=dep.trigger_on,
                    parent_status=dep.parent_status,
                )
                if satisfied:
                    continue
                if impossible:
                    impossible_reasons.append(
                        {
                            "parent_action_log_id": dep.parent_action_log_id,
                            "trigger_on": dep.trigger_on,
                            "parent_status": dep.parent_status,
                        }
                    )
                else:
                    waiting = True

            if impossible_reasons:
                await self.action_logs.mark_failed(
                    action_log_id=child_id,
                    result=self._audit_result(
                        audit_policy=None,
                        payload={
                            "error": "action_dependency_not_satisfied",
                            "message": "Dependency condition cannot be satisfied",
                            "dependencies": impossible_reasons,
                        },
                    ),
                )
                continue
            if waiting:
                continue

            try:
                await self._emit_deferred_action_command(
                    log_rec=child,
                    triggered_by_action_log_id=parent_action_log_id,
                )
            except Exception as exc:
                logger.exception(
                    "Failed to dispatch deferred action command (child=%s triggered_by=%s)",
                    child_id,
                    parent_action_log_id,
                )
                await self.action_logs.mark_failed(
                    action_log_id=child_id,
                    result=self._audit_result(
                        audit_policy=None,
                        payload={
                            "error": "action_dependency_dispatch_failed",
                            "message": str(exc),
                            "triggered_by": parent_action_log_id,
                        },
                    ),
                )
                continue

    async def _ensure_branch(self, *, repository: str, branch: str) -> None:
        await ensure_lakefs_branch(lakefs_client=self.lakefs_client, repository=repository, branch=branch, source="main")

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
        except LakeFSConflictError as exc:
            logger.debug(
                "Writeback patchset staging branch already exists (repository=%s branch=%s staging_branch=%s): %s",
                repository,
                branch,
                staging_branch,
                exc,
            )

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
        try:
            await self.lakefs_client.delete_branch(repository=repository, name=staging_branch)
        except LakeFSError as exc:
            logger.warning(
                "Failed to cleanup writeback patchset staging branch %s: %s",
                staging_branch,
                exc,
                exc_info=True,
            )
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
            try:
                await self.lakefs_client.delete_branch(repository=repository, name=staging_branch)
            except LakeFSError as exc:
                logger.warning(
                    "Failed to cleanup empty writeback queue staging branch %s: %s",
                    staging_branch,
                    exc,
                    exc_info=True,
                )
            return

        try:
            await self.lakefs_client.create_branch(repository=repository, name=staging_branch, source=branch)
        except LakeFSConflictError as exc:
            logger.debug(
                "Writeback queue staging branch already exists (repository=%s branch=%s staging_branch=%s): %s",
                repository,
                branch,
                staging_branch,
                exc,
            )

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
        try:
            await self.lakefs_client.delete_branch(repository=repository, name=staging_branch)
        except LakeFSError as exc:
            logger.warning(
                "Failed to cleanup writeback queue staging branch %s: %s",
                staging_branch,
                exc,
                exc_info=True,
            )


async def main() -> None:
    await run_worker_until_stopped(
        ActionWorker(),
        task_name="action-worker.run",
    )


if __name__ == "__main__":
    asyncio.run(main())
