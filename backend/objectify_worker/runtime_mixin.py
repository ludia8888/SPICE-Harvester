from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.config.settings import get_settings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited
from shared.models.objectify_job import ObjectifyJob
from shared.security.auth_utils import get_expected_token
from shared.services.core.sheet_import_service import FieldMapping
from shared.services.grpc.oms_gateway_client import OMSGrpcHttpCompatClient
from shared.services.kafka.processed_event_worker import HeartbeatOptions, RegistryKey
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.storage.elasticsearch_service import create_elasticsearch_service
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.utils.import_type_normalization import resolve_import_type
from shared.utils.object_type_backing import list_backing_sources, select_primary_backing_source

from objectify_worker import input_context as _input_context
from objectify_worker import job_processing as _job_processing
from objectify_worker.ontology_contracts import (
    build_property_type_context as _build_property_type_context_impl,
    extract_ontology_fields as _extract_ontology_fields_impl,
    extract_ontology_pk_targets as _extract_ontology_pk_targets_impl,
    is_blank as _is_blank_impl,
    map_mappings_by_target as _map_mappings_by_target_impl,
    normalize_constraints as _normalize_constraints_impl,
    normalize_ontology_payload as _normalize_ontology_payload_impl,
    normalize_relationship_ref as _normalize_relationship_ref_impl,
    resolve_object_type_key_contract as _resolve_object_type_key_contract_impl,
    validate_value_constraints as _validate_value_constraints_impl,
    validate_value_constraints_single as _validate_value_constraints_single_impl,
)
from objectify_worker.runtime_helpers import ObjectifyNonRetryableError
from objectify_worker.validation_codes import ObjectifyValidationCode as VC
from objectify_worker.write_paths import (
    DatasetPrimaryIndexWritePath,
    WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
)

logger = logging.getLogger("objectify_worker.main")


class ObjectifyWorkerRuntimeMixin:
    def _build_error_report(
        self,
        *,
        error: str,
        report: Optional[Dict[str, Any]] = None,
        job: Optional[ObjectifyJob] = None,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        report_payload = dict(report or {})
        errors_payload = report_payload.pop("errors", None)
        context_payload: Dict[str, Any] = dict(context or {})
        for key, value in report_payload.items():
            context_payload.setdefault(key, value)
        if job:
            context_payload.setdefault("job_id", job.job_id)
            context_payload.setdefault("db_name", job.db_name)
            context_payload.setdefault("dataset_id", job.dataset_id)
            context_payload.setdefault("dataset_version_id", job.dataset_version_id)
            context_payload.setdefault("artifact_id", job.artifact_id)
            context_payload.setdefault("artifact_key", job.artifact_key)
            context_payload.setdefault("artifact_output_name", job.artifact_output_name)
            context_payload.setdefault("dataset_branch", job.dataset_branch)
            context_payload.setdefault("mapping_spec_id", job.mapping_spec_id)
            context_payload.setdefault("mapping_spec_version", job.mapping_spec_version)
            context_payload.setdefault("target_class_id", job.target_class_id)
        if not context_payload:
            context_payload = None
        if message is None:
            message = "Objectify validation failed" if error.startswith("validation_failed") else "Objectify job failed"
        return build_error_envelope(
            service_name="objectify-worker",
            message=message,
            detail=error,
            errors=errors_payload,
            objectify_error=error,
            context=context_payload,
        )

    async def _record_gate_result(
        self,
        *,
        job: ObjectifyJob,
        status: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.dataset_registry:
            return
        try:
            await self.dataset_registry.record_gate_result(
                scope="objectify_job",
                subject_type="objectify_job",
                subject_id=job.job_id,
                status=status,
                details={
                    "job_id": job.job_id,
                    "dataset_id": job.dataset_id,
                    "dataset_version_id": job.dataset_version_id,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "target_class_id": job.target_class_id,
                    "details": details or {},
                },
            )
        except Exception as exc:
            logger.warning("Failed to record objectify gate result: %s", exc)

    async def _emit_objectify_completed_event(
        self,
        *,
        job: ObjectifyJob,
        total_rows: int,
        prepared_instances: int,
        indexed_instances: int,
        execution_mode: str,
        ontology_version: Optional[Dict[str, str]] = None,
    ) -> None:
        """Emit OBJECTIFY_COMPLETED event via pipeline control-plane."""
        try:
            from shared.services.pipeline.pipeline_control_plane_events import (
                emit_pipeline_control_plane_event,
            )

            pipeline_id = str(
                (job.options or {}).get("pipeline_id")
                or (job.options or {}).get("run_id")
                or job.job_id
            ).strip()

            await emit_pipeline_control_plane_event(
                event_type="OBJECTIFY_COMPLETED",
                pipeline_id=pipeline_id,
                event_id=job.job_id,
                data={
                    "job_id": job.job_id,
                    "db_name": job.db_name,
                    "target_class_id": job.target_class_id,
                    "dataset_id": job.dataset_id,
                    "dataset_version_id": job.dataset_version_id,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "execution_mode": execution_mode,
                    "total_rows": total_rows,
                    "prepared_instances": prepared_instances,
                    "indexed_instances": indexed_instances,
                    "ontology_version": ontology_version or {},
                    "write_path_mode": WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
                    "branch": job.ontology_branch or job.dataset_branch or "main",
                },
                kind="objectify_control_plane",
            )
            logger.info(
                "Emitted OBJECTIFY_COMPLETED event for job %s (class=%s, indexed=%d)",
                job.job_id,
                job.target_class_id,
                indexed_instances,
            )
        except Exception as exc:
            logger.warning(
                "Failed to emit OBJECTIFY_COMPLETED event for job %s: %s",
                job.job_id,
                exc,
                exc_info=True,
            )

    async def _update_object_type_active_version(
        self,
        *,
        job: ObjectifyJob,
        mapping_spec: Any,
    ) -> None:
        if not self.http or not self.dataset_registry:
            return
        try:
            resource_payload = await self._fetch_object_type_contract(job)
            resource = resource_payload.get("data") if isinstance(resource_payload, dict) else None
            if not isinstance(resource, dict):
                resource = resource_payload if isinstance(resource_payload, dict) else {}
            if not resource:
                return
            spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
            if not isinstance(spec, dict):
                spec = {}
            backing_sources = list_backing_sources(spec)
            backing_source = select_primary_backing_source(spec)
            if not backing_source:
                return

            backing_version_id = mapping_spec.backing_datasource_version_id if mapping_spec else None
            if not backing_version_id and job.dataset_version_id:
                backing_ref = str(
                    backing_source.get("ref")
                    or backing_source.get("backing_datasource_id")
                    or ""
                ).strip()
                if backing_ref:
                    backing_version = await self.dataset_registry.get_backing_datasource_version_for_dataset(
                        backing_id=backing_ref,
                        dataset_version_id=job.dataset_version_id,
                    )
                else:
                    backing_version = await self.dataset_registry.get_backing_datasource_version_by_dataset_version(
                        dataset_version_id=job.dataset_version_id
                    )
                if backing_version:
                    backing_version_id = backing_version.version_id
                    backing_source.setdefault("schema_hash", backing_version.schema_hash)
                    backing_source.setdefault("ref", backing_version.backing_id)
            if not backing_version_id:
                return

            backing_source["version_id"] = backing_version_id
            if job.dataset_version_id:
                backing_source["dataset_version_id"] = job.dataset_version_id
            if backing_sources:
                backing_sources[0] = backing_source
            else:
                backing_sources = [backing_source]
            spec["backing_source"] = backing_source
            spec["backing_sources"] = backing_sources
            resource["spec"] = spec

            branch = job.ontology_branch or job.dataset_branch or "main"
            expected_head = f"branch:{branch}"
            resp = await self.http.put(
                f"/api/v1/database/{job.db_name}/ontology/resources/object_type/{job.target_class_id}",
                params={"branch": branch, "expected_head_commit": expected_head},
                json=resource,
            )
            if resp.status_code >= 400:
                logger.warning(
                    "Failed to update object_type active version (status=%s): %s",
                    resp.status_code,
                    resp.text,
                )
        except Exception as exc:
            logger.warning("Failed to update object_type active version: %s", exc)

    @staticmethod
    def _normalize_ontology_payload(payload: Any) -> Dict[str, Any]:
        return _normalize_ontology_payload_impl(payload)

    @classmethod
    def _extract_ontology_fields(cls, payload: Any) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        return _extract_ontology_fields_impl(payload)

    @staticmethod
    def _is_blank(value: Any) -> bool:
        return _is_blank_impl(value)

    @staticmethod
    def _normalize_relationship_ref(value: Any, *, target_class: str) -> str:
        return _normalize_relationship_ref_impl(value, target_class=target_class)

    @staticmethod
    def _normalize_constraints(
        constraints: Any, *, raw_type: Optional[Any] = None
    ) -> Dict[str, Any]:
        return _normalize_constraints_impl(constraints, raw_type=raw_type)

    @staticmethod
    def _resolve_import_type(raw_type: Any) -> Optional[str]:
        return resolve_import_type(raw_type)

    def _validate_value_constraints(
        self,
        value: Any,
        *,
        constraints: Any,
        raw_type: Optional[Any],
    ) -> Optional[str]:
        return _validate_value_constraints_impl(value, constraints=constraints, raw_type=raw_type)

    def _validate_value_constraints_single(
        self,
        value: Any,
        *,
        constraints: Any,
        raw_type: Optional[Any],
    ) -> Optional[str]:
        return _validate_value_constraints_single_impl(value, constraints=constraints, raw_type=raw_type)

    def _extract_ontology_pk_targets(self, payload: Any) -> List[str]:
        return _extract_ontology_pk_targets_impl(payload)

    @staticmethod
    def _map_mappings_by_target(mappings: List[FieldMapping]) -> Dict[str, List[str]]:
        return _map_mappings_by_target_impl(mappings)

    def _has_p0_errors(self, errors: List[Dict[str, Any]]) -> bool:
        for err in errors:
            code = err.get("code") if isinstance(err, dict) else None
            if not code or code in self.P0_ERROR_CODES:
                return True
        return False

    async def _load_value_type_defs_with_validation(
        self,
        *,
        job: ObjectifyJob,
        prop_map: Dict[str, Dict[str, Any]],
        fail_job: Any,
    ) -> Dict[str, Dict[str, Any]]:
        value_type_refs = {
            str(meta.get("value_type_ref") or meta.get("valueTypeRef") or "").strip()
            for meta in prop_map.values()
            if isinstance(meta, dict)
        }
        value_type_refs = {ref for ref in value_type_refs if ref}
        value_type_defs, missing_value_types = await self._fetch_value_type_defs(job, value_type_refs)
        if missing_value_types:
            await fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": VC.VALUE_TYPE_NOT_FOUND.value,
                            "value_type_refs": sorted(missing_value_types),
                            "message": "Referenced value types are missing",
                        }
                    ]
                },
            )
        return value_type_defs

    def _build_property_type_context(
        self,
        *,
        prop_map: Dict[str, Dict[str, Any]],
        value_type_defs: Dict[str, Dict[str, Any]],
        target_class_id: str,
    ) -> Tuple[
        Dict[str, str],
        Dict[str, Any],
        Dict[str, Optional[Any]],
        set[str],
        set[str],
        List[str],
    ]:
        return _build_property_type_context_impl(
            prop_map=prop_map,
            value_type_defs=value_type_defs,
            target_class_id=target_class_id,
        )

    async def _resolve_object_type_key_contract(
        self,
        *,
        job: ObjectifyJob,
        ontology_payload: Any,
        prop_map: Dict[str, Dict[str, Any]],
        fail_job: Any,
        warnings: List[Dict[str, Any]],
    ) -> Tuple[List[str], List[str], List[List[str]], List[str], set[str]]:
        return await _resolve_object_type_key_contract_impl(
            job=job,
            ontology_payload=ontology_payload,
            prop_map=prop_map,
            fail_job=fail_job,
            warnings=warnings,
            fetch_object_type_contract=self._fetch_object_type_contract,
            ontology_pk_validation_mode=self.ontology_pk_validation_mode,
        )

    async def initialize(self) -> None:
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()

        self.objectify_registry = ObjectifyRegistry()
        await self.objectify_registry.initialize()

        self.pipeline_registry = PipelineRegistry()
        await self.pipeline_registry.initialize()

        self.processed = await create_processed_event_registry()

        self.lineage_store = None
        if self.enable_lineage:
            try:
                self.lineage_store = LineageStore()
                await self.lineage_store.initialize()
            except Exception as exc:
                if self.lineage_required:
                    raise RuntimeError("LineageStore unavailable") from exc
                log_exception_rate_limited(
                    logger,
                    zone=RuntimeZone.OBSERVABILITY,
                    operation="objectify_worker.initialize.lineage",
                    exc=exc,
                    code=ErrorCode.LINEAGE_UNAVAILABLE,
                    category=ErrorCategory.UPSTREAM,
                )
                self.lineage_store = None

        settings = get_settings()
        self.storage = create_lakefs_storage_service(settings)
        if self.storage is None:
            raise RuntimeError("LakeFS storage service is required for objectify worker")

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        token = get_expected_token(("OMS_CLIENT_TOKEN", "OMS_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"))
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Admin-Token"] = token
        self.http = OMSGrpcHttpCompatClient()

        self.elasticsearch_service = create_elasticsearch_service(settings)
        await self.elasticsearch_service.connect()
        self.instance_storage = create_storage_service(settings)
        if self.instance_storage:
            logger.info("instance-events StorageService initialized (bucket=%s)", settings.storage.instance_bucket)
        else:
            logger.warning("instance-events StorageService unavailable; S3 command files will not be written")
        self.instance_write_path = DatasetPrimaryIndexWritePath(
            elasticsearch_service=self.elasticsearch_service,
            storage_service=self.instance_storage,
            instance_bucket=settings.storage.instance_bucket,
            chunk_size=self.dataset_primary_index_chunk_size,
            refresh=self.dataset_primary_refresh,
            prune_stale_on_full=self.dataset_primary_prune_stale_on_full,
        )
        logger.info(
            "Objectify write path mode: %s (chunk_size=%s, refresh=%s, prune_stale_on_full=%s)",
            WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
            self.dataset_primary_index_chunk_size,
            self.dataset_primary_refresh,
            self.dataset_primary_prune_stale_on_full,
        )

        self._initialize_safe_consumer_runtime(
            group_id=self.group_id,
            topics=[self.topic],
            service_name="objectify-worker",
            thread_name_prefix="objectify-worker-kafka",
            reset_partition_state=True,
            max_poll_interval_ms=600_000,
            session_timeout_ms=120_000,
        )

        self.dlq_producer = create_kafka_dlq_producer(
            bootstrap_servers=settings.database.kafka_servers,
            client_id=settings.observability.service_name or "objectify-worker",
        )

    async def close(self) -> None:
        await self._close_consumer_runtime()
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.elasticsearch_service:
            await self.elasticsearch_service.disconnect()
            self.elasticsearch_service = None
        self.instance_write_path = None
        if self.lineage_store:
            await self.lineage_store.close()
            self.lineage_store = None
        if self.processed:
            await self.processed.close()
            self.processed = None
        if self.objectify_registry:
            await self.objectify_registry.close()
            self.objectify_registry = None
        if self.pipeline_registry:
            await self.pipeline_registry.close()
            self.pipeline_registry = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None
        await close_kafka_producer(
            producer=self.dlq_producer,
            timeout_s=5.0,
            warning_logger=logger,
            warning_message="DLQ producer flush failed during shutdown: %s",
        )
        self.dlq_producer = None

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        logger.info("ObjectifyWorker started (topic=%s)", self.topic)
        try:
            await self.run_partitioned_loop(poll_timeout=0.25, idle_sleep=0.0)
        finally:
            await self._cancel_inflight_tasks()
            await self.close()

    # --- ProcessedEventKafkaWorker Strategy hooks ---
    def _parse_payload(self, payload: Any) -> ObjectifyJob:  # type: ignore[override]
        return ObjectifyJob.model_validate_json(payload)

    def _registry_key(self, payload: ObjectifyJob) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(
            event_id=str(payload.job_id),
            aggregate_id=str(payload.dataset_id) if payload.dataset_id else None,
            sequence_number=None,
        )

    async def _process_payload(self, payload: ObjectifyJob) -> None:  # type: ignore[override]
        try:
            await self._process_job(payload)
        except Exception:
            logger.error(
                "Objectify job failed (job_id=%s db_name=%s mapping_spec_id=%s mapping_spec_version=%s): %s",
                payload.job_id,
                payload.db_name,
                payload.mapping_spec_id,
                payload.mapping_spec_version,
                payload,
                exc_info=True,
            )
            raise

    def _fallback_metadata(self, payload: ObjectifyJob) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        return {
            "job_id": payload.job_id,
            "db_name": payload.db_name,
            "dataset_id": payload.dataset_id,
            "dataset_version_id": payload.dataset_version_id,
            "mapping_spec_id": payload.mapping_spec_id,
            "mapping_spec_version": payload.mapping_spec_version,
            "target_class_id": payload.target_class_id,
            "artifact_id": payload.artifact_id,
            "artifact_key": payload.artifact_key,
            "artifact_output_name": payload.artifact_output_name,
        }

    def _span_name(self, *, payload: ObjectifyJob) -> str:  # type: ignore[override]
        return "objectify_worker.process_message"

    def _span_attributes(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: ObjectifyJob,
        registry_key: RegistryKey,
    ) -> Dict[str, Any]:
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        attrs.update(
            {
                "objectify.job_id": payload.job_id,
                "objectify.db_name": payload.db_name,
                "objectify.dataset_id": payload.dataset_id,
                "objectify.dataset_version_id": payload.dataset_version_id,
                "objectify.mapping_spec_id": payload.mapping_spec_id,
                "objectify.mapping_spec_version": payload.mapping_spec_version,
                "objectify.target_class_id": payload.target_class_id,
            }
        )
        return attrs

    def _metric_event_name(self, *, payload: ObjectifyJob) -> Optional[str]:  # type: ignore[override]
        return "OBJECTIFY_JOB"

    def _heartbeat_options(self) -> HeartbeatOptions:  # type: ignore[override]
        return HeartbeatOptions(
            warning_message="Objectify heartbeat failed (handler=%s event_id=%s): %s",
        )

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:  # type: ignore[override]
        try:
            record = getattr(self.tracing, "record_exception", None)
            if callable(record):
                record(error)
        except Exception as exc:
            logger.warning("Tracing record_exception failed during parse error handling: %s", exc, exc_info=True)
        await super()._on_parse_error(msg=msg, raw_payload=raw_payload, error=error)

    def _is_retryable_error(self, exc: Exception, *, payload: ObjectifyJob) -> bool:  # type: ignore[override]
        try:
            record = getattr(self.tracing, "record_exception", None)
            if callable(record):
                record(exc)
        except Exception as exc:
            logger.warning("Tracing record_exception failed during retryability check: %s", exc, exc_info=True)
        return self._is_retryable_error_impl(exc)

    async def _persist_objectify_failure_status(
        self,
        *,
        job: ObjectifyJob,
        status: str,
        error: str,
        attempt_count: int,
        retryable: bool,
        completed_at: Optional[datetime],
    ) -> None:
        if not self.objectify_registry:
            return
        try:
            existing = await self.objectify_registry.get_objectify_job(job_id=job.job_id)
            existing_status = str(getattr(existing, "status", "") or "").strip().upper()
            report_payload = self._build_error_report(
                error=error,
                job=job,
                context={"attempt_count": int(attempt_count), "retryable": bool(retryable)},
            )
            if existing_status in {"COMMITTING", "COMPLETED"}:
                committed_report = dict(getattr(existing, "report", None) or {})
                committed_report["post_commit_error"] = report_payload
                await self.objectify_registry.update_objectify_job_status(
                    job_id=job.job_id,
                    status=existing_status,
                    error=(error or "")[:4000],
                    report=committed_report,
                    completed_at=getattr(existing, "completed_at", None),
                )
                return
            await self.objectify_registry.update_objectify_job_status(
                job_id=job.job_id,
                status=status,
                error=(error or "")[:4000],
                report=report_payload,
                completed_at=completed_at,
            )
        except Exception as status_err:
            logger.warning(
                "Failed to persist objectify failure status (job_id=%s): %s",
                job.job_id,
                status_err,
                exc_info=True,
            )

    async def _on_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: ObjectifyJob,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        await self._persist_objectify_failure_status(
            job=payload,
            status="RETRYING",
            error=error,
            attempt_count=attempt_count,
            retryable=retryable,
            completed_at=None,
        )
        logger.warning(
            "Objectify job failed; will retry (job_id=%s attempt=%s backoff=%ss): %s",
            payload.job_id,
            attempt_count,
            int(backoff_s),
            error,
        )

    async def _on_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: ObjectifyJob,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        await self._persist_objectify_failure_status(
            job=payload,
            status="FAILED",
            error=error,
            attempt_count=attempt_count,
            retryable=retryable,
            completed_at=datetime.now(timezone.utc),
        )
        logger.error(
            "Objectify job max retries exceeded; sending to DLQ (job_id=%s attempt=%s)",
            payload.job_id,
            attempt_count,
        )

    async def _resolve_job_input_context(
        self,
        *,
        job: ObjectifyJob,
        fail_job: Any,
    ) -> Tuple[str, Optional[str], Optional[str]]:
        return await _input_context.resolve_job_input_context(
            self,
            job=job,
            fail_job=fail_job,
        )

    async def _resolve_mapping_spec_for_job(
        self,
        *,
        job: ObjectifyJob,
        fail_job: Any,
    ) -> Any:
        return await _input_context.resolve_mapping_spec_for_job(
            self,
            job=job,
            fail_job=fail_job,
        )

    async def _process_job(self, job: ObjectifyJob) -> None:
        from objectify_worker.runtime_helpers import (
            _auto_detect_watermark_column,
            _compute_lakefs_delta,
            _extract_instance_relationships,
        )

        await _job_processing.process_job(
            self,
            job=job,
            fail_exception_cls=ObjectifyNonRetryableError,
            compute_lakefs_delta=_compute_lakefs_delta,
            extract_instance_relationships=_extract_instance_relationships,
            auto_detect_watermark_column=_auto_detect_watermark_column,
        )
