"""
Objectify Worker
Dataset version -> Ontology instance bulk-create pipeline.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import queue as queue_module
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx
from confluent_kafka import Producer

from shared.services.kafka.dlq_publisher import DlqPublishSpec, publish_contextual_dlq_json
from shared.services.kafka.processed_event_worker import (
    HeartbeatOptions,
    ProcessedEventKafkaWorker,
    RegistryKey,
)
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.kafka.retry_classifier import (
    OBJECTIFY_JOB_RETRY_PROFILE,
    classify_retryable_with_profile,
)
from shared.services.kafka.safe_consumer import SafeKafkaConsumer

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.lineage_edge_types import EDGE_DATASET_VERSION_OBJECTIFIED
from shared.models.objectify_job import ObjectifyJob
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.services.grpc.oms_gateway_client import OMSGrpcHttpCompatClient
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.core.sheet_import_service import FieldMapping, SheetImportService
from shared.utils.deterministic_ids import deterministic_uuid5_hex_prefix
from shared.utils.import_type_normalization import normalize_import_target_type, resolve_import_type
from shared.utils.key_spec import normalize_key_spec, normalize_object_type_key_spec
from shared.utils.object_type_backing import list_backing_sources, select_primary_backing_source
from shared.utils.s3_uri import parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash_from_payload
from shared.utils.string_list_utils import normalize_string_list
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited, record_lineage_or_raise
from shared.security.auth_utils import get_expected_token
from shared.services.pipeline.objectify_delta_utils import (
    DeltaResult,
    ObjectifyDeltaComputer,
    create_delta_computer_for_mapping_spec,
)
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConfig
from shared.utils.app_logger import configure_logging
from shared.services.core.relationship_extractor import (
    extract_relationships as _extract_instance_relationships_raw,
)
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
from objectify_worker import batch_loading as _batch_loading
from objectify_worker import delta_processing as _delta_processing
from objectify_worker import input_context as _input_context
from objectify_worker import instance_validation as _instance_validation
from objectify_worker import job_processing as _job_processing
from objectify_worker import key_constraint_scanning as _key_constraint_scanning
from objectify_worker import lineage_helpers as _lineage_helpers
from objectify_worker import link_index_processing as _link_index_processing
from objectify_worker import ontology_runtime as _ontology_runtime
from objectify_worker.validation_codes import ObjectifyValidationCode as VC
from objectify_worker.write_paths import (
    DatasetPrimaryIndexWritePath,
    ObjectifyWritePath,
    WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
)

logger = logging.getLogger(__name__)

# Well-known watermark column names in priority order
_WATERMARK_CANDIDATES = (
    "updated_at", "modified_at", "last_modified", "last_updated",
    "created_at", "timestamp", "event_time", "ingested_at",
)


def _auto_detect_watermark_column(
    *,
    columns: Optional[List[str]],
    options: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Auto-detect a watermark column from dataset schema or options.

    Checks well-known temporal column names in priority order.
    """
    # Try from explicit column list (e.g. dataset schema)
    if columns:
        lower_map = {c.lower(): c for c in columns}
        for candidate in _WATERMARK_CANDIDATES:
            if candidate in lower_map:
                return lower_map[candidate]

    # Try from options hint
    if options and isinstance(options, dict):
        hint = options.get("watermark_column_hint")
        if hint and isinstance(hint, str):
            return hint.strip() or None

    return None


async def _compute_lakefs_delta(
    *,
    job: ObjectifyJob,
    delta_computer: ObjectifyDeltaComputer,
    storage: Any,
) -> Optional[DeltaResult]:
    """Compute row-level delta between two LakeFS commits using the diff API.

    Returns a DeltaResult, or None if the diff could not be computed.
    """
    base_commit = job.base_commit_id
    if not base_commit:
        return None

    # Derive lakeFS repository/ref/path from the artifact_key S3 gateway URL:
    #   s3://<repo>/<ref>/<path>
    # In this stack, <ref> is typically a commit id (not a branch).
    repository = ""
    target_ref = ""
    diff_prefix = ""
    if job.artifact_key:
        parts = job.artifact_key.replace("s3://", "").split("/", 2)
        if len(parts) >= 2:
            repository = parts[0]
            target_ref = parts[1]
            raw_path = parts[2] if len(parts) >= 3 else ""
            if raw_path:
                # Use a stable dataset-level prefix so diffs include old/new staging paths.
                # Example:
                #   datasets/<db>/<dataset_id>/<dataset_name>/staging/<ingest_id>/source.csv
                # → datasets/<db>/<dataset_id>/<dataset_name>
                if "/staging/" in raw_path:
                    diff_prefix = raw_path.split("/staging/")[0]
                else:
                    diff_prefix = raw_path.rsplit("/", 1)[0] if "/" in raw_path else raw_path

    if not repository or not target_ref:
        return None

    try:
        lakefs_config = LakeFSConfig.from_env()
        lakefs_client = LakeFSClient(config=lakefs_config)

        async def _read_file_rows(repo: str, ref: str, path: str) -> List[Dict[str, Any]]:
            """Read rows from a file at a given LakeFS ref via S3 gateway."""
            import csv
            import io
            bucket = repo
            key = f"{ref}/{path}" if not path.startswith(ref) else path
            try:
                raw = await storage.load_bytes(bucket, key)
                text = raw.decode("utf-8", errors="replace")
                reader = csv.DictReader(io.StringIO(text))
                return list(reader)
            except Exception as exc:
                logger.warning("Failed to read %s/%s at ref %s: %s", repo, path, ref, exc)
                return []

        result = await delta_computer.compute_delta_from_lakefs_diff(
            lakefs_client=lakefs_client,
            repository=repository,
            base_ref=base_commit,
            target_ref=target_ref,
            path=diff_prefix,
            file_reader=_read_file_rows,
        )
        return result
    except Exception as exc:
        logger.error("LakeFS delta computation failed: %s", exc, exc_info=True)
        return None


def _extract_instance_relationships(
    instance: Dict[str, Any],
    *,
    rel_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Thin wrapper: extract relationships for a single instance using pre-parsed rel_map."""
    try:
        return _extract_instance_relationships_raw(
            instance, rel_map=rel_map, allow_pattern_fallback=True,
        )
    except Exception as exc:
        raise ObjectifyNonRetryableError(
            f"relationship extraction failed for instance {instance.get('instance_id') or '<unknown>'}: {exc}"
        ) from exc


class ObjectifyNonRetryableError(RuntimeError):
    """Raised for objectify failures that should not be retried."""


class ObjectifyWorker(ProcessedEventKafkaWorker[ObjectifyJob, None]):
    P0_ERROR_CODES = {
        "SOURCE_FIELD_MISSING",
        "SOURCE_FIELD_UNKNOWN",
        "TARGET_FIELD_UNKNOWN",
        "UNSUPPORTED_TARGET_TYPE",
        "TYPE_COERCION_FAILED",
        "MAPPING_SPEC_TARGET_UNKNOWN",
        "MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED",
        "MAPPING_SPEC_UNSUPPORTED_TYPE",
        "MAPPING_SPEC_REQUIRED_MISSING",
        "MAPPING_SPEC_PRIMARY_KEY_MISSING",
        "MAPPING_SPEC_TITLE_KEY_MISSING",
        "MAPPING_SPEC_UNIQUE_KEY_MISSING",
        "MAPPING_SPEC_TARGET_TYPE_MISMATCH",
        "OBJECT_TYPE_CONTRACT_MISSING",
        "OBJECT_TYPE_INACTIVE",
        "OBJECT_TYPE_PRIMARY_KEY_MISSING",
        "OBJECT_TYPE_TITLE_KEY_MISSING",
        "OBJECT_TYPE_PRIMARY_KEY_MISMATCH",
        "OBJECT_TYPE_KEY_FIELDS_MISSING",
        "KEY_SPEC_PRIMARY_KEY_MISSING",
        "KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH",
        "PRIMARY_KEY_MISSING",
        "PRIMARY_KEY_DUPLICATE",
        "UNIQUE_KEY_DUPLICATE",
        "REQUIRED_FIELD_MISSING",
        "VALUE_CONSTRAINT_FAILED",
    }

    @staticmethod
    def _source_lineage_branch(job: ObjectifyJob) -> str:
        return str(job.dataset_branch or job.ontology_branch or "main")

    @staticmethod
    def _target_lineage_branch(job: ObjectifyJob) -> str:
        return str(job.ontology_branch or job.dataset_branch or "main")

    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.objectify

        self.running = False
        self.service_name = "objectify-worker"
        self.topic = (AppConfig.OBJECTIFY_JOBS_TOPIC or "objectify-jobs").strip() or "objectify-jobs"
        self.dlq_topic = (AppConfig.OBJECTIFY_JOBS_DLQ_TOPIC or "objectify-jobs-dlq").strip() or "objectify-jobs-dlq"
        self._dlq_spec = DlqPublishSpec(
            dlq_topic=self.dlq_topic,
            service_name=self.service_name,
            span_name="objectify_worker.dlq_produce",
            flush_timeout_seconds=10.0,
        )
        self.group_id = (AppConfig.OBJECTIFY_JOBS_GROUP or "objectify-worker-group").strip()
        self.handler = str(cfg.worker_handler or "objectify_worker").strip() or "objectify_worker"
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.pipeline_registry: Optional[PipelineRegistry] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage_store: Optional[LineageStore] = None
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.lineage_required = bool(settings.observability.lineage_required_effective and self.enable_lineage)
        self.storage = None
        self.instance_storage = None  # MinIO/S3 StorageService for instance-events
        self.http: Optional[OMSGrpcHttpCompatClient] = None
        self.elasticsearch_service: Optional[ElasticsearchService] = None
        self.instance_write_path: Optional[ObjectifyWritePath] = None

        self.batch_size_default = int(cfg.batch_size)
        self.row_batch_size_default = int(cfg.row_batch_size)
        self.bulk_update_batch_size = int(cfg.bulk_update_batch_size_effective)
        self.list_page_size = int(cfg.list_page_size)
        self.max_rows_default = int(cfg.max_rows)
        self.lineage_max_links = int(cfg.lineage_max_links)
        self.max_retries = int(cfg.max_retries)
        self.backoff_base = int(cfg.backoff_base_seconds)
        self.backoff_max = int(cfg.backoff_max_seconds)
        self.ontology_pk_validation_mode = str(cfg.ontology_pk_validation_mode or "warn").strip().lower() or "warn"
        self.dataset_primary_index_chunk_size = int(cfg.dataset_primary_index_chunk_size)
        self.dataset_primary_refresh = bool(cfg.dataset_primary_refresh)
        self.dataset_primary_prune_stale_on_full = bool(cfg.dataset_primary_prune_stale_on_full)
        self.tracing = get_tracing_service(self.service_name)
        self.metrics = get_metrics_collector(self.service_name)
        self._init_partition_state()

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
        """Extract ontology-declared primaryKey fields in order (best-effort)."""
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
            max_poll_interval_ms=600_000,   # 10min (default 5min) — bulk-create batches need time
            session_timeout_ms=120_000,     # 2min (default 45s) — prevent heartbeat timeout during HTTP calls
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
            report_payload = self._build_error_report(
                error=error,
                job=job,
                context={"attempt_count": int(attempt_count), "retryable": bool(retryable)},
            )
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
        await _job_processing.process_job(
            self,
            job=job,
            fail_exception_cls=ObjectifyNonRetryableError,
            compute_lakefs_delta=_compute_lakefs_delta,
            extract_instance_relationships=_extract_instance_relationships,
            auto_detect_watermark_column=_auto_detect_watermark_column,
        )

    async def _bulk_update_instances(
        self,
        job: ObjectifyJob,
        updates: List[Dict[str, Any]],
        *,
        ontology_version: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if not self.http:
            raise RuntimeError("OMS client not initialized")
        branch = job.ontology_branch or job.dataset_branch or "main"
        metadata = {
            "objectify_job_id": job.job_id,
            "mapping_spec_id": job.mapping_spec_id,
            "mapping_spec_version": job.mapping_spec_version,
            "dataset_id": job.dataset_id,
            "dataset_version_id": job.dataset_version_id,
            "artifact_id": job.artifact_id,
            "artifact_output_name": job.artifact_output_name,
            "options": job.options,
            "link_index": True,
        }
        if ontology_version:
            metadata["ontology"] = ontology_version
        resp = await self.http.post(
            f"/api/v1/instances/{job.db_name}/async/{job.target_class_id}/bulk-update",
            params={"branch": branch},
            json={
                "instances": updates,
                "metadata": metadata,
            },
        )
        resp.raise_for_status()
        if not resp.text:
            return {}
        return resp.json()

    async def _iter_class_instance_ids(
        self,
        *,
        db_name: str,
        class_id: str,
        branch: str,
        limit: int = 1000,
    ) -> AsyncIterator[str]:
        if not self.http:
            raise RuntimeError("OMS client not initialized")
        offset = 0
        while True:
            resp = await self.http.get(
                f"/api/v1/instance/{db_name}/class/{class_id}/instances",
                params={"limit": limit, "offset": offset, "branch": branch},
            )
            resp.raise_for_status()
            if not resp.text:
                return
            payload = resp.json()
            instances = payload.get("instances") if isinstance(payload, dict) else None
            if not isinstance(instances, list) or not instances:
                return
            for inst in instances:
                if not isinstance(inst, dict):
                    continue
                raw_id = inst.get("instance_id") or inst.get("id") or inst.get("@id")
                if not raw_id:
                    continue
                value = str(raw_id)
                if "/" in value:
                    value = value.split("/")[-1]
                value = value.strip()
                if value:
                    yield value
            if len(instances) < limit:
                return
            offset += limit

    async def _resolve_artifact_output(self, job: ObjectifyJob) -> Tuple[str, str]:
        return await _input_context.resolve_artifact_output(self, job)

    async def _fetch_target_field_types(self, job: ObjectifyJob) -> Dict[str, str]:
        return await _ontology_runtime.fetch_target_field_types(self, job)

    async def _fetch_class_schema(self, job: ObjectifyJob) -> Dict[str, Any]:
        return await _ontology_runtime.fetch_class_schema(self, job)

    async def _fetch_object_type_contract(self, job: ObjectifyJob) -> Dict[str, Any]:
        return await _ontology_runtime.fetch_object_type_contract(self, job)

    async def _fetch_value_type_defs(
        self,
        job: ObjectifyJob,
        value_type_refs: set[str],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        return await _ontology_runtime.fetch_value_type_defs(self, job, value_type_refs)

    async def _fetch_ontology_version(self, job: ObjectifyJob) -> Dict[str, str]:
        return await _ontology_runtime.fetch_ontology_version(self, job)

    @staticmethod
    def _normalize_pk_fields(value: Any) -> List[str]:
        return normalize_string_list(value)

    @staticmethod
    def _hash_payload(payload: Any) -> str:
        raw = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _derive_row_key(
        self,
        *,
        columns: List[Any],
        col_index: Dict[str, int],
        row: Optional[List[Any]],
        instance: Dict[str, Any],
        pk_fields: List[str],
        pk_targets: List[str],
    ) -> Optional[str]:
        if pk_targets:
            if all(field in instance and not self._is_blank(instance.get(field)) for field in pk_targets):
                values = [str(instance.get(field)) for field in pk_targets]
                return f"target:{'|'.join(values)}"
        if pk_fields:
            if all(field in instance and not self._is_blank(instance.get(field)) for field in pk_fields):
                values = [str(instance.get(field)) for field in pk_fields]
                return f"target:{'|'.join(values)}"
            if row is not None and all(field in col_index for field in pk_fields):
                values = []
                for field in pk_fields:
                    idx = col_index.get(field)
                    raw = row[idx] if idx is not None and idx < len(row) else None
                    if self._is_blank(raw):
                        return None
                    values.append(str(raw))
                return f"source:{'|'.join(values)}"

        return None

    def _derive_unique_key(self, instance: Dict[str, Any], key_fields: List[str]) -> Optional[str]:
        values: List[str] = []
        for field in key_fields:
            if field not in instance or self._is_blank(instance.get(field)):
                return None
            values.append(str(instance.get(field)))
        return "|".join(values) if values else None

    async def _iter_dataset_batches(
        self,
        *,
        job: ObjectifyJob,
        options: Dict[str, Any],
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        async for columns, rows, row_offset in _batch_loading.iter_dataset_batches(
            self,
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset

    async def _download_object_to_file(
        self,
        *,
        bucket: str,
        key: str,
        dest_path: str,
    ) -> None:
        await _batch_loading.download_object_to_file(
            self,
            bucket=bucket,
            key=key,
            dest_path=dest_path,
        )

    async def _iter_parquet_object_batches(
        self,
        *,
        bucket: str,
        parquet_keys: List[str],
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        async for columns, rows, row_offset in _batch_loading.iter_parquet_object_batches(
            self,
            bucket=bucket,
            parquet_keys=parquet_keys,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset

    async def _iter_csv_batches(
        self,
        *,
        bucket: str,
        key: str,
        delimiter: str,
        has_header: bool,
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        async for columns, rows, row_offset in _batch_loading.iter_csv_batches(
            self,
            bucket=bucket,
            key=key,
            delimiter=delimiter,
            has_header=has_header,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset

    async def _iter_json_part_batches(
        self,
        *,
        bucket: str,
        prefix: str,
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        async for columns, rows, row_offset in _batch_loading.iter_json_part_batches(
            self,
            bucket=bucket,
            prefix=prefix,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset

    async def _iter_dataset_batches_incremental(
        self,
        *,
        job: ObjectifyJob,
        options: Dict[str, Any],
        row_batch_size: int,
        max_rows: Optional[int],
        mapping_spec: Any,
    ) -> AsyncIterator[Tuple[List[str], List[List[Any]], int, Optional[str]]]:
        async for columns, rows, row_offset, new_watermark in _batch_loading.iter_dataset_batches_incremental(
            self,
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
            mapping_spec=mapping_spec,
        ):
            yield columns, rows, row_offset, new_watermark

    async def _update_watermark_after_job(
        self,
        *,
        job: ObjectifyJob,
        new_watermark: Optional[str],
    ) -> None:
        await _batch_loading.update_watermark_after_job(
            self,
            job=job,
            new_watermark=new_watermark,
        )

    def _build_instances_with_validation(
        self,
        *,
        columns: List[Any],
        rows: List[List[Any]],
        row_offset: int,
        mappings: List[FieldMapping],
        relationship_mappings: List[FieldMapping],
        relationship_meta: Dict[str, Dict[str, Any]],
        target_field_types: Dict[str, str],
        mapping_sources: List[str],
        sources_by_target: Dict[str, List[str]],
        required_targets: set[str],
        pk_targets: List[str],
        pk_fields: List[str],
        field_constraints: Dict[str, Any],
        field_raw_types: Dict[str, Optional[Any]],
        seen_row_keys: Optional[set[str]] = None,
    ) -> Dict[str, Any]:
        return _instance_validation.build_instances_with_validation(
            self,
            columns=columns,
            rows=rows,
            row_offset=row_offset,
            mappings=mappings,
            relationship_mappings=relationship_mappings,
            relationship_meta=relationship_meta,
            target_field_types=target_field_types,
            mapping_sources=mapping_sources,
            sources_by_target=sources_by_target,
            required_targets=required_targets,
            pk_targets=pk_targets,
            pk_fields=pk_fields,
            field_constraints=field_constraints,
            field_raw_types=field_raw_types,
            seen_row_keys=seen_row_keys,
        )

    async def _run_link_index_job(
        self,
        *,
        job: ObjectifyJob,
        mapping_spec: Any,
        options: Dict[str, Any],
        mappings: List[FieldMapping],
        mapping_sources: List[str],
        mapping_targets: List[str],
        sources_by_target: Dict[str, List[str]],
        prop_map: Dict[str, Dict[str, Any]],
        rel_map: Dict[str, Dict[str, Any]],
        relationship_mappings: List[FieldMapping],
        stable_seed: str,
        row_batch_size: int,
        max_rows: Optional[int],
    ) -> None:
        await _link_index_processing.run_link_index_job(
            self,
            job=job,
            mapping_spec=mapping_spec,
            options=options,
            mappings=mappings,
            mapping_sources=mapping_sources,
            mapping_targets=mapping_targets,
            sources_by_target=sources_by_target,
            prop_map=prop_map,
            rel_map=rel_map,
            relationship_mappings=relationship_mappings,
            stable_seed=stable_seed,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
            non_retryable_error_cls=ObjectifyNonRetryableError,
        )

    async def _validate_batches(
        self,
        *,
        job: ObjectifyJob,
        options: Dict[str, Any],
        mappings: List[FieldMapping],
        relationship_mappings: List[FieldMapping],
        relationship_meta: Dict[str, Dict[str, Any]],
        target_field_types: Dict[str, str],
        mapping_sources: List[str],
        sources_by_target: Dict[str, List[str]],
        required_targets: set[str],
        pk_targets: List[str],
        pk_fields: List[str],
        field_constraints: Dict[str, Any],
        field_raw_types: Dict[str, Optional[Any]],
        row_batch_size: int,
        max_rows: Optional[int],
    ) -> Tuple[int, List[Dict[str, Any]], List[int], Dict[str, Any]]:
        errors: List[Dict[str, Any]] = []
        error_row_indices: List[int] = []
        total_rows = 0
        error_count = 0
        seen_row_keys: set[str] = set()

        async for columns, rows, row_offset in self._iter_dataset_batches(
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            if not rows:
                continue
            total_rows += len(rows)
            batch = self._build_instances_with_validation(
                columns=columns,
                rows=rows,
                row_offset=row_offset,
                mappings=mappings,
                relationship_mappings=relationship_mappings,
                relationship_meta=relationship_meta,
                target_field_types=target_field_types,
                mapping_sources=mapping_sources,
                sources_by_target=sources_by_target,
                required_targets=required_targets,
                pk_targets=pk_targets,
                pk_fields=pk_fields,
                field_constraints=field_constraints,
                field_raw_types=field_raw_types,
                seen_row_keys=seen_row_keys,
            )
            batch_errors = batch.get("errors") or []
            for err in batch_errors:
                if len(errors) < 200:
                    errors.append(err)
            error_row_indices.extend(batch.get("error_row_indices") or [])
            error_count += len(batch_errors)
            if batch_errors and batch.get("fatal"):
                break

        error_row_indices = sorted(set(error_row_indices))
        stats = {
            "input_rows": total_rows,
            "error_rows": len(error_row_indices),
            "error_count": error_count,
        }
        return total_rows, errors, error_row_indices, stats

    async def _scan_key_constraints(
        self,
        *,
        job: ObjectifyJob,
        options: Dict[str, Any],
        mappings: List[FieldMapping],
        relationship_meta: Dict[str, Dict[str, Any]],
        target_field_types: Dict[str, str],
        sources_by_target: Dict[str, List[str]],
        required_targets: set[str],
        pk_targets: List[str],
        pk_fields: List[str],
        unique_keys: List[List[str]],
        row_batch_size: int,
        max_rows: Optional[int],
    ) -> Tuple[int, List[Dict[str, Any]], List[int], Dict[str, Any]]:
        return await _key_constraint_scanning.scan_key_constraints(
            self,
            job=job,
            options=options,
            mappings=mappings,
            relationship_meta=relationship_meta,
            target_field_types=target_field_types,
            sources_by_target=sources_by_target,
            required_targets=required_targets,
            pk_targets=pk_targets,
            pk_fields=pk_fields,
            unique_keys=unique_keys,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        )

    def _ensure_instance_ids(
        self,
        instances: List[Dict[str, Any]],
        *,
        class_id: str,
        stable_seed: str,
        mapping_spec_version: int,
        row_keys: Optional[List[str]] = None,
        instance_id_field: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        expected_key = instance_id_field or f"{class_id.lower()}_id"
        instance_ids: List[str] = []
        for idx, inst in enumerate(instances):
            if not isinstance(inst, dict):
                continue
            candidate = inst.get(expected_key)
            if not candidate:
                for key, value in inst.items():
                    if key.endswith("_id") and value:
                        candidate = value
                        break
            if not candidate:
                if not row_keys or idx >= len(row_keys) or not row_keys[idx]:
                    raise ValueError("row_key is required to derive instance id")
                row_key = row_keys[idx]
                seed = f"{stable_seed}:{mapping_spec_version}:{row_key}"
                suffix = deterministic_uuid5_hex_prefix(f"objectify:{seed}", length=12)
                candidate = f"{class_id.lower()}_{suffix}"
                inst[expected_key] = candidate
            str_candidate = str(candidate)
            inst["instance_id"] = str_candidate
            instance_ids.append(str_candidate)
        return instances, instance_ids

    @staticmethod
    def _build_column_lineage_pairs(mappings: Any, *, limit: int = 1000) -> List[Dict[str, str]]:
        return _lineage_helpers.build_column_lineage_pairs(mappings, limit=limit)

    async def _record_lineage_header(
        self,
        *,
        job: ObjectifyJob,
        mapping_spec: Any,
        ontology_version: Optional[Dict[str, str]],
        input_type: str,
        artifact_output_name: Optional[str] = None,
    ) -> Optional[str]:
        return await _lineage_helpers.record_lineage_header(
            self,
            job=job,
            mapping_spec=mapping_spec,
            ontology_version=ontology_version,
            input_type=input_type,
            artifact_output_name=artifact_output_name,
        )

    async def _record_instance_lineage(
        self,
        *,
        job: ObjectifyJob,
        job_node_id: Optional[str],
        instance_ids: List[str],
        mapping_spec_id: str,
        mapping_spec_version: int,
        column_lineage_pairs: Optional[List[Dict[str, str]]],
        ontology_version: Optional[Dict[str, str]],
        limit_remaining: int,
        input_type: str,
        artifact_output_name: Optional[str] = None,
    ) -> int:
        return await _lineage_helpers.record_instance_lineage(
            self,
            job=job,
            job_node_id=job_node_id,
            instance_ids=instance_ids,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
            column_lineage_pairs=column_lineage_pairs,
            ontology_version=ontology_version,
            limit_remaining=limit_remaining,
            input_type=input_type,
            artifact_output_name=artifact_output_name,
        )

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: ObjectifyJob,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        if not self.dlq_producer:
            logger.error("DLQ producer not configured; dropping objectify DLQ payload: %s", error)
            return
        dlq_payload = {
            "kind": "objectify_job_dlq",
            "job": payload.model_dump(mode="json"),
            "error": error,
            "attempt_count": int(attempt_count),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "raw_payload": raw_payload,
        }
        key = str(payload.job_id or "objectify-job").encode("utf-8")
        try:
            await publish_contextual_dlq_json(
                producer=self.dlq_producer,
                spec=self._dlq_spec,
                payload=dlq_payload,
                message_key=key,
                msg=msg,
                tracing=self.tracing,
                metrics=self.metrics,
            )
        except Exception as exc:
            logger.warning("Failed to publish objectify DLQ payload (job_id=%s): %s", payload.job_id, exc, exc_info=True)

    @staticmethod
    def _is_retryable_error_impl(exc: Exception) -> bool:
        if isinstance(exc, ObjectifyNonRetryableError):
            return False
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            if status == 429 or status >= 500:
                return True
            return False
        if isinstance(exc, httpx.RequestError):
            return True
        return classify_retryable_with_profile(exc, OBJECTIFY_JOB_RETRY_PROFILE)


async def main() -> None:
    configure_logging(get_settings().observability.log_level)
    worker = ObjectifyWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
