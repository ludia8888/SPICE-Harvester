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
import queue as queue_module
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
from shared.config.search_config import get_instances_index_name
from shared.config.settings import get_settings
from shared.models.objectify_job import ObjectifyJob
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.core.sheet_import_service import FieldMapping, SheetImportService
from shared.utils.deterministic_ids import deterministic_uuid5_hex_prefix
from shared.utils.import_type_normalization import normalize_import_target_type, resolve_import_type
from shared.utils.key_spec import normalize_key_spec
from shared.utils.ontology_type_normalization import normalize_ontology_base_type
from shared.utils.s3_uri import parse_s3_uri
from shared.utils.blank_utils import is_blank_value
from shared.utils.string_list_utils import normalize_string_list
from shared.errors.error_envelope import build_error_envelope
from shared.security.auth_utils import get_expected_token
from shared.validators import get_validator
from shared.validators.constraint_validator import ConstraintValidator
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

    # Determine target commit (current dataset version)
    target_commit = job.dataset_version_id or "main"
    # Determine repository (dataset_id is typically the lakeFS repository)
    repository = job.dataset_id

    try:
        lakefs_config = LakeFSConfig.from_env()
        lakefs_client = LakeFSClient(lakefs_config)

        # The artifact_key typically has format "s3://repo/branch/path"
        prefix = ""
        if job.artifact_key:
            parts = job.artifact_key.replace("s3://", "").split("/", 2)
            if len(parts) >= 3:
                prefix = parts[2]

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
            target_ref=target_commit,
            path=prefix,
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
        logger.debug("Relationship extraction failed for instance: %s", exc)
        return {}


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
        self.storage = None
        self.http: Optional[httpx.AsyncClient] = None
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
            backing_source = spec.get("backing_source") if isinstance(spec.get("backing_source"), dict) else {}
            if not backing_source:
                return

            backing_version_id = mapping_spec.backing_datasource_version_id if mapping_spec else None
            if not backing_version_id and job.dataset_version_id:
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
            spec["backing_source"] = backing_source
            resource["spec"] = spec

            expected_head = await self._fetch_ontology_head_commit(job)
            if not expected_head:
                return
            branch = job.ontology_branch or job.dataset_branch or "main"
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
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload["data"]
        if isinstance(payload, dict):
            return payload
        return {}

    @classmethod
    def _extract_ontology_fields(cls, payload: Any) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        data = cls._normalize_ontology_payload(payload)
        properties = data.get("properties") if isinstance(data, dict) else None
        relationships = data.get("relationships") if isinstance(data, dict) else None

        prop_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(properties, list):
            for prop in properties:
                if not isinstance(prop, dict):
                    continue
                name = str(prop.get("name") or "").strip()
                if not name:
                    continue
                prop_map[name] = prop

        rel_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(relationships, list):
            for rel in relationships:
                if not isinstance(rel, dict):
                    continue
                predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
                if predicate:
                    rel_map[predicate] = rel

        return prop_map, rel_map

    @staticmethod
    def _is_blank(value: Any) -> bool:
        return is_blank_value(value)

    @staticmethod
    def _normalize_relationship_ref(value: Any, *, target_class: str) -> str:
        if isinstance(value, dict):
            candidate = value.get("@id") or value.get("id")
            if candidate is not None:
                value = candidate
        if isinstance(value, list):
            raise ValueError("Relationship value must be a scalar")
        if value is None:
            raise ValueError("Relationship value is empty")
        raw = str(value).strip()
        if not raw:
            raise ValueError("Relationship value is empty")
        if "/" in raw:
            target, instance_id = raw.split("/", 1)
            if target != target_class:
                raise ValueError(
                    f"Relationship target mismatch: expected {target_class}, got {target}"
                )
            if not instance_id.strip():
                raise ValueError("Relationship instance id is empty")
            return f"{target}/{instance_id}"
        return f"{target_class}/{raw}"

    @staticmethod
    def _normalize_constraints(
        constraints: Any, *, raw_type: Optional[Any] = None
    ) -> Dict[str, Any]:
        if not isinstance(constraints, dict):
            constraints = {}
        normalized = dict(constraints)

        if "min" in normalized and "minimum" not in normalized:
            normalized["minimum"] = normalized["min"]
        if "max" in normalized and "maximum" not in normalized:
            normalized["maximum"] = normalized["max"]
        if "min_length" in normalized and "minLength" not in normalized:
            normalized["minLength"] = normalized["min_length"]
        if "max_length" in normalized and "maxLength" not in normalized:
            normalized["maxLength"] = normalized["max_length"]
        if "min_items" in normalized and "minItems" not in normalized:
            normalized["minItems"] = normalized["min_items"]
        if "max_items" in normalized and "maxItems" not in normalized:
            normalized["maxItems"] = normalized["max_items"]
        if "unique_items" in normalized and "uniqueItems" not in normalized:
            normalized["uniqueItems"] = normalized["unique_items"]
        if "enum_values" in normalized and "enum" not in normalized:
            normalized["enum"] = normalized["enum_values"]
        if "enumValues" in normalized and "enum" not in normalized:
            normalized["enum"] = normalized["enumValues"]
        if "regex" in normalized and "pattern" not in normalized:
            normalized["pattern"] = normalized["regex"]

        type_hint = str(raw_type or "").strip().lower()
        if type_hint.startswith("xsd:"):
            type_hint = type_hint[4:]
        if "format" not in normalized:
            if type_hint == "email":
                normalized["format"] = "email"
            elif type_hint in {"url", "uri"}:
                normalized["format"] = "uri"
            elif type_hint == "uuid":
                normalized["format"] = "uuid"

        normalized.pop("required", None)
        normalized.pop("nullable", None)
        return normalized

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
        if value is None:
            return None
        if isinstance(constraints, list):
            for constraint_set in constraints:
                message = self._validate_value_constraints_single(
                    value, constraints=constraint_set, raw_type=raw_type
                )
                if message:
                    return message
            return None
        return self._validate_value_constraints_single(value, constraints=constraints, raw_type=raw_type)

    def _validate_value_constraints_single(
        self,
        value: Any,
        *,
        constraints: Any,
        raw_type: Optional[Any],
    ) -> Optional[str]:
        if value is None:
            return None
        normalized = self._normalize_constraints(constraints, raw_type=raw_type)

        enum_values = normalized.get("enum")
        if enum_values is not None:
            if not isinstance(enum_values, list):
                enum_values = list(enum_values) if isinstance(enum_values, (set, tuple)) else [enum_values]
            if value not in enum_values:
                return f"Value must be one of: {enum_values}"

        type_hint = str(raw_type or "").strip().lower()
        if type_hint.startswith("xsd:"):
            type_hint = type_hint[4:]
        format_hint = str(normalized.get("format") or "").strip().lower()

        canonical_type = normalize_ontology_base_type(type_hint) if type_hint else None

        validator_key = None
        if canonical_type in {
            "array",
            "struct",
            "vector",
            "geopoint",
            "geoshape",
            "cipher",
            "marking",
            "media",
            "attachment",
            "time_series",
        }:
            validator_key = canonical_type
        elif type_hint in {"email", "url", "uri", "uuid", "ip", "phone"}:
            validator_key = "url" if type_hint in {"url", "uri"} else type_hint
        elif format_hint in {"email", "uuid", "uri", "url", "ipv4", "ipv6"}:
            validator_key = "url" if format_hint in {"uri", "url"} else format_hint
            if validator_key in {"ipv4", "ipv6"}:
                validator_key = "ip"
                normalized = dict(normalized)
                normalized.setdefault("version", "4" if format_hint == "ipv4" else "6")

        if validator_key:
            validator = get_validator(validator_key)
            if validator:
                result = validator.validate(value, normalized)
                if not result.is_valid:
                    return result.message
                if "format" in normalized:
                    normalized = dict(normalized)
                    normalized.pop("format", None)

        if normalized:
            result = ConstraintValidator.validate_constraints(value, "unknown", normalized)
            if not result.is_valid:
                return result.message
        return None

    @staticmethod
    def _map_mappings_by_target(mappings: List[FieldMapping]) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for item in mappings:
            target = str(item.target_field or "").strip()
            source = str(item.source_field or "").strip()
            if not target or not source:
                continue
            mapping.setdefault(target, []).append(source)
        return mapping

    def _has_p0_errors(self, errors: List[Dict[str, Any]]) -> bool:
        for err in errors:
            code = err.get("code") if isinstance(err, dict) else None
            if not code or code in self.P0_ERROR_CODES:
                return True
        return False

    async def initialize(self) -> None:
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()

        self.objectify_registry = ObjectifyRegistry()
        await self.objectify_registry.initialize()

        self.pipeline_registry = PipelineRegistry()
        await self.pipeline_registry.initialize()

        self.processed = await create_processed_event_registry()

        try:
            self.lineage_store = LineageStore()
            await self.lineage_store.initialize()
        except Exception as exc:
            logger.warning("LineageStore unavailable: %s", exc)
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
        self.http = httpx.AsyncClient(base_url=settings.services.oms_base_url, timeout=60.0, headers=headers)

        self.elasticsearch_service = create_elasticsearch_service(settings)
        await self.elasticsearch_service.connect()
        self.instance_write_path = DatasetPrimaryIndexWritePath(
            elasticsearch_service=self.elasticsearch_service,
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
        except Exception:
            pass
        await super()._on_parse_error(msg=msg, raw_payload=raw_payload, error=error)

    def _is_retryable_error(self, exc: Exception, *, payload: ObjectifyJob) -> bool:  # type: ignore[override]
        try:
            record = getattr(self.tracing, "record_exception", None)
            if callable(record):
                record(exc)
        except Exception:
            pass
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

    async def _process_job(self, job: ObjectifyJob) -> None:
        if not self.objectify_registry or not self.dataset_registry:
            raise RuntimeError("ObjectifyRegistry not initialized")
        if not self.instance_write_path:
            raise RuntimeError("Objectify write path not initialized")

        async def _fail_job(error: str, *, report: Optional[Dict[str, Any]] = None) -> None:
            # Log detailed validation errors before they get overwritten by terminal failure handler
            if report:
                errs = report.get("errors")
                if errs:
                    logger.error(
                        "Objectify validation errors (job_id=%s, first 5): %s",
                        job.job_id, errs[:5],
                    )
                stats = report.get("stats")
                if stats:
                    logger.error("Objectify validation stats (job_id=%s): %s", job.job_id, stats)
            report_payload = self._build_error_report(error=error, report=report, job=job)
            if error.startswith("validation_failed"):
                await self._record_gate_result(
                    job=job,
                    status="FAIL",
                    details=report_payload,
                )
            await self.objectify_registry.update_objectify_job_status(
                job_id=job.job_id,
                status="FAILED",
                error=error[:4000],
                report=report_payload,
                completed_at=datetime.now(timezone.utc),
            )
            raise ObjectifyNonRetryableError(error)

        record = await self.objectify_registry.get_objectify_job(job_id=job.job_id)
        if record and record.status in {"SUBMITTED", "COMPLETED"}:
            logger.info("Objectify job already submitted (job_id=%s); skipping", job.job_id)
            return

        dataset = await self.dataset_registry.get_dataset(dataset_id=job.dataset_id)
        if not dataset:
            await _fail_job(f"dataset_not_found:{job.dataset_id}")
        if dataset.db_name != job.db_name:
            await _fail_job("dataset_db_name_mismatch")

        input_type = "dataset_version" if job.dataset_version_id else "artifact"
        resolved_output_name: Optional[str] = None
        stable_seed: Optional[str] = None

        if job.dataset_version_id and job.artifact_id:
            await _fail_job("objectify_input_conflict")
        if not job.dataset_version_id and not job.artifact_id:
            await _fail_job("objectify_input_missing")

        if job.dataset_version_id:
            version = await self.dataset_registry.get_version(version_id=job.dataset_version_id)
            if not version or version.dataset_id != job.dataset_id:
                await _fail_job("dataset_version_mismatch")
            resolved_artifact_key = job.artifact_key or version.artifact_key
            if not resolved_artifact_key:
                await _fail_job("artifact_key_missing")
            if version.artifact_key and job.artifact_key and version.artifact_key != job.artifact_key:
                await _fail_job("artifact_key_mismatch")
            stable_seed = job.dataset_version_id
        else:
            resolved_artifact_key, resolved_output_name = await self._resolve_artifact_output(job)
            if job.artifact_key and job.artifact_key != resolved_artifact_key:
                await _fail_job("artifact_key_mismatch")
            stable_seed = f"artifact:{job.artifact_id}:{resolved_output_name}"

        job.artifact_key = resolved_artifact_key
        if resolved_output_name and not job.artifact_output_name:
            job.artifact_output_name = resolved_output_name

        mapping_spec = await self.objectify_registry.get_mapping_spec(mapping_spec_id=job.mapping_spec_id)
        if not mapping_spec:
            await _fail_job(f"mapping_spec_not_found:{job.mapping_spec_id}")
        if mapping_spec.dataset_id != job.dataset_id:
            await _fail_job("mapping_spec_dataset_mismatch")
        if int(mapping_spec.version) != int(job.mapping_spec_version):
            await _fail_job(
                f"mapping_spec_version_mismatch(job={job.mapping_spec_version} spec={mapping_spec.version})"
            )
        if mapping_spec.backing_datasource_version_id:
            if job.artifact_id:
                await _fail_job("backing_datasource_version_conflict")
            backing_version = await self.dataset_registry.get_backing_datasource_version(
                version_id=mapping_spec.backing_datasource_version_id
            )
            if not backing_version:
                await _fail_job("backing_datasource_version_missing")
            if not job.dataset_version_id:
                await _fail_job("backing_datasource_version_required")
            if backing_version.dataset_version_id != job.dataset_version_id:
                await _fail_job("backing_datasource_version_mismatch")

        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="RUNNING",
        )

        options: Dict[str, Any] = dict(mapping_spec.options or {})
        if isinstance(job.options, dict):
            options.update(job.options)
        link_index_mode = str(options.get("mode") or options.get("job_type") or "").strip().lower() == "link_index"
        if link_index_mode:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_UNSUPPORTED_TYPE",
                            "message": (
                                "Link-index mode is no longer supported in objectify worker. "
                                "Use relationship-aware ingestion or dedicated link indexing pipeline."
                            ),
                        }
                    ]
                },
            )

        max_rows = job.max_rows if job.max_rows is not None else options.get("max_rows")
        if max_rows is None:
            max_rows = self.max_rows_default
        try:
            max_rows = int(max_rows)
        except Exception:
            max_rows = self.max_rows_default
        if max_rows <= 0:
            max_rows = None

        batch_size = int(job.batch_size or options.get("batch_size") or self.batch_size_default)
        batch_size = max(1, min(batch_size, 5000))
        row_batch_size = int(options.get("row_batch_size") or options.get("read_batch_size") or self.row_batch_size_default)
        row_batch_size = max(1, min(row_batch_size, 50000))
        allow_partial = bool(job.allow_partial)

        mappings = [
            FieldMapping(source_field=str(m.get("source_field") or ""), target_field=str(m.get("target_field") or ""))
            for m in mapping_spec.mappings
            if isinstance(m, dict)
        ]
        mapping_sources: List[str] = []
        mapping_targets: List[str] = []
        for m in mappings:
            if m.source_field:
                mapping_sources.append(m.source_field)
            if m.target_field:
                mapping_targets.append(m.target_field)
        mapping_sources = sorted({s for s in mapping_sources if s})
        mapping_targets = sorted({t for t in mapping_targets if t})
        sources_by_target = self._map_mappings_by_target(mappings)

        ontology_payload = await self._fetch_class_schema(job)
        prop_map, rel_map = self._extract_ontology_fields(ontology_payload)
        if not prop_map:
            await _fail_job("validation_failed", report={"errors": [{"code": "ONTOLOGY_SCHEMA_MISSING"}]})

        unknown_targets = [t for t in mapping_targets if t not in prop_map and t not in rel_map]
        if unknown_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_TARGET_UNKNOWN",
                            "targets": unknown_targets,
                            "message": "Mapping targets missing from ontology schema",
                        }
                    ]
                },
            )
        relationship_targets = [t for t in mapping_targets if t in rel_map]
        relationship_meta_override = options.get("relationship_meta") if isinstance(options.get("relationship_meta"), dict) else {}
        if link_index_mode and relationship_meta_override:
            relationship_targets.extend([t for t in mapping_targets if t in relationship_meta_override])
        relationship_targets = sorted(set(relationship_targets))
        if not link_index_mode:
            unsupported_relationships: List[str] = []
            for target in relationship_targets:
                rel = rel_map.get(target) or {}
                cardinality = str(rel.get("cardinality") or "").strip().lower()
                if cardinality.endswith(":n") or cardinality.endswith(":m"):
                    unsupported_relationships.append(target)
            if unsupported_relationships:
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": [
                            {
                                "code": "MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED",
                                "targets": unsupported_relationships,
                                "message": "Relationship cardinality requires join-table mapping",
                            }
                        ]
                    },
                )
        elif not relationship_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_RELATIONSHIP_REQUIRED",
                            "message": "link_index requires relationship targets",
                        }
                    ]
                },
            )

        property_mappings = [m for m in mappings if m.target_field in prop_map]
        relationship_target_set = set(relationship_targets)
        relationship_mappings = [m for m in mappings if m.target_field in relationship_target_set]

        if link_index_mode:
            await self._run_link_index_job(
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
                stable_seed=stable_seed or job.job_id,
                row_batch_size=row_batch_size,
                max_rows=max_rows,
            )
            return

        value_type_refs = {
            str(meta.get("value_type_ref") or meta.get("valueTypeRef") or "").strip()
            for meta in prop_map.values()
            if isinstance(meta, dict)
        }
        value_type_refs = {ref for ref in value_type_refs if ref}
        value_type_defs, missing_value_types = await self._fetch_value_type_defs(job, value_type_refs)
        if missing_value_types:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "VALUE_TYPE_NOT_FOUND",
                            "value_type_refs": sorted(missing_value_types),
                            "message": "Referenced value types are missing",
                        }
                    ]
                },
            )

        resolved_field_types: Dict[str, str] = {}
        field_constraints: Dict[str, Any] = {}
        field_raw_types: Dict[str, Optional[Any]] = {}
        required_targets: set[str] = set()
        explicit_pk_targets: set[str] = set()
        unsupported_targets: List[str] = []
        for name, meta in prop_map.items():
            raw_type = meta.get("type") or meta.get("data_type") or meta.get("datatype")
            raw_type_norm = normalize_ontology_base_type(raw_type) or raw_type
            value_type_ref = str(meta.get("value_type_ref") or meta.get("valueTypeRef") or "").strip() or None
            value_type_spec = value_type_defs.get(value_type_ref) if value_type_ref else None
            value_type_base = None
            value_type_constraints = None
            if value_type_spec:
                value_type_base = value_type_spec.get("base_type") or value_type_spec.get("baseType")
                value_type_constraints = value_type_spec.get("constraints") or value_type_spec.get("constraint") or {}

            if value_type_base and normalize_ontology_base_type(raw_type_norm) in {None, "string"}:
                raw_type_norm = value_type_base

            field_raw_types[name] = raw_type_norm

            is_relationship = bool(
                raw_type_norm == "link"
                or meta.get("isRelationship")
                or meta.get("target")
                or meta.get("linkTarget")
            )
            items = meta.get("items") if isinstance(meta, dict) else None
            if isinstance(items, dict):
                item_type = items.get("type")
                if item_type == "link" and (items.get("target") or items.get("linkTarget")):
                    is_relationship = True

            if is_relationship:
                unsupported_targets.append(name)
                continue
            import_type = self._resolve_import_type(raw_type_norm)
            if not import_type:
                unsupported_targets.append(name)
                continue
            resolved_field_types[name] = import_type
            prop_constraints = self._normalize_constraints(meta.get("constraints"), raw_type=raw_type_norm)
            base_constraints: Dict[str, Any] = {}
            if normalize_ontology_base_type(raw_type_norm) == "array":
                base_constraints = {"noNullItems": True, "noNestedArrays": True}
            elif normalize_ontology_base_type(raw_type_norm) == "struct":
                base_constraints = {"noNestedStructs": True, "noArrayFields": True}

            constraint_sets: List[Dict[str, Any]] = []
            if base_constraints:
                constraint_sets.append(base_constraints)
            if value_type_constraints:
                constraint_sets.append(self._normalize_constraints(value_type_constraints, raw_type=value_type_base))
            if prop_constraints:
                constraint_sets.append(prop_constraints)
            field_constraints[name] = constraint_sets if constraint_sets else {}
            if bool(meta.get("required")):
                required_targets.add(name)
            if bool(meta.get("primary_key") or meta.get("primaryKey")):
                explicit_pk_targets.add(name)

        if not explicit_pk_targets:
            expected_pk = f"{job.target_class_id.lower()}_id"
            if expected_pk in prop_map:
                explicit_pk_targets.add(expected_pk)

        object_type_resource = await self._fetch_object_type_contract(job)
        object_type_data = object_type_resource.get("data") if isinstance(object_type_resource, dict) else None
        if not isinstance(object_type_data, dict):
            object_type_data = object_type_resource if isinstance(object_type_resource, dict) else {}
        object_type_spec = object_type_data.get("spec") if isinstance(object_type_data.get("spec"), dict) else {}
        if not object_type_spec:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "OBJECT_TYPE_CONTRACT_MISSING",
                            "message": "Object type contract is required for objectify",
                        }
                    ]
                },
            )
        status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
        if status_value != "ACTIVE":
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "OBJECT_TYPE_INACTIVE",
                            "status": status_value,
                            "message": "Object type contract is not active",
                        }
                    ]
                },
            )
        object_type_key_spec = normalize_key_spec(object_type_spec.get("pk_spec") or {}, columns=list(prop_map.keys()))
        object_type_pk_targets = [str(v).strip() for v in object_type_key_spec.get("primary_key") or [] if str(v).strip()]
        object_type_title_targets = [str(v).strip() for v in object_type_key_spec.get("title_key") or [] if str(v).strip()]
        object_type_unique_keys = [
            [str(v).strip() for v in key if str(v).strip()]
            for key in object_type_key_spec.get("unique_keys") or []
            if isinstance(key, list)
        ]
        object_type_required_fields = [str(v).strip() for v in object_type_key_spec.get("required_fields") or [] if str(v).strip()]
        object_type_nullable_fields = {str(v).strip() for v in object_type_key_spec.get("nullable_fields") or [] if str(v).strip()}

        if not object_type_pk_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "OBJECT_TYPE_PRIMARY_KEY_MISSING",
                            "message": "Object type pk_spec.primary_key is required",
                        }
                    ]
                },
            )
        if not object_type_title_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "OBJECT_TYPE_TITLE_KEY_MISSING",
                            "message": "Object type pk_spec.title_key is required",
                        }
                    ]
                },
            )

        missing_contract_fields = sorted(
            {
                *object_type_pk_targets,
                *object_type_title_targets,
                *object_type_required_fields,
            }
            - set(prop_map.keys())
        )
        if missing_contract_fields:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "OBJECT_TYPE_KEY_FIELDS_MISSING",
                            "fields": missing_contract_fields,
                            "message": "Object type key spec fields missing from ontology schema",
                        }
                    ]
                },
            )

        property_targets = [t for t in mapping_targets if t in prop_map]
        unsupported_mapped = [t for t in property_targets if t in unsupported_targets or t not in resolved_field_types]
        if unsupported_mapped:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_UNSUPPORTED_TYPE",
                            "targets": unsupported_mapped,
                            "message": "Mapping targets include unsupported property types",
                        }
                    ]
                },
            )

        warnings: List[Dict[str, Any]] = []

        def _extract_ontology_pk_targets(payload: Any) -> List[str]:
            """Extract ontology-declared primaryKey fields in order (best-effort)."""
            data = self._normalize_ontology_payload(payload)
            meta = data.get("metadata") if isinstance(data, dict) else None
            if isinstance(meta, dict):
                key_spec = meta.get("key_spec") or meta.get("keySpec") or meta.get("key_specification")
                if isinstance(key_spec, dict):
                    raw_pk = (
                        key_spec.get("primary_key")
                        or key_spec.get("primaryKey")
                        or key_spec.get("primaryKeys")
                        or []
                    )
                    if isinstance(raw_pk, list):
                        ordered = [str(v).strip() for v in raw_pk if str(v).strip()]
                        if ordered:
                            return ordered
            props = data.get("properties") if isinstance(data, dict) else None
            out: List[str] = []
            seen: set[str] = set()
            if isinstance(props, list):
                for prop in props:
                    if not isinstance(prop, dict):
                        continue
                    if not bool(prop.get("primary_key") or prop.get("primaryKey")):
                        continue
                    name = str(prop.get("name") or "").strip()
                    if name and name not in seen:
                        seen.add(name)
                        out.append(name)
            return out

        # P0 (enterprise-safe): object_type.pk_spec is the operational source of truth.
        # If ontology still carries primaryKey metadata (e.g. round-trip preserved), we validate it
        # against pk_spec as an additional contract check (warn/fail configurable).
        ontology_pk_targets = _extract_ontology_pk_targets(ontology_payload)
        if ontology_pk_targets and ontology_pk_targets != object_type_pk_targets:
            issue = {
                "code": "ONTOLOGY_OBJECT_TYPE_PRIMARY_KEY_MISMATCH",
                "expected": object_type_pk_targets,
                "observed": ontology_pk_targets,
                "message": "Ontology primaryKey does not match object_type pk_spec (fields+order)",
            }
            if self.ontology_pk_validation_mode == "fail":
                await _fail_job("validation_failed", report={"errors": [issue]})
            if self.ontology_pk_validation_mode == "warn":
                warnings.append(issue)

        pk_fields = self._normalize_pk_fields(
            options.get("primary_key_fields")
            or options.get("primary_keys")
            or options.get("row_pk_fields")
            or options.get("pk_fields")
            or options.get("primary_key_columns")
            or options.get("row_pk_columns")
        )
        requested_pk_targets = self._normalize_pk_fields(
            options.get("primary_key_targets") or options.get("target_primary_keys")
        )
        if requested_pk_targets and requested_pk_targets != object_type_pk_targets:
            if set(requested_pk_targets) == set(object_type_pk_targets):
                warnings.append(
                    {
                        "code": "OBJECTIFY_PRIMARY_KEY_ORDER_OVERRIDE_IGNORED",
                        "expected": object_type_pk_targets,
                        "observed": requested_pk_targets,
                        "message": "Ignoring primary key order override; using object_type pk_spec order",
                    }
                )
            else:
                warnings.append(
                    {
                        "code": "OBJECTIFY_PRIMARY_KEY_OVERRIDE_IGNORED",
                        "expected": object_type_pk_targets,
                        "observed": requested_pk_targets,
                        "message": "Ignoring primary key override; using object_type pk_spec",
                    }
                )
        pk_targets = object_type_pk_targets or sorted(explicit_pk_targets)

        key_spec = await self.dataset_registry.get_key_spec_for_dataset(
            dataset_id=job.dataset_id,
            dataset_version_id=job.dataset_version_id,
        )
        if key_spec:
            normalized_spec = normalize_key_spec(key_spec.spec, columns=mapping_sources)
            key_pk_sources = set(normalized_spec.get("primary_key") or [])
            mapping_source_set = {src for src in mapping_sources if src}
            if key_pk_sources:
                missing_pk_sources = sorted(key_pk_sources - mapping_source_set)
                if missing_pk_sources:
                    await _fail_job(
                        "validation_failed",
                        report={
                            "errors": [
                                {
                                    "code": "KEY_SPEC_PRIMARY_KEY_MISSING",
                                    "sources": missing_pk_sources,
                                    "message": "Key spec primary key columns are not mapped",
                                }
                            ]
                        },
                    )
                invalid_pk_targets = sorted(
                    {
                        m.target_field
                        for m in mappings
                        if m.source_field in key_pk_sources and m.target_field not in set(pk_targets or [])
                    }
                )
                if invalid_pk_targets:
                    await _fail_job(
                        "validation_failed",
                        report={
                            "errors": [
                                {
                                    "code": "KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH",
                                    "targets": invalid_pk_targets,
                                    "message": "Key spec primary key sources must map to ontology primary keys",
                                }
                            ]
                        },
                    )
            required_sources = set(normalized_spec.get("required_fields") or [])
            if required_sources:
                for mapping in mappings:
                    if mapping.source_field in required_sources:
                        required_targets.add(mapping.target_field)
            options.setdefault("key_spec_id", key_spec.key_spec_id)

        title_required_targets = {t for t in object_type_title_targets if t not in object_type_nullable_fields}
        required_targets.update(object_type_required_fields)
        required_targets.update(title_required_targets)

        missing_required = sorted(required_targets - set(mapping_targets))
        if missing_required:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_REQUIRED_MISSING",
                            "targets": missing_required,
                            "message": "Required ontology fields are not mapped",
                        }
                    ]
                },
            )
        missing_title_targets = sorted(title_required_targets - set(mapping_targets))
        if missing_title_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_TITLE_KEY_MISSING",
                            "targets": missing_title_targets,
                            "message": "Title key targets are not mapped",
                        }
                    ]
                },
            )
        unique_key_fields = sorted({field for keys in object_type_unique_keys for field in keys if field})
        missing_unique_targets = sorted(set(unique_key_fields) - set(mapping_targets))
        if missing_unique_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_UNIQUE_KEY_MISSING",
                            "targets": missing_unique_targets,
                            "message": "Unique key targets are not mapped",
                        }
                    ]
                },
            )
        if not pk_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_PRIMARY_KEY_MISSING",
                            "message": "primary_key_targets is required when no primary key is defined on target class",
                        }
                    ]
                },
            )
        missing_pk_targets = sorted(set(pk_targets) - set(mapping_targets))
        if missing_pk_targets:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": "MAPPING_SPEC_PRIMARY_KEY_MISSING",
                            "targets": missing_pk_targets,
                            "message": "Primary key targets are not mapped",
                        }
                    ]
                },
            )

        target_field_types = {target: resolved_field_types[target] for target in property_targets}
        existing_field_types = mapping_spec.target_field_types or {}
        if existing_field_types:
            mismatches: List[Dict[str, Any]] = []
            for target in property_targets:
                expected = resolved_field_types.get(target)
                provided = existing_field_types.get(target)
                if not provided:
                    mismatches.append(
                        {"target_field": target, "reason": "missing", "expected": expected}
                    )
                else:
                    normalized = normalize_import_target_type(provided)
                    if expected and normalized != expected:
                        mismatches.append(
                            {
                                "target_field": target,
                                "reason": "mismatch",
                                "expected": expected,
                                "provided": provided,
                            }
                        )
            if mismatches:
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": [
                            {
                                "code": "MAPPING_SPEC_TARGET_TYPE_MISMATCH",
                                "mismatches": mismatches,
                                "message": "Mapping spec target types do not match ontology",
                            }
                        ]
                    },
                )

        ontology_version = await self._fetch_ontology_version(job)
        job_node_id = await self._record_lineage_header(
            job=job,
            mapping_spec=mapping_spec,
            ontology_version=ontology_version,
            input_type=input_type,
            artifact_output_name=resolved_output_name,
        )

        (
            key_scan_total_rows,
            key_scan_errors,
            key_scan_error_rows,
            key_scan_stats,
        ) = await self._scan_key_constraints(
            job=job,
            options=options,
            mappings=property_mappings,
            relationship_meta=rel_map,
            target_field_types=target_field_types,
            sources_by_target=sources_by_target,
            required_targets=required_targets,
            pk_targets=pk_targets,
            pk_fields=pk_fields,
            unique_keys=object_type_unique_keys,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        )
        if key_scan_total_rows == 0:
            await _fail_job("no_rows_loaded")
        if key_scan_errors:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": key_scan_errors[:200],
                    "stats": key_scan_stats,
                    "error_row_indices": key_scan_error_rows[:200],
                },
            )

        validation_errors: List[Dict[str, Any]] = []
        validation_error_rows: List[int] = []
        validation_stats: Dict[str, Any] = {}
        validated_total_rows = 0

        if not allow_partial:
            (
                validated_total_rows,
                validation_errors,
                validation_error_rows,
                validation_stats,
            ) = await self._validate_batches(
                job=job,
                options=options,
                mappings=property_mappings,
                relationship_mappings=relationship_mappings,
                relationship_meta=rel_map,
                target_field_types=target_field_types,
                mapping_sources=mapping_sources,
                sources_by_target=sources_by_target,
                required_targets=required_targets,
                pk_targets=pk_targets,
                pk_fields=pk_fields,
                field_constraints=field_constraints,
                field_raw_types=field_raw_types,
                row_batch_size=row_batch_size,
                max_rows=max_rows,
            )
            if validated_total_rows == 0:
                await _fail_job("no_rows_loaded")
            if validation_errors:
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": validation_errors[:200],
                        "stats": validation_stats,
                        "error_row_indices": validation_error_rows[:200],
                    },
                )

        command_ids: List[str] = []
        indexed_instance_ids: set[str] = set()
        prepared_instances = 0
        instance_ids_sample: List[str] = []
        error_rows: List[int] = []
        errors: List[Dict[str, Any]] = []
        total_rows_seen = 0
        lineage_remaining = self.lineage_max_links
        seen_row_keys: set[str] = set()

        # Incremental processing setup
        execution_mode = job.execution_mode or options.get("execution_mode", "full")
        watermark_column = job.watermark_column or options.get("watermark_column")
        previous_watermark = job.previous_watermark
        latest_watermark: Optional[str] = None
        delta_computer: Optional[ObjectifyDeltaComputer] = None
        lakefs_delta_result: Optional[DeltaResult] = None

        # Auto-detect watermark column when incremental mode is set but no column specified
        if execution_mode in ("incremental",) and not watermark_column:
            watermark_column = _auto_detect_watermark_column(columns=None, options=options)
            if not watermark_column:
                logger.info(
                    "Incremental mode requested but no watermark column detected; falling back to full mode"
                )
                execution_mode = "full"

        # LakeFS delta mode: compute diff between commits
        if execution_mode == "delta" and job.base_commit_id:
            delta_computer = create_delta_computer_for_mapping_spec(
                vars(mapping_spec) if hasattr(mapping_spec, "__dict__") else dict(mapping_spec or {})
            )
            lakefs_delta_result = await _compute_lakefs_delta(
                job=job,
                delta_computer=delta_computer,
                storage=self.storage,
            )
            if lakefs_delta_result and lakefs_delta_result.has_changes:
                logger.info(
                    "LakeFS delta computed: added=%d, modified=%d, deleted=%d",
                    lakefs_delta_result.stats.get("added_count", 0),
                    lakefs_delta_result.stats.get("modified_count", 0),
                    lakefs_delta_result.stats.get("deleted_count", 0),
                )
            elif lakefs_delta_result and not lakefs_delta_result.has_changes:
                logger.info("LakeFS delta: no changes detected between commits")
            else:
                logger.warning("LakeFS delta computation failed; falling back to full mode")
                execution_mode = "full"
                lakefs_delta_result = None
        elif execution_mode in ("incremental", "delta") and watermark_column:
            delta_computer = create_delta_computer_for_mapping_spec(
                vars(mapping_spec) if hasattr(mapping_spec, "__dict__") else dict(mapping_spec or {})
            )
            logger.info(
                "Incremental objectify mode enabled: execution_mode=%s, watermark_column=%s, previous=%s",
                execution_mode, watermark_column, previous_watermark,
            )

        # --- LakeFS delta mode: process delta rows directly, skip dataset iteration ---
        if lakefs_delta_result and lakefs_delta_result.has_changes:
            delta_rows_all = lakefs_delta_result.added_rows + lakefs_delta_result.modified_rows
            if delta_rows_all:
                # Convert list-of-dicts to (columns, rows_as_lists) format
                _delta_cols = list(delta_rows_all[0].keys()) if delta_rows_all else []
                _delta_rows_as_lists = [[row.get(c) for c in _delta_cols] for row in delta_rows_all]
                total_rows_seen = len(_delta_rows_as_lists)

                batch = self._build_instances_with_validation(
                    columns=_delta_cols,
                    rows=_delta_rows_as_lists,
                    row_offset=0,
                    mappings=property_mappings,
                    relationship_mappings=relationship_mappings,
                    relationship_meta=rel_map,
                    target_field_types=target_field_types,
                    mapping_sources=mapping_sources,
                    sources_by_target=sources_by_target,
                    pk_targets=pk_targets,
                    pk_target_map=pk_target_map,
                    instance_id_field=instance_id_field,
                    job=job,
                    options=options,
                    allow_partial=allow_partial,
                    seen_row_keys=seen_row_keys,
                )
                instances = batch.get("instances") or []
                instance_ids = batch.get("instance_ids") or []

                if instances:
                    for idx in range(0, len(instances), batch_size):
                        instance_batch = instances[idx : idx + batch_size]
                        batch_rels: Dict[str, Dict[str, Any]] = {}
                        if rel_map:
                            for inst in instance_batch:
                                if not isinstance(inst, dict):
                                    continue
                                iid = str(inst.get("instance_id") or "").strip()
                                if iid:
                                    batch_rels[iid] = _extract_instance_relationships(inst, rel_map=rel_map)
                        write_result = await self.instance_write_path.write_instances(
                            job=job,
                            instances=instance_batch,
                            ontology_version=ontology_version,
                            objectify_pk_fields=pk_targets,
                            objectify_instance_id_field=instance_id_field,
                            instance_relationships=batch_rels if batch_rels else None,
                            target_field_types=target_field_types,
                        )
                        if write_result.command_ids:
                            command_ids.extend([str(v) for v in write_result.command_ids if str(v).strip()])
                        if write_result.indexed_instance_ids:
                            indexed_instance_ids.update(str(v).strip() for v in write_result.indexed_instance_ids if str(v).strip())

                    prepared_instances += len(instances)
                    if len(instance_ids_sample) < 10:
                        instance_ids_sample.extend(instance_ids[: max(0, 10 - len(instance_ids_sample))])

            # Handle deletions from delta
            if lakefs_delta_result.deleted_keys:
                branch = job.ontology_branch or job.dataset_branch or "main"
                index_name = get_instances_index_name(job.db_name, branch=branch)
                for deleted_key in lakefs_delta_result.deleted_keys:
                    try:
                        await self.instance_write_path._es.delete_document(
                            index=index_name, doc_id=deleted_key, refresh=False,
                        )
                    except Exception:
                        logger.debug("Delete failed for stale delta key: %s", deleted_key)
                logger.info("Deleted %d stale instances from LakeFS delta", len(lakefs_delta_result.deleted_keys))

        else:
            # --- Standard iteration (full or watermark-incremental mode) ---
            pass

        async for columns, rows, row_offset in self._iter_dataset_batches(
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows if not delta_computer else None,  # Filter manually in incremental mode
        ):
            # Skip dataset iteration when LakeFS delta was already processed
            if lakefs_delta_result and lakefs_delta_result.has_changes:
                break

            if not rows:
                continue

            # Incremental filtering: skip rows that haven't changed since previous watermark
            if delta_computer and watermark_column:
                col_map = {col: idx for idx, col in enumerate(columns)}
                wm_col_idx = col_map.get(watermark_column)

                if wm_col_idx is not None:
                    filtered_rows: List[List[Any]] = []
                    for row in rows:
                        row_wm = row[wm_col_idx] if wm_col_idx < len(row) else None
                        if row_wm is None:
                            continue  # Skip rows with null watermark
                        if previous_watermark is not None:
                            cmp = delta_computer._compare_watermarks(row_wm, previous_watermark)
                            if cmp <= 0:
                                continue  # Skip rows not newer than previous watermark
                        filtered_rows.append(row)
                        # Track max watermark
                        if latest_watermark is None:
                            latest_watermark = str(row_wm)
                        elif delta_computer._compare_watermarks(row_wm, latest_watermark) > 0:
                            latest_watermark = str(row_wm)

                    rows = filtered_rows
                    if not rows:
                        continue

                    # Apply max_rows limit in incremental mode
                    if max_rows and total_rows_seen + len(rows) > max_rows:
                        rows = rows[:max(0, max_rows - total_rows_seen)]
                        if not rows:
                            break

            total_rows_seen += len(rows)

            # Check max_rows limit
            if max_rows and total_rows_seen > max_rows:
                break

            batch = self._build_instances_with_validation(
                columns=columns,
                rows=rows,
                row_offset=row_offset,
                mappings=property_mappings,
                relationship_mappings=relationship_mappings,
                relationship_meta=rel_map,
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
            if batch_errors:
                if self._has_p0_errors(batch_errors):
                    await _fail_job("validation_failed", report={"errors": batch_errors[:200]})
                if allow_partial:
                    remaining = max(0, 200 - len(errors))
                    if remaining:
                        errors.extend(batch_errors[:remaining])
                else:
                    await _fail_job("validation_failed", report={"errors": batch_errors[:200]})

            instances = batch.get("instances") or []
            instance_row_indices = batch.get("instance_row_indices") or []
            row_keys = batch.get("row_keys") or []
            error_row_indices = set(batch.get("error_row_indices") or [])

            if error_row_indices:
                for idx in sorted(error_row_indices):
                    error_rows.append(int(idx))
                if allow_partial:
                    filtered_instances = []
                    filtered_indices = []
                    filtered_row_keys = []
                    for inst, row_idx, row_key in zip(instances, instance_row_indices, row_keys):
                        absolute_idx = int(row_offset) + int(row_idx)
                        if absolute_idx in error_row_indices:
                            continue
                        filtered_instances.append(inst)
                        filtered_indices.append(row_idx)
                        filtered_row_keys.append(row_key)
                    instances = filtered_instances
                    instance_row_indices = filtered_indices
                    row_keys = filtered_row_keys
                else:
                    await _fail_job("validation_failed", report={"errors": batch_errors[:200]})

            if not instances:
                continue

            if any(not key for key in row_keys):
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": [
                            {
                                "code": "PRIMARY_KEY_MISSING",
                                "message": "Row key cannot be derived from primary key values",
                            }
                        ]
                    },
                )

            instance_id_field = None
            if pk_targets:
                for target in pk_targets:
                    if target in target_field_types:
                        instance_id_field = target
                        break
            if not instance_id_field:
                for candidate in (f"{job.target_class_id.lower()}_id", "id"):
                    if candidate in target_field_types:
                        instance_id_field = candidate
                        break

            instances, instance_ids = self._ensure_instance_ids(
                instances,
                class_id=job.target_class_id,
                stable_seed=stable_seed or job.job_id,
                mapping_spec_version=job.mapping_spec_version,
                row_keys=row_keys,
                instance_id_field=instance_id_field,
            )

            for idx in range(0, len(instances), batch_size):
                instance_batch = instances[idx : idx + batch_size]
                batch_rels: Dict[str, Dict[str, Any]] = {}
                if rel_map:
                    for inst in instance_batch:
                        if not isinstance(inst, dict):
                            continue
                        iid = str(inst.get("instance_id") or "").strip()
                        if iid:
                            batch_rels[iid] = _extract_instance_relationships(
                                inst, rel_map=rel_map,
                            )
                write_result = await self.instance_write_path.write_instances(
                    job=job,
                    instances=instance_batch,
                    ontology_version=ontology_version,
                    objectify_pk_fields=pk_targets,
                    objectify_instance_id_field=instance_id_field,
                    instance_relationships=batch_rels if batch_rels else None,
                    target_field_types=target_field_types,
                )
                if write_result.command_ids:
                    command_ids.extend([str(v) for v in write_result.command_ids if str(v).strip()])
                if write_result.indexed_instance_ids:
                    indexed_instance_ids.update(str(v).strip() for v in write_result.indexed_instance_ids if str(v).strip())

            prepared_instances += len(instances)
            if len(instance_ids_sample) < 10:
                instance_ids_sample.extend(instance_ids[: max(0, 10 - len(instance_ids_sample))])

            lineage_remaining = await self._record_instance_lineage(
                job=job,
                job_node_id=job_node_id,
                instance_ids=instance_ids,
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=mapping_spec.version,
                ontology_version=ontology_version,
                limit_remaining=lineage_remaining,
                input_type=input_type,
                artifact_output_name=resolved_output_name,
            )

        total_rows = validated_total_rows if not allow_partial else total_rows_seen
        if total_rows == 0:
            await _fail_job("no_rows_loaded")
        if prepared_instances == 0:
            await _fail_job(
                "no_valid_instances",
                report={
                    "errors": (errors or validation_errors)[:200],
                    "error_row_indices": error_rows[:200],
                },
            )

        write_path_report = await self.instance_write_path.finalize_job(
            job=job,
            execution_mode=execution_mode,
            indexed_instance_ids=indexed_instance_ids,
        )
        terminal_status = "COMPLETED"
        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status=terminal_status,
            command_id=command_ids[0] if command_ids else None,
            report={
                "total_rows": total_rows,
                "prepared_instances": prepared_instances,
                "warnings": warnings[:200],
                "validation": {"warnings": warnings[:200]},
                "errors": (errors or validation_errors)[:200],
                "error_row_indices": (error_rows or validation_error_rows)[:200],
                "command_ids": command_ids,
                "instance_ids_sample": instance_ids_sample[:10],
                "indexed_instances": len(indexed_instance_ids),
                "write_path_mode": WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
                "write_path": write_path_report,
                "ontology_version": ontology_version or {},
            },
            completed_at=datetime.now(timezone.utc),
        )
        await self._record_gate_result(
            job=job,
            status="PASS",
            details={
                "total_rows": total_rows,
                "prepared_instances": prepared_instances,
                "command_ids": command_ids,
                "indexed_instances": len(indexed_instance_ids),
                "write_path_mode": WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
                "warning_count": len(warnings),
                "error_count": len(errors or validation_errors or []),
            },
        )
        await self._update_object_type_active_version(job=job, mapping_spec=mapping_spec)

        # Update watermark after successful incremental job
        if execution_mode in ("incremental", "delta") and latest_watermark:
            await self._update_watermark_after_job(
                job=job,
                new_watermark=latest_watermark,
            )

        # Emit OBJECTIFY_COMPLETED control-plane event for downstream consumers
        await self._emit_objectify_completed_event(
            job=job,
            total_rows=total_rows,
            prepared_instances=prepared_instances,
            indexed_instances=len(indexed_instance_ids),
            execution_mode=execution_mode,
            ontology_version=ontology_version,
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
        if not self.pipeline_registry:
            raise RuntimeError("PipelineRegistry not initialized")
        if not job.artifact_id:
            raise ValueError("artifact_id is required")
        artifact = await self.pipeline_registry.get_artifact(artifact_id=job.artifact_id)
        if not artifact:
            raise ValueError(f"artifact_not_found:{job.artifact_id}")
        if str(artifact.status or "").upper() != "SUCCESS":
            raise ValueError("artifact_not_success")
        if str(artifact.mode or "").lower() != "build":
            raise ValueError("artifact_not_build")
        outputs = artifact.outputs or []
        if not outputs:
            raise ValueError("artifact_outputs_missing")
        output_name = (job.artifact_output_name or "").strip()
        if not output_name:
            if len(outputs) == 1:
                output = outputs[0]
                output_name = (
                    str(output.get("output_name") or output.get("dataset_name") or output.get("node_id") or "").strip()
                )
            if not output_name:
                raise ValueError("artifact_output_name_required")

        matches = []
        for output in outputs:
            for key in ("output_name", "dataset_name", "node_id"):
                candidate = str(output.get(key) or "").strip()
                if candidate and candidate == output_name:
                    matches.append(output)
                    break
        if not matches:
            raise ValueError("artifact_output_not_found")
        if len(matches) > 1:
            raise ValueError("artifact_output_ambiguous")

        selected = matches[0]
        artifact_key = str(selected.get("artifact_commit_key") or selected.get("artifact_key") or "").strip() or None
        if not artifact_key:
            raise ValueError("artifact_key_missing")
        if not parse_s3_uri(artifact_key):
            raise ValueError("invalid_artifact_key")
        return artifact_key, output_name

    async def _fetch_target_field_types(self, job: ObjectifyJob) -> Dict[str, str]:
        payload = await self._fetch_class_schema(job)
        prop_map, _ = self._extract_ontology_fields(payload)
        field_types: Dict[str, str] = {}
        for name, meta in prop_map.items():
            raw_type = meta.get("type") or meta.get("data_type") or meta.get("datatype")
            is_relationship = bool(
                raw_type == "link"
                or meta.get("isRelationship")
                or meta.get("target")
                or meta.get("linkTarget")
            )
            items = meta.get("items") if isinstance(meta, dict) else None
            if isinstance(items, dict):
                item_type = items.get("type")
                if item_type == "link" and (items.get("target") or items.get("linkTarget")):
                    is_relationship = True
            if is_relationship:
                continue
            import_type = self._resolve_import_type(raw_type)
            if import_type:
                field_types[name] = import_type
        return field_types

    async def _fetch_class_schema(self, job: ObjectifyJob) -> Dict[str, Any]:
        if not self.http:
            return {}
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.get(
            f"/api/v1/database/{job.db_name}/ontology/{job.target_class_id}",
            params={"branch": branch},
        )
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    async def _fetch_object_type_contract(self, job: ObjectifyJob) -> Dict[str, Any]:
        if not self.http:
            return {}
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.get(
            f"/api/v1/database/{job.db_name}/ontology/resources/object_type/{job.target_class_id}",
            params={"branch": branch},
        )
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    async def _fetch_value_type_defs(
        self,
        job: ObjectifyJob,
        value_type_refs: set[str],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        if not self.http or not value_type_refs:
            return {}, []

        branch = job.ontology_branch or job.dataset_branch or "main"
        defs: Dict[str, Dict[str, Any]] = {}
        missing: List[str] = []

        for ref in sorted(value_type_refs):
            resp = await self.http.get(
                f"/api/v1/database/{job.db_name}/ontology/resources/value_type/{ref}",
                params={"branch": branch},
            )
            if resp.status_code == 404:
                missing.append(ref)
                continue
            resp.raise_for_status()
            payload = resp.json() if resp.text else {}
            resource = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(resource, dict):
                resource = payload if isinstance(payload, dict) else {}
            spec = resource.get("spec") if isinstance(resource, dict) else {}
            if not isinstance(spec, dict):
                spec = {}
            defs[ref] = {
                "base_type": spec.get("base_type") or spec.get("baseType"),
                "constraints": spec.get("constraints") or {},
            }

        return defs, missing

    async def _fetch_ontology_version(self, job: ObjectifyJob) -> Dict[str, str]:
        if not self.http:
            return {}
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.get(
            f"/api/v1/version/{job.db_name}/head",
            params={"branch": branch},
        )
        if resp.status_code == 404:
            return {"ref": f"branch:{branch}"}
        resp.raise_for_status()
        payload = resp.json() if resp.text else {}
        data = payload.get("data") if isinstance(payload, dict) else {}
        head_commit = None
        if isinstance(data, dict):
            head_commit = data.get("head_commit_id") or data.get("commit")
        if head_commit:
            return {"ref": f"branch:{branch}", "commit": str(head_commit)}
        return {"ref": f"branch:{branch}"}

    async def _fetch_ontology_head_commit(self, job: ObjectifyJob) -> Optional[str]:
        if not self.http:
            return None
        branch = job.ontology_branch or job.dataset_branch or "main"
        resp = await self.http.get(
            f"/api/v1/version/{job.db_name}/head",
            params={"branch": branch},
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        payload = resp.json() if resp.text else {}
        data = payload.get("data") if isinstance(payload, dict) else {}
        if isinstance(data, dict):
            return data.get("head_commit_id") or data.get("commit") or data.get("head_commit")
        return None

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
        if not self.storage:
            raise RuntimeError("Storage service not initialized")
        parsed = parse_s3_uri(job.artifact_key)
        if not parsed:
            raise ValueError(f"Invalid artifact_key: {job.artifact_key}")
        bucket, key = parsed
        has_header = options.get("source_has_header", True)
        delimiter = options.get("delimiter") or options.get("csv_delimiter") or ","

        if key.endswith(".csv") or key.endswith(".tsv") or key.endswith(".txt"):
            async for columns, rows, row_offset in self._iter_csv_batches(
                bucket=bucket,
                key=key,
                delimiter=delimiter,
                has_header=bool(has_header),
                row_batch_size=row_batch_size,
                max_rows=max_rows,
            ):
                yield columns, rows, row_offset
            return

        if key.endswith(".xlsx") or key.endswith(".xlsm") or key.endswith(".xls"):
            raise RuntimeError("Excel artifacts are not supported for objectify; convert via pipeline first.")

        async for columns, rows, row_offset in self._iter_json_part_batches(
            bucket=bucket,
            prefix=key,
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
        if not self.storage:
            raise RuntimeError("Storage service not initialized")

        q: queue_module.Queue = queue_module.Queue(maxsize=2)
        stop_flag = threading.Event()

        def _safe_put(item: Tuple[str, Any, Any, Any]) -> None:
            while not stop_flag.is_set():
                try:
                    q.put(item, timeout=0.5)
                    return
                except queue_module.Full:
                    continue

        def _reader() -> None:
            body = None
            try:
                resp = self.storage.client.get_object(Bucket=bucket, Key=key)
                body = resp.get("Body")
                text_stream = io.TextIOWrapper(body, encoding="utf-8", errors="replace")
                reader = csv.reader(text_stream, delimiter=delimiter)

                columns: List[str] = []
                rows: List[List[Any]] = []
                row_offset = 0
                seen_rows = 0

                if has_header:
                    header = next(reader, None)
                    if header:
                        columns = [str(c).strip() for c in header]
                else:
                    first = next(reader, None)
                    if first is None:
                        _safe_put(("done", columns, [], row_offset))
                        return
                    columns = [f"col_{i}" for i in range(len(first))]
                    rows.append(first)
                    seen_rows += 1

                for row in reader:
                    if stop_flag.is_set():
                        break
                    if max_rows is not None and seen_rows >= max_rows:
                        break
                    if not columns:
                        columns = [f"col_{i}" for i in range(len(row))]
                    rows.append(row)
                    seen_rows += 1
                    if len(rows) >= row_batch_size:
                        _safe_put(("batch", columns, rows, row_offset))
                        row_offset += len(rows)
                        rows = []

                if rows and not stop_flag.is_set():
                    _safe_put(("batch", columns, rows, row_offset))
                    row_offset += len(rows)
                _safe_put(("done", columns, [], row_offset))
            except Exception as exc:
                _safe_put(("error", exc, None, None))
            finally:
                try:
                    if body:
                        body.close()
                except Exception:
                    pass

        thread = threading.Thread(target=_reader, daemon=True)
        thread.start()

        try:
            while True:
                kind, columns, rows, row_offset = await asyncio.to_thread(q.get)
                if kind == "error":
                    raise columns
                if kind == "done":
                    break
                if kind == "batch":
                    yield columns, rows, row_offset
        finally:
            stop_flag.set()

    async def _iter_json_part_batches(
        self,
        *,
        bucket: str,
        prefix: str,
        row_batch_size: int,
        max_rows: Optional[int],
    ):
        if not self.storage:
            raise RuntimeError("Storage service not initialized")
        rows: List[List[Any]] = []
        columns: List[str] = []
        row_offset = 0
        total_rows = 0

        async for obj in self.storage.iter_objects(bucket=bucket, prefix=prefix, max_keys=self.list_page_size):
            obj_key = obj.get("Key")
            if not obj_key or obj_key.endswith("/"):
                continue
            if not obj_key.startswith(prefix):
                continue
            raw = await self.storage.load_bytes(bucket, obj_key)
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    if not columns:
                        columns = list(payload.keys())
                    row = [payload.get(col) for col in columns]
                elif isinstance(payload, list):
                    if not columns:
                        columns = [f"col_{i}" for i in range(len(payload))]
                    row = payload
                else:
                    continue

                rows.append(row)
                total_rows += 1
                if max_rows is not None and total_rows >= max_rows:
                    break
                if len(rows) >= row_batch_size:
                    yield columns, rows, row_offset
                    row_offset += len(rows)
                    rows = []

            if max_rows is not None and total_rows >= max_rows:
                break

        if rows:
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
        """
        Iterate dataset batches with incremental filtering.

        Yields: (columns, rows, row_offset, new_watermark)
        """
        execution_mode = job.execution_mode or "full"
        watermark_column = job.watermark_column or options.get("watermark_column")
        previous_watermark = job.previous_watermark

        # For full mode, delegate to standard iterator
        if execution_mode == "full" or not watermark_column:
            async for columns, rows, row_offset in self._iter_dataset_batches(
                job=job,
                options=options,
                row_batch_size=row_batch_size,
                max_rows=max_rows,
            ):
                yield columns, rows, row_offset, None
            return

        # Create delta computer for incremental processing
        delta_computer = create_delta_computer_for_mapping_spec(
            vars(mapping_spec) if hasattr(mapping_spec, "__dict__") else dict(mapping_spec or {})
        )

        max_watermark: Optional[Any] = previous_watermark
        total_yielded = 0

        async for columns, rows, row_offset in self._iter_dataset_batches(
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=None,  # We filter rows ourselves
        ):
            if not rows:
                continue

            # Convert rows to dicts for watermark filtering
            col_map = {col: idx for idx, col in enumerate(columns)}
            wm_col_idx = col_map.get(watermark_column)

            if wm_col_idx is None:
                # Watermark column not found, yield all rows
                logger.warning(
                    "Watermark column %s not found in dataset columns; falling back to full mode",
                    watermark_column,
                )
                yield columns, rows, row_offset, None
                total_yielded += len(rows)
                if max_rows and total_yielded >= max_rows:
                    break
                continue

            # Filter rows by watermark
            filtered_rows: List[List[Any]] = []
            for row in rows:
                row_wm = row[wm_col_idx] if wm_col_idx < len(row) else None

                # Skip rows with null watermark
                if row_wm is None:
                    continue

                # Compare with previous watermark
                if previous_watermark is not None:
                    cmp = delta_computer._compare_watermarks(row_wm, previous_watermark)
                    if cmp <= 0:
                        # Row is not newer than previous watermark, skip
                        continue

                filtered_rows.append(row)

                # Track max watermark
                if max_watermark is None:
                    max_watermark = row_wm
                elif delta_computer._compare_watermarks(row_wm, max_watermark) > 0:
                    max_watermark = row_wm

            if filtered_rows:
                # Apply max_rows limit if specified
                if max_rows and total_yielded + len(filtered_rows) > max_rows:
                    remaining = max_rows - total_yielded
                    filtered_rows = filtered_rows[:remaining]

                yield columns, filtered_rows, row_offset, str(max_watermark) if max_watermark else None
                total_yielded += len(filtered_rows)

                if max_rows and total_yielded >= max_rows:
                    break

        # Final yield with max watermark if no rows were yielded
        if total_yielded == 0 and max_watermark and max_watermark != previous_watermark:
            logger.info(
                "Incremental objectify: no new rows found (max_watermark=%s, previous=%s)",
                max_watermark, previous_watermark,
            )

    async def _update_watermark_after_job(
        self,
        *,
        job: ObjectifyJob,
        new_watermark: Optional[str],
    ) -> None:
        """Update watermark in registry after successful incremental job."""
        if not self.objectify_registry or not new_watermark:
            return

        watermark_column = job.watermark_column or job.options.get("watermark_column")
        if not watermark_column:
            return

        try:
            await self.objectify_registry.update_watermark(
                mapping_spec_id=job.mapping_spec_id,
                dataset_branch=job.dataset_branch,
                watermark_column=watermark_column,
                watermark_value=new_watermark,
                lakefs_commit_id=job.dataset_version_id,
            )
            logger.info(
                "Updated watermark for mapping_spec %s: %s=%s",
                job.mapping_spec_id, watermark_column, new_watermark,
            )
        except Exception as e:
            logger.error("Failed to update watermark: %s", e)

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
        col_index = SheetImportService.build_column_index(columns)
        missing_sources = sorted({s for s in mapping_sources if s and s not in col_index})
        if missing_sources:
            return {
                "instances": [],
                "instance_row_indices": [],
                "errors": [
                    {
                        "code": "SOURCE_FIELD_MISSING",
                        "missing_sources": missing_sources,
                        "message": "Mapping source fields missing from dataset schema",
                    }
                ],
                "error_row_indices": [],
                "row_keys": [],
                "fatal": True,
            }

        build = SheetImportService.build_instances(
            columns=columns,
            rows=rows,
            mappings=mappings,
            target_field_types=target_field_types,
        )
        raw_errors = build.get("errors") or []
        errors: List[Dict[str, Any]] = []
        error_row_indices: set[int] = set()
        for err in raw_errors:
            if isinstance(err, dict):
                adjusted = dict(err)
            else:
                adjusted = {"message": str(err)}
            row_index = adjusted.get("row_index")
            if row_index is not None:
                try:
                    row_index = int(row_index) + int(row_offset)
                except Exception:
                    row_index = None
                adjusted["row_index"] = row_index
            errors.append(adjusted)
            if row_index is not None:
                error_row_indices.add(row_index)

        for idx in build.get("error_row_indices") or []:
            try:
                error_row_indices.add(int(row_offset) + int(idx))
            except Exception:
                continue

        instances = build.get("instances") or []
        instance_row_indices = build.get("instance_row_indices") or []

        if relationship_mappings:
            for inst, row_idx in zip(instances, instance_row_indices):
                if not isinstance(inst, dict):
                    continue
                if row_idx >= len(rows):
                    continue
                row = rows[row_idx]
                for mapping in relationship_mappings:
                    source_name = mapping.source_field
                    target_name = mapping.target_field
                    idx = col_index.get(source_name)
                    if idx is None or idx >= len(row):
                        continue
                    raw = row[idx]
                    if self._is_blank(raw):
                        continue
                    rel_meta = relationship_meta.get(target_name) or {}
                    target_class = str(rel_meta.get("target") or rel_meta.get("linkTarget") or "").strip()
                    if not target_class:
                        errors.append(
                            {
                                "row_index": int(row_offset) + int(row_idx),
                                "source_field": source_name,
                                "target_field": target_name,
                                "code": "RELATIONSHIP_TARGET_MISSING",
                                "message": "Relationship target class missing from ontology",
                            }
                        )
                        error_row_indices.add(int(row_offset) + int(row_idx))
                        continue
                    try:
                        ref = self._normalize_relationship_ref(raw, target_class=target_class)
                    except ValueError as exc:
                        errors.append(
                            {
                                "row_index": int(row_offset) + int(row_idx),
                                "source_field": source_name,
                                "target_field": target_name,
                                "raw_value": raw,
                                "code": "RELATIONSHIP_REF_INVALID",
                                "message": str(exc),
                            }
                        )
                        error_row_indices.add(int(row_offset) + int(row_idx))
                        continue
                    inst[target_name] = ref

        mapping_sources_set = {s for s in mapping_sources if s}
        required_union = set(required_targets) | set(pk_targets)
        for row_idx, row in enumerate(rows):
            if not mapping_sources_set:
                break
            row_has_value = False
            for source in mapping_sources_set:
                idx = col_index.get(source)
                if idx is None or idx >= len(row):
                    continue
                if not self._is_blank(row[idx]):
                    row_has_value = True
                    break
            if not row_has_value:
                continue
            absolute_idx = int(row_offset) + int(row_idx)
            for target in required_union:
                sources = sources_by_target.get(target) or []
                if not sources:
                    continue
                present = False
                for source in sources:
                    idx = col_index.get(source)
                    if idx is None or idx >= len(row):
                        continue
                    if not self._is_blank(row[idx]):
                        present = True
                        break
                if not present:
                    code = "PRIMARY_KEY_MISSING" if target in pk_targets else "REQUIRED_FIELD_MISSING"
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "target_field": target,
                            "code": code,
                            "message": f"Missing required field '{target}'",
                        }
                    )
                    error_row_indices.add(absolute_idx)

        for inst, row_idx in zip(instances, instance_row_indices):
            absolute_idx = int(row_offset) + int(row_idx)
            if not isinstance(inst, dict):
                continue
            for field, value in inst.items():
                constraints = field_constraints.get(field) or {}
                if not constraints:
                    continue
                raw_type = field_raw_types.get(field)
                message = self._validate_value_constraints(value, constraints=constraints, raw_type=raw_type)
                if message:
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "target_field": field,
                            "code": "VALUE_CONSTRAINT_FAILED",
                            "message": message,
                        }
                    )
                    error_row_indices.add(absolute_idx)

        row_keys: List[Optional[str]] = []
        for inst, row_idx in zip(instances, instance_row_indices):
            row = rows[row_idx] if row_idx < len(rows) else None
            row_key = self._derive_row_key(
                columns=columns,
                col_index=col_index,
                row=row,
                instance=inst,
                pk_fields=pk_fields,
                pk_targets=pk_targets,
            )
            row_keys.append(row_key)

        if seen_row_keys is not None:
            for row_key, row_idx in zip(row_keys, instance_row_indices):
                absolute_idx = int(row_offset) + int(row_idx)
                if not row_key:
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": "PRIMARY_KEY_MISSING",
                            "message": "Row key cannot be derived from primary key values",
                        }
                    )
                    error_row_indices.add(absolute_idx)
                    continue
                if row_key in seen_row_keys:
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": "PRIMARY_KEY_DUPLICATE",
                            "row_key": row_key,
                            "message": "Duplicate primary key detected in job batch",
                        }
                    )
                    error_row_indices.add(absolute_idx)
                    continue
                seen_row_keys.add(row_key)

        return {
            "instances": instances,
            "instance_row_indices": instance_row_indices,
            "errors": errors,
            "error_row_indices": sorted(error_row_indices),
            "row_keys": row_keys,
            "fatal": False,
        }

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
        lineage_payload = {
            "job_id": job.job_id,
            "mapping_spec_id": mapping_spec.mapping_spec_id,
            "mapping_spec_version": mapping_spec.version,
            "dataset_id": job.dataset_id,
            "dataset_version_id": job.dataset_version_id,
        }

        async def _fail_link(errors: List[Dict[str, Any]], stats: Dict[str, Any]) -> None:
            await self._record_gate_result(
                job=job,
                status="FAIL",
                details={"errors": errors[:200], "stats": stats},
            )
            relationship_spec_id = str(options.get("relationship_spec_id") or "").strip()
            if relationship_spec_id and self.dataset_registry:
                try:
                    await self.dataset_registry.record_relationship_index_result(
                        relationship_spec_id=relationship_spec_id,
                        status="FAIL",
                        stats=stats,
                        errors=errors,
                        dataset_version_id=job.dataset_version_id,
                        mapping_spec_version=mapping_spec.version,
                        lineage=lineage_payload,
                    )
                except Exception as exc:
                    logger.warning("Failed to update relationship index status: %s", exc)
            await self.objectify_registry.update_objectify_job_status(
                job_id=job.job_id,
                status="FAILED",
                error="validation_failed",
                report={"errors": errors[:200], "stats": stats},
                completed_at=datetime.now(timezone.utc),
            )
            raise ObjectifyNonRetryableError("validation_failed")

        if not relationship_mappings:
            await _fail_link(
                [{"code": "RELATIONSHIP_MAPPING_MISSING", "message": "No relationship mappings provided"}],
                {"input_rows": 0},
            )

        object_type_resource = await self._fetch_object_type_contract(job)
        object_type_data = object_type_resource.get("data") if isinstance(object_type_resource, dict) else None
        if not isinstance(object_type_data, dict):
            object_type_data = object_type_resource if isinstance(object_type_resource, dict) else {}
        object_type_spec = object_type_data.get("spec") if isinstance(object_type_data.get("spec"), dict) else {}
        if not object_type_spec:
            await _fail_link(
                [{"code": "OBJECT_TYPE_CONTRACT_MISSING", "message": "Object type contract is required"}],
                {"input_rows": 0},
            )

        status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
        if status_value != "ACTIVE":
            await _fail_link(
                [{"code": "OBJECT_TYPE_INACTIVE", "status": status_value}],
                {"input_rows": 0},
            )

        object_type_key_spec = normalize_key_spec(object_type_spec.get("pk_spec") or {}, columns=list(prop_map.keys()))
        pk_targets = [str(v).strip() for v in object_type_key_spec.get("primary_key") or [] if str(v).strip()]
        if not pk_targets:
            await _fail_link(
                [{"code": "OBJECT_TYPE_PRIMARY_KEY_MISSING", "message": "pk_spec.primary_key is required"}],
                {"input_rows": 0},
            )

        pk_source_fields: List[str] = []
        pk_source_map: Dict[str, str] = {}
        for target in pk_targets:
            sources = sources_by_target.get(target) or []
            unique_sources = [s for s in sources if s]
            if len(unique_sources) != 1:
                await _fail_link(
                    [
                        {
                            "code": "PRIMARY_KEY_MAPPING_INVALID",
                            "target": target,
                            "sources": unique_sources,
                        }
                    ],
                    {"input_rows": 0},
                )
            pk_source_map[target] = unique_sources[0]
            pk_source_fields.append(unique_sources[0])

        relationship_meta = {m.target_field: rel_map.get(m.target_field) or {} for m in relationship_mappings}
        relationship_meta_override = options.get("relationship_meta") if isinstance(options.get("relationship_meta"), dict) else {}
        if relationship_meta_override:
            for target_field, meta in relationship_meta_override.items():
                if not isinstance(meta, dict):
                    continue
                existing = relationship_meta.get(target_field) or {}
                relationship_meta[target_field] = {**existing, **meta}
        rel_cardinality: Dict[str, str] = {}
        for target, meta in relationship_meta.items():
            cardinality = str(meta.get("cardinality") or "").strip().lower()
            rel_cardinality[target] = cardinality

        dangling_policy = str(options.get("dangling_policy") or "FAIL").strip().upper() or "FAIL"
        if dangling_policy not in {"FAIL", "WARN"}:
            dangling_policy = "FAIL"
        dedupe_policy = str(options.get("dedupe_policy") or options.get("dedupePolicy") or "DEDUP").strip().upper() or "DEDUP"
        if dedupe_policy not in {"DEDUP", "WARN", "FAIL"}:
            dedupe_policy = "DEDUP"
        relationship_kind = str(options.get("relationship_kind") or "").strip().lower()
        full_sync = bool(options.get("full_sync") or options.get("fullSync") or False)
        if relationship_kind in {"join_table", "object_backed"} and "full_sync" not in options and "fullSync" not in options:
            full_sync = True

        target_instances: Dict[str, set[str]] = {}
        target_fetch_errors: List[Dict[str, Any]] = []
        dangling_target_samples: List[Dict[str, Any]] = []
        missing_target_count = 0
        target_classes = sorted(
            {
                str(meta.get("target") or meta.get("linkTarget") or "").strip()
                for meta in relationship_meta.values()
                if isinstance(meta, dict)
            }
        )
        target_classes = [value for value in target_classes if value]
        if target_classes:
            branch = job.ontology_branch or job.dataset_branch or "main"
            for target_class in target_classes:
                try:
                    ids: set[str] = set()
                    async for instance_id in self._iter_class_instance_ids(
                        db_name=job.db_name,
                        class_id=target_class,
                        branch=branch,
                        limit=self.list_page_size,
                    ):
                        ids.add(instance_id)
                    target_instances[target_class] = ids
                except Exception as exc:
                    target_fetch_errors.append(
                        {
                            "code": "RELATIONSHIP_TARGET_LOOKUP_FAILED",
                            "target_class": target_class,
                            "error": str(exc),
                        }
                    )
        if target_fetch_errors and dangling_policy == "FAIL":
            await _fail_link(
                target_fetch_errors,
                {"input_rows": 0, "target_lookup_errors": target_fetch_errors},
            )

        total_rows = 0
        error_rows: List[int] = []
        errors: List[Dict[str, Any]] = []
        dangling_count = 0
        duplicate_count = 0

        updates_by_instance: Dict[str, Dict[str, List[str]]] = {}
        row_keys_seen: set[str] = set()

        async for columns, rows, row_offset in self._iter_dataset_batches(
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            if not rows:
                continue
            col_index = SheetImportService.build_column_index(columns)
            missing_sources = sorted({s for s in mapping_sources if s and s not in col_index})
            if missing_sources:
                await _fail_link(
                    [{"code": "SOURCE_FIELD_MISSING", "missing_sources": missing_sources}],
                    {"input_rows": total_rows},
                )
            for idx, row in enumerate(rows):
                total_rows += 1
                absolute_idx = int(row_offset) + int(idx)
                instance_payload: Dict[str, Any] = {}
                missing_pk = False
                for target, source_field in pk_source_map.items():
                    raw = row[col_index[source_field]] if source_field in col_index else None
                    if self._is_blank(raw):
                        missing_pk = True
                        break
                    instance_payload[target] = str(raw)
                if missing_pk:
                    error_rows.append(absolute_idx)
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": "PRIMARY_KEY_MISSING",
                            "message": "Primary key value is missing",
                        }
                    )
                    continue

                row_key = self._derive_row_key(
                    columns=columns,
                    col_index=col_index,
                    row=row,
                    instance=instance_payload,
                    pk_fields=pk_source_fields,
                    pk_targets=pk_targets,
                )
                if not row_key:
                    error_rows.append(absolute_idx)
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": "ROW_KEY_MISSING",
                            "message": "Row key could not be derived",
                        }
                    )
                    continue

                if row_key in row_keys_seen and dedupe_policy == "FAIL":
                    error_rows.append(absolute_idx)
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": "DUPLICATE_ROW_KEY",
                            "row_key": row_key,
                        }
                    )
                    continue
                row_keys_seen.add(row_key)

                inst_list, inst_ids = self._ensure_instance_ids(
                    [instance_payload],
                    class_id=job.target_class_id,
                    stable_seed=stable_seed,
                    mapping_spec_version=mapping_spec.version,
                    row_keys=[row_key],
                )
                instance_id = inst_ids[0]

                for mapping in relationship_mappings:
                    source_field = mapping.source_field
                    target_field = mapping.target_field
                    rel_meta = relationship_meta.get(target_field) or {}
                    target_class = str(rel_meta.get("target") or rel_meta.get("linkTarget") or "").strip()
                    if not target_class:
                        error_rows.append(absolute_idx)
                        errors.append(
                            {
                                "row_index": absolute_idx,
                                "code": "RELATIONSHIP_TARGET_MISSING",
                                "target_field": target_field,
                            }
                        )
                        continue
                    raw = row[col_index[source_field]] if source_field in col_index else None
                    if self._is_blank(raw):
                        dangling_count += 1
                        if dangling_policy == "FAIL":
                            error_rows.append(absolute_idx)
                            errors.append(
                                {
                                    "row_index": absolute_idx,
                                    "code": "RELATIONSHIP_VALUE_MISSING",
                                    "target_field": target_field,
                                }
                            )
                        continue
                    try:
                        ref = self._normalize_relationship_ref(raw, target_class=target_class)
                    except Exception as exc:
                        dangling_count += 1
                        if dangling_policy == "FAIL":
                            error_rows.append(absolute_idx)
                            errors.append(
                                {
                                    "row_index": absolute_idx,
                                    "code": "RELATIONSHIP_VALUE_INVALID",
                                    "target_field": target_field,
                                    "error": str(exc),
                                }
                            )
                        continue
                    target_ids = target_instances.get(target_class)
                    if target_ids is not None:
                        ref_id = ref.split("/", 1)[1] if "/" in ref else ref
                        if ref_id not in target_ids:
                            dangling_count += 1
                            missing_target_count += 1
                            if len(dangling_target_samples) < 20:
                                dangling_target_samples.append(
                                    {
                                        "row_index": absolute_idx,
                                        "target_field": target_field,
                                        "target_class": target_class,
                                        "target_id": ref_id,
                                    }
                                )
                            if dangling_policy == "FAIL":
                                error_rows.append(absolute_idx)
                                errors.append(
                                    {
                                        "row_index": absolute_idx,
                                        "code": "RELATIONSHIP_TARGET_MISSING",
                                        "target_field": target_field,
                                        "target_class": target_class,
                                        "target_id": ref_id,
                                    }
                                )
                            continue
                    entry = updates_by_instance.setdefault(instance_id, {})
                    bucket = entry.setdefault(target_field, [])
                    if ref not in bucket:
                        bucket.append(ref)
                    else:
                        duplicate_count += 1

        stats = {
            "input_rows": total_rows,
            "error_rows": len(error_rows),
            "dangling_count": dangling_count,
            "dangling_missing_targets": missing_target_count,
            "duplicate_count": duplicate_count,
            "dedupe_policy": dedupe_policy,
            "relationship_kind": relationship_kind,
        }
        if dangling_target_samples:
            stats["dangling_missing_target_samples"] = dangling_target_samples[:10]
        if target_fetch_errors:
            stats["dangling_target_fetch_errors"] = target_fetch_errors[:5]

        dedupe_warnings: List[Dict[str, Any]] = []
        dangling_warnings: List[Dict[str, Any]] = []
        if duplicate_count:
            if dedupe_policy == "FAIL":
                errors.append(
                    {
                        "code": "RELATIONSHIP_DUPLICATE",
                        "message": "Duplicate relationship pairs detected",
                        "duplicate_count": duplicate_count,
                    }
                )
                await _fail_link(errors, stats)
            elif dedupe_policy == "WARN":
                dedupe_warnings.append(
                    {
                        "code": "RELATIONSHIP_DUPLICATE",
                        "message": "Duplicate relationship pairs deduped",
                        "duplicate_count": duplicate_count,
                    }
                )
                stats["dedupe_warnings"] = duplicate_count
        if missing_target_count and dangling_policy != "FAIL":
            dangling_warnings.append(
                {
                    "code": "RELATIONSHIP_TARGET_MISSING",
                    "missing_count": missing_target_count,
                    "samples": dangling_target_samples[:10],
                }
            )
        if target_fetch_errors and dangling_policy != "FAIL":
            dangling_warnings.append(
                {
                    "code": "RELATIONSHIP_TARGET_LOOKUP_FAILED",
                    "errors": target_fetch_errors[:5],
                }
            )

        if errors and dangling_policy == "FAIL":
            await _fail_link(errors, stats)

        link_edit_errors: List[Dict[str, Any]] = []
        link_edits_applied = 0
        if self.dataset_registry:
            link_type_id = str(options.get("link_type_id") or "").strip()
            branch = job.ontology_branch or job.dataset_branch or "main"
            if link_type_id:
                try:
                    edits = await self.dataset_registry.list_link_edits(
                        db_name=job.db_name,
                        link_type_id=link_type_id,
                        branch=branch,
                        status="ACTIVE",
                        limit=10000,
                    )
                except Exception as exc:
                    edits = []
                    link_edit_errors.append({"code": "LINK_EDIT_FETCH_FAILED", "error": str(exc)})
                for edit in edits:
                    predicate = str(edit.predicate or "").strip()
                    rel_meta = relationship_meta.get(predicate) or {}
                    target_class = str(rel_meta.get("target") or rel_meta.get("linkTarget") or "").strip()
                    if not predicate or not target_class:
                        link_edit_errors.append(
                            {
                                "code": "LINK_EDIT_PREDICATE_UNKNOWN",
                                "predicate": predicate,
                            }
                        )
                        continue
                    try:
                        ref = self._normalize_relationship_ref(edit.target_instance_id, target_class=target_class)
                    except Exception as exc:
                        link_edit_errors.append(
                            {
                                "code": "LINK_EDIT_TARGET_INVALID",
                                "predicate": predicate,
                                "error": str(exc),
                            }
                        )
                        continue
                    target_ids = target_instances.get(target_class)
                    if target_ids is not None:
                        ref_id = ref.split("/", 1)[1] if "/" in ref else ref
                        if ref_id not in target_ids:
                            link_edit_errors.append(
                                {
                                    "code": "LINK_EDIT_TARGET_MISSING",
                                    "predicate": predicate,
                                    "target_class": target_class,
                                    "target_id": ref_id,
                                }
                            )
                            continue

                    entry = updates_by_instance.setdefault(edit.source_instance_id, {})
                    bucket = entry.setdefault(predicate, [])
                    action = str(edit.edit_type or "").strip().upper()
                    if action == "ADD":
                        if ref not in bucket:
                            bucket.append(ref)
                        link_edits_applied += 1
                    elif action in {"REMOVE", "DELETE"}:
                        if ref in bucket:
                            bucket.remove(ref)
                        link_edits_applied += 1
                    else:
                        link_edit_errors.append(
                            {"code": "LINK_EDIT_TYPE_INVALID", "edit_type": edit.edit_type}
                        )

        if link_edits_applied or link_edit_errors:
            stats["link_edits_applied"] = link_edits_applied
            if link_edit_errors:
                stats["link_edit_errors"] = link_edit_errors[:200]

        if full_sync and relationship_kind in {"join_table", "object_backed"}:
            try:
                total_instances = 0
                cleared_instances = 0
                branch = job.ontology_branch or job.dataset_branch or "main"
                async for instance_id in self._iter_class_instance_ids(
                    db_name=job.db_name,
                    class_id=job.target_class_id,
                    branch=branch,
                ):
                    total_instances += 1
                    if instance_id in updates_by_instance:
                        continue
                    entry = updates_by_instance.setdefault(instance_id, {})
                    for field in relationship_meta.keys():
                        entry.setdefault(field, [])
                    cleared_instances += 1
                stats["full_sync_total_instances"] = total_instances
                stats["full_sync_cleared"] = cleared_instances
            except Exception as exc:
                # Full-sync enumeration is an OMS transport operation and can fail transiently
                # (RemoteProtocolError "Server disconnected...", timeouts, etc). Treat those
                # as retryable so the Kafka worker can backoff/retry instead of marking a
                # permanent FAIL + DLQ on the relationship spec.
                if isinstance(exc, (httpx.RequestError, httpx.TimeoutException)):
                    raise
                await _fail_link(
                    [
                        {
                            "code": "FULL_SYNC_FAILED",
                            "message": "Unable to enumerate instances for full sync",
                            "error": str(exc),
                        }
                    ],
                    stats,
                )

        updates: List[Dict[str, Any]] = []
        for instance_id, rel_values in updates_by_instance.items():
            payload: Dict[str, Any] = {}
            for field, refs in rel_values.items():
                cardinality = rel_cardinality.get(field) or ""
                allow_multiple = any(token in cardinality for token in (":n", ":m", "many"))
                if not allow_multiple:
                    if not refs:
                        payload[field] = None
                        continue
                    if len(refs) > 1:
                        errors.append(
                            {
                                "code": "RELATIONSHIP_CARDINALITY_VIOLATION",
                                "instance_id": instance_id,
                                "field": field,
                                "count": len(refs),
                            }
                        )
                        continue
                    payload[field] = refs[0]
                else:
                    payload[field] = refs
            if payload:
                updates.append({"instance_id": instance_id, "data": payload})

        if errors and dangling_policy == "FAIL":
            await _fail_link(errors, stats)

        if not updates:
            await _fail_link(
                [{"code": "NO_RELATIONSHIPS_INDEXED", "message": "No link updates produced"}],
                stats,
            )

        ontology_version = await self._fetch_ontology_version(job)
        command_ids: List[str] = []
        for idx in range(0, len(updates), self.bulk_update_batch_size):
            batch = updates[idx : idx + self.bulk_update_batch_size]
            resp = await self._bulk_update_instances(job, batch, ontology_version=ontology_version)
            command_id = resp.get("command_id") if isinstance(resp, dict) else None
            if command_id:
                command_ids.append(str(command_id))

        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="SUBMITTED",
            command_id=command_ids[0] if command_ids else None,
            report={
                "link_index": True,
                "total_rows": total_rows,
                "update_count": len(updates),
                "command_ids": command_ids,
                "stats": stats,
            },
            completed_at=datetime.now(timezone.utc),
        )
        await self._record_gate_result(
            job=job,
            status="PASS",
            details={"stats": stats, "command_ids": command_ids},
        )

        relationship_spec_id = str(options.get("relationship_spec_id") or "").strip()
        if relationship_spec_id and self.dataset_registry:
            record_errors: List[Dict[str, Any]] = []
            if dangling_policy != "FAIL":
                record_errors.extend(errors)
            record_errors.extend(dedupe_warnings)
            record_errors.extend(dangling_warnings)
            record_errors.extend(link_edit_errors)
            status_value = "WARN" if record_errors else "PASS"
            try:
                await self.dataset_registry.record_relationship_index_result(
                    relationship_spec_id=relationship_spec_id,
                    status=status_value,
                    stats=stats,
                    errors=record_errors,
                    dataset_version_id=job.dataset_version_id,
                    mapping_spec_version=mapping_spec.version,
                    lineage=lineage_payload,
                )
            except Exception as exc:
                logger.warning("Failed to update relationship index status: %s", exc)

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
        errors: List[Dict[str, Any]] = []
        error_row_indices: List[int] = []
        total_rows = 0
        pk_duplicate_count = 0
        pk_missing_count = 0
        unique_duplicate_count = 0
        pk_duplicate_samples: List[str] = []
        unique_duplicate_samples: List[str] = []
        seen_row_keys: set[str] = set()
        seen_unique: Dict[Tuple[str, ...], set[str]] = {
            tuple(keys): set() for keys in unique_keys if keys
        }

        key_targets = set(pk_targets)
        for keys in unique_keys:
            key_targets.update(keys)
        key_mappings = [m for m in mappings if m.target_field in key_targets]
        key_sources_by_target = {t: sources_by_target.get(t, []) for t in key_targets}
        key_mapping_sources = [m.source_field for m in key_mappings if m.source_field]
        key_field_types = {t: target_field_types.get(t) for t in key_targets if t in target_field_types}

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
                mappings=key_mappings,
                relationship_mappings=[],
                relationship_meta=relationship_meta,
                target_field_types=key_field_types,
                mapping_sources=key_mapping_sources,
                sources_by_target=key_sources_by_target,
                required_targets=required_targets,
                pk_targets=pk_targets,
                pk_fields=pk_fields,
                field_constraints={},
                field_raw_types={},
                seen_row_keys=seen_row_keys,
            )
            batch_errors = batch.get("errors") or []
            for err in batch_errors:
                code = err.get("code") if isinstance(err, dict) else None
                if code == "PRIMARY_KEY_DUPLICATE":
                    pk_duplicate_count += 1
                    row_key = err.get("row_key") if isinstance(err, dict) else None
                    if row_key and len(pk_duplicate_samples) < 5:
                        pk_duplicate_samples.append(str(row_key))
                if code == "PRIMARY_KEY_MISSING":
                    pk_missing_count += 1
                if len(errors) < 200:
                    errors.append(err)
            error_row_indices.extend(batch.get("error_row_indices") or [])

            instances = batch.get("instances") or []
            instance_row_indices = batch.get("instance_row_indices") or []
            for inst, row_idx in zip(instances, instance_row_indices):
                absolute_idx = int(row_offset) + int(row_idx)
                if not isinstance(inst, dict):
                    continue
                for keys in unique_keys:
                    if not keys:
                        continue
                    key_value = self._derive_unique_key(inst, keys)
                    if not key_value:
                        continue
                    key_tuple = tuple(keys)
                    seen_set = seen_unique.setdefault(key_tuple, set())
                    if key_value in seen_set:
                        errors.append(
                            {
                                "row_index": absolute_idx,
                                "code": "UNIQUE_KEY_DUPLICATE",
                                "key_fields": keys,
                                "key_value": key_value,
                                "message": "Duplicate unique key detected",
                            }
                        )
                        unique_duplicate_count += 1
                        if len(unique_duplicate_samples) < 5:
                            unique_duplicate_samples.append(key_value)
                        error_row_indices.append(absolute_idx)
                    else:
                        seen_set.add(key_value)

        error_row_indices = sorted(set(error_row_indices))
        stats = {
            "input_rows": total_rows,
            "error_rows": len(error_row_indices),
            "error_count": len(errors),
            "pk_duplicates": pk_duplicate_count,
            "pk_missing": pk_missing_count,
            "unique_key_duplicates": unique_duplicate_count,
            "pk_duplicate_samples": pk_duplicate_samples,
            "unique_duplicate_samples": unique_duplicate_samples,
            "unique_keys_checked": len(unique_keys),
        }
        return total_rows, errors, error_row_indices, stats

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

    async def _record_lineage_header(
        self,
        *,
        job: ObjectifyJob,
        mapping_spec: Any,
        ontology_version: Optional[Dict[str, str]],
        input_type: str,
        artifact_output_name: Optional[str] = None,
    ) -> Optional[str]:
        if not self.lineage_store:
            return None
        try:
            job_node = self.lineage_store.node_aggregate("ObjectifyJob", job.job_id)
            if input_type == "artifact":
                source_node = self.lineage_store.node_aggregate("PipelineArtifact", str(job.artifact_id))
                edge_type = "pipeline_artifact_objectify_job"
                edge_metadata = {
                    "db_name": job.db_name,
                    "dataset_id": job.dataset_id,
                    "artifact_id": job.artifact_id,
                    "artifact_output_name": artifact_output_name or job.artifact_output_name,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "target_class_id": job.target_class_id,
                }
            else:
                source_node = self.lineage_store.node_aggregate("DatasetVersion", str(job.dataset_version_id))
                edge_type = "dataset_version_objectify_job"
                edge_metadata = {
                    "db_name": job.db_name,
                    "dataset_id": job.dataset_id,
                    "dataset_version_id": job.dataset_version_id,
                    "mapping_spec_id": job.mapping_spec_id,
                    "mapping_spec_version": job.mapping_spec_version,
                    "target_class_id": job.target_class_id,
                }
            await self.lineage_store.record_link(
                from_node_id=source_node,
                to_node_id=job_node,
                edge_type=edge_type,
                occurred_at=datetime.now(timezone.utc),
                db_name=job.db_name,
                edge_metadata=edge_metadata,
            )

            mapping_version_id = f"{mapping_spec.mapping_spec_id}:v{mapping_spec.version}"
            mapping_node = self.lineage_store.node_aggregate("MappingSpecVersion", mapping_version_id)
            await self.lineage_store.record_link(
                from_node_id=job_node,
                to_node_id=mapping_node,
                edge_type="objectify_job_mapping_spec",
                occurred_at=datetime.now(timezone.utc),
                db_name=job.db_name,
                edge_metadata={
                    "db_name": job.db_name,
                    "mapping_spec_id": mapping_spec.mapping_spec_id,
                    "mapping_spec_version": mapping_spec.version,
                    "dataset_id": job.dataset_id,
                },
            )

            if ontology_version:
                branch = job.ontology_branch or job.dataset_branch or "main"
                ont_id = f"{job.db_name}:{branch}:{ontology_version.get('commit') or 'head'}"
                ont_node = self.lineage_store.node_aggregate("OntologyVersion", ont_id)
                await self.lineage_store.record_link(
                    from_node_id=job_node,
                    to_node_id=ont_node,
                    edge_type="objectify_job_ontology_version",
                    occurred_at=datetime.now(timezone.utc),
                    db_name=job.db_name,
                    edge_metadata={
                        "db_name": job.db_name,
                        "branch": branch,
                        "ontology": ontology_version,
                    },
                )
            return job_node
        except Exception as exc:
            logger.warning("Failed to record objectify job lineage header: %s", exc)
            return None

    async def _record_instance_lineage(
        self,
        *,
        job: ObjectifyJob,
        job_node_id: Optional[str],
        instance_ids: List[str],
        mapping_spec_id: str,
        mapping_spec_version: int,
        ontology_version: Optional[Dict[str, str]],
        limit_remaining: int,
        input_type: str,
        artifact_output_name: Optional[str] = None,
    ) -> int:
        if not self.lineage_store:
            return limit_remaining
        if limit_remaining <= 0:
            return limit_remaining
        if input_type == "artifact":
            source_node = self.lineage_store.node_aggregate("PipelineArtifact", str(job.artifact_id))
            edge_type = "pipeline_artifact_objectified"
            edge_metadata = {
                "db_name": job.db_name,
                "dataset_id": job.dataset_id,
                "artifact_id": job.artifact_id,
                "artifact_output_name": artifact_output_name or job.artifact_output_name,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "target_class_id": job.target_class_id,
                "ontology": ontology_version or {},
            }
        else:
            source_node = self.lineage_store.node_aggregate("DatasetVersion", str(job.dataset_version_id))
            edge_type = "dataset_version_objectified"
            edge_metadata = {
                "db_name": job.db_name,
                "dataset_id": job.dataset_id,
                "dataset_version_id": job.dataset_version_id,
                "mapping_spec_id": mapping_spec_id,
                "mapping_spec_version": mapping_spec_version,
                "target_class_id": job.target_class_id,
                "ontology": ontology_version or {},
            }
        for instance_id in instance_ids:
            if limit_remaining <= 0:
                break
            aggregate_id = f"{job.db_name}:{job.dataset_branch}:{job.target_class_id}:{instance_id}"
            instance_node = self.lineage_store.node_aggregate("Instance", aggregate_id)
            try:
                await self.lineage_store.record_link(
                    from_node_id=source_node,
                    to_node_id=instance_node,
                    edge_type=edge_type,
                    occurred_at=datetime.now(timezone.utc),
                    db_name=job.db_name,
                    edge_metadata=edge_metadata,
                )
                if job_node_id:
                    await self.lineage_store.record_link(
                        from_node_id=job_node_id,
                        to_node_id=instance_node,
                        edge_type="objectify_job_created_instance",
                        occurred_at=datetime.now(timezone.utc),
                        db_name=job.db_name,
                        edge_metadata={
                            "db_name": job.db_name,
                            "dataset_id": job.dataset_id,
                            "dataset_version_id": job.dataset_version_id,
                            "artifact_id": job.artifact_id,
                            "artifact_output_name": artifact_output_name or job.artifact_output_name,
                            "mapping_spec_id": mapping_spec_id,
                            "mapping_spec_version": mapping_spec_version,
                            "target_class_id": job.target_class_id,
                            "ontology": ontology_version or {},
                        },
                    )
                limit_remaining -= 1
            except Exception as exc:
                logger.warning("Failed to record lineage for instance %s: %s", instance_id, exc)
        return limit_remaining

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
