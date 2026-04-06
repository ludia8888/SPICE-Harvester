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
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx
from confluent_kafka import Producer

from shared.services.kafka.dlq_publisher import DlqPublishSpec
from shared.services.kafka.processed_event_worker import HeartbeatOptions, JsonModelKafkaWorker, RegistryKey
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
from objectify_worker.runtime_helpers import (
    ObjectifyNonRetryableError,
    _auto_detect_watermark_column,
    _compute_lakefs_delta,
    _extract_instance_relationships,
)
from objectify_worker.runtime_mixin import ObjectifyWorkerRuntimeMixin

logger = logging.getLogger(__name__)

class ObjectifyWorker(ObjectifyWorkerRuntimeMixin, JsonModelKafkaWorker[ObjectifyJob, None]):
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

    async def _publish_to_dlq(
        self,
        *,
        msg: Any,
        stage: str,
        error: str,
        attempt_count: Optional[int],
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        kafka_headers: Optional[Any] = None,
        fallback_metadata: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            sent = await self._publish_standard_dlq_record(
                producer=self.dlq_producer,
                msg=msg,
                worker=self.service_name,
                dlq_spec=self._dlq_spec,
                error=error,
                attempt_count=int(attempt_count) if attempt_count is not None else None,
                stage=stage,
                payload_text=payload_text,
                payload_obj=payload_obj,
                extra=extra,
                tracing=self.tracing,
                metrics=self.metrics,
                kafka_headers=kafka_headers,
                fallback_metadata=fallback_metadata,
                raise_on_missing_producer=False,
                missing_producer_message="DLQ producer not configured",
            )
            if not sent:
                return
            logger.info("Sent message to objectify DLQ (topic=%s stage=%s)", self.dlq_topic, stage)
        except Exception as exc:
            logger.warning("Failed to publish objectify DLQ payload (stage=%s): %s", stage, exc, exc_info=True)

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
