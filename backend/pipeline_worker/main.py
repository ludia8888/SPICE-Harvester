"""
Pipeline Worker (Spark/Flink-ready execution runtime).

Consumes Kafka pipeline-jobs and executes dataset transforms using Spark.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from uuid import UUID, uuid4

from confluent_kafka import Producer

from shared.services.kafka.safe_consumer import SafeKafkaConsumer

try:
    from pyspark.sql import DataFrame, SparkSession  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.types import StructType  # type: ignore
    from pyspark.sql.window import Window  # type: ignore

    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover
    # Keep helper functions importable in lightweight/unit-test environments.
    _PYSPARK_AVAILABLE = False
    SparkSession = Any  # type: ignore[assignment,misc]
    DataFrame = Any  # type: ignore[assignment,misc]
    F = None  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]
    StructType = Any  # type: ignore[assignment,misc]

from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited, record_lineage_or_raise
from shared.services.core.write_path_contract import (
    build_write_path_contract,
    followup_completed,
    followup_degraded,
)
from shared.models.pipeline_job import PipelineJob
from shared.models.lineage_edge_types import EDGE_PIPELINE_OUTPUT_STORED
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.kafka.processed_event_worker import HeartbeatOptions, JsonModelKafkaWorker, RegistryKey
from shared.services.kafka.dlq_publisher import DlqPublishSpec
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError
from shared.services.registries.lineage_store import LineageStore
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.services.pipeline.pipeline_kafka_avro import (
    fetch_kafka_avro_schema_from_registry,
    resolve_inline_avro_schema,
    resolve_kafka_avro_schema_registry_reference,
)
from shared.services.pipeline.dataset_output_semantics import (
    resolve_dataset_write_policy,
    validate_dataset_output_metadata,
)
from shared.services.pipeline.output_plugins import (
    OUTPUT_KIND_DATASET,
    resolve_output_kind,
    validate_output_payload,
)
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.pipeline.pipeline_control_plane_events import emit_pipeline_control_plane_event
from shared.services.pipeline.pipeline_definition_validator import (
    normalize_transform_metadata,
)
from shared.services.pipeline.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline.pipeline_parameter_utils import normalize_parameters
from shared.services.pipeline.pipeline_definition_utils import (
    build_expectations_with_pk,
    normalize_expectation_columns,
    resolve_delete_column,
    resolve_incremental_config,
    resolve_pk_columns,
    resolve_pk_semantics,
    validate_pk_semantics,
)
from shared.services.pipeline.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline.pipeline_validation_utils import (
    TableOps,
    validate_expectations,
    validate_schema_checks,
    validate_schema_contract,
)
from shared.services.pipeline.pipeline_udf_runtime import resolve_udf_reference
from shared.services.pipeline.pipeline_schema_casts import extract_schema_casts
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.services.pipeline.pipeline_type_utils import normalize_cast_mode, spark_type_to_xsd, xsd_to_spark_type
from shared.services.pipeline.pipeline_transform_spec import (
    SUPPORTED_TRANSFORMS,
    normalize_operation,
)
from pipeline_worker.preview_sampling import (
    apply_sampling_strategy as apply_preview_sampling_strategy,
    attach_sampling_snapshot as attach_preview_sampling_snapshot,
    resolve_preview_flag as resolve_preview_meta_flag,
    resolve_preview_limit as resolve_preview_meta_limit,
    resolve_sampling_strategy as resolve_preview_sampling_strategy,
)
from pipeline_worker import downstream_enqueuing as _downstream_enqueuing
from pipeline_worker import execution_node_resolution as _execution_node_resolution
from pipeline_worker import execution_modes as _execution_modes
from pipeline_worker import fk_validation as _fk_validation
from pipeline_worker.ingest_domain import PipelineIngestDomain
from pipeline_worker import input_loading as _input_loading
from pipeline_worker import output_preparation as _output_preparation
from pipeline_worker import output_publication as _output_publication
from pipeline_worker.output_domain import PipelineOutputDomain
from pipeline_worker.run_domain import PipelineRunDomain
from pipeline_worker.validation_domain import PipelineValidationDomain
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.backing_source_adapter import MappingSpecResolver, is_oms_mapping_spec
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.pipeline.pipeline_lock import PipelineLock, PipelineLockError
from shared.services.storage.redis_service import RedisService, create_redis_service
from shared.services.grpc.oms_gateway_client import OMSGrpcHttpCompatClient
from shared.services.storage.storage_service import StorageService
from shared.utils.path_utils import safe_lakefs_ref
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.app_logger import configure_logging
from shared.utils.time_utils import utcnow

from pipeline_worker.worker_helpers import (
    _collect_input_commit_map,
    _collect_watermark_keys_from_snapshots,
    _inputs_diff_empty,
    _is_sensitive_conf_key,
    _max_watermark_from_snapshots,
    _resolve_code_version,
    _resolve_external_read_mode,
    _resolve_execution_semantics,
    _resolve_lakefs_repository,
    _resolve_output_format,
    _resolve_partition_columns,
    _resolve_streaming_timeout_seconds,
    _resolve_streaming_trigger_mode,
    _resolve_watermark_column,
    _watermark_values_match,
)
from pipeline_worker.spark_transform_engine import apply_spark_transform
from pipeline_worker.spark_schema_helpers import (
    _hash_schema_columns,
    _is_data_object,
    _schema_diff,
    _schema_from_dataframe,
)
from pipeline_worker.runtime_mixin import PipelineWorkerRuntimeMixin

logger = logging.getLogger(__name__)

SUPPORTED_TRANSFORMS_SPARK = SUPPORTED_TRANSFORMS

T = TypeVar("T")


def _schema_column_names(columns: Optional[List[Any]]) -> set[str]:
    names: set[str] = set()
    for item in columns or []:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("column") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            names.add(name)
    return names


@dataclass(frozen=True)
class _DatasetInputLoadContext:
    dataset: Any
    version: Any
    requested_branch: Optional[str]
    resolved_branch: Optional[str]
    used_fallback: bool
    bucket: str
    key: str
    current_commit_id: str
    artifact_prefix: str


def _resolve_declared_output_kind(
    *,
    declared_outputs: List[Dict[str, Any]],
    output_node_id: Optional[str],
    output_name: Optional[str],
) -> str:
    def _normalize_declared_kind(
        *,
        item: Dict[str, Any],
        resolved_node_id: str,
        resolved_output_name: str,
    ) -> str:
        raw_kind = str(item.get("output_kind") or item.get("outputKind") or OUTPUT_KIND_DATASET).strip().lower()
        try:
            resolved = resolve_output_kind(raw_kind)
        except ValueError:
            return OUTPUT_KIND_DATASET
        if resolved.used_alias:
            logger.warning(
                "Output kind alias normalized: raw=%s normalized=%s node_id=%s output_name=%s",
                resolved.raw_kind,
                resolved.normalized_kind,
                resolved_node_id,
                resolved_output_name,
            )
        return resolved.normalized_kind

    node_id = str(output_node_id or "").strip()
    name = str(output_name or "").strip()
    for item in declared_outputs:
        if not isinstance(item, dict):
            continue
        declared_node_id = str(item.get("node_id") or item.get("nodeId") or "").strip()
        declared_name = str(
            item.get("output_name")
            or item.get("outputName")
            or item.get("dataset_name")
            or item.get("datasetName")
            or ""
        ).strip()
        if node_id and declared_node_id == node_id:
            return _normalize_declared_kind(
                item=item,
                resolved_node_id=node_id,
                resolved_output_name=name or declared_name,
            )
        if name and declared_name and declared_name == name:
            return _normalize_declared_kind(
                item=item,
                resolved_node_id=node_id or declared_node_id,
                resolved_output_name=name,
            )
    return OUTPUT_KIND_DATASET


def _validate_output_kind_metadata(
    *,
    output_kind: str,
    output_metadata: Dict[str, Any],
    node_id: str,
) -> None:
    errors = validate_output_payload(kind=output_kind, payload=output_metadata)
    if errors:
        details = "; ".join(str(item) for item in errors)
        raise ValueError(f"Invalid output metadata for node {node_id} ({output_kind}): {details}")


class PipelineWorker(PipelineWorkerRuntimeMixin, JsonModelKafkaWorker[PipelineJob, None]):
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        pipeline_settings = settings.pipeline

        self.running = False
        self.topic = AppConfig.PIPELINE_JOBS_TOPIC
        self.dlq_topic = AppConfig.PIPELINE_JOBS_DLQ_TOPIC
        self.group_id = pipeline_settings.jobs_group
        self.handler = pipeline_settings.worker_handler
        self.pipeline_label = pipeline_settings.worker_name
        self._pipeline_worker_name = str(self.pipeline_label or "pipeline-worker")

        self.max_retries = pipeline_settings.jobs_max_retries
        self.backoff_base = pipeline_settings.jobs_backoff_base_seconds
        self.backoff_max = pipeline_settings.jobs_backoff_max_seconds
        # Spark jobs can legitimately take longer than Kafka's default 5m max poll interval.
        # This must be long enough to cover worst-case preview/build/deploy runtimes.
        self.max_poll_interval_ms = pipeline_settings.jobs_max_poll_interval_ms
        # If the worker is restarted mid-job, we want fast recovery; a 15m lease blocks the
        # entire Kafka partition. Keep this short for pipeline jobs and rely on heartbeats.
        self.processed_event_lease_timeout_seconds = pipeline_settings.processed_event_lease_timeout_seconds

        self.consumer: Optional[SafeKafkaConsumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.pipeline_registry: Optional[PipelineRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.mapping_resolver: Optional[MappingSpecResolver] = None
        self.objectify_job_queue: Optional[ObjectifyJobQueue] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage: Optional[LineageStore] = None
        self.storage: Optional[StorageService] = None
        self.lakefs_client: Optional[LakeFSClient] = None
        self.sheets: Optional[GoogleSheetsService] = None
        self.http: Optional[OMSGrpcHttpCompatClient] = None
        self.spark: Optional[SparkSession] = None
        self._spark_executor: Optional[ThreadPoolExecutor] = None
        self.redis: Optional[RedisService] = None
        self.lock_enabled = pipeline_settings.locks_enabled
        self.lock_required = pipeline_settings.locks_required
        self.lock_ttl_seconds = pipeline_settings.lock_ttl_seconds
        self.lock_renew_seconds = pipeline_settings.lock_renew_seconds
        self.lock_retry_seconds = pipeline_settings.lock_retry_seconds
        self.lock_acquire_timeout_seconds = pipeline_settings.lock_acquire_timeout_seconds

        self.spark_ansi_enabled = pipeline_settings.spark_ansi_enabled
        self.spark_adaptive_enabled = pipeline_settings.spark_adaptive_enabled
        self.spark_shuffle_partitions = int(pipeline_settings.spark_shuffle_partitions)
        self.spark_console_progress = pipeline_settings.spark_console_progress
        self.spark_driver_memory = str(pipeline_settings.spark_driver_memory or "512m").strip() or "512m"
        self.spark_streaming_enabled = bool(pipeline_settings.spark_streaming_enabled)
        self.spark_streaming_default_trigger = str(
            pipeline_settings.spark_streaming_default_trigger or "available_now"
        ).strip().lower() or "available_now"
        self.spark_streaming_await_timeout_seconds = int(pipeline_settings.spark_streaming_await_timeout_seconds)
        self.kafka_schema_registry_timeout_seconds = int(pipeline_settings.kafka_schema_registry_timeout_seconds)
        self.cast_mode = normalize_cast_mode(pipeline_settings.cast_mode)
        self._udf_require_reference = bool(pipeline_settings.udf_require_reference)
        self._udf_require_version_pinning = bool(pipeline_settings.udf_require_version_pinning)
        self._udf_spark_parity_enabled = bool(pipeline_settings.udf_spark_parity_enabled)
        self._udf_code_cache: Dict[str, str] = {}
        self._kafka_avro_schema_cache: Dict[str, str] = {}
        self.use_lakefs_diff = pipeline_settings.lakefs_diff_enabled
        self.processed_event_heartbeat_interval_seconds = settings.event_sourcing.processed_event_heartbeat_interval_seconds
        self.service_name = (settings.observability.service_name or "").strip() or None
        self.enable_lineage = bool(settings.observability.enable_lineage)
        self.lineage_required = bool(settings.observability.lineage_required_effective and self.enable_lineage)
        self.bff_admin_token = (settings.clients.bff_admin_token or "").strip() or None

        # Consumer safety (enterprise):
        # - Keep Kafka polling responsive even when Spark actions take minutes.
        # - Track in-flight message handlers per partition and pause/resume partitions.
        self._init_partition_state()
        self._dlq_lock = asyncio.Lock()
        self.tracing = get_tracing_service("pipeline-worker")
        self.metrics = get_metrics_collector("pipeline-worker")
        self._run_domain = PipelineRunDomain(self)
        self._ingest_domain = PipelineIngestDomain(self)
        self._output_domain = PipelineOutputDomain(self)
        self._validation_domain = PipelineValidationDomain(self)
        self._dlq_spec = DlqPublishSpec(
            dlq_topic=self.dlq_topic,
            service_name=self._pipeline_worker_name,
            flush_timeout_seconds=10.0,
            poll_after_produce=True,
        )

    def _plan_execution_scope(
        self,
        *,
        job: PipelineJob,
        nodes: Dict[str, Dict[str, Any]],
        order: List[str],
        incoming: Dict[str, List[str]],
        output_nodes: Dict[str, Dict[str, Any]],
        execution_semantics: str,
    ) -> Tuple[List[str], set[str], List[str], Optional[str]]:
        requested_node_error: Optional[str] = None
        target_node_ids: List[str]
        if execution_semantics == "streaming" and job.node_id:
            requested_node_error = "Streaming pipelines do not support node_id targeting; run the whole pipeline"
            target_node_ids = []
        elif job.node_id:
            if job.node_id not in nodes:
                requested_node_error = f"Requested node_id not found: {job.node_id}"
                target_node_ids = []
            else:
                target_node_ids = [job.node_id]
        elif output_nodes:
            target_node_ids = list(output_nodes.keys())
        elif order:
            target_node_ids = [order[-1]]
        else:
            target_node_ids = []

        required_node_ids: set[str] = set()
        stack = list(target_node_ids)
        while stack:
            current = stack.pop()
            if current in required_node_ids:
                continue
            required_node_ids.add(current)
            for parent in incoming.get(current, []):
                if parent not in required_node_ids:
                    stack.append(parent)
        order_run = [node_id for node_id in order if node_id in required_node_ids]
        return target_node_ids, required_node_ids, order_run, requested_node_error

    async def _load_previous_watermark_state(
        self,
        *,
        job: PipelineJob,
        resolved_pipeline_id: Optional[str],
        execution_semantics: str,
        watermark_column: Optional[str],
    ) -> Tuple[Optional[Any], List[str], Dict[str, str]]:
        previous_watermark: Optional[Any] = None
        previous_watermark_keys: List[str] = []
        previous_input_commits: Dict[str, str] = {}
        if execution_semantics not in {"incremental", "streaming"} or not resolved_pipeline_id:
            return previous_watermark, previous_watermark_keys, previous_input_commits

        try:
            state = await self.pipeline_registry.get_watermarks(
                pipeline_id=resolved_pipeline_id,
                branch=job.branch or "main",
            )
            stored_column = str(
                state.get("watermark_column")
                or state.get("watermarkColumn")
                or state.get("column")
                or ""
            ).strip()
            stored_value = (
                state.get("watermark_value")
                if "watermark_value" in state
                else state.get("watermarkValue")
                if "watermarkValue" in state
                else state.get("value")
            )
            if watermark_column:
                if stored_column and stored_column != watermark_column:
                    previous_watermark = None
                    previous_watermark_keys = []
                else:
                    previous_watermark = stored_value
                    stored_keys = None
                    if isinstance(state, dict):
                        stored_keys = state.get("watermark_keys") or state.get("watermarkKeys")
                    if isinstance(stored_keys, list):
                        for item in stored_keys:
                            item_str = str(item or "").strip()
                            if item_str:
                                previous_watermark_keys.append(item_str)
            commits_payload = (
                state.get("input_commits")
                if isinstance(state, dict)
                else None
            )
            if commits_payload is None and isinstance(state, dict):
                commits_payload = state.get("inputCommits")
            if isinstance(commits_payload, dict):
                for key, value in commits_payload.items():
                    node_key = str(key or "").strip()
                    commit_value = str(value or "").strip()
                    if node_key and commit_value:
                        previous_input_commits[node_key] = commit_value
        except Exception as exc:
            logger.warning("Failed to load pipeline watermarks (pipeline_id=%s): %s", resolved_pipeline_id, exc)

        return previous_watermark, previous_watermark_keys, previous_input_commits

    async def _run_preview_mode(
        self,
        *,
        job: PipelineJob,
        definition: Dict[str, Any],
        preview_meta: Dict[str, Any],
        nodes: Dict[str, Dict[str, Any]],
        target_node_ids: List[str],
        tables: Dict[str, DataFrame],
        input_snapshots: List[Dict[str, Any]],
        input_sampling: Dict[str, Any],
        temp_dirs: List[str],
        preview_limit: int,
        execution_semantics: str,
        declared_outputs: List[Dict[str, Any]],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
        code_version: Optional[str],
        spark_conf: Dict[str, Any],
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_preview: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
    ) -> None:
        await _execution_modes.run_preview_mode(
            self,
            job=job,
            definition=definition,
            preview_meta=preview_meta,
            nodes=nodes,
            target_node_ids=target_node_ids,
            tables=tables,
            input_snapshots=input_snapshots,
            input_sampling=input_sampling,
            temp_dirs=temp_dirs,
            preview_limit=preview_limit,
            execution_semantics=execution_semantics,
            declared_outputs=declared_outputs,
            pipeline_spec_hash=pipeline_spec_hash,
            pipeline_spec_commit_id=pipeline_spec_commit_id,
            code_version=code_version,
            spark_conf=spark_conf,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            record_preview=record_preview,
            record_run=record_run,
            record_artifact=record_artifact,
            schema_from_dataframe=_schema_from_dataframe,
            resolve_declared_output_kind=_resolve_declared_output_kind,
            validate_output_kind_metadata=_validate_output_kind_metadata,
            resolve_pk_semantics=resolve_pk_semantics,
            resolve_delete_column=resolve_delete_column,
            resolve_pk_columns=resolve_pk_columns,
            validate_pk_semantics=validate_pk_semantics,
            build_expectations_with_pk=build_expectations_with_pk,
            validate_expectations=validate_expectations,
            hash_schema_columns=_hash_schema_columns,
            utcnow_fn=utcnow,
        )

    def _build_output_validation_payload(
        self,
        *,
        stage: str,
        errors: List[str],
        job: PipelineJob,
        node_id: str,
        execution_semantics: str,
        output_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        return _output_preparation.build_output_validation_payload(
            self,
            stage=stage,
            errors=errors,
            job=job,
            node_id=node_id,
            execution_semantics=execution_semantics,
            output_name=output_name,
        )

    async def _prepare_output_work_item(
        self,
        *,
        mode_label: str,
        job: PipelineJob,
        definition: Dict[str, Any],
        declared_outputs: List[Dict[str, Any]],
        output_nodes: Dict[str, Dict[str, Any]],
        node_id: str,
        output_df: DataFrame,
        pipeline_ref: str,
        output_write_mode: str,
        execution_semantics: str,
        has_incremental_input: bool,
        incremental_inputs_have_additive_updates: Optional[bool],
        preview_limit: int,
        temp_dirs: List[str],
        persist_output: bool,
        persisted_dfs: List[DataFrame],
    ) -> Dict[str, Any]:
        return await _output_preparation.prepare_output_work_item(
            self,
            mode_label=mode_label,
            job=job,
            definition=definition,
            declared_outputs=declared_outputs,
            output_nodes=output_nodes,
            node_id=node_id,
            output_df=output_df,
            pipeline_ref=pipeline_ref,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            preview_limit=preview_limit,
            temp_dirs=temp_dirs,
            persist_output=persist_output,
            persisted_dfs=persisted_dfs,
        )

    def _log_output_validation_failure(
        self,
        *,
        mode_label: str,
        failure: _output_preparation._OutputNodeValidationError,
    ) -> None:
        _output_preparation.log_output_validation_failure(
            mode_label=mode_label,
            failure=failure,
        )

    async def _record_build_output_failure(
        self,
        *,
        job: PipelineJob,
        failure: _output_preparation._OutputNodeValidationError,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await _output_preparation.record_build_output_failure(
            self,
            job=job,
            failure=failure,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            record_run=record_run,
            record_artifact=record_artifact,
            emit_job_event=emit_job_event,
        )

    async def _record_deploy_output_failure(
        self,
        *,
        job: PipelineJob,
        failure: _output_preparation._OutputNodeValidationError,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await _output_preparation.record_deploy_output_failure(
            self,
            job=job,
            failure=failure,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            record_build=record_build,
            record_run=record_run,
            record_artifact=record_artifact,
            emit_job_event=emit_job_event,
        )

    async def _prepare_build_output_work(
        self,
        *,
        job: PipelineJob,
        lock: Optional[PipelineLock],
        tables: Dict[str, DataFrame],
        target_node_ids: List[str],
        output_nodes: Dict[str, Dict[str, Any]],
        definition: Dict[str, Any],
        declared_outputs: List[Dict[str, Any]],
        pipeline_ref: str,
        output_write_mode: str,
        execution_semantics: str,
        has_incremental_input: bool,
        incremental_inputs_have_additive_updates: Optional[bool],
        preview_limit: int,
        temp_dirs: List[str],
        persisted_dfs: List[DataFrame],
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> Optional[Tuple[List[Dict[str, Any]], bool]]:
        return await _output_preparation.prepare_output_work(
            self,
            mode_label="build",
            job=job,
            lock=lock,
            tables=tables,
            target_node_ids=target_node_ids,
            output_nodes=output_nodes,
            definition=definition,
            declared_outputs=declared_outputs,
            pipeline_ref=pipeline_ref,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            preview_limit=preview_limit,
            temp_dirs=temp_dirs,
            persisted_dfs=persisted_dfs,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            record_run=record_run,
            record_artifact=record_artifact,
            emit_job_event=emit_job_event,
            persist_output=True,
        )

    async def _create_build_branch(
        self,
        *,
        artifact_repo: str,
        pipeline_ref: str,
        run_ref: str,
        base_branch: str,
    ) -> str:
        return await _output_publication.create_build_branch(
            self,
            artifact_repo=artifact_repo,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
            base_branch=base_branch,
        )

    async def _materialize_build_outputs(
        self,
        *,
        job: PipelineJob,
        artifact_repo: str,
        build_branch: str,
        base_branch: str,
        output_work: List[Dict[str, Any]],
        output_write_mode: str,
        execution_semantics: str,
        incremental_inputs_have_additive_updates: Optional[bool],
    ) -> List[Dict[str, Any]]:
        return await _output_publication.materialize_build_outputs(
            self,
            job=job,
            artifact_repo=artifact_repo,
            build_branch=build_branch,
            base_branch=base_branch,
            output_work=output_work,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )

    async def _run_build_mode(
        self,
        *,
        job: PipelineJob,
        lock: Optional[PipelineLock],
        tables: Dict[str, DataFrame],
        target_node_ids: List[str],
        output_nodes: Dict[str, Dict[str, Any]],
        definition: Dict[str, Any],
        declared_outputs: List[Dict[str, Any]],
        pipeline_ref: str,
        run_ref: str,
        execution_semantics: str,
        diff_empty_inputs: bool,
        has_incremental_input: bool,
        incremental_inputs_have_additive_updates: Optional[bool],
        preview_limit: int,
        temp_dirs: List[str],
        persisted_dfs: List[DataFrame],
        input_snapshots: List[Dict[str, Any]],
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
        code_version: Optional[str],
        spark_conf: Dict[str, Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await _execution_modes.run_build_mode(
            self,
            job=job,
            lock=lock,
            tables=tables,
            target_node_ids=target_node_ids,
            output_nodes=output_nodes,
            definition=definition,
            declared_outputs=declared_outputs,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
            execution_semantics=execution_semantics,
            diff_empty_inputs=diff_empty_inputs,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            preview_limit=preview_limit,
            temp_dirs=temp_dirs,
            persisted_dfs=persisted_dfs,
            input_snapshots=input_snapshots,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            pipeline_spec_hash=pipeline_spec_hash,
            pipeline_spec_commit_id=pipeline_spec_commit_id,
            code_version=code_version,
            spark_conf=spark_conf,
            record_build=record_build,
            record_run=record_run,
            record_artifact=record_artifact,
            emit_job_event=emit_job_event,
            resolve_lakefs_repository=_resolve_lakefs_repository,
            safe_lakefs_ref=safe_lakefs_ref,
            build_s3_uri=build_s3_uri,
            utcnow_fn=utcnow,
        )

    async def _prepare_deploy_output_work(
        self,
        *,
        job: PipelineJob,
        lock: Optional[PipelineLock],
        tables: Dict[str, DataFrame],
        target_node_ids: List[str],
        output_nodes: Dict[str, Dict[str, Any]],
        definition: Dict[str, Any],
        declared_outputs: List[Dict[str, Any]],
        pipeline_ref: str,
        output_write_mode: str,
        execution_semantics: str,
        has_incremental_input: bool,
        incremental_inputs_have_additive_updates: Optional[bool],
        preview_limit: int,
        temp_dirs: List[str],
        persisted_dfs: List[DataFrame],
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> Optional[Tuple[List[Dict[str, Any]], bool]]:
        return await _output_preparation.prepare_output_work(
            self,
            mode_label="deploy",
            job=job,
            lock=lock,
            tables=tables,
            target_node_ids=target_node_ids,
            output_nodes=output_nodes,
            definition=definition,
            declared_outputs=declared_outputs,
            pipeline_ref=pipeline_ref,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            preview_limit=preview_limit,
            temp_dirs=temp_dirs,
            persisted_dfs=persisted_dfs,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            record_build=record_build,
            record_run=record_run,
            record_artifact=record_artifact,
            emit_job_event=emit_job_event,
            persist_output=False,
        )

    async def _create_deploy_run_branch(
        self,
        *,
        artifact_repo: str,
        base_branch: str,
        pipeline_ref: str,
        run_ref: str,
    ) -> str:
        return await _output_publication.create_deploy_run_branch(
            self,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
        )

    async def _materialize_deploy_staged_outputs(
        self,
        *,
        job: PipelineJob,
        run_branch: str,
        artifact_repo: str,
        base_branch: str,
        output_work: List[Dict[str, Any]],
        output_write_mode: str,
        execution_semantics: str,
        incremental_inputs_have_additive_updates: Optional[bool],
    ) -> List[Dict[str, Any]]:
        return await _output_publication.materialize_deploy_staged_outputs(
            self,
            job=job,
            run_branch=run_branch,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            output_work=output_work,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )

    async def _commit_and_merge_deploy_branch(
        self,
        *,
        job: PipelineJob,
        lock: Optional[PipelineLock],
        pipeline_ref: str,
        artifact_repo: str,
        run_branch: str,
        base_branch: str,
    ) -> Tuple[str, str]:
        return await _output_publication.commit_and_merge_deploy_branch(
            self,
            job=job,
            lock=lock,
            pipeline_ref=pipeline_ref,
            artifact_repo=artifact_repo,
            run_branch=run_branch,
            base_branch=base_branch,
        )

    async def _stage_deploy_outputs(
        self,
        *,
        job: PipelineJob,
        lock: Optional[PipelineLock],
        pipeline_ref: str,
        run_ref: str,
        artifact_repo: str,
        base_branch: str,
        output_work: List[Dict[str, Any]],
        output_write_mode: str,
        execution_semantics: str,
        incremental_inputs_have_additive_updates: Optional[bool],
    ) -> Tuple[str, str, str, List[Dict[str, Any]]]:
        return await _output_publication.stage_deploy_outputs(
            self,
            job=job,
            lock=lock,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            output_work=output_work,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )

    def _build_deploy_version_sample_payload(
        self,
        *,
        item: Dict[str, Any],
        schema_columns: List[Dict[str, Any]],
        output_sample: List[Dict[str, Any]],
        total_row_count: int,
        delta_row_count: int,
        resolved_runtime_write_mode: str,
    ) -> Dict[str, Any]:
        return _output_publication.build_deploy_version_sample_payload(
            item=item,
            schema_columns=schema_columns,
            output_sample=output_sample,
            total_row_count=total_row_count,
            delta_row_count=delta_row_count,
            resolved_runtime_write_mode=resolved_runtime_write_mode,
        )

    async def _resolve_deploy_output_publication(
        self,
        *,
        job: PipelineJob,
        pipeline_ref: str,
        artifact_repo: str,
        base_branch: str,
        merge_commit_id: str,
        output_write_mode: str,
        item: Dict[str, Any],
    ) -> Dict[str, Any]:
        return await _output_publication.resolve_deploy_output_publication(
            self,
            job=job,
            pipeline_ref=pipeline_ref,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            merge_commit_id=merge_commit_id,
            output_write_mode=output_write_mode,
            item=item,
        )

    def _build_deploy_output_record(
        self,
        *,
        item: Dict[str, Any],
        publication: Dict[str, Any],
        merge_commit_id: str,
        base_branch: str,
    ) -> Dict[str, Any]:
        return _output_publication.build_deploy_output_record(
            item=item,
            publication=publication,
            merge_commit_id=merge_commit_id,
            base_branch=base_branch,
        )

    async def _record_deploy_output_lineage(
        self,
        *,
        job: PipelineJob,
        pipeline_ref: str,
        artifact_key: str,
        dataset_name: str,
        node_id: Optional[str],
        merge_commit_id: str,
        base_branch: str,
    ) -> None:
        await _output_publication.record_deploy_output_lineage(
            self,
            job=job,
            pipeline_ref=pipeline_ref,
            artifact_key=artifact_key,
            dataset_name=dataset_name,
            node_id=node_id,
            merge_commit_id=merge_commit_id,
            base_branch=base_branch,
        )

    async def _publish_deploy_outputs(
        self,
        *,
        job: PipelineJob,
        pipeline_ref: str,
        artifact_repo: str,
        base_branch: str,
        merge_commit_id: str,
        output_write_mode: str,
        staged_outputs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        return await _output_publication.publish_deploy_outputs(
            self,
            job=job,
            pipeline_ref=pipeline_ref,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            merge_commit_id=merge_commit_id,
            output_write_mode=output_write_mode,
            staged_outputs=staged_outputs,
        )

    def _merge_watermark_keys(
        self,
        *,
        next_watermark: Optional[Any],
        previous_watermark: Optional[Any],
        previous_watermark_keys: List[str],
        next_watermark_keys: List[str],
    ) -> List[str]:
        return _output_publication.merge_watermark_keys(
            next_watermark=next_watermark,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
            next_watermark_keys=next_watermark_keys,
        )

    async def _persist_deploy_watermarks(
        self,
        *,
        resolved_pipeline_id: Optional[str],
        branch: str,
        execution_semantics: str,
        input_snapshots: List[Dict[str, Any]],
        watermark_column: Optional[str],
        previous_watermark: Optional[Any],
        previous_watermark_keys: List[str],
    ) -> Optional[Dict[str, Any]]:
        return await _output_publication.persist_deploy_watermarks(
            self,
            resolved_pipeline_id=resolved_pipeline_id,
            branch=branch,
            execution_semantics=execution_semantics,
            input_snapshots=input_snapshots,
            watermark_column=watermark_column,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
        )

    async def _run_deploy_mode(
        self,
        *,
        job: PipelineJob,
        lock: Optional[PipelineLock],
        tables: Dict[str, DataFrame],
        target_node_ids: List[str],
        output_nodes: Dict[str, Dict[str, Any]],
        definition: Dict[str, Any],
        declared_outputs: List[Dict[str, Any]],
        pipeline_ref: str,
        run_ref: str,
        execution_semantics: str,
        diff_empty_inputs: bool,
        has_incremental_input: bool,
        incremental_inputs_have_additive_updates: Optional[bool],
        preview_limit: int,
        temp_dirs: List[str],
        persisted_dfs: List[DataFrame],
        input_snapshots: List[Dict[str, Any]],
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
        code_version: Optional[str],
        spark_conf: Dict[str, Any],
        resolved_pipeline_id: Optional[str],
        previous_watermark: Optional[Any],
        previous_watermark_keys: List[str],
        watermark_column: Optional[str],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await _execution_modes.run_deploy_mode(
            self,
            job=job,
            lock=lock,
            tables=tables,
            target_node_ids=target_node_ids,
            output_nodes=output_nodes,
            definition=definition,
            declared_outputs=declared_outputs,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
            execution_semantics=execution_semantics,
            diff_empty_inputs=diff_empty_inputs,
            has_incremental_input=has_incremental_input,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            preview_limit=preview_limit,
            temp_dirs=temp_dirs,
            persisted_dfs=persisted_dfs,
            input_snapshots=input_snapshots,
            input_commit_payload=input_commit_payload,
            inputs_payload=inputs_payload,
            pipeline_spec_hash=pipeline_spec_hash,
            pipeline_spec_commit_id=pipeline_spec_commit_id,
            code_version=code_version,
            spark_conf=spark_conf,
            resolved_pipeline_id=resolved_pipeline_id,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
            watermark_column=watermark_column,
            record_build=record_build,
            record_run=record_run,
            record_artifact=record_artifact,
            emit_job_event=emit_job_event,
            resolve_lakefs_repository=_resolve_lakefs_repository,
            safe_lakefs_ref=safe_lakefs_ref,
            utcnow_fn=utcnow,
        )

    async def _resolve_execution_node_dataframe(
        self,
        *,
        job: PipelineJob,
        node_id: str,
        node: Dict[str, Any],
        tables: Dict[str, DataFrame],
        incoming: Dict[str, List[str]],
        parameters: Dict[str, Any],
        preview_meta: Dict[str, Any],
        preview_sampling_seed: int,
        input_snapshots: List[Dict[str, Any]],
        input_sampling: Dict[str, Any],
        temp_dirs: List[str],
        previous_input_commits: Dict[str, str],
        use_lakefs_diff: bool,
        execution_semantics: str,
        watermark_column: Optional[str],
        previous_watermark: Optional[Any],
        previous_watermark_keys: List[str],
        is_preview: bool,
        preview_input_sample_limit: int,
    ) -> DataFrame:
        return await _execution_node_resolution.resolve_execution_node_dataframe(
            self,
            job=job,
            node_id=node_id,
            node=node,
            tables=tables,
            incoming=incoming,
            parameters=parameters,
            preview_meta=preview_meta,
            preview_sampling_seed=preview_sampling_seed,
            input_snapshots=input_snapshots,
            input_sampling=input_sampling,
            temp_dirs=temp_dirs,
            previous_input_commits=previous_input_commits,
            use_lakefs_diff=use_lakefs_diff,
            execution_semantics=execution_semantics,
            watermark_column=watermark_column,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
            is_preview=is_preview,
            preview_input_sample_limit=preview_input_sample_limit,
            normalize_operation=normalize_operation,
            resolve_udf_reference=resolve_udf_reference,
        )

    async def _record_schema_check_failure(
        self,
        *,
        job: PipelineJob,
        node_id: str,
        schema_errors: List[str],
        execution_semantics: str,
        is_preview: bool,
        is_build: bool,
        run_mode: str,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        record_preview: Callable[..., Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        schema_payload = self._build_error_payload(
            message="Pipeline schema checks failed",
            errors=schema_errors,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=422,
            external_code="PIPELINE_SCHEMA_CHECK_FAILED",
            stage="schema_checks",
            job=job,
            node_id=node_id,
            context={"execution_semantics": execution_semantics},
        )
        if is_preview:
            await record_preview(
                status="FAILED",
                row_count=0,
                sample_json=schema_payload,
                job_id=job.job_id,
                node_id=job.node_id,
            )
        elif not is_build:
            await record_build(
                status="FAILED",
                output_json=schema_payload,
            )
        await record_run(
            job_id=job.job_id,
            mode=run_mode,
            status="FAILED",
            node_id=job.node_id,
            sample_json=schema_payload if is_preview else None,
            output_json=schema_payload if not is_preview else None,
            finished_at=utcnow(),
        )
        await record_artifact(status="FAILED", errors=schema_errors)
        await emit_job_event(status="FAILED", errors=schema_errors)
        logger.error("Pipeline schema checks failed: %s", schema_errors)

    async def _record_validation_failure(
        self,
        *,
        job: PipelineJob,
        validation_errors: List[str],
        execution_semantics: str,
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
        is_preview: bool,
        is_build: bool,
        run_mode: str,
        record_preview: Callable[..., Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        validation_payload = self._build_error_payload(
            message="Pipeline validation failed",
            errors=validation_errors,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=422,
            external_code="PIPELINE_DEFINITION_INVALID",
            stage="validate",
            job=job,
            context={
                "execution_semantics": execution_semantics,
                "pipeline_spec_hash": pipeline_spec_hash,
                "pipeline_spec_commit_id": pipeline_spec_commit_id,
            },
        )
        if is_preview:
            await record_preview(
                status="FAILED",
                row_count=0,
                sample_json=validation_payload,
                job_id=job.job_id,
                node_id=job.node_id,
            )
        elif not is_build:
            await record_build(
                status="FAILED",
                output_json=validation_payload,
            )
        await record_run(
            job_id=job.job_id,
            mode=run_mode,
            status="FAILED",
            node_id=job.node_id,
            sample_json=validation_payload if is_preview else None,
            output_json=validation_payload if not is_preview else None,
            finished_at=utcnow(),
        )
        await record_artifact(status="FAILED", errors=validation_errors)
        await emit_job_event(status="FAILED", errors=validation_errors)
        logger.error("Pipeline validation failed: %s", validation_errors)

    def _derive_post_node_execution_state(
        self,
        *,
        input_snapshots: List[Dict[str, Any]],
        execution_semantics: str,
        preview_limit: Optional[int],
    ) -> Tuple[
        Optional[List[Dict[str, Any]]],
        Dict[str, Any],
        bool,
        bool,
        Optional[bool],
        int,
    ]:
        input_commit_payload = self._build_input_commit_payload(input_snapshots)
        inputs_payload = {
            "snapshots": input_snapshots,
            "lakefs_commits": input_commit_payload,
        }
        diff_empty_inputs = _inputs_diff_empty(input_snapshots)
        has_incremental_input = execution_semantics in {"incremental", "streaming"}
        incremental_inputs_have_additive_updates: Optional[bool] = None
        if has_incremental_input:
            has_diff_signal = any(
                isinstance(snapshot, dict) and bool(snapshot.get("diff_requested"))
                for snapshot in input_snapshots
            )
            if has_diff_signal:
                incremental_inputs_have_additive_updates = not diff_empty_inputs
        try:
            resolved_preview_limit = int(preview_limit or 500)
        except (TypeError, ValueError):
            resolved_preview_limit = 500
        resolved_preview_limit = max(1, min(500, resolved_preview_limit))
        return (
            input_commit_payload,
            inputs_payload,
            diff_empty_inputs,
            has_incremental_input,
            incremental_inputs_have_additive_updates,
            resolved_preview_limit,
        )

    def _validate_execution_prerequisites(self) -> None:
        self._validation_domain.validate_execution_prerequisites()

    def _build_execution_graph(
        self,
        *,
        definition: Dict[str, Any],
    ) -> Tuple[
        Dict[str, Any],
        Dict[str, Dict[str, Any]],
        Dict[str, List[str]],
        Dict[str, Any],
        Dict[str, Dict[str, Any]],
        List[str],
    ]:
        preview_meta = definition.get("__preview_meta__") or {}
        nodes = normalize_nodes(definition.get("nodes"))
        for node in nodes.values():
            node["metadata"] = normalize_transform_metadata(node.get("metadata"))
        edges = normalize_edges(definition.get("edges"))
        incoming = build_incoming(edges)
        parameters = normalize_parameters(definition.get("parameters"))
        output_nodes = {
            node_id: node for node_id, node in nodes.items() if str(node.get("type") or "") == "output"
        }
        order = topological_sort(nodes, edges, include_unordered=False)
        return preview_meta, nodes, incoming, parameters, output_nodes, order

    async def _resolve_execution_pipeline_context(
        self,
        *,
        job: PipelineJob,
        definition: Dict[str, Any],
        execution_semantics: str,
    ) -> Optional[
        Tuple[
            Optional[str],
            str,
            Optional[str],
            Optional[Any],
            List[str],
            Dict[str, str],
        ]
    ]:
        resolved_pipeline_id = await self._resolve_pipeline_id(job)
        if resolved_pipeline_id:
            try:
                pipeline_record = await self.pipeline_registry.get_pipeline(pipeline_id=resolved_pipeline_id)
            except Exception as exc:
                logger.warning(
                    "Failed to resolve pipeline record (pipeline_id=%s job_id=%s): %s",
                    resolved_pipeline_id,
                    job.job_id,
                    exc,
                )
                pipeline_record = None
            if not pipeline_record:
                logger.warning(
                    "Dropping stale pipeline job for missing pipeline (pipeline_id=%s job_id=%s mode=%s)",
                    resolved_pipeline_id,
                    job.job_id,
                    getattr(job, "mode", None),
                )
                return None

        pipeline_ref = resolved_pipeline_id or str(job.pipeline_id)
        watermark_column = _resolve_watermark_column(
            incremental=resolve_incremental_config(definition),
            metadata=definition.get("settings") if isinstance(definition.get("settings"), dict) else {},
        )
        (
            previous_watermark,
            previous_watermark_keys,
            previous_input_commits,
        ) = await self._load_previous_watermark_state(
            job=job,
            resolved_pipeline_id=resolved_pipeline_id,
            execution_semantics=execution_semantics,
            watermark_column=watermark_column,
        )
        return (
            resolved_pipeline_id,
            pipeline_ref,
            watermark_column,
            previous_watermark,
            previous_watermark_keys,
            previous_input_commits,
        )

    def _resolve_run_mode(self, *, job: PipelineJob) -> Tuple[str, bool, bool]:
        job_mode = str(job.mode or "deploy").strip().lower()
        is_preview = job_mode == "preview"
        is_build = job_mode == "build"
        run_mode = "preview" if is_preview else "build" if is_build else "deploy"
        return run_mode, is_preview, is_build

    def _resolve_execution_semantics(self, *, job: PipelineJob, definition: Dict[str, Any]) -> str:
        return _resolve_execution_semantics(job=job, definition=definition)

    def _resolve_code_version(self) -> Optional[str]:
        return _resolve_code_version()

    def _build_run_record_callbacks(
        self,
        *,
        resolved_pipeline_id: Optional[str],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
        spark_conf: Dict[str, Any],
        code_version: Optional[str],
    ) -> Tuple[
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
    ]:
        async def record_preview(**kwargs: Any) -> None:
            if not resolved_pipeline_id or not self.pipeline_registry:
                return
            await self.pipeline_registry.record_preview(pipeline_id=resolved_pipeline_id, **kwargs)

        async def record_build(**kwargs: Any) -> None:
            if not resolved_pipeline_id or not self.pipeline_registry:
                return
            await self.pipeline_registry.record_build(pipeline_id=resolved_pipeline_id, **kwargs)

        async def record_run(**kwargs: Any) -> None:
            if not resolved_pipeline_id or not self.pipeline_registry:
                return
            await self.pipeline_registry.record_run(
                pipeline_id=resolved_pipeline_id,
                pipeline_spec_commit_id=pipeline_spec_commit_id,
                pipeline_spec_hash=pipeline_spec_hash,
                spark_conf=spark_conf,
                code_version=code_version,
                **kwargs,
            )

        return record_preview, record_build, record_run

    def _build_artifact_recorder_callback(
        self,
        *,
        job: PipelineJob,
        resolved_pipeline_id: Optional[str],
        run_mode: str,
        declared_outputs: List[Dict[str, Any]],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
    ) -> Tuple[
        Callable[..., Any],
        Dict[str, Optional[str]],
    ]:
        artifact_state: Dict[str, Optional[str]] = {"artifact_id": None, "run_id": None}

        async def record_artifact(
            *,
            status: str,
            outputs: Optional[List[Dict[str, Any]]] = None,
            inputs: Optional[Dict[str, Any]] = None,
            lakefs: Optional[Dict[str, Any]] = None,
            sampling_strategy: Optional[Dict[str, Any]] = None,
            errors: Optional[List[str]] = None,
        ) -> Optional[str]:
            if not resolved_pipeline_id or not self.pipeline_registry:
                return None
            if run_mode not in {"preview", "build"}:
                return None
            error_payload: Dict[str, Any] = {}
            if errors:
                error_payload["errors"] = errors
            lakefs_payload = lakefs or {}
            artifact_state["artifact_id"] = await self.pipeline_registry.upsert_artifact(
                pipeline_id=resolved_pipeline_id,
                job_id=job.job_id,
                run_id=artifact_state.get("run_id"),
                mode=run_mode,
                status=status,
                artifact_id=artifact_state.get("artifact_id"),
                definition_hash=job.definition_hash,
                definition_commit_id=job.definition_commit_id,
                pipeline_spec_hash=pipeline_spec_hash,
                pipeline_spec_commit_id=pipeline_spec_commit_id,
                inputs=inputs,
                lakefs_repository=lakefs_payload.get("repository"),
                lakefs_branch=lakefs_payload.get("branch"),
                lakefs_commit_id=lakefs_payload.get("commit_id"),
                outputs=outputs,
                declared_outputs=declared_outputs,
                sampling_strategy=sampling_strategy,
                error=error_payload,
            )
            return artifact_state.get("artifact_id")

        return record_artifact, artifact_state

    def _build_job_event_callback(
        self,
        *,
        job: PipelineJob,
        pipeline_ref: str,
        run_mode: str,
        execution_semantics: str,
    ) -> Callable[..., Any]:
        async def emit_job_event(
            *,
            status: str,
            errors: Optional[List[str]] = None,
            lakefs: Optional[Dict[str, Any]] = None,
            output: Optional[Dict[str, Any]] = None,
        ) -> None:
            if run_mode not in {"build", "deploy"}:
                return
            event_type = "PIPELINE_JOB_SUCCEEDED" if status in {"SUCCESS", "DEPLOYED"} else "PIPELINE_JOB_FAILED"
            payload: Dict[str, Any] = {
                "pipeline_id": pipeline_ref,
                "job_id": job.job_id,
                "mode": run_mode,
                "status": status,
                "db_name": job.db_name,
                "branch": job.branch or "main",
                "node_id": job.node_id,
                "definition_hash": job.definition_hash,
                "execution_semantics": execution_semantics,
            }
            if errors:
                payload["errors"] = errors
            if lakefs:
                payload["lakefs"] = lakefs
            if output:
                payload["output"] = output
            await emit_pipeline_control_plane_event(
                event_type=event_type,
                pipeline_id=pipeline_ref,
                event_id=job.job_id,
                data=payload,
            )

        return emit_job_event

    def _build_artifact_event_callbacks(
        self,
        *,
        job: PipelineJob,
        resolved_pipeline_id: Optional[str],
        pipeline_ref: str,
        run_mode: str,
        execution_semantics: str,
        declared_outputs: List[Dict[str, Any]],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
    ) -> Tuple[
        Callable[..., Any],
        Callable[..., Any],
        Dict[str, Optional[str]],
    ]:
        record_artifact, artifact_state = self._build_artifact_recorder_callback(
            job=job,
            resolved_pipeline_id=resolved_pipeline_id,
            run_mode=run_mode,
            declared_outputs=declared_outputs,
            pipeline_spec_hash=pipeline_spec_hash,
            pipeline_spec_commit_id=pipeline_spec_commit_id,
        )
        emit_job_event = self._build_job_event_callback(
            job=job,
            pipeline_ref=pipeline_ref,
            run_mode=run_mode,
            execution_semantics=execution_semantics,
        )
        return record_artifact, emit_job_event, artifact_state

    def _build_execution_recorders(
        self,
        *,
        job: PipelineJob,
        resolved_pipeline_id: Optional[str],
        pipeline_ref: str,
        run_mode: str,
        execution_semantics: str,
        declared_outputs: List[Dict[str, Any]],
        pipeline_spec_hash: Optional[str],
        pipeline_spec_commit_id: Optional[str],
        spark_conf: Dict[str, Any],
        code_version: Optional[str],
    ) -> Tuple[
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
        Dict[str, Optional[str]],
    ]:
        record_preview, record_build, record_run = self._build_run_record_callbacks(
            resolved_pipeline_id=resolved_pipeline_id,
            pipeline_spec_hash=pipeline_spec_hash,
            pipeline_spec_commit_id=pipeline_spec_commit_id,
            spark_conf=spark_conf,
            code_version=code_version,
        )
        record_artifact, emit_job_event, artifact_state = self._build_artifact_event_callbacks(
            job=job,
            resolved_pipeline_id=resolved_pipeline_id,
            pipeline_ref=pipeline_ref,
            run_mode=run_mode,
            execution_semantics=execution_semantics,
            declared_outputs=declared_outputs,
            pipeline_spec_hash=pipeline_spec_hash,
            pipeline_spec_commit_id=pipeline_spec_commit_id,
        )
        return record_preview, record_build, record_run, record_artifact, emit_job_event, artifact_state

    async def _start_execution_tracking(
        self,
        *,
        job: PipelineJob,
        resolved_pipeline_id: Optional[str],
        run_mode: str,
        is_preview: bool,
        is_build: bool,
        artifact_state: Dict[str, Optional[str]],
        record_preview: Callable[..., Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
    ) -> str:
        if is_preview:
            await record_preview(
                status="RUNNING",
                row_count=0,
                sample_json={"job_id": job.job_id},
                job_id=job.job_id,
                node_id=job.node_id,
            )
        elif not is_build:
            await record_build(
                status="RUNNING",
                output_json={"job_id": job.job_id},
            )

        await record_run(
            job_id=job.job_id,
            mode=run_mode,
            status="RUNNING",
            node_id=job.node_id,
        )

        if resolved_pipeline_id and self.pipeline_registry:
            try:
                run_record = await self.pipeline_registry.get_run(
                    pipeline_id=resolved_pipeline_id,
                    job_id=job.job_id,
                )
                if run_record:
                    artifact_state["run_id"] = run_record.get("run_id")
            except Exception as exc:
                logger.debug("Failed to resolve run_id for job %s: %s", job.job_id, exc)

        await record_artifact(status="RUNNING")
        return str(artifact_state.get("run_id") or job.job_id)

    async def _execute_ordered_nodes(
        self,
        *,
        job: PipelineJob,
        order_run: List[str],
        nodes: Dict[str, Dict[str, Any]],
        tables: Dict[str, DataFrame],
        incoming: Dict[str, List[str]],
        parameters: Dict[str, Any],
        preview_meta: Dict[str, Any],
        preview_sampling_seed: int,
        input_snapshots: List[Dict[str, Any]],
        input_sampling: Dict[str, Any],
        temp_dirs: List[str],
        previous_input_commits: Dict[str, str],
        use_lakefs_diff: bool,
        execution_semantics: str,
        watermark_column: Optional[str],
        previous_watermark: Optional[Any],
        previous_watermark_keys: List[str],
        is_preview: bool, is_build: bool,
        run_mode: str, input_commit_payload: Optional[List[Dict[str, Any]]],
        record_preview: Callable[..., Any], record_build: Callable[..., Any],
        record_run: Callable[..., Any], record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> bool:
        preview_input_sample_limit = self._resolve_preview_limit(
            preview_limit=job.preview_limit,
            preview_meta=preview_meta,
            default=500,
        )
        skip_production_checks = is_preview and self._resolve_preview_flag(
            preview_meta,
            snake_case_key="skip_production_checks",
            camel_case_key="skipProductionChecks",
            default=True,
        )
        for node_id in order_run:
            node = nodes[node_id]
            metadata = node.get("metadata") or {}
            df = await self._resolve_execution_node_dataframe(
                job=job,
                node_id=node_id,
                node=node,
                tables=tables,
                incoming=incoming,
                parameters=parameters,
                preview_meta=preview_meta,
                preview_sampling_seed=preview_sampling_seed,
                input_snapshots=input_snapshots,
                input_sampling=input_sampling,
                temp_dirs=temp_dirs,
                previous_input_commits=previous_input_commits,
                use_lakefs_diff=use_lakefs_diff,
                execution_semantics=execution_semantics,
                watermark_column=watermark_column,
                previous_watermark=previous_watermark,
                previous_watermark_keys=previous_watermark_keys,
                is_preview=is_preview,
                preview_input_sample_limit=preview_input_sample_limit,
            )
            if not skip_production_checks:
                table_ops = self._build_table_ops(df)
                schema_errors = await self._run_spark(
                    lambda: validate_schema_checks(
                        table_ops,
                        metadata.get("schemaChecks") or [],
                        error_prefix=f"schema check failed ({node_id}): ",
                    ),
                    label=f"schema_checks:{node_id}",
                )
                if schema_errors:
                    await self._record_schema_check_failure(
                        job=job,
                        node_id=node_id,
                        schema_errors=schema_errors,
                        execution_semantics=execution_semantics,
                        is_preview=is_preview,
                        is_build=is_build,
                        run_mode=run_mode,
                        input_commit_payload=input_commit_payload,
                        record_preview=record_preview,
                        record_build=record_build,
                        record_run=record_run,
                        record_artifact=record_artifact,
                        emit_job_event=emit_job_event,
                    )
                    return False

            tables[node_id] = df
        return True

    async def _record_execution_failure(
        self,
        *,
        job: PipelineJob,
        exc: Exception,
        is_preview: bool,
        is_build: bool,
        run_mode: str,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        record_preview: Callable[..., Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await self._run_domain.record_execution_failure(
            job=job,
            exc=exc,
            is_preview=is_preview,
            is_build=is_build,
            run_mode=run_mode,
            input_commit_payload=input_commit_payload,
            record_preview=record_preview,
            record_build=record_build,
            record_run=record_run,
            emit_job_event=emit_job_event,
        )

    async def _cleanup_execution_resources(
        self,
        *,
        lock: Optional[PipelineLock],
        temp_dirs: List[str],
        persisted_dfs: List[DataFrame],
        prev_spark_conf: Dict[str, Optional[str]],
        prev_cast_mode: str,
    ) -> None:
        await self._run_domain.cleanup_execution_resources(
            lock=lock,
            temp_dirs=temp_dirs,
            persisted_dfs=persisted_dfs,
            prev_spark_conf=prev_spark_conf,
            prev_cast_mode=prev_cast_mode,
        )

    async def _execute_job(self, job: PipelineJob) -> None:
        await self._run_domain.execute_job(job)

    async def _maybe_enqueue_objectify_job(self, *, dataset, version) -> Optional[str]:
        return await _downstream_enqueuing.maybe_enqueue_objectify_job(
            self,
            dataset=dataset,
            version=version,
        )

    async def _maybe_enqueue_relationship_jobs(self, *, dataset, version) -> List[str]:
        return await _downstream_enqueuing.maybe_enqueue_relationship_jobs(
            self,
            dataset=dataset,
            version=version,
        )

    async def _materialize_output_by_kind(
        self,
        *,
        output_kind: str,
        output_metadata: Dict[str, Any],
        df: DataFrame,
        artifact_bucket: str,
        prefix: str,
        db_name: Optional[str] = None,
        branch: Optional[str] = None,
        dataset_name: Optional[str] = None,
        execution_semantics: str = "snapshot",
        incremental_inputs_have_additive_updates: Optional[bool] = None,
        write_mode: str = "overwrite",
        file_prefix: Optional[str] = None,
        file_format: str = "parquet",
        partition_cols: Optional[List[str]] = None,
        base_row_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self._output_domain.materialize_output_by_kind(
            output_kind=output_kind,
            output_metadata=output_metadata,
            df=df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            db_name=db_name,
            branch=branch,
            dataset_name=dataset_name,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
            base_row_count=base_row_count,
        )

    async def _materialize_dataset_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: DataFrame,
        artifact_bucket: str,
        prefix: str,
        db_name: Optional[str],
        branch: Optional[str],
        dataset_name: Optional[str],
        execution_semantics: str,
        incremental_inputs_have_additive_updates: Optional[bool],
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
        base_row_count: Optional[int],
    ) -> Dict[str, Any]:
        return await self._output_domain.materialize_dataset_output(
            output_metadata=output_metadata,
            df=df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            db_name=db_name,
            branch=branch,
            dataset_name=dataset_name,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
            base_row_count=base_row_count,
        )

    async def _materialize_geotemporal_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: DataFrame,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
    ) -> str:
        return await self._output_domain.materialize_geotemporal_output(
            output_metadata=output_metadata,
            df=df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    async def _materialize_media_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: DataFrame,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
    ) -> str:
        return await self._output_domain.materialize_media_output(
            output_metadata=output_metadata,
            df=df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    async def _materialize_virtual_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: DataFrame,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
        row_count_hint: Optional[int],
    ) -> str:
        return await self._output_domain.materialize_virtual_output(
            output_metadata=output_metadata,
            df=df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
            row_count_hint=row_count_hint,
        )

    async def _materialize_ontology_output(
        self,
        *,
        output_metadata: Dict[str, Any],
        df: DataFrame,
        artifact_bucket: str,
        prefix: str,
        write_mode: str,
        file_prefix: Optional[str],
        file_format: str,
        partition_cols: Optional[List[str]],
    ) -> str:
        return await self._output_domain.materialize_ontology_output(
            output_metadata=output_metadata,
            df=df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    def _ensure_output_columns_present(
        self,
        *,
        df: DataFrame,
        required_columns: List[str],
        output_kind: str,
    ) -> None:
        self._output_domain.ensure_output_columns_present(
            df=df,
            required_columns=required_columns,
            output_kind=output_kind,
        )

    async def _load_existing_output_dataset(
        self,
        *,
        db_name: Optional[str],
        branch: Optional[str],
        dataset_name: Optional[str],
    ) -> DataFrame:
        return await _input_loading.load_existing_output_dataset(
            self,
            db_name=db_name,
            branch=branch,
            dataset_name=dataset_name,
        )

    def _align_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        if not columns:
            return df
        aligned = df
        existing = set(df.columns or [])
        for column in columns:
            if column not in existing:
                aligned = aligned.withColumn(column, F.lit(None))
        return aligned.select(*columns)

    def _select_new_or_changed_rows(
        self,
        *,
        input_df: DataFrame,
        existing_df: DataFrame,
        pk_columns: List[str],
        dedupe_input: bool = True,
    ) -> DataFrame:
        selected_input = input_df.dropDuplicates(pk_columns) if (pk_columns and dedupe_input) else input_df
        if not existing_df.columns or not pk_columns:
            return selected_input

        tracked_columns = [column for column in selected_input.columns if column not in set(pk_columns)]
        existing_selected = [F.col(column) for column in pk_columns]
        existing_selected.append(F.lit(1).alias("__existing_present__"))
        existing_selected.extend(
            F.col(column).alias(f"__existing_{column}") for column in tracked_columns
        )
        existing_lookup = existing_df.dropDuplicates(pk_columns).select(*existing_selected)
        joined = selected_input.join(existing_lookup, on=pk_columns, how="left")
        if not tracked_columns:
            return joined.filter(F.col("__existing_present__").isNull()).select(*selected_input.columns)

        changed_expr = F.col("__existing_present__").isNull()
        for column in tracked_columns:
            changed_expr = changed_expr | (~F.col(column).eqNullSafe(F.col(f"__existing_{column}")))
        return joined.filter(changed_expr).select(*selected_input.columns)

    def _post_filter_false_expr(self, *, post_filtering_column: str) -> Any:
        normalized = F.lower(F.trim(F.col(post_filtering_column).cast("string")))
        return normalized.isin("0", "false", "f", "no", "n", "off")

    async def _materialize_output_dataframe(
        self,
        df: DataFrame,
        *,
        artifact_bucket: str,
        prefix: str,
        write_mode: str = "overwrite",
        file_prefix: Optional[str] = None,
        file_format: str = "parquet",
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        return await self._output_domain.materialize_output_dataframe(
            df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=write_mode,
            file_prefix=file_prefix,
            file_format=file_format,
            partition_cols=partition_cols,
        )

    def _row_hash_expr(self, df: DataFrame) -> Any:
        columns = sorted(df.columns)
        if not columns:
            return F.lit("")
        parts = [
            F.concat(
                F.lit(f"{column}="),
                F.coalesce(F.col(column).cast("string"), F.lit("__NULL__")),
            )
            for column in columns
        ]
        return F.sha2(F.concat_ws("||", *parts), 256)

    def _apply_watermark_filter(
        self,
        df: DataFrame,
        *,
        watermark_column: str,
        watermark_after: Optional[Any],
        watermark_keys: Optional[list[str]],
    ) -> DataFrame:
        return self._ingest_domain.apply_watermark_filter(
            df,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
        )

    def _collect_watermark_keys(
        self,
        df: DataFrame,
        *,
        watermark_column: str,
        watermark_value: Any,
    ) -> list[str]:
        return self._ingest_domain.collect_watermark_keys(
            df,
            watermark_column=watermark_column,
            watermark_value=watermark_value,
        )

    def _build_dataset_input_snapshot(
        self,
        *,
        node_id: str,
        context: _DatasetInputLoadContext,
        lakefs_commit_id: Any,
    ) -> Dict[str, Any]:
        return _input_loading.build_dataset_input_snapshot(
            self,
            node_id=node_id,
            context=context,
            lakefs_commit_id=lakefs_commit_id,
        )

    def _annotate_diff_snapshot(
        self,
        *,
        snapshot: Dict[str, Any],
        previous_commit_id: Optional[str],
        diff_ok: bool,
        diff_paths_count: int,
    ) -> None:
        _input_loading.annotate_diff_snapshot(
            self,
            snapshot=snapshot,
            previous_commit_id=previous_commit_id,
            diff_ok=diff_ok,
            diff_paths_count=diff_paths_count,
        )

    def _append_input_snapshot(
        self,
        *,
        input_snapshots: Optional[list[dict[str, Any]]],
        snapshot: Optional[dict[str, Any]],
    ) -> None:
        _input_loading.append_input_snapshot(
            self,
            input_snapshots=input_snapshots,
            snapshot=snapshot,
        )

    async def _load_external_input_dataframe_with_snapshot(
        self,
        *,
        read_config: Dict[str, Any],
        node_id: str,
        temp_dirs: list[str],
        input_snapshots: Optional[list[dict[str, Any]]],
    ) -> DataFrame:
        return await _input_loading.load_external_input_dataframe_with_snapshot(
            self,
            read_config=read_config,
            node_id=node_id,
            temp_dirs=temp_dirs,
            input_snapshots=input_snapshots,
        )

    async def _resolve_dataset_input_load_context(
        self,
        *,
        db_name: str,
        node_id: str,
        selection: Any,
        input_snapshots: Optional[list[dict[str, Any]]],
    ) -> Tuple[_DatasetInputLoadContext, Optional[dict[str, Any]]]:
        return await _input_loading.resolve_dataset_input_load_context(
            self,
            db_name=db_name,
            node_id=node_id,
            selection=selection,
            input_snapshots=input_snapshots,
        )

    async def _load_full_dataset_input_dataframe(
        self,
        *,
        context: _DatasetInputLoadContext,
        metadata: Dict[str, Any],
        temp_dirs: list[str],
        node_id: str,
    ) -> DataFrame:
        return await _input_loading.load_full_dataset_input_dataframe(
            self,
            context=context,
            metadata=metadata,
            temp_dirs=temp_dirs,
            node_id=node_id,
        )

    async def _apply_input_watermark_and_snapshot(
        self,
        *,
        df: DataFrame,
        node_id: str,
        snapshot: Optional[dict[str, Any]],
        watermark_column: Optional[str],
        watermark_after: Optional[Any],
        watermark_keys: Optional[list[str]],
        label_scope: Optional[str],
        tolerate_max_errors: bool,
        always_compute_watermark_max: bool,
    ) -> DataFrame:
        return await self._ingest_domain.apply_input_watermark_and_snapshot(
            df=df,
            node_id=node_id,
            snapshot=snapshot,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
            label_scope=label_scope,
            tolerate_max_errors=tolerate_max_errors,
            always_compute_watermark_max=always_compute_watermark_max,
        )

    async def _try_load_diff_input_dataframe(
        self,
        *,
        context: _DatasetInputLoadContext,
        node_id: str,
        temp_dirs: list[str],
        input_snapshots: Optional[list[dict[str, Any]]],
        base_snapshot: Optional[dict[str, Any]],
        previous_commit_id: Optional[str],
        use_lakefs_diff: bool,
        watermark_column: Optional[str],
        watermark_after: Optional[Any],
        watermark_keys: Optional[list[str]],
    ) -> Optional[DataFrame]:
        return await _input_loading.try_load_diff_input_dataframe(
            self,
            context=context,
            node_id=node_id,
            temp_dirs=temp_dirs,
            input_snapshots=input_snapshots,
            base_snapshot=base_snapshot,
            previous_commit_id=previous_commit_id,
            use_lakefs_diff=use_lakefs_diff,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
        )

    async def _load_input_dataframe(
        self,
        db_name: str,
        metadata: Dict[str, Any],
        temp_dirs: list[str],
        branch: Optional[str],
        *,
        node_id: str,
        input_snapshots: Optional[list[dict[str, Any]]] = None,
        previous_commit_id: Optional[str] = None,
        use_lakefs_diff: bool = False,
        watermark_column: Optional[str] = None,
        watermark_after: Optional[Any] = None,
        watermark_keys: Optional[list[str]] = None,
    ) -> DataFrame:
        return await _input_loading.load_input_dataframe(
            self,
            db_name=db_name,
            metadata=metadata,
            temp_dirs=temp_dirs,
            branch=branch,
            node_id=node_id,
            input_snapshots=input_snapshots,
            previous_commit_id=previous_commit_id,
            use_lakefs_diff=use_lakefs_diff,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
        )

    def _preview_sampling_seed(self, job_id: str) -> int:
        seed_raw = abs(hash(str(job_id)))
        return seed_raw % 2_147_483_647

    def _resolve_preview_flag(
        self,
        preview_meta: Dict[str, Any],
        *,
        snake_case_key: str,
        camel_case_key: str,
        default: bool,
    ) -> bool:
        return resolve_preview_meta_flag(
            preview_meta,
            snake_case_key=snake_case_key,
            camel_case_key=camel_case_key,
            default=default,
        )

    def _resolve_preview_limit(
        self,
        *,
        preview_limit: Optional[Any],
        preview_meta: Optional[Dict[str, Any]] = None,
        default: int = 500,
    ) -> int:
        return resolve_preview_meta_limit(
            preview_limit=preview_limit,
            preview_meta=preview_meta,
            default=default,
        )

    def _resolve_sampling_strategy(
        self,
        metadata: Dict[str, Any],
        preview_meta: Dict[str, Any],
        *,
        preview_limit: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        return resolve_preview_sampling_strategy(
            metadata,
            preview_meta,
            preview_limit=preview_limit,
        )

    def _attach_sampling_snapshot(
        self,
        input_snapshots: list[dict[str, Any]],
        *,
        node_id: str,
        sampling_strategy: Dict[str, Any],
    ) -> None:
        attach_preview_sampling_snapshot(
            input_snapshots,
            node_id=node_id,
            sampling_strategy=sampling_strategy,
        )

    def _apply_sampling_strategy(
        self,
        df: DataFrame,
        sampling_strategy: Dict[str, Any],
        *,
        node_id: str,
        seed: Optional[int],
    ) -> DataFrame:
        return apply_preview_sampling_strategy(
            df,
            sampling_strategy,
            node_id=node_id,
            seed=seed,
        )

    def _strip_commit_prefix(self, key: str, commit_id: str) -> str:
        normalized = (key or "").lstrip("/")
        if commit_id and normalized.startswith(f"{commit_id}/"):
            return normalized[len(commit_id) + 1 :]
        return normalized

    async def _list_lakefs_diff_paths(
        self,
        *,
        repository: str,
        ref: str,
        since: str,
        prefix: str,
        node_id: str,
    ) -> tuple[List[str], bool]:
        if not self.lakefs_client:
            return [], False
        resolved_prefix = str(prefix or "").lstrip("/")
        try:
            diff_items = await self.lakefs_client.list_diff_objects(
                repository=repository,
                ref=ref,
                since=since,
                prefix=resolved_prefix or None,
            )
        except Exception as exc:
            logger.warning(
                "lakeFS diff failed for input node %s (repo=%s ref=%s since=%s): %s",
                node_id,
                repository,
                ref,
                since,
                exc,
            )
            return [], False
        paths: List[str] = []
        for item in diff_items:
            if not isinstance(item, dict):
                continue
            change_type = str(item.get("type") or item.get("change_type") or "").lower()
            if change_type and change_type not in {"added", "changed", "modified"}:
                continue
            path = item.get("path") or item.get("path_uri") or item.get("key")
            if not isinstance(path, str) or not path.strip():
                continue
            resolved_path = path.lstrip("/")
            if resolved_prefix and not resolved_path.startswith(resolved_prefix):
                continue
            if _is_data_object(resolved_path):
                paths.append(resolved_path)
        return paths, True

    async def _load_parquet_keys_dataframe(
        self,
        *,
        bucket: str,
        keys: List[str],
        temp_dirs: list[str],
        prefix: str,
    ) -> DataFrame:
        if not self.spark or not self.storage:
            raise RuntimeError("Storage/Spark not initialized")
        if not keys:
            return self._empty_dataframe()
        temp_dir = tempfile.mkdtemp(prefix="pipeline-input-")
        temp_dirs.append(temp_dir)
        normalized_prefix = (prefix or "").rstrip("/") + "/"
        local_paths: list[str] = []
        for object_key in keys:
            key = str(object_key or "").lstrip("/")
            if not key:
                continue
            rel_path = (
                os.path.relpath(key, normalized_prefix)
                if normalized_prefix and key.startswith(normalized_prefix)
                else os.path.basename(key)
            )
            local_path = os.path.join(temp_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            await self._download_object_to_path(bucket, key, local_path)
            local_paths.append(local_path)
        if not local_paths:
            return self._empty_dataframe()
        reader = self.spark.read.option("basePath", temp_dir)
        return await self._run_spark(lambda: reader.parquet(*local_paths), label="read_parquet_keys")

    async def _load_media_prefix_dataframe(self, bucket: str, key: str, *, node_id: str) -> DataFrame:
        """
        Treat the artifact_key as an unstructured/media prefix.

        We do not download/parse the blobs; we list objects and return a DataFrame of references.
        """
        if not self.storage or not self.spark:
            raise RuntimeError("Storage/Spark not initialized")

        prefix = key.rstrip("/")
        if not prefix:
            raise ValueError(f"Input node {node_id} media artifact_key has empty prefix")
        prefix = f"{prefix}/"

        objects = await self.storage.list_objects(bucket, prefix=prefix)
        rows: list[dict[str, Any]] = []
        for obj in objects or []:
            object_key = obj.get("Key")
            if not object_key or not isinstance(object_key, str):
                continue
            if object_key.endswith("/"):
                continue
            filename = os.path.basename(object_key)
            parts = filename.split("_", 1)
            if len(parts) == 2 and len(parts[0]) == 4 and parts[0].isdigit():
                filename = parts[1]
            content_type = "application/octet-stream"
            try:
                import mimetypes

                guessed, _ = mimetypes.guess_type(filename)
                if guessed:
                    content_type = guessed
            except Exception as exc:
                logger.warning("Failed to infer MIME type for %s: %s", filename, exc, exc_info=True)
            rows.append(
                {
                    "s3_uri": build_s3_uri(bucket, object_key),
                    "filename": filename,
                    "content_type": content_type,
                    "size_bytes": int(obj.get("Size") or 0),
                }
            )

        if not rows:
            return self._empty_dataframe()
        return await self._run_spark(
            lambda: self.spark.createDataFrame(rows),
            label=f"create_df:media:{node_id}",
        )

    async def _resolve_pipeline_id(self, job: PipelineJob) -> Optional[str]:
        if not self.pipeline_registry:
            return None
        pipeline_id = str(job.pipeline_id)
        try:
            UUID(pipeline_id)
            return pipeline_id
        except ValueError:
            logger.warning("Invalid pipeline_id in objectify payload, trying name lookup: %s", pipeline_id, exc_info=True)
        if job.db_name and pipeline_id:
            pipeline = await self.pipeline_registry.get_pipeline_by_name(
                db_name=job.db_name,
                name=pipeline_id,
                branch=job.branch or "main",
            )
            if pipeline:
                return pipeline.pipeline_id
        return None

    def _collect_spark_conf(self) -> Dict[str, Any]:
        if not self.spark:
            return {}
        conf_items = self.spark.sparkContext.getConf().getAll()
        conf: Dict[str, Any] = {}
        for key, value in conf_items:
            if not key:
                continue
            if not key.startswith(("spark.", "hive.", "hadoop.", "fs.")):
                continue
            if _is_sensitive_conf_key(key):
                continue
            conf[key] = value
        conf.setdefault("spark.version", getattr(self.spark, "version", None))
        return conf

    def _build_input_commit_payload(self, input_snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for snapshot in input_snapshots or []:
            commit_id = str(snapshot.get("lakefs_commit_id") or "").strip()
            if not commit_id:
                continue
            dataset_id = str(snapshot.get("dataset_id") or "").strip()
            key = (dataset_id, commit_id)
            if key in seen:
                continue
            seen.add(key)
            payload.append(
                {
                    "node_id": snapshot.get("node_id"),
                    "dataset_id": snapshot.get("dataset_id"),
                    "dataset_name": snapshot.get("dataset_name"),
                    "dataset_branch": snapshot.get("dataset_branch"),
                    "lakefs_commit_id": commit_id,
                    "artifact_key": snapshot.get("artifact_key"),
                }
            )
        return payload

    async def _acquire_pipeline_lock(self, job: PipelineJob) -> Optional[PipelineLock]:
        if not self.lock_enabled:
            return None
        if not self.redis:
            raise PipelineLockError("Pipeline locks enabled but Redis is not configured")
        branch = safe_lakefs_ref(job.branch or "main")
        key = f"pipeline-lock:{job.pipeline_id}:{branch}"
        token = f"{job.job_id}:{uuid4().hex}"
        start = time.monotonic()
        while True:
            acquired = await self.redis.client.set(
                key,
                token,
                nx=True,
                ex=self.lock_ttl_seconds,
            )
            if acquired:
                lock = PipelineLock(
                    redis_client=self.redis.client,
                    key=key,
                    token=token,
                    ttl_seconds=self.lock_ttl_seconds,
                    renew_seconds=min(self.lock_renew_seconds, max(10, self.lock_ttl_seconds // 3)),
                )
                await lock.start()
                return lock
            elapsed = time.monotonic() - start
            if elapsed >= self.lock_acquire_timeout_seconds:
                raise PipelineLockError(f"Timed out waiting for pipeline lock: {key}")
            await asyncio.sleep(self.lock_retry_seconds)

    def _validate_required_subgraph(
        self,
        nodes: Dict[str, Dict[str, Any]],
        incoming: Dict[str, List[str]],
        required_node_ids: set[str],
    ) -> List[str]:
        return self._validation_domain.validate_required_subgraph(
            nodes=nodes,
            incoming=incoming,
            required_node_ids=required_node_ids,
        )

    def _validate_definition(self, definition: Dict[str, Any], *, require_output: bool = True) -> List[str]:
        return self._validation_domain.validate_definition(
            definition=definition,
            require_output=require_output,
        )

    def _build_table_ops(self, df: DataFrame) -> TableOps:
        columns = set(df.columns)
        type_map = {field.name: spark_type_to_xsd(field.dataType) for field in df.schema.fields}
        total_cache: Optional[int] = None

        def total_count() -> int:
            nonlocal total_cache
            if total_cache is None:
                total_cache = int(df.count())
            return total_cache

        def has_null(column: str) -> bool:
            if column not in columns:
                return total_count() > 0
            return df.filter(F.col(column).isNull()).limit(1).count() > 0

        def has_empty(column: str) -> bool:
            if column not in columns:
                return total_count() > 0
            return df.filter(F.col(column).isNull() | (F.trim(F.col(column)) == "")).limit(1).count() > 0

        def unique_count(cols: List[str]) -> int:
            if not cols:
                return 0
            if any(col not in columns for col in cols):
                return 1 if total_count() > 0 else 0
            if len(cols) == 1:
                return df.select(cols[0]).distinct().count()
            return df.select(*cols).distinct().count()

        def min_max(column: str) -> tuple[Optional[float], Optional[float]]:
            if column not in columns:
                return (None, None)
            result = df.agg(F.min(F.col(column)).alias("min"), F.max(F.col(column)).alias("max")).collect()[0]
            min_value = result["min"]
            max_value = result["max"]
            try:
                min_value = float(min_value) if min_value is not None else None
            except (TypeError, ValueError):
                min_value = None
            try:
                max_value = float(max_value) if max_value is not None else None
            except (TypeError, ValueError):
                max_value = None
            return (min_value, max_value)

        def regex_mismatch(column: str, pattern: str) -> bool:
            if column not in columns:
                if total_count() == 0:
                    return False
                import re
                return re.search(pattern, "") is None
            return df.filter(~F.col(column).rlike(pattern)).limit(1).count() > 0

        def in_set_mismatch(column: str, allowed: List[Any]) -> bool:
            if column not in columns:
                return total_count() > 0 and (None not in allowed)
            return df.filter(~F.col(column).isin(allowed)).limit(1).count() > 0

        return TableOps(
            columns=columns,
            type_map=type_map,
            total_count=total_count,
            has_null=has_null,
            has_empty=has_empty,
            unique_count=unique_count,
            min_max=min_max,
            regex_mismatch=regex_mismatch,
            in_set_mismatch=in_set_mismatch,
        )

    def _sql_ident(self, name: str) -> str:
        return f"`{str(name).replace('`', '``')}`"

    def _clean_string_column(self, column: Any) -> Any:
        trimmed = F.trim(column.cast("string"))
        return F.when((trimmed.isNull()) | (F.length(trimmed) == 0), F.lit(None)).otherwise(trimmed)

    def _try_cast_column(self, column: str, spark_type: str) -> Any:
        ident = self._sql_ident(column)
        cleaned = f"nullif(trim({ident}), '')"
        if spark_type in {"bigint", "int", "long"}:
            numeric = f"regexp_replace({cleaned}, '[,_\\s]', '')"
            return F.expr(f"try_cast({numeric} as bigint)")
        if spark_type in {"double", "decimal"}:
            numeric = f"regexp_replace({cleaned}, '[,_\\s]', '')"
            numeric = f"regexp_replace({numeric}, '%$', '')"
            return F.expr(f"try_cast({numeric} as double)")
        if spark_type == "boolean":
            lowered = f"lower({cleaned})"
            return F.expr(
                "case "
                f"when {lowered} in ('true','t','1','yes','y') then true "
                f"when {lowered} in ('false','f','0','no','n') then false "
                "else null end"
            )
        if spark_type == "timestamp":
            # Use try_cast so ANSI mode won't raise on malformed inputs; return NULL instead.
            return F.expr(f"try_cast({cleaned} as timestamp)")
        if spark_type == "date":
            # Use try_cast so ANSI mode won't raise on malformed inputs; return NULL instead.
            return F.expr(f"try_cast({cleaned} as date)")
        return F.expr(cleaned)

    def _safe_cast_column(self, column: str, target_type: str) -> Any:
        spark_type = xsd_to_spark_type(target_type)
        source = F.col(str(column))
        if self.cast_mode == "STRICT":
            if spark_type == "string":
                return self._clean_string_column(source)
            return source.cast(spark_type)
        if spark_type == "string":
            return self._clean_string_column(source)
        return self._try_cast_column(column, spark_type)

    def _apply_casts(self, df: DataFrame, casts: List[Dict[str, Any]]) -> DataFrame:
        if not casts:
            return df
        output = df
        for cast in casts:
            column = str(cast.get("column") or "").strip().lstrip("\ufeff")
            if not column or column not in output.columns:
                continue
            data_type = normalize_schema_type(cast.get("type") or "xsd:string")
            output = output.withColumn(column, self._safe_cast_column(column, data_type))
        return output

    def _apply_schema_casts(self, df: DataFrame, *, dataset: Any, version: Any) -> DataFrame:
        schema_json = getattr(dataset, "schema_json", None)
        casts = extract_schema_casts(schema_json)
        if not casts and version is not None:
            casts = extract_schema_casts(getattr(version, "sample_json", None))
        if not casts:
            return df
        return self._apply_casts(df, casts)

    def _parse_fk_expectation(
        self,
        expectation: Dict[str, Any],
        *,
        default_branch: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        rule = str(expectation.get("rule") or "").strip().lower()
        if rule != "fk_exists":
            return None
        columns = normalize_expectation_columns(expectation.get("columns") or expectation.get("column"))
        reference = expectation.get("reference") if isinstance(expectation.get("reference"), dict) else {}
        ref_columns = normalize_expectation_columns(
            reference.get("columns")
            or reference.get("column")
            or expectation.get("ref_columns")
            or expectation.get("ref_column")
        )
        if not ref_columns and columns:
            ref_columns = list(columns)
        dataset_id = (
            reference.get("datasetId")
            or reference.get("dataset_id")
            or expectation.get("datasetId")
            or expectation.get("dataset_id")
        )
        dataset_name = (
            reference.get("datasetName")
            or reference.get("dataset_name")
            or expectation.get("datasetName")
            or expectation.get("dataset_name")
        )
        ref_branch = (
            reference.get("branch")
            or expectation.get("branch")
            or default_branch
            or "main"
        )
        allow_nulls = expectation.get("allow_nulls") if "allow_nulls" in expectation else expectation.get("allowNulls")
        allow_nulls = True if allow_nulls is None else bool(allow_nulls)
        return {
            "columns": columns,
            "ref_columns": ref_columns,
            "dataset_id": str(dataset_id) if dataset_id else None,
            "dataset_name": str(dataset_name) if dataset_name else None,
            "branch": str(ref_branch) if ref_branch else None,
            "allow_nulls": allow_nulls,
        }

    async def _load_fk_reference_dataframe(
        self,
        *,
        db_name: str,
        dataset_id: Optional[str],
        dataset_name: Optional[str],
        branch: Optional[str],
        temp_dirs: list[str],
    ) -> Optional[DataFrame]:
        return await _fk_validation.load_fk_reference_dataframe(
            self,
            db_name=db_name,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            branch=branch,
            temp_dirs=temp_dirs,
        )

    async def _evaluate_fk_expectations(
        self,
        *,
        expectations: List[Dict[str, Any]],
        output_df: DataFrame,
        db_name: str,
        branch: Optional[str],
        temp_dirs: list[str],
    ) -> List[str]:
        return await _fk_validation.evaluate_fk_expectations(
            self,
            expectations=expectations,
            output_df=output_df,
            db_name=db_name,
            branch=branch,
            temp_dirs=temp_dirs,
        )

    def _normalize_read_options(self, read_config: Dict[str, Any]) -> Dict[str, str]:
        return _input_loading.normalize_read_options(self, read_config)

    def _mask_sensitive_options(self, options: Dict[str, str]) -> Dict[str, str]:
        return _input_loading.mask_sensitive_options(self, options)

    def _schema_ddl_from_read_config(self, read_config: Dict[str, Any]) -> Optional[str]:
        return _input_loading.schema_ddl_from_read_config(self, read_config)

    def _resolve_read_format(self, *, path: str, read_config: Dict[str, Any]) -> str:
        return _input_loading.resolve_read_format(self, path=path, read_config=read_config)

    def _resolve_streaming_checkpoint_location(self, *, read_config: Dict[str, Any], node_id: str) -> str:
        return _input_loading.resolve_streaming_checkpoint_location(self, read_config=read_config, node_id=node_id)

    def _resolve_kafka_value_format(self, *, read_config: Dict[str, Any]) -> str:
        return _input_loading.resolve_kafka_value_format(self, read_config=read_config)

    def _resolve_kafka_schema_registry_headers(self, *, read_config: Dict[str, Any]) -> Dict[str, str]:
        return _input_loading.resolve_kafka_schema_registry_headers(self, read_config=read_config)

    def _resolve_kafka_avro_schema(
        self,
        *,
        read_config: Dict[str, Any],
        node_id: str,
    ) -> str:
        return _input_loading.resolve_kafka_avro_schema(self, read_config=read_config, node_id=node_id)

    @staticmethod
    def _fetch_kafka_avro_schema_from_registry(**kwargs: Any) -> str:
        return fetch_kafka_avro_schema_from_registry(**kwargs)

    def _apply_kafka_value_parsing(
        self,
        *,
        df: DataFrame,
        read_config: Dict[str, Any],
        node_id: str,
    ) -> DataFrame:
        return _input_loading.apply_kafka_value_parsing(
            self,
            df=df,
            read_config=read_config,
            node_id=node_id,
        )

    def _load_external_streaming_dataframe(
        self,
        *,
        read_config: Dict[str, Any],
        node_id: str,
        fmt: str,
        temp_dirs: list[str],
    ) -> DataFrame:
        return _input_loading.load_external_streaming_dataframe(
            self,
            read_config=read_config,
            node_id=node_id,
            fmt=fmt,
            temp_dirs=temp_dirs,
        )

    def _load_external_input_dataframe(
        self,
        read_config: Dict[str, Any],
        *,
        node_id: str,
        temp_dirs: list[str],
    ) -> DataFrame:
        return _input_loading.load_external_input_dataframe(
            self,
            read_config,
            node_id=node_id,
            temp_dirs=temp_dirs,
        )

    async def _load_artifact_dataframe(
        self,
        bucket: str,
        key: str,
        temp_dirs: list[str],
        *,
        read_config: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        return await _input_loading.load_artifact_dataframe(
            self,
            bucket,
            key,
            temp_dirs,
            read_config=read_config,
        )

    async def _collect_prefix_local_paths(
        self,
        *,
        bucket: str,
        prefix: str,
        temp_dirs: list[str],
    ) -> Tuple[Optional[str], List[str]]:
        return await _input_loading.collect_prefix_local_paths(
            self,
            bucket=bucket,
            prefix=prefix,
            temp_dirs=temp_dirs,
        )

    async def _read_prefix_dataframe_from_local_paths(
        self,
        *,
        temp_dir: str,
        local_paths: List[str],
        read_config: Dict[str, Any],
        bucket: str,
        prefix: str,
    ) -> DataFrame:
        return await _input_loading.read_prefix_dataframe_from_local_paths(
            self,
            temp_dir=temp_dir,
            local_paths=local_paths,
            read_config=read_config,
            bucket=bucket,
            prefix=prefix,
        )

    async def _load_prefix_dataframe(
        self,
        bucket: str,
        prefix: str,
        temp_dirs: list[str],
        *,
        read_config: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        return await _input_loading.load_prefix_dataframe(
            self,
            bucket,
            prefix,
            temp_dirs,
            read_config=read_config,
        )

    async def _download_object_to_path(self, bucket: str, key: str, local_path: str) -> None:
        await _input_loading.download_object_to_path(self, bucket, key, local_path)

    async def _download_object(
        self,
        bucket: str,
        key: str,
        temp_dirs: list[str],
        *,
        temp_dir: Optional[str] = None,
    ) -> str:
        return await _input_loading.download_object(
            self,
            bucket,
            key,
            temp_dirs,
            temp_dir=temp_dir,
        )

    def _read_local_file(self, path: str, *, read_config: Optional[Dict[str, Any]] = None) -> DataFrame:
        return _input_loading.read_local_file(self, path, read_config=read_config)

    def _strip_bom_headers(self, df: DataFrame) -> DataFrame:
        return _input_loading.strip_bom_headers(self, df)

    def _load_excel_path(self, path: str) -> DataFrame:
        return _input_loading.load_excel_path(self, path)

    def _load_json_path(self, path: str, *, reader: Optional[Any] = None) -> DataFrame:
        return _input_loading.load_json_path(self, path, reader=reader)

    def _empty_dataframe(self) -> DataFrame:
        return _input_loading.empty_dataframe(self)

    def _apply_transform(
        self,
        metadata: Dict[str, Any],
        inputs: List[DataFrame],
        parameters: Dict[str, Any],
    ) -> DataFrame:
        if not inputs:
            return self._empty_dataframe()
        return apply_spark_transform(
            metadata=metadata,
            inputs=inputs,
            parameters=parameters,
            apply_casts=self._apply_casts,
        )


async def main() -> None:
    configure_logging(get_settings().observability.log_level)
    worker = PipelineWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
