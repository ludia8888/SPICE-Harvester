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
from functools import reduce
import operator
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from uuid import UUID, uuid4

from confluent_kafka import Producer

from shared.services.kafka.safe_consumer import SafeKafkaConsumer

try:
    from pyspark.sql import DataFrame, SparkSession  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.types import StructType  # type: ignore
    from pyspark.sql.window import Window  # type: ignore
    from pyspark import StorageLevel  # type: ignore

    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover
    # Keep helper functions importable in lightweight/unit-test environments.
    _PYSPARK_AVAILABLE = False
    SparkSession = Any  # type: ignore[assignment,misc]
    DataFrame = Any  # type: ignore[assignment,misc]
    F = None  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]
    StorageLevel = Any  # type: ignore[assignment,misc]
    StructType = Any  # type: ignore[assignment,misc]

from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited, record_lineage_or_raise
from shared.models.pipeline_job import PipelineJob
from shared.models.lineage_edge_types import EDGE_PIPELINE_OUTPUT_STORED
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.kafka.processed_event_worker import HeartbeatOptions, ProcessedEventKafkaWorker, RegistryKey
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
    DatasetWriteMode,
    resolve_dataset_write_policy,
    validate_dataset_output_format_constraints,
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
    PipelineDefinitionValidationPolicy,
    normalize_transform_metadata,
    validate_pipeline_definition,
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
from pipeline_worker.ingest_domain import PipelineIngestDomain
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


class _PipelinePayloadParseError(ValueError):
    def __init__(
        self,
        *,
        stage: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.stage = str(stage)
        self.payload_text = payload_text
        self.payload_obj = payload_obj
        self.cause = cause


class _OutputNodeValidationError(ValueError):
    def __init__(
        self,
        *,
        stage: str,
        node_id: str,
        errors: List[str],
        row_count: int,
        payload: Dict[str, Any],
    ) -> None:
        super().__init__(f"{stage}:{node_id}")
        self.stage = str(stage)
        self.node_id = str(node_id)
        self.errors = list(errors)
        self.row_count = int(row_count)
        self.payload = payload


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


class PipelineWorker(ProcessedEventKafkaWorker[PipelineJob, None]):
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

    def _build_error_payload(
        self,
        *,
        message: str,
        errors: Optional[List[str]] = None,
        code: ErrorCode,
        category: ErrorCategory,
        status_code: int,
        external_code: Optional[str] = None,
        stage: Optional[str] = None,
        job: Optional[PipelineJob] = None,
        pipeline_id: Optional[str] = None,
        node_id: Optional[str] = None,
        mode: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context_payload: Dict[str, Any] = dict(context or {})
        if stage:
            context_payload["stage"] = stage
        if job:
            context_payload.setdefault("job_id", job.job_id)
            context_payload.setdefault("pipeline_id", str(job.pipeline_id))
            context_payload.setdefault("db_name", job.db_name)
            context_payload.setdefault("branch", job.branch or "main")
            context_payload.setdefault("mode", job.mode)
            context_payload.setdefault("node_id", job.node_id)
        if pipeline_id:
            context_payload.setdefault("pipeline_id", pipeline_id)
        if node_id:
            context_payload.setdefault("node_id", node_id)
        if mode:
            context_payload.setdefault("mode", mode)
        if not context_payload:
            context_payload = None
        return build_error_envelope(
            service_name=self.pipeline_label or "pipeline-worker",
            message=message,
            detail=message,
            code=code,
            category=category,
            status_code=status_code,
            errors=errors,
            context=context_payload,
            external_code=external_code,
        )

    async def initialize(self) -> None:
        settings = self.settings
        self.dataset_registry = DatasetRegistry()
        await self.dataset_registry.initialize()

        self.pipeline_registry = PipelineRegistry()
        await self.pipeline_registry.initialize()

        self.objectify_registry = ObjectifyRegistry()
        try:
            await self.objectify_registry.initialize()
        except Exception as exc:
            logger.warning("ObjectifyRegistry unavailable: %s", exc)
            self.objectify_registry = None
        if self.objectify_registry:
            self.objectify_job_queue = ObjectifyJobQueue(objectify_registry=self.objectify_registry)

        self.http = OMSGrpcHttpCompatClient()

        # MappingSpecResolver: OMS-first, PostgreSQL-fallback (gRPC bridge transport)
        oms_base = ""
        oms_token = ""
        self.mapping_resolver = MappingSpecResolver(
            http_client=self.http,
            oms_base_url=oms_base,
            admin_token=oms_token or None,
            objectify_registry=self.objectify_registry,
        )

        self.processed = await create_processed_event_registry(
            lease_timeout_seconds=int(self.processed_event_lease_timeout_seconds),
        )

        self.lineage = None
        if self.enable_lineage:
            self.lineage = LineageStore()
            try:
                await self.lineage.initialize()
            except Exception as exc:
                if self.lineage_required:
                    raise RuntimeError("LineageStore unavailable") from exc
                log_exception_rate_limited(
                    logger,
                    zone=RuntimeZone.OBSERVABILITY,
                    operation="pipeline_worker.initialize.lineage",
                    exc=exc,
                    code=ErrorCode.LINEAGE_UNAVAILABLE,
                    category=ErrorCategory.UPSTREAM,
                )
                self.lineage = None

        self.storage = await self.pipeline_registry.get_lakefs_storage()
        self.lakefs_client = await self.pipeline_registry.get_lakefs_client()

        api_key = (settings.google_sheets.google_sheets_api_key or "").strip() or None
        self.sheets = GoogleSheetsService(api_key=api_key)

        if self.lock_enabled:
            self.redis = create_redis_service(settings)
            try:
                await self.redis.connect()
            except Exception as exc:
                if self.lock_required:
                    raise
                logger.warning("Pipeline locks disabled (redis unavailable): %s", exc)
                self.redis = None
                self.lock_enabled = False

        self.spark = self._create_spark_session()
        if not self._spark_executor:
            max_workers = max(1, int(settings.pipeline.spark_executor_threads))
            self._spark_executor = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="pipeline-spark",
            )

        self._initialize_safe_consumer_runtime(
            group_id=self.group_id,
            topics=[self.topic],
            service_name="pipeline-worker",
            thread_name_prefix="pipeline-worker-kafka",
            max_poll_interval_ms=int(self.max_poll_interval_ms),
            reset_partition_state=True,
        )
        logger.info("PipelineWorker initialized (topic=%s)", self.topic)

        self.dlq_producer = create_kafka_dlq_producer(
            bootstrap_servers=settings.database.kafka_servers,
            client_id=self.service_name or "pipeline-worker-dlq",
        )

    async def close(self) -> None:
        await self._close_consumer_runtime()
        self._pending_by_partition.clear()
        await close_kafka_producer(
            producer=self.dlq_producer,
            timeout_s=5.0,
            warning_logger=logger,
            warning_message="DLQ producer flush failed during shutdown: %s",
        )
        self.dlq_producer = None
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.sheets:
            await self.sheets.close()
            self.sheets = None
        if self.lineage:
            await self.lineage.close()
            self.lineage = None
        if self.processed:
            await self.processed.close()
            self.processed = None
        if self.pipeline_registry:
            await self.pipeline_registry.close()
            self.pipeline_registry = None
        if self.objectify_registry:
            await self.objectify_registry.close()
            self.objectify_registry = None
        if self.dataset_registry:
            await self.dataset_registry.close()
            self.dataset_registry = None
        if self.redis:
            await self.redis.disconnect()
            self.redis = None
        if self.spark:
            self.spark.stop()
            self.spark = None
        if self._spark_executor:
            try:
                self._spark_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                self._spark_executor.shutdown(wait=False)
            self._spark_executor = None

    def _create_spark_session(self) -> SparkSession:
        builder = (
            SparkSession.builder.appName("spice-pipeline-worker")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true" if self.spark_ansi_enabled else "false")
        )

        shuffle_partitions = max(1, int(getattr(self, "spark_shuffle_partitions", 0) or 0))
        if shuffle_partitions:
            builder = (
                builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
                .config("spark.default.parallelism", str(shuffle_partitions))
            )

        if bool(getattr(self, "spark_adaptive_enabled", True)):
            builder = (
                builder.config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
            )
        else:
            builder = builder.config("spark.sql.adaptive.enabled", "false")

        if not bool(getattr(self, "spark_console_progress", False)):
            builder = builder.config("spark.ui.showConsoleProgress", "false")

        return builder.getOrCreate()

    def _extract_job_settings(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        settings = definition.get("settings")
        return dict(settings) if isinstance(settings, dict) else {}

    def _extract_job_spark_conf(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        settings = self._extract_job_settings(definition)
        raw = settings.get("spark_conf") or settings.get("sparkConf") or settings.get("spark_conf_overrides")
        spark_conf = dict(raw) if isinstance(raw, dict) else {}

        # Convenience aliases so planners can use booleans without remembering config keys.
        if "ansi_enabled" in settings and "spark.sql.ansi.enabled" not in spark_conf:
            spark_conf["spark.sql.ansi.enabled"] = "true" if bool(settings.get("ansi_enabled")) else "false"
        if "aqe_enabled" in settings and "spark.sql.adaptive.enabled" not in spark_conf:
            spark_conf["spark.sql.adaptive.enabled"] = "true" if bool(settings.get("aqe_enabled")) else "false"
        if "timezone" in settings and "spark.sql.session.timeZone" not in spark_conf:
            tz = str(settings.get("timezone") or "").strip()
            if tz:
                spark_conf["spark.sql.session.timeZone"] = tz
        return spark_conf

    def _apply_job_overrides(self, definition: Dict[str, Any]) -> tuple[Dict[str, Optional[str]], str]:
        """
        Apply per-job Spark/cast overrides from definition.settings.

        Returns:
        - previous Spark conf values for keys we changed
        - previous cast_mode so it can be restored
        """
        prev_conf: Dict[str, Optional[str]] = {}
        prev_cast_mode = self.cast_mode

        if self.spark:
            spark_conf = self._extract_job_spark_conf(definition)
            for key, value in spark_conf.items():
                conf_key = str(key or "").strip()
                if not conf_key:
                    continue
                if _is_sensitive_conf_key(conf_key):
                    # Avoid accidentally persisting secrets into Spark conf/log snapshots.
                    continue
                try:
                    prev_conf[conf_key] = self.spark.conf.get(conf_key)
                except Exception as exc:
                    logger.warning("Failed to read existing Spark conf %s before override: %s", conf_key, exc, exc_info=True)
                    prev_conf[conf_key] = None
                try:
                    self.spark.conf.set(conf_key, str(value))
                except Exception as exc:
                    logger.warning("Failed to set Spark conf %s: %s", conf_key, exc)

        settings = self._extract_job_settings(definition)
        for cast_key in ("cast_mode", "castMode"):
            if cast_key in settings and settings.get(cast_key) is not None:
                raw = str(settings.get(cast_key) or "").strip()
                if raw:
                    self.cast_mode = normalize_cast_mode(raw)
                break

        return prev_conf, prev_cast_mode

    def _restart_spark_session(self) -> None:
        if self.spark:
            try:
                self.spark.stop()
            except Exception as exc:
                logger.warning("Failed to stop Spark session: %s", exc)
        try:
            from pyspark import SparkContext

            # Best-effort reset of PySpark globals so builder.getOrCreate() doesn't
            # try to reuse a dead gateway/JVM after Py4JNetworkError/ConnectionRefusedError.
            for attr in (
                "_instantiatedContext",
                "_activeSession",
                "_defaultSession",
            ):
                if hasattr(SparkSession, attr):
                    setattr(SparkSession, attr, None)
            for attr in (
                "_active_spark_context",
                "_gateway",
                "_jvm",
            ):
                if hasattr(SparkContext, attr):
                    setattr(SparkContext, attr, None)
        except Exception as exc:
            logger.warning(
                "Failed to reset SparkContext attributes before Spark recreation: %s",
                exc,
                exc_info=True,
            )
        self.spark = self._create_spark_session()

    @staticmethod
    def _is_spark_gateway_error(exc: BaseException) -> bool:
        if isinstance(exc, (ConnectionRefusedError, BrokenPipeError)):
            return True
        msg = str(exc)
        if "SparkConf does not exist in the JVM" in msg:
            return True
        if "Answer from Java side is empty" in msg:
            return True
        if "GatewayClient" in msg and "Connection refused" in msg:
            return True
        try:
            from py4j.java_gateway import Py4JJavaError  # type: ignore
            from py4j.protocol import Py4JError, Py4JNetworkError  # type: ignore
        except ImportError:
            return False
        return isinstance(exc, (Py4JError, Py4JNetworkError, Py4JJavaError))

    async def _run_spark(self, fn: Callable[[], T], *, label: str) -> T:
        """
        Run a blocking Spark action off the main event loop.

        Why:
        - Spark actions (count/collect/write) can take minutes.
        - If they run on the asyncio event loop thread, the Kafka consumer cannot poll
          and can get kicked out of the group (rebalance / max.poll.interval).
        """
        executor = self._spark_executor
        if executor is None:
            return fn()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(executor, fn)
        except Exception:
            logger.error("Spark action failed (%s)", label, exc_info=True)
            raise

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        try:
            await self.run_partitioned_loop(poll_timeout=0.25, idle_sleep=0.0)
        finally:
            await self._cancel_inflight_tasks()
            await self.close()

    # --- ProcessedEventKafkaWorker Strategy hooks ---
    def _service_name(self) -> Optional[str]:  # type: ignore[override]
        return "pipeline-worker"

    def _cancel_inflight_on_revoke(self) -> bool:  # type: ignore[override]
        # Pipeline jobs can be long-running (Spark actions, lakeFS/S3 IO). Rely on the
        # ProcessedEventRegistry lease + heartbeats for idempotency across rebalances
        # instead of canceling in-flight tasks and restarting expensive work.
        return False

    def _buffer_messages(self) -> bool:  # type: ignore[override]
        return True

    def _pending_log_thresholds(self) -> set[int]:  # type: ignore[override]
        return {1, 10, 50, 100}

    def _uses_commit_state(self) -> bool:  # type: ignore[override]
        return True

    def _parse_payload(self, payload: Any) -> PipelineJob:  # type: ignore[override]
        if not isinstance(payload, (bytes, bytearray)):
            raise _PipelinePayloadParseError(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                cause=TypeError("Pipeline job payload must be bytes"),
            )
        try:
            raw_text = payload.decode("utf-8")
        except Exception as exc:
            raise _PipelinePayloadParseError(
                stage="decode",
                payload_text=None,
                payload_obj=None,
                cause=exc,
            ) from exc

        try:
            payload_obj = json.loads(raw_text)
        except Exception as exc:
            raise _PipelinePayloadParseError(
                stage="json",
                payload_text=raw_text,
                payload_obj=None,
                cause=exc,
            ) from exc

        try:
            job = PipelineJob.model_validate(payload_obj)
        except Exception as exc:
            raise _PipelinePayloadParseError(
                stage="validate",
                payload_text=raw_text,
                payload_obj=payload_obj if isinstance(payload_obj, dict) else None,
                cause=exc,
            ) from exc
        return job

    def _registry_key(self, payload: PipelineJob) -> RegistryKey:  # type: ignore[override]
        return RegistryKey(
            event_id=str(payload.job_id),
            aggregate_id=str(payload.pipeline_id),
            # Pipeline jobs are not "append-only state" per pipeline; avoid stale-skips.
            sequence_number=None,
        )

    async def _process_payload(self, payload: PipelineJob) -> None:  # type: ignore[override]
        logger.info(
            "Executing pipeline job (job_id=%s pipeline_id=%s mode=%s)",
            payload.job_id,
            payload.pipeline_id,
            getattr(payload, "mode", None),
        )
        try:
            await self._execute_job(payload)
        except Exception as exc:
            if not self._is_spark_gateway_error(exc):
                raise

            logger.warning(
                "Spark gateway error; restarting Spark session and retrying once (job_id=%s): %s",
                payload.job_id,
                exc,
            )
            try:
                self._restart_spark_session()
            except Exception as restart_exc:
                err = f"{exc} (spark_restart_failed: {restart_exc})"
                if self.processed:
                    try:
                        await self.processed.mark_failed(
                            handler=self.handler,
                            event_id=str(payload.job_id),
                            error=err,
                        )
                    except Exception as mark_err:
                        logger.warning(
                            "Failed to mark pipeline job failed before restart: %s",
                            mark_err,
                            exc_info=True,
                        )
                logger.warning(
                    "Failed to restart Spark session after gateway error: %s",
                    restart_exc,
                )
                raise SystemExit(1) from restart_exc

            # Single in-process retry (keeps latency low vs Kafka backoff/seek) while still
            # allowing the outer retry/DLQ logic to handle persistent failures.
            await self._execute_job(payload)
        logger.info(
            "Pipeline job complete (job_id=%s pipeline_id=%s)",
            payload.job_id,
            payload.pipeline_id,
        )

    def _fallback_metadata(self, payload: PipelineJob) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        return {
            "job_id": payload.job_id,
            "pipeline_id": str(payload.pipeline_id),
            "db_name": payload.db_name,
            "branch": payload.branch,
            "mode": getattr(payload, "mode", None),
        }

    def _span_name(self, *, payload: PipelineJob) -> str:  # type: ignore[override]
        return "pipeline_worker.process_job"

    def _span_attributes(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: PipelineJob,
        registry_key: RegistryKey,
    ) -> Dict[str, Any]:
        attrs = super()._span_attributes(msg=msg, payload=payload, registry_key=registry_key)
        attrs.update(
            {
                "pipeline.job_id": payload.job_id,
                "pipeline.pipeline_id": str(payload.pipeline_id),
                "pipeline.db_name": payload.db_name,
                "pipeline.branch": payload.branch,
                "pipeline.mode": getattr(payload, "mode", None),
            }
        )
        return attrs

    def _metric_event_name(self, *, payload: PipelineJob) -> Optional[str]:  # type: ignore[override]
        return "PIPELINE_JOB"

    def _heartbeat_options(self) -> HeartbeatOptions:  # type: ignore[override]
        return HeartbeatOptions(
            interval_seconds=int(self.processed_event_heartbeat_interval_seconds),
        )

    def _is_retryable_error(self, exc: Exception, *, payload: PipelineJob) -> bool:  # type: ignore[override]
        return True

    async def _mark_retryable_failure(  # type: ignore[override]
        self,
        *,
        payload: PipelineJob,
        registry_key: RegistryKey,
        handler: str,
        error: str,
    ) -> None:
        await self.processed.mark_retrying(
            handler=handler,
            event_id=str(registry_key.event_id),
            error=error,
        )

    async def _on_parse_error(self, *, msg: Any, raw_payload: Optional[str], error: Exception) -> None:  # type: ignore[override]
        async def _send(
            stage: str,
            cause_text: str,
            payload_text: Optional[str],
            payload_obj: Optional[Dict[str, Any]],
            _kafka_headers: Optional[Any],
            _fallback_metadata: Optional[Dict[str, Any]],
        ) -> None:
            await self._publish_to_dlq(
                msg=msg,
                stage=stage,
                error=cause_text,
                payload_text=payload_text,
                payload_obj=payload_obj,
                job=None,
                attempt_count=None,
            )
            if stage != "validate" or not isinstance(payload_obj, dict):
                return
            try:
                await self._best_effort_record_invalid_job(payload_obj, error=cause_text)
            except Exception as record_exc:
                logger.warning(
                    "Failed to record invalid pipeline job (best-effort): %s",
                    record_exc,
                    exc_info=True,
                )

        await self._publish_parse_error_to_dlq(
            msg=msg,
            raw_payload=raw_payload,
            error=error,
            dlq_sender=_send,
            publish_failure_message="Failed to publish invalid pipeline job payload to DLQ: %s",
            invalid_payload_message="Invalid pipeline job payload; skipping: %s",
            raise_on_publish_failure=False,
            logger_instance=logger,
        )

    async def _on_retry_scheduled(  # type: ignore[override]
        self,
        *,
        payload: PipelineJob,
        error: str,
        attempt_count: int,
        backoff_s: int,
        retryable: bool,
    ) -> None:
        logger.warning(
            "Pipeline job failed; will retry (job_id=%s attempt=%s backoff=%ss): %s",
            payload.job_id,
            attempt_count,
            int(backoff_s),
            error,
        )

    async def _on_terminal_failure(  # type: ignore[override]
        self,
        *,
        payload: PipelineJob,
        error: str,
        attempt_count: int,
        retryable: bool,
    ) -> None:
        logger.error(
            "Pipeline job max retries exceeded; sending to DLQ (job_id=%s attempt=%s)",
            payload.job_id,
            attempt_count,
        )

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: PipelineJob,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        await self._publish_to_dlq(
            msg=msg,
            stage="execute",
            error=error,
            payload_text=raw_payload,
            payload_obj=None,
            job=payload,
            attempt_count=int(attempt_count),
        )

    async def _publish_to_dlq(
        self,
        *,
        msg: Any,
        stage: str,
        error: str,
        payload_text: Optional[str],
        payload_obj: Optional[Dict[str, Any]],
        job: Optional[PipelineJob],
        attempt_count: Optional[int],
    ) -> None:
        extra: Optional[Dict[str, Any]] = None
        if job is not None:
            extra = {"job": job.model_dump(mode="json")}

        try:
            sent = await self._publish_standard_dlq_record(
                producer=self.dlq_producer,
                msg=msg,
                worker=self._pipeline_worker_name,
                dlq_spec=self._dlq_spec,
                error=error,
                attempt_count=int(attempt_count) if attempt_count is not None else None,
                stage=stage,
                payload_text=payload_text,
                payload_obj=payload_obj,
                extra=extra,
                tracing=None,
                metrics=None,
                kafka_headers=None,
                fallback_metadata=None,
                lock=self._dlq_lock,
                raise_on_missing_producer=False,
                missing_producer_message="DLQ producer not configured",
            )
            if not sent:
                return
            logger.info("Sent message to pipeline DLQ (topic=%s stage=%s)", self.dlq_topic, stage)
        except Exception as exc:
            logger.error("Failed to send message to pipeline DLQ (stage=%s): %s", stage, exc)

    async def _best_effort_record_invalid_job(self, payload: Dict[str, Any], *, error: str) -> None:
        if not self.pipeline_registry:
            return

        job_id = str(payload.get("job_id") or payload.get("jobId") or "").strip()
        pipeline_id_raw = str(payload.get("pipeline_id") or payload.get("pipelineId") or "").strip()
        db_name = str(payload.get("db_name") or payload.get("dbName") or "").strip()
        branch = str(payload.get("branch") or payload.get("pipelineBranch") or payload.get("pipeline_branch") or "main").strip() or "main"
        mode = str(payload.get("mode") or "deploy").strip().lower() or "deploy"
        mode = "preview" if mode == "preview" else "deploy"

        if not (job_id and pipeline_id_raw and db_name):
            return

        resolved_pipeline_id = await self._resolve_pipeline_id_from_fields(
            db_name=db_name,
            pipeline_id=pipeline_id_raw,
            branch=branch,
        )
        if not resolved_pipeline_id:
            return

        payload_obj = self._build_error_payload(
            message="Pipeline job payload invalid",
            errors=[error],
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=422,
            external_code="PIPELINE_REQUEST_INVALID",
            stage="validate",
            pipeline_id=resolved_pipeline_id,
            mode=mode,
            context={"job_id": job_id, "db_name": db_name, "branch": branch},
        )
        try:
            await self.pipeline_registry.record_run(
                pipeline_id=resolved_pipeline_id,
                job_id=job_id,
                mode=mode,
                status="FAILED",
                sample_json=payload_obj if mode == "preview" else None,
                output_json=payload_obj if mode != "preview" else None,
                finished_at=utcnow(),
            )
        except Exception as exc:
            logger.warning("Failed to record invalid pipeline job run (job_id=%s): %s", job_id, exc)

    async def _resolve_pipeline_id_from_fields(self, *, db_name: str, pipeline_id: str, branch: str) -> Optional[str]:
        if not self.pipeline_registry:
            return None
        pipeline_id = str(pipeline_id)
        try:
            UUID(pipeline_id)
            return pipeline_id
        except ValueError:
            logger.warning("Invalid pipeline_id format, trying name lookup: %s", pipeline_id, exc_info=True)
        pipeline = await self.pipeline_registry.get_pipeline_by_name(
            db_name=db_name,
            name=pipeline_id,
            branch=branch or "main",
        )
        if pipeline:
            return pipeline.pipeline_id
        return None

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
        skip_production_checks = self._resolve_preview_flag(
            preview_meta,
            snake_case_key="skip_production_checks",
            camel_case_key="skipProductionChecks",
            default=True,
        )
        skip_output_recording = self._resolve_preview_flag(
            preview_meta,
            snake_case_key="skip_output_recording",
            camel_case_key="skipOutputRecording",
            default=True,
        )
        primary_id = target_node_ids[0] if target_node_ids else None
        output_df = tables.get(primary_id) if primary_id else self._empty_dataframe()
        schema_columns = _schema_from_dataframe(output_df)
        row_count = int(await self._run_spark(lambda: output_df.count(), label=f"count:preview:{primary_id}"))
        output_ops = self._build_table_ops(output_df)
        if skip_production_checks:
            contract_errors: List[str] = []
        else:
            schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
            contract_errors = await self._run_spark(
                lambda: validate_schema_contract(output_ops, schema_contract),
                label=f"schema_contract:preview:{primary_id}",
            )
        if contract_errors:
            contract_payload = self._build_error_payload(
                message="Pipeline schema contract failed",
                errors=contract_errors,
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
                status_code=422,
                external_code="PIPELINE_SCHEMA_CONTRACT_FAILED",
                stage="schema_contract",
                job=job,
                node_id=primary_id,
                context={"execution_semantics": execution_semantics},
            )
            await record_preview(
                status="FAILED",
                row_count=row_count,
                sample_json=contract_payload,
                job_id=job.job_id,
                node_id=job.node_id,
            )
            await record_run(
                job_id=job.job_id,
                mode="preview",
                status="FAILED",
                node_id=job.node_id,
                row_count=row_count,
                sample_json=contract_payload,
                input_lakefs_commits=input_commit_payload,
                finished_at=utcnow(),
            )
            if not skip_output_recording:
                await record_artifact(
                    status="FAILED",
                    errors=contract_errors,
                    inputs=inputs_payload,
                )
            logger.error("Pipeline schema contract failed: %s", contract_errors)
            return

        output_meta = nodes.get(primary_id, {}) if primary_id else {}
        output_metadata = output_meta.get("metadata") or {}
        output_name = (
            output_metadata.get("outputName")
            or output_metadata.get("datasetName")
            or output_meta.get("title")
            or (primary_id or "preview_output")
        )
        output_kind = _resolve_declared_output_kind(
            declared_outputs=declared_outputs,
            output_node_id=primary_id,
            output_name=str(output_name or ""),
        )
        _validate_output_kind_metadata(
            output_kind=output_kind,
            output_metadata=output_metadata,
            node_id=str(primary_id or "preview_output"),
        )
        expectation_errors: List[str] = []
        if not skip_production_checks:
            pk_semantics = resolve_pk_semantics(
                execution_semantics=execution_semantics,
                definition=definition,
                output_metadata=output_metadata,
            )
            delete_column = resolve_delete_column(
                definition=definition,
                output_metadata=output_metadata,
            )
            pk_columns = resolve_pk_columns(
                definition=definition,
                output_metadata=output_metadata,
                output_name=output_name,
                output_node_id=primary_id,
                declared_outputs=declared_outputs,
            )
            available_columns = output_ops.columns
            pk_semantic_errors = validate_pk_semantics(
                available_columns=available_columns,
                pk_semantics=pk_semantics,
                pk_columns=pk_columns,
                delete_column=delete_column,
            )
            expectations = build_expectations_with_pk(
                definition=definition,
                output_metadata=output_metadata,
                output_name=output_name,
                output_node_id=primary_id,
                declared_outputs=declared_outputs,
                pk_semantics=pk_semantics,
                delete_column=delete_column,
                pk_columns=pk_columns,
                available_columns=available_columns,
            )
            expectation_errors = pk_semantic_errors + await self._run_spark(
                lambda: validate_expectations(output_ops, expectations),
                label=f"expectations:preview:{primary_id}",
            )
            fk_errors = await self._evaluate_fk_expectations(
                expectations=expectations,
                output_df=output_df,
                db_name=job.db_name,
                branch=job.branch or "main",
                temp_dirs=temp_dirs,
            )
            expectation_errors = expectation_errors + fk_errors
        sample_rows = await self._run_spark(
            lambda: output_df.limit(preview_limit).collect(),
            label=f"collect:preview:{primary_id}",
        )
        output_sample = [row.asDict(recursive=True) for row in sample_rows]
        column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)
        sample_payload: Dict[str, Any] = {
            "columns": schema_columns,
            "rows": output_sample,
            "job_id": job.job_id,
            "row_count": row_count,
            "sample_row_count": len(output_sample),
            "sampling_strategy": {
                "output": {"type": "limit", "limit": preview_limit},
                **({"input": input_sampling} if input_sampling else {}),
            },
            "column_stats": column_stats,
            "definition_hash": job.definition_hash,
            "branch": job.branch or "main",
            "input_snapshots": input_snapshots,
            "execution_semantics": execution_semantics,
            "pipeline_spec_hash": pipeline_spec_hash,
            "pipeline_spec_commit_id": pipeline_spec_commit_id,
            "code_version": code_version,
            "spark_conf": spark_conf,
            "production_checks_skipped": skip_production_checks,
            "output_recording_skipped": skip_output_recording,
        }
        if primary_id:
            sample_payload["node_id"] = primary_id
        if expectation_errors:
            sample_payload["expectations"] = expectation_errors
        preview_outputs = [
            {
                "node_id": primary_id,
                "output_name": output_name,
                "output_kind": output_kind,
                "dataset_name": output_name,
                "row_count": row_count,
                "columns": schema_columns,
                "schema_hash": _hash_schema_columns(schema_columns),
                "schema_json": {"columns": schema_columns},
                "rows": output_sample,
                "sample_row_count": len(output_sample),
                "column_stats": column_stats,
            }
        ]
        artifact_status = "FAILED" if expectation_errors else "SUCCESS"
        if not skip_output_recording:
            artifact_id = await record_artifact(
                status=artifact_status,
                outputs=preview_outputs,
                inputs=inputs_payload,
                sampling_strategy={
                    "output": {"type": "limit", "limit": preview_limit},
                    **({"input": input_sampling} if input_sampling else {}),
                },
                errors=expectation_errors if expectation_errors else None,
            )
            if artifact_id:
                sample_payload["artifact_id"] = artifact_id

        await record_preview(
            status="FAILED" if expectation_errors else "SUCCESS",
            row_count=row_count,
            sample_json=sample_payload,
            job_id=job.job_id,
            node_id=job.node_id,
        )
        await record_run(
            job_id=job.job_id,
            mode="preview",
            status="FAILED" if expectation_errors else "SUCCESS",
            node_id=job.node_id,
            row_count=row_count,
            sample_json=sample_payload,
            input_lakefs_commits=input_commit_payload,
            finished_at=utcnow(),
        )
        if expectation_errors:
            logger.error("Pipeline expectations failed: %s", expectation_errors)

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
        stage_config = {
            "schema_contract": (
                "Pipeline schema contract failed",
                "PIPELINE_SCHEMA_CONTRACT_FAILED",
            ),
            "expectations": (
                "Pipeline expectations failed",
                "PIPELINE_EXPECTATIONS_FAILED",
            ),
            "output_metadata": (
                "Dataset output metadata invalid",
                "PIPELINE_DATASET_OUTPUT_METADATA_INVALID",
            ),
        }
        message, external_code = stage_config.get(
            stage,
            ("Pipeline output validation failed", "PIPELINE_OUTPUT_VALIDATION_FAILED"),
        )
        context: Dict[str, Any] = {"execution_semantics": execution_semantics}
        if output_name:
            context["output_name"] = output_name
        return self._build_error_payload(
            message=message,
            errors=errors,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            status_code=422,
            external_code=external_code,
            stage=stage,
            job=job,
            node_id=node_id,
            context=context,
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
        if persist_output and _PYSPARK_AVAILABLE:
            # Build mode touches the same DataFrame multiple times (count/sample/write). Persist the
            # final output to avoid recomputation spikes that can OOM-kill the worker in Docker.
            try:
                output_df = output_df.persist(StorageLevel.DISK_ONLY)
                persisted_dfs.append(output_df)
            except Exception as exc:
                logger.warning(
                    "Failed to persist output dataframe for node %s: %s",
                    node_id,
                    exc,
                    exc_info=True,
                )

        schema_columns = _schema_from_dataframe(output_df)
        delta_row_count = int(
            await self._run_spark(lambda: output_df.count(), label=f"count:{mode_label}:{node_id}")
        )
        schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
        output_ops = self._build_table_ops(output_df)
        contract_errors = await self._run_spark(
            lambda: validate_schema_contract(output_ops, schema_contract),
            label=f"schema_contract:{mode_label}:{node_id}",
        )
        if contract_errors:
            raise _OutputNodeValidationError(
                stage="schema_contract",
                node_id=node_id,
                errors=contract_errors,
                row_count=delta_row_count,
                payload=self._build_output_validation_payload(
                    stage="schema_contract",
                    errors=contract_errors,
                    job=job,
                    node_id=node_id,
                    execution_semantics=execution_semantics,
                ),
            )

        output_meta = output_nodes.get(node_id) or {}
        metadata = output_meta.get("metadata") or {}
        output_name = (
            metadata.get("outputName")
            or metadata.get("datasetName")
            or output_meta.get("title")
            or job.output_dataset_name
        )
        output_kind = _resolve_declared_output_kind(
            declared_outputs=declared_outputs,
            output_node_id=node_id,
            output_name=str(output_name or ""),
        )
        _validate_output_kind_metadata(
            output_kind=output_kind,
            output_metadata=metadata,
            node_id=str(node_id),
        )
        pk_semantics = resolve_pk_semantics(
            execution_semantics=execution_semantics,
            definition=definition,
            output_metadata=metadata,
        )
        delete_column = resolve_delete_column(
            definition=definition,
            output_metadata=metadata,
        )
        pk_columns = resolve_pk_columns(
            definition=definition,
            output_metadata=metadata,
            output_name=output_name,
            output_node_id=node_id,
            declared_outputs=declared_outputs,
        )
        available_columns = output_ops.columns
        pk_semantic_errors = validate_pk_semantics(
            available_columns=available_columns,
            pk_semantics=pk_semantics,
            pk_columns=pk_columns,
            delete_column=delete_column,
        )
        expectations = build_expectations_with_pk(
            definition=definition,
            output_metadata=metadata,
            output_name=output_name,
            output_node_id=node_id,
            declared_outputs=declared_outputs,
            pk_semantics=pk_semantics,
            delete_column=delete_column,
            pk_columns=pk_columns,
            available_columns=available_columns,
        )
        expectation_errors = pk_semantic_errors + await self._run_spark(
            lambda: validate_expectations(output_ops, expectations),
            label=f"expectations:{mode_label}:{node_id}",
        )
        fk_errors = await self._evaluate_fk_expectations(
            expectations=expectations,
            output_df=output_df,
            db_name=job.db_name,
            branch=job.branch or "main",
            temp_dirs=temp_dirs,
        )
        expectation_errors = expectation_errors + fk_errors
        if expectation_errors:
            raise _OutputNodeValidationError(
                stage="expectations",
                node_id=node_id,
                errors=expectation_errors,
                row_count=delta_row_count,
                payload=self._build_output_validation_payload(
                    stage="expectations",
                    errors=expectation_errors,
                    job=job,
                    node_id=node_id,
                    execution_semantics=execution_semantics,
                ),
            )

        dataset_name = str(output_name or job.output_dataset_name)
        write_mode_requested = output_write_mode
        write_mode_resolved = output_write_mode
        runtime_write_mode = output_write_mode
        write_policy_hash: Optional[str] = None
        pk_columns_policy: List[str] = []
        post_filtering_column: Optional[str] = None
        if output_kind == OUTPUT_KIND_DATASET:
            dataset_policy = resolve_dataset_write_policy(
                definition=definition,
                output_metadata=metadata,
                execution_semantics=execution_semantics,
                has_incremental_input=has_incremental_input,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            )
            metadata = dict(dataset_policy.normalized_metadata)
            write_mode_requested = dataset_policy.requested_write_mode
            write_mode_resolved = dataset_policy.resolved_write_mode.value
            runtime_write_mode = dataset_policy.runtime_write_mode
            write_policy_hash = dataset_policy.policy_hash
            pk_columns_policy = list(dataset_policy.primary_key_columns)
            post_filtering_column = dataset_policy.post_filtering_column
            dataset_output_errors = validate_dataset_output_metadata(
                definition=definition,
                output_metadata=metadata,
                execution_semantics=execution_semantics,
                has_incremental_input=has_incremental_input,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                available_columns=_schema_column_names(schema_columns),
            )
            if dataset_output_errors:
                raise _OutputNodeValidationError(
                    stage="output_metadata",
                    node_id=node_id,
                    errors=dataset_output_errors,
                    row_count=delta_row_count,
                    payload=self._build_output_validation_payload(
                        stage="output_metadata",
                        errors=dataset_output_errors,
                        job=job,
                        node_id=node_id,
                        execution_semantics=execution_semantics,
                        output_name=dataset_name,
                    ),
                )
            output_format = dataset_policy.output_format
            partition_cols = list(dataset_policy.partition_by)
        else:
            output_format = _resolve_output_format(definition=definition, output_metadata=metadata)
            partition_cols = _resolve_partition_columns(
                definition=definition,
                output_metadata=metadata,
            )

        schema_hash = _hash_schema_columns(schema_columns)
        sample_rows = await self._run_spark(
            lambda: output_df.limit(preview_limit).collect(),
            label=f"collect:{mode_label}:{node_id}",
        )
        output_sample = [row.asDict(recursive=True) for row in sample_rows]
        sample_row_count = len(output_sample)
        column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)
        safe_name = dataset_name.replace(" ", "_")
        artifact_prefix = f"pipelines/{job.db_name}/{pipeline_ref}/{safe_name}"
        return {
            "node_id": node_id,
            "output_name": output_name,
            "output_kind": output_kind,
            "output_metadata": dict(metadata),
            "dataset_name": dataset_name,
            "output_df": output_df,
            "artifact_prefix": artifact_prefix,
            "output_format": output_format,
            "partition_columns": partition_cols,
            "write_mode_requested": write_mode_requested,
            "write_mode_resolved": write_mode_resolved,
            "runtime_write_mode": runtime_write_mode,
            "write_policy_hash": write_policy_hash,
            "pk_columns": pk_columns_policy,
            "post_filtering_column": post_filtering_column,
            "has_incremental_input": has_incremental_input,
            "incremental_inputs_have_additive_updates": incremental_inputs_have_additive_updates,
            "delta_row_count": delta_row_count,
            "row_count": delta_row_count,
            "schema_columns": schema_columns,
            "columns": schema_columns,
            "schema_hash": schema_hash,
            "schema_json": {"columns": schema_columns},
            "output_sample": output_sample,
            "rows": output_sample,
            "sample_row_count": sample_row_count,
            "column_stats": column_stats,
        }

    def _log_output_validation_failure(
        self,
        *,
        mode_label: str,
        failure: _OutputNodeValidationError,
    ) -> None:
        stage_messages = {
            "schema_contract": "Pipeline schema contract failed",
            "expectations": "Pipeline expectations failed",
            "output_metadata": "Pipeline dataset output metadata invalid",
        }
        suffix = " (build)" if mode_label == "build" else ""
        logger.error(
            "%s%s: %s",
            stage_messages.get(failure.stage, "Pipeline output validation failed"),
            suffix,
            failure.errors,
        )

    async def _record_build_output_failure(
        self,
        *,
        job: PipelineJob,
        failure: _OutputNodeValidationError,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await record_run(
            job_id=job.job_id,
            mode="build",
            status="FAILED",
            node_id=failure.node_id,
            row_count=failure.row_count,
            output_json=failure.payload,
            input_lakefs_commits=input_commit_payload,
            finished_at=utcnow(),
        )
        await record_artifact(
            status="FAILED",
            errors=failure.errors,
            inputs=inputs_payload,
        )
        await emit_job_event(status="FAILED", errors=failure.errors)
        self._log_output_validation_failure(mode_label="build", failure=failure)

    async def _record_deploy_output_failure(
        self,
        *,
        job: PipelineJob,
        failure: _OutputNodeValidationError,
        input_commit_payload: Optional[List[Dict[str, Any]]],
        inputs_payload: Dict[str, Any],
        record_build: Callable[..., Any],
        record_run: Callable[..., Any],
        record_artifact: Callable[..., Any],
        emit_job_event: Callable[..., Any],
    ) -> None:
        await record_build(
            status="FAILED",
            output_json=failure.payload,
        )
        await record_run(
            job_id=job.job_id,
            mode="deploy",
            status="FAILED",
            node_id=failure.node_id,
            row_count=failure.row_count,
            output_json=failure.payload,
            input_lakefs_commits=input_commit_payload,
            finished_at=utcnow(),
        )
        await record_artifact(
            status="FAILED",
            errors=failure.errors,
            inputs=inputs_payload,
        )
        await emit_job_event(status="FAILED", errors=failure.errors)
        self._log_output_validation_failure(mode_label="deploy", failure=failure)

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
        output_work: List[Dict[str, Any]] = []
        has_output_rows = False
        for node_id in target_node_ids:
            if lock:
                lock.raise_if_lost()
            output_df = tables.get(node_id, self._empty_dataframe())
            try:
                output_item = await self._prepare_output_work_item(
                    mode_label="build",
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
                    persist_output=True,
                    persisted_dfs=persisted_dfs,
                )
            except _OutputNodeValidationError as failure:
                await self._record_build_output_failure(
                    job=job,
                    failure=failure,
                    input_commit_payload=input_commit_payload,
                    inputs_payload=inputs_payload,
                    record_run=record_run,
                    record_artifact=record_artifact,
                    emit_job_event=emit_job_event,
                )
                return None
            if int(output_item.get("delta_row_count") or 0) > 0:
                has_output_rows = True
            output_work.append(output_item)
        return output_work, has_output_rows

    async def _create_build_branch(
        self,
        *,
        artifact_repo: str,
        pipeline_ref: str,
        run_ref: str,
        base_branch: str,
    ) -> str:
        build_branch = safe_lakefs_ref(f"build/{pipeline_ref}/{run_ref}")
        try:
            await self.lakefs_client.create_branch(  # type: ignore[union-attr]
                repository=artifact_repo,
                name=build_branch,
                source=base_branch,
            )
        except LakeFSConflictError:
            build_branch = safe_lakefs_ref(f"build/{pipeline_ref}/{run_ref}/{uuid4().hex[:8]}")
            await self.lakefs_client.create_branch(  # type: ignore[union-attr]
                repository=artifact_repo,
                name=build_branch,
                source=base_branch,
            )
        return build_branch

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
        build_outputs: List[Dict[str, Any]] = []
        for item in output_work:
            output_df = item["output_df"]
            materialized = await self._materialize_output_by_kind(
                output_kind=str(item.get("output_kind") or OUTPUT_KIND_DATASET),
                output_metadata=item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                df=output_df,
                artifact_bucket=artifact_repo,
                prefix=f"{build_branch}/{item['artifact_prefix']}",
                db_name=job.db_name,
                branch=base_branch,
                dataset_name=str(item.get("dataset_name") or ""),
                execution_semantics=execution_semantics,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                write_mode=str(item.get("runtime_write_mode") or output_write_mode),
                file_prefix=job.job_id,
                file_format=item["output_format"],
                partition_cols=item["partition_columns"],
                base_row_count=int(item.get("delta_row_count") or 0),
            )
            if _PYSPARK_AVAILABLE:
                try:
                    output_df.unpersist(blocking=False)
                except Exception as exc:
                    logger.warning("Failed to unpersist output dataframe: %s", exc, exc_info=True)
            resolved_runtime_write_mode = str(
                materialized.get("runtime_write_mode")
                or item.get("runtime_write_mode")
                or output_write_mode
            )
            delta_row_count = int(
                materialized.get("delta_row_count")
                if materialized.get("delta_row_count") is not None
                else int(item.get("delta_row_count") or 0)
            )
            row_count = delta_row_count
            if resolved_runtime_write_mode == "append":
                try:
                    existing_dataset = await self.dataset_registry.get_dataset_by_name(
                        db_name=job.db_name,
                        name=item["dataset_name"],
                        branch=base_branch,
                    )
                    if existing_dataset:
                        existing_version = await self.dataset_registry.get_latest_version(
                            dataset_id=existing_dataset.dataset_id
                        )
                        if existing_version and existing_version.row_count is not None:
                            row_count = int(existing_version.row_count) + delta_row_count
                except Exception as exc:
                    logger.debug("Failed to resolve previous row_count for incremental build: %s", exc)
            build_outputs.append(
                {
                    "node_id": item["node_id"],
                    "output_name": item["output_name"],
                    "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
                    "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                    "dataset_name": item["dataset_name"],
                    "artifact_key": materialized["artifact_key"],
                    "artifact_prefix": item["artifact_prefix"],
                    "output_format": materialized.get("output_format") or item["output_format"],
                    "partition_columns": materialized.get("partition_by") or item["partition_columns"],
                    "row_count": row_count,
                    "delta_row_count": delta_row_count if resolved_runtime_write_mode == "append" else None,
                    "write_mode_requested": materialized.get("write_mode_requested") or item.get("write_mode_requested"),
                    "write_mode_resolved": materialized.get("write_mode_resolved") or item.get("write_mode_resolved"),
                    "runtime_write_mode": resolved_runtime_write_mode,
                    "pk_columns": materialized.get("pk_columns") or item.get("pk_columns") or [],
                    "post_filtering_column": materialized.get("post_filtering_column") or item.get("post_filtering_column"),
                    "write_policy_hash": materialized.get("write_policy_hash") or item.get("write_policy_hash"),
                    "has_incremental_input": (
                        materialized.get("has_incremental_input")
                        if materialized.get("has_incremental_input") is not None
                        else item.get("has_incremental_input")
                    ),
                    "incremental_inputs_have_additive_updates": (
                        materialized.get("incremental_inputs_have_additive_updates")
                        if materialized.get("incremental_inputs_have_additive_updates") is not None
                        else item.get("incremental_inputs_have_additive_updates")
                    ),
                    "columns": item["schema_columns"],
                    "schema_hash": item["schema_hash"],
                    "schema_json": {"columns": item["schema_columns"]},
                    "rows": item["output_sample"],
                    "sample_row_count": len(item["output_sample"]),
                    "column_stats": item["column_stats"],
                }
            )
        return build_outputs

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
        if not self.storage or not self.lakefs_client:
            raise RuntimeError("lakeFS services are not configured")
        if lock:
            lock.raise_if_lost()

        artifact_repo = _resolve_lakefs_repository()
        await self.storage.create_bucket(artifact_repo)
        output_write_mode = "append" if execution_semantics in {"incremental", "streaming"} else "overwrite"
        base_branch = safe_lakefs_ref(job.branch or "main")
        prepared = await self._prepare_build_output_work(
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
        )
        if not prepared:
            return
        output_work, has_output_rows = prepared

        no_op_candidate = execution_semantics in {"incremental", "streaming"} and diff_empty_inputs
        if no_op_candidate and not has_output_rows:
            no_op_payload = {
                "outputs": [],
                "definition_hash": job.definition_hash,
                "branch": job.branch or "main",
                "execution_semantics": execution_semantics,
                "pipeline_spec_hash": pipeline_spec_hash,
                "pipeline_spec_commit_id": pipeline_spec_commit_id,
                "code_version": code_version,
                "spark_conf": spark_conf,
                "no_op": True,
                "input_snapshots": input_snapshots,
            }
            await record_build(status="SUCCESS", output_json=no_op_payload)
            await record_run(
                job_id=job.job_id,
                mode="build",
                status="SUCCESS",
                node_id=job.node_id,
                input_lakefs_commits=input_commit_payload,
                output_json=no_op_payload,
                finished_at=utcnow(),
            )
            await record_artifact(
                status="SUCCESS",
                outputs=[],
                inputs=inputs_payload,
            )
            await emit_job_event(status="SUCCESS", output={"no_op": True, "outputs": []})
            return

        build_branch = await self._create_build_branch(
            artifact_repo=artifact_repo,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
            base_branch=base_branch,
        )
        build_outputs = await self._materialize_build_outputs(
            job=job,
            artifact_repo=artifact_repo,
            build_branch=build_branch,
            base_branch=base_branch,
            output_work=output_work,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )

        if lock:
            lock.raise_if_lost()
        commit_id = await self.lakefs_client.commit(
            repository=artifact_repo,
            branch=build_branch,
            message=f"Build pipeline {job.db_name}/{pipeline_ref} ({job.job_id})",
            metadata={
                "pipeline_id": pipeline_ref,
                "pipeline_job_id": job.job_id,
                "db_name": job.db_name,
                "mode": "build",
            },
        )
        for item in build_outputs:
            artifact_prefix = str(item.get("artifact_prefix") or "").lstrip("/")
            if artifact_prefix:
                item["artifact_commit_key"] = build_s3_uri(artifact_repo, f"{commit_id}/{artifact_prefix}")

        artifact_id = await record_artifact(
            status="SUCCESS",
            outputs=build_outputs,
            inputs=inputs_payload,
            lakefs={
                "repository": artifact_repo,
                "branch": build_branch,
                "commit_id": commit_id,
            },
        )

        output_json = {
            "outputs": build_outputs,
            "definition_hash": job.definition_hash,
            "branch": job.branch or "main",
            "execution_semantics": execution_semantics,
            "pipeline_spec_hash": pipeline_spec_hash,
            "pipeline_spec_commit_id": pipeline_spec_commit_id,
            "code_version": code_version,
            "spark_conf": spark_conf,
            "lakefs": {
                "repository": artifact_repo,
                "base_branch": base_branch,
                "build_branch": build_branch,
                "commit_id": commit_id,
            },
            "ontology": (
                (definition.get("__build_meta__") or {}).get("ontology")
                if isinstance(definition.get("__build_meta__"), dict)
                else None
            ),
            "input_snapshots": input_snapshots,
        }
        if artifact_id:
            output_json["artifact_id"] = artifact_id

        await record_run(
            job_id=job.job_id,
            mode="build",
            status="SUCCESS",
            node_id=job.node_id,
            input_lakefs_commits=input_commit_payload,
            output_lakefs_commit_id=commit_id,
            output_json=output_json,
            finished_at=utcnow(),
        )
        output_payload = {"outputs": build_outputs}
        if artifact_id:
            output_payload["artifact_id"] = artifact_id
        await emit_job_event(
            status="SUCCESS",
            lakefs={
                "repository": artifact_repo,
                "base_branch": base_branch,
                "build_branch": build_branch,
                "commit_id": commit_id,
            },
            output=output_payload,
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
        output_work: List[Dict[str, Any]] = []
        has_output_rows = False
        for node_id in target_node_ids:
            if lock:
                lock.raise_if_lost()
            output_df = tables.get(node_id, self._empty_dataframe())
            try:
                output_item = await self._prepare_output_work_item(
                    mode_label="deploy",
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
                    persist_output=False,
                    persisted_dfs=persisted_dfs,
                )
            except _OutputNodeValidationError as failure:
                await self._record_deploy_output_failure(
                    job=job,
                    failure=failure,
                    input_commit_payload=input_commit_payload,
                    inputs_payload=inputs_payload,
                    record_build=record_build,
                    record_run=record_run,
                    record_artifact=record_artifact,
                    emit_job_event=emit_job_event,
                )
                return None
            if int(output_item.get("delta_row_count") or 0) > 0:
                has_output_rows = True
            output_work.append(output_item)
        return output_work, has_output_rows

    async def _create_deploy_run_branch(
        self,
        *,
        artifact_repo: str,
        base_branch: str,
        pipeline_ref: str,
        run_ref: str,
    ) -> str:
        run_branch = safe_lakefs_ref(f"run/{pipeline_ref}/{run_ref}")
        try:
            await self.lakefs_client.create_branch(  # type: ignore[union-attr]
                repository=artifact_repo,
                name=run_branch,
                source=base_branch,
            )
        except LakeFSConflictError:
            run_branch = safe_lakefs_ref(f"run/{pipeline_ref}/{run_ref}/{uuid4().hex[:8]}")
            await self.lakefs_client.create_branch(  # type: ignore[union-attr]
                repository=artifact_repo,
                name=run_branch,
                source=base_branch,
            )
        return run_branch

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
        staged_outputs: List[Dict[str, Any]] = []
        for item in output_work:
            branch_prefix = f"{run_branch}/{item['artifact_prefix']}"
            materialized = await self._materialize_output_by_kind(
                output_kind=str(item.get("output_kind") or OUTPUT_KIND_DATASET),
                output_metadata=item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                df=item["output_df"],
                artifact_bucket=artifact_repo,
                prefix=branch_prefix,
                db_name=job.db_name,
                branch=base_branch,
                dataset_name=str(item.get("dataset_name") or ""),
                execution_semantics=execution_semantics,
                incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
                write_mode=str(item.get("runtime_write_mode") or output_write_mode),
                file_prefix=job.job_id,
                file_format=item["output_format"],
                partition_cols=item["partition_columns"],
                base_row_count=int(item.get("row_count") or 0),
            )
            resolved_runtime_write_mode = str(
                materialized.get("runtime_write_mode")
                or item.get("runtime_write_mode")
                or output_write_mode
            )
            delta_row_count = int(
                materialized.get("delta_row_count")
                if materialized.get("delta_row_count") is not None
                else int(item.get("row_count") or 0)
            )
            staged_outputs.append(
                {
                    "node_id": item["node_id"],
                    "output_name": item.get("output_name"),
                    "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
                    "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                    "dataset_name": item["dataset_name"],
                    "artifact_prefix": item["artifact_prefix"],
                    "output_format": materialized.get("output_format") or item["output_format"],
                    "partition_columns": materialized.get("partition_by") or item["partition_columns"],
                    "row_count": delta_row_count,
                    "write_mode_requested": materialized.get("write_mode_requested") or item.get("write_mode_requested"),
                    "write_mode_resolved": materialized.get("write_mode_resolved") or item.get("write_mode_resolved"),
                    "runtime_write_mode": resolved_runtime_write_mode,
                    "pk_columns": materialized.get("pk_columns") or item.get("pk_columns") or [],
                    "post_filtering_column": materialized.get("post_filtering_column") or item.get("post_filtering_column"),
                    "write_policy_hash": materialized.get("write_policy_hash") or item.get("write_policy_hash"),
                    "has_incremental_input": (
                        materialized.get("has_incremental_input")
                        if materialized.get("has_incremental_input") is not None
                        else item.get("has_incremental_input")
                    ),
                    "incremental_inputs_have_additive_updates": (
                        materialized.get("incremental_inputs_have_additive_updates")
                        if materialized.get("incremental_inputs_have_additive_updates") is not None
                        else item.get("incremental_inputs_have_additive_updates")
                    ),
                    "columns": item["columns"],
                    "rows": item["rows"],
                    "sample_row_count": item["sample_row_count"],
                    "column_stats": item["column_stats"],
                }
            )
        return staged_outputs

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
        if lock:
            lock.raise_if_lost()
        commit_id = await self.lakefs_client.commit(  # type: ignore[union-attr]
            repository=artifact_repo,
            branch=run_branch,
            message=f"Build pipeline outputs {job.db_name}/{pipeline_ref} ({job.job_id})",
            metadata={
                "pipeline_id": pipeline_ref,
                "pipeline_job_id": job.job_id,
                "db_name": job.db_name,
                "mode": "deploy",
            },
        )
        if lock:
            lock.raise_if_lost()
        merge_commit_id = await self.lakefs_client.merge(  # type: ignore[union-attr]
            repository=artifact_repo,
            source_ref=run_branch,
            destination_branch=base_branch,
            message=f"Publish pipeline outputs {job.db_name}/{pipeline_ref} ({job.job_id})",
            metadata={
                "pipeline_id": pipeline_ref,
                "pipeline_job_id": job.job_id,
                "db_name": job.db_name,
                "mode": "deploy",
                "run_branch": run_branch,
                "run_commit_id": commit_id,
            },
            allow_empty=False,
        )
        try:
            await self.lakefs_client.delete_branch(repository=artifact_repo, name=run_branch)  # type: ignore[union-attr]
        except Exception as exc:
            logger.info("Failed to delete run branch %s after merge: %s", run_branch, exc)
        return commit_id, merge_commit_id

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
        run_branch = await self._create_deploy_run_branch(
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            pipeline_ref=pipeline_ref,
            run_ref=run_ref,
        )
        staged_outputs = await self._materialize_deploy_staged_outputs(
            job=job,
            run_branch=run_branch,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            output_work=output_work,
            output_write_mode=output_write_mode,
            execution_semantics=execution_semantics,
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )
        commit_id, merge_commit_id = await self._commit_and_merge_deploy_branch(
            job=job,
            lock=lock,
            pipeline_ref=pipeline_ref,
            artifact_repo=artifact_repo,
            run_branch=run_branch,
            base_branch=base_branch,
        )
        return run_branch, commit_id, merge_commit_id, staged_outputs

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
        return {
            "columns": schema_columns,
            "rows": output_sample,
            "row_count": total_row_count,
            "delta_row_count": delta_row_count if resolved_runtime_write_mode == "append" else None,
            "write_mode_requested": item.get("write_mode_requested"),
            "write_mode_resolved": item.get("write_mode_resolved"),
            "runtime_write_mode": resolved_runtime_write_mode,
            "pk_columns": item.get("pk_columns") or [],
            "write_policy_hash": item.get("write_policy_hash"),
            "has_incremental_input": item.get("has_incremental_input"),
            "incremental_inputs_have_additive_updates": item.get("incremental_inputs_have_additive_updates"),
            "sample_row_count": len(output_sample),
            "column_stats": item.get("column_stats") or {},
        }

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
        artifact_prefix = str(item.get("artifact_prefix") or "").lstrip("/")
        artifact_key = build_s3_uri(artifact_repo, f"{merge_commit_id}/{artifact_prefix}")
        dataset_name = str(item.get("dataset_name") or "")
        schema_columns = item.get("columns") if isinstance(item.get("columns"), list) else []
        output_sample = item.get("rows") if isinstance(item.get("rows"), list) else []
        delta_row_count = int(item.get("row_count") or 0)

        dataset = await self.dataset_registry.get_dataset_by_name(
            db_name=job.db_name,
            name=dataset_name,
            branch=base_branch,
        )
        resolved_runtime_write_mode = str(item.get("runtime_write_mode") or output_write_mode)
        total_row_count = delta_row_count
        if resolved_runtime_write_mode == "append" and dataset:
            previous = await self.dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
            if previous and previous.row_count is not None:
                total_row_count = int(previous.row_count) + delta_row_count
        if not dataset:
            dataset = await self.dataset_registry.create_dataset(
                db_name=job.db_name,
                name=dataset_name,
                description=None,
                source_type="pipeline",
                source_ref=pipeline_ref,
                schema_json={"columns": schema_columns},
                branch=base_branch,
            )
        version = await self.dataset_registry.add_version(
            dataset_id=dataset.dataset_id,
            lakefs_commit_id=merge_commit_id,
            artifact_key=artifact_key,
            row_count=total_row_count,
            sample_json=self._build_deploy_version_sample_payload(
                item=item,
                schema_columns=schema_columns,
                output_sample=output_sample,
                total_row_count=total_row_count,
                delta_row_count=delta_row_count,
                resolved_runtime_write_mode=resolved_runtime_write_mode,
            ),
            schema_json={"columns": schema_columns},
        )
        objectify_job_id = await self._maybe_enqueue_objectify_job(dataset=dataset, version=version)
        relationship_job_ids = await self._maybe_enqueue_relationship_jobs(dataset=dataset, version=version)
        return {
            "dataset_name": dataset_name,
            "artifact_key": artifact_key,
            "total_row_count": total_row_count,
            "delta_row_count": delta_row_count,
            "resolved_runtime_write_mode": resolved_runtime_write_mode,
            "objectify_job_id": objectify_job_id,
            "relationship_job_ids": relationship_job_ids,
        }

    def _build_deploy_output_record(
        self,
        *,
        item: Dict[str, Any],
        publication: Dict[str, Any],
        merge_commit_id: str,
        base_branch: str,
    ) -> Dict[str, Any]:
        return {
            "node_id": item.get("node_id"),
            "output_name": item.get("output_name"),
            "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
            "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
            "dataset_name": publication["dataset_name"],
            "artifact_key": publication["artifact_key"],
            "row_count": publication["total_row_count"],
            "delta_row_count": (
                publication["delta_row_count"]
                if publication["resolved_runtime_write_mode"] == "append"
                else None
            ),
            "write_mode_requested": item.get("write_mode_requested"),
            "write_mode_resolved": item.get("write_mode_resolved"),
            "runtime_write_mode": publication["resolved_runtime_write_mode"],
            "pk_columns": item.get("pk_columns") or [],
            "post_filtering_column": item.get("post_filtering_column"),
            "write_policy_hash": item.get("write_policy_hash"),
            "has_incremental_input": item.get("has_incremental_input"),
            "incremental_inputs_have_additive_updates": item.get("incremental_inputs_have_additive_updates"),
            "lakefs_commit_id": merge_commit_id,
            "lakefs_branch": base_branch,
            "objectify_job_id": publication["objectify_job_id"],
            "relationship_job_ids": publication["relationship_job_ids"],
        }

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
        parsed = parse_s3_uri(artifact_key)
        if not parsed:
            return
        bucket, key = parsed

        async def _record_pipeline_output_lineage() -> None:
            await self.lineage.record_link(  # type: ignore[union-attr]
                from_node_id=LineageStore.node_aggregate("Pipeline", pipeline_ref),
                to_node_id=LineageStore.node_artifact("s3", bucket, key),
                edge_type=EDGE_PIPELINE_OUTPUT_STORED,
                occurred_at=utcnow(),
                db_name=job.db_name,
                edge_metadata={
                    "db_name": job.db_name,
                    "pipeline_id": pipeline_ref,
                    "artifact_key": artifact_key,
                    "dataset_name": dataset_name,
                    "node_id": node_id,
                    "lakefs_commit_id": merge_commit_id,
                    "lakefs_branch": base_branch,
                },
            )

        await record_lineage_or_raise(
            lineage_store=self.lineage,
            required=self.lineage_required,
            record_call=_record_pipeline_output_lineage,
            logger=logger,
            operation="pipeline_worker.deploy.pipeline_output_stored",
            zone=RuntimeZone.CORE,
            context={
                "db_name": job.db_name,
                "pipeline_id": pipeline_ref,
                "artifact_key": artifact_key,
                "dataset_name": dataset_name,
                "node_id": node_id,
            },
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
        build_outputs: List[Dict[str, Any]] = []
        for item in staged_outputs:
            publication = await self._resolve_deploy_output_publication(
                job=job,
                pipeline_ref=pipeline_ref,
                artifact_repo=artifact_repo,
                base_branch=base_branch,
                merge_commit_id=merge_commit_id,
                output_write_mode=output_write_mode,
                item=item,
            )
            build_outputs.append(
                self._build_deploy_output_record(
                    item=item,
                    publication=publication,
                    merge_commit_id=merge_commit_id,
                    base_branch=base_branch,
                )
            )
            await self._record_deploy_output_lineage(
                job=job,
                pipeline_ref=pipeline_ref,
                artifact_key=str(publication["artifact_key"]),
                dataset_name=str(publication["dataset_name"]),
                node_id=str(item.get("node_id") or "") or None,
                merge_commit_id=merge_commit_id,
                base_branch=base_branch,
            )
        return build_outputs

    def _merge_watermark_keys(
        self,
        *,
        next_watermark: Optional[Any],
        previous_watermark: Optional[Any],
        previous_watermark_keys: List[str],
        next_watermark_keys: List[str],
    ) -> List[str]:
        merged_keys: List[str] = []
        if next_watermark is not None:
            if (
                previous_watermark is not None
                and previous_watermark_keys
                and _watermark_values_match(previous_watermark, next_watermark)
            ):
                merged_keys = [*previous_watermark_keys, *next_watermark_keys]
            else:
                merged_keys = list(next_watermark_keys)
        elif previous_watermark is not None and previous_watermark_keys:
            merged_keys = list(previous_watermark_keys)
        if not merged_keys:
            return []
        deduped_keys: List[str] = []
        seen_keys: set[str] = set()
        for item in merged_keys:
            if item in seen_keys:
                continue
            seen_keys.add(item)
            deduped_keys.append(item)
        return deduped_keys

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
        watermark_update: Optional[Dict[str, Any]] = None
        if (
            execution_semantics not in {"incremental", "streaming"}
            or not resolved_pipeline_id
            or not input_snapshots
        ):
            return watermark_update

        next_watermark = (
            _max_watermark_from_snapshots(input_snapshots, watermark_column=watermark_column)
            if watermark_column
            else None
        )
        next_watermark_keys = (
            _collect_watermark_keys_from_snapshots(
                input_snapshots,
                watermark_column=watermark_column,
                watermark_value=next_watermark,
            )
            if watermark_column and next_watermark is not None
            else []
        )
        input_commit_map = _collect_input_commit_map(input_snapshots)
        if (next_watermark is None and not input_commit_map) or not self.pipeline_registry:
            return watermark_update

        watermarks_payload: Dict[str, Any] = {}
        if watermark_column:
            watermarks_payload["watermark_column"] = watermark_column
            if next_watermark is not None:
                watermarks_payload["watermark_value"] = next_watermark
            elif previous_watermark is not None:
                watermarks_payload["watermark_value"] = previous_watermark
            merged_keys = self._merge_watermark_keys(
                next_watermark=next_watermark,
                previous_watermark=previous_watermark,
                previous_watermark_keys=previous_watermark_keys,
                next_watermark_keys=next_watermark_keys,
            )
            if merged_keys:
                watermarks_payload["watermark_keys"] = merged_keys
        if input_commit_map:
            watermarks_payload["input_commits"] = input_commit_map

        try:
            await self.pipeline_registry.upsert_watermarks(
                pipeline_id=resolved_pipeline_id,
                branch=branch,
                watermarks=watermarks_payload,
            )
            watermark_update = watermarks_payload
        except Exception as exc:
            logger.warning(
                "Failed to persist pipeline watermarks (pipeline_id=%s): %s",
                resolved_pipeline_id,
                exc,
            )
        return watermark_update

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
        if not self.storage or not self.lakefs_client:
            raise RuntimeError("lakeFS services are not configured")
        if lock:
            lock.raise_if_lost()

        artifact_repo = _resolve_lakefs_repository()
        await self.storage.create_bucket(artifact_repo)
        base_branch = safe_lakefs_ref(job.branch or "main")
        output_write_mode = "append" if execution_semantics in {"incremental", "streaming"} else "overwrite"
        prepared = await self._prepare_deploy_output_work(
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
        )
        if not prepared:
            return

        output_work, has_output_rows = prepared
        no_op_candidate = execution_semantics in {"incremental", "streaming"} and diff_empty_inputs
        if no_op_candidate and not has_output_rows:
            no_op_payload = {
                "outputs": [],
                "definition_hash": job.definition_hash,
                "branch": base_branch,
                "execution_semantics": execution_semantics,
                "pipeline_spec_hash": pipeline_spec_hash,
                "pipeline_spec_commit_id": pipeline_spec_commit_id,
                "code_version": code_version,
                "spark_conf": spark_conf,
                "no_op": True,
                "input_snapshots": input_snapshots,
            }
            await record_build(
                status="DEPLOYED",
                output_json=no_op_payload,
            )
            await record_run(
                job_id=job.job_id,
                mode="deploy",
                status="DEPLOYED",
                node_id=job.node_id,
                input_lakefs_commits=input_commit_payload,
                output_json=no_op_payload,
                finished_at=utcnow(),
            )
            await emit_job_event(status="DEPLOYED", output={"no_op": True, "outputs": []})
            return

        run_branch, commit_id, merge_commit_id, staged_outputs = await self._stage_deploy_outputs(
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
        build_outputs = await self._publish_deploy_outputs(
            job=job,
            pipeline_ref=pipeline_ref,
            artifact_repo=artifact_repo,
            base_branch=base_branch,
            merge_commit_id=merge_commit_id,
            output_write_mode=output_write_mode,
            staged_outputs=staged_outputs,
        )

        await record_build(
            status="DEPLOYED",
            output_json={
                "outputs": build_outputs,
                "definition_hash": job.definition_hash,
                "branch": base_branch,
                "execution_semantics": execution_semantics,
                "input_snapshots": input_snapshots,
                "lakefs": {
                    "repository": artifact_repo,
                    "base_branch": base_branch,
                    "run_branch": run_branch,
                    "commit_id": commit_id,
                    "merge_commit_id": merge_commit_id,
                },
            },
            deployed_commit_id=merge_commit_id,
        )

        watermark_update = await self._persist_deploy_watermarks(
            resolved_pipeline_id=resolved_pipeline_id,
            branch=job.branch or "main",
            execution_semantics=execution_semantics,
            input_snapshots=input_snapshots,
            watermark_column=watermark_column,
            previous_watermark=previous_watermark,
            previous_watermark_keys=previous_watermark_keys,
        )

        await record_run(
            job_id=job.job_id,
            mode="deploy",
            status="DEPLOYED",
            node_id=job.node_id,
            input_lakefs_commits=input_commit_payload,
            output_lakefs_commit_id=merge_commit_id,
            output_json={
                "outputs": build_outputs,
                "definition_hash": job.definition_hash,
                "branch": base_branch,
                "execution_semantics": execution_semantics,
                "pipeline_spec_hash": pipeline_spec_hash,
                "pipeline_spec_commit_id": pipeline_spec_commit_id,
                "code_version": code_version,
                "spark_conf": spark_conf,
                "input_snapshots": input_snapshots,
                "watermarks": watermark_update,
                "lakefs": {
                    "repository": artifact_repo,
                    "base_branch": base_branch,
                    "run_branch": run_branch,
                    "commit_id": commit_id,
                    "merge_commit_id": merge_commit_id,
                },
            },
            finished_at=utcnow(),
        )
        await emit_job_event(
            status="DEPLOYED",
            lakefs={
                "repository": artifact_repo,
                "base_branch": base_branch,
                "run_branch": run_branch,
                "commit_id": commit_id,
                "merge_commit_id": merge_commit_id,
            },
            output={"outputs": build_outputs},
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
        metadata = node.get("metadata") or {}
        node_type = str(node.get("type") or "transform")
        inputs = [tables[src] for src in incoming.get(node_id, []) if src in tables]

        if node_type == "input":
            df = await self._load_input_dataframe(
                job.db_name,
                metadata,
                temp_dirs,
                job.branch,
                node_id=node_id,
                input_snapshots=input_snapshots,
                previous_commit_id=previous_input_commits.get(node_id),
                use_lakefs_diff=use_lakefs_diff and execution_semantics in {"incremental", "streaming"},
                watermark_column=watermark_column if execution_semantics in {"incremental", "streaming"} else None,
                watermark_after=previous_watermark if execution_semantics in {"incremental", "streaming"} else None,
                watermark_keys=previous_watermark_keys if execution_semantics in {"incremental", "streaming"} else None,
            )
            if is_preview:
                sampling_strategy = self._resolve_sampling_strategy(
                    metadata,
                    preview_meta,
                    preview_limit=preview_input_sample_limit,
                )
                if sampling_strategy:
                    df = self._apply_sampling_strategy(
                        df,
                        sampling_strategy,
                        node_id=node_id,
                        seed=preview_sampling_seed,
                    )
                    input_sampling[node_id] = sampling_strategy
                    self._attach_sampling_snapshot(
                        input_snapshots,
                        node_id=node_id,
                        sampling_strategy=sampling_strategy,
                    )
            return df

        if node_type == "output":
            return inputs[0] if inputs else self._empty_dataframe()

        transform_metadata = dict(metadata)
        operation = normalize_operation(transform_metadata.get("operation"))
        if operation == "udf" and self._udf_spark_parity_enabled:
            resolved = await resolve_udf_reference(
                metadata=transform_metadata,
                pipeline_registry=self.pipeline_registry,
                require_reference=self._udf_require_reference,
                require_version_pinning=self._udf_require_version_pinning,
                code_cache=self._udf_code_cache,
            )
            transform_metadata["__resolved_udf_code"] = resolved.code
        return self._apply_transform(transform_metadata, inputs, parameters)

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
        if not self.objectify_registry:
            return None
        if not getattr(version, "artifact_key", None):
            return None
        resolved_output_name = getattr(dataset, "name", None)
        schema_hash = compute_schema_hash(version.sample_json.get("columns")) if isinstance(getattr(version, "sample_json", None), dict) else None
        if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
            schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])

        # OMS-first resolution via MappingSpecResolver, fallback to PostgreSQL
        mapping_spec = None
        if self.mapping_resolver:
            try:
                mapping_spec = await self.mapping_resolver.resolve(
                    db_name=dataset.db_name,
                    dataset_id=dataset.dataset_id,
                    branch=dataset.branch,
                    schema_hash=schema_hash,
                    artifact_output_name=resolved_output_name,
                )
            except Exception as resolver_exc:
                logger.warning(
                    "MappingSpecResolver failed (dataset=%s), falling back to PostgreSQL: %s",
                    dataset.dataset_id, resolver_exc,
                )
        if not mapping_spec:
            mapping_spec = await self.objectify_registry.get_active_mapping_spec(
                dataset_id=dataset.dataset_id,
                dataset_branch=dataset.branch,
                artifact_output_name=resolved_output_name,
                schema_hash=schema_hash,
            )
        if not mapping_spec or not mapping_spec.auto_sync:
            if self.dataset_registry and schema_hash:
                try:
                    candidates = await self.objectify_registry.list_mapping_specs(dataset_id=dataset.dataset_id)
                    mismatched = [
                        spec.schema_hash
                        for spec in candidates
                        if spec.artifact_output_name == resolved_output_name and spec.schema_hash != schema_hash
                    ]
                    if mismatched:
                        current_columns: List[Dict[str, Any]] = []
                        if isinstance(getattr(version, "sample_json", None), dict):
                            current_columns = version.sample_json.get("columns") or []
                        if not current_columns and isinstance(getattr(dataset, "schema_json", None), dict):
                            current_columns = dataset.schema_json.get("columns") or []

                        expected_columns: List[Dict[str, Any]] = []
                        latest_spec = None
                        if candidates:
                            latest_spec = max(candidates, key=lambda spec: spec.created_at or datetime.min)
                        if latest_spec and latest_spec.backing_datasource_version_id:
                            backing_version = await self.dataset_registry.get_backing_datasource_version(
                                version_id=latest_spec.backing_datasource_version_id
                            )
                            if backing_version:
                                expected_version = await self.dataset_registry.get_version(
                                    version_id=backing_version.dataset_version_id
                                )
                                if expected_version and isinstance(expected_version.sample_json, dict):
                                    expected_columns = expected_version.sample_json.get("columns") or []
                                if not expected_columns and expected_version and isinstance(expected_version.schema_json, dict):
                                    expected_columns = expected_version.schema_json.get("columns") or []

                        schema_diff = (
                            _schema_diff(current_columns=current_columns, expected_columns=expected_columns)
                            if current_columns and expected_columns
                            else None
                        )
                        await self.dataset_registry.record_gate_result(
                            scope="objectify_schema",
                            subject_type="dataset_version",
                            subject_id=version.version_id,
                            status="FAIL",
                            details={
                                "dataset_id": dataset.dataset_id,
                                "dataset_version_id": version.version_id,
                                "observed_schema_hash": schema_hash,
                                "expected_schema_hashes": sorted(set(mismatched)),
                                "schema_diff": schema_diff,
                                "message": "Schema hash mismatch; migration required",
                            },
                        )
                except Exception as exc:
                    logger.warning("Failed to record schema gate: %s", exc)
            return None
        # For OMS-sourced specs, use class_id as dedupe key component (not uuid mapping_spec_id)
        oms_sourced = is_oms_mapping_spec(mapping_spec.mapping_spec_id) if mapping_spec.mapping_spec_id else False
        dedupe_spec_id = mapping_spec.target_class_id if oms_sourced else mapping_spec.mapping_spec_id
        dedupe_key = self.objectify_registry.build_dedupe_key(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            mapping_spec_id=dedupe_spec_id,
            mapping_spec_version=mapping_spec.version,
            dataset_version_id=version.version_id,
            artifact_id=None,
            artifact_output_name=resolved_output_name,
        )
        existing = await self.objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
        if existing:
            return existing.job_id
        job_id = str(uuid4())
        options = dict(mapping_spec.options or {})
        # OMS-sourced specs: don't store "oms:..." in PostgreSQL uuid column
        pg_mapping_spec_id = None if oms_sourced else mapping_spec.mapping_spec_id
        pg_mapping_spec_version = None if oms_sourced else mapping_spec.version
        job = ObjectifyJob(
            job_id=job_id,
            db_name=dataset.db_name,
            dataset_id=dataset.dataset_id,
            dataset_version_id=version.version_id,
            artifact_output_name=resolved_output_name,
            dedupe_key=dedupe_key,
            dataset_branch=dataset.branch,
            artifact_key=version.artifact_key or "",
            mapping_spec_id=pg_mapping_spec_id,
            mapping_spec_version=pg_mapping_spec_version,
            target_class_id=mapping_spec.target_class_id,
            ontology_branch=options.get("ontology_branch"),
            max_rows=options.get("max_rows"),
            batch_size=options.get("batch_size"),
            allow_partial=bool(options.get("allow_partial")),
            options=options,
        )
        try:
            if self.objectify_job_queue:
                await self.objectify_job_queue.publish(job, require_delivery=False)
        except Exception as exc:
            logger.warning("Failed to enqueue objectify job %s: %s", job_id, exc)
        return job_id

    async def _maybe_enqueue_relationship_jobs(self, *, dataset, version) -> List[str]:
        if not self.objectify_registry or not self.dataset_registry:
            return []
        if not getattr(version, "artifact_key", None):
            return []
        try:
            specs = await self.dataset_registry.list_relationship_specs(
                dataset_id=dataset.dataset_id,
                status="ACTIVE",
            )
        except Exception as exc:
            logger.warning("Failed to load relationship specs: %s", exc)
            return []

        job_ids: List[str] = []
        schema_hash = compute_schema_hash(version.sample_json.get("columns")) if isinstance(getattr(version, "sample_json", None), dict) else None
        if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
            schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])

        for spec in specs:
            if not spec.auto_sync:
                continue
            if spec.dataset_version_id and spec.dataset_version_id != version.version_id:
                continue
            mapping_spec = await self.objectify_registry.get_mapping_spec(mapping_spec_id=spec.mapping_spec_id)
            if not mapping_spec or not mapping_spec.auto_sync:
                continue
            if schema_hash and mapping_spec.schema_hash and mapping_spec.schema_hash != schema_hash:
                try:
                    current_columns: List[Dict[str, Any]] = []
                    if isinstance(getattr(version, "sample_json", None), dict):
                        current_columns = version.sample_json.get("columns") or []
                    if not current_columns and isinstance(getattr(dataset, "schema_json", None), dict):
                        current_columns = dataset.schema_json.get("columns") or []

                    expected_columns: List[Dict[str, Any]] = []
                    if mapping_spec.backing_datasource_version_id:
                        backing_version = await self.dataset_registry.get_backing_datasource_version(
                            version_id=mapping_spec.backing_datasource_version_id
                        )
                        if backing_version:
                            expected_version = await self.dataset_registry.get_version(
                                version_id=backing_version.dataset_version_id
                            )
                            if expected_version and isinstance(expected_version.sample_json, dict):
                                expected_columns = expected_version.sample_json.get("columns") or []
                            if not expected_columns and expected_version and isinstance(expected_version.schema_json, dict):
                                expected_columns = expected_version.schema_json.get("columns") or []

                    schema_diff = (
                        _schema_diff(current_columns=current_columns, expected_columns=expected_columns)
                        if current_columns and expected_columns
                        else None
                    )
                    await self.dataset_registry.record_gate_result(
                        scope="relationship_schema",
                        subject_type="dataset_version",
                        subject_id=version.version_id,
                        status="FAIL",
                        details={
                            "dataset_id": dataset.dataset_id,
                            "dataset_version_id": version.version_id,
                            "relationship_spec_id": spec.relationship_spec_id,
                            "observed_schema_hash": schema_hash,
                            "expected_schema_hash": mapping_spec.schema_hash,
                            "schema_diff": schema_diff,
                            "message": "Relationship mapping schema hash mismatch",
                        },
                    )
                except Exception as exc:
                    logger.warning("Failed to record relationship schema gate: %s", exc)
                continue
            dedupe_key = self.objectify_registry.build_dedupe_key(
                dataset_id=dataset.dataset_id,
                dataset_branch=dataset.branch,
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=spec.mapping_spec_version,
                dataset_version_id=version.version_id,
                artifact_id=None,
                artifact_output_name=dataset.name,
            )
            existing = await self.objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
            if existing:
                job_ids.append(existing.job_id)
                continue
            job_id = str(uuid4())
            options = dict(mapping_spec.options or {})
            job = ObjectifyJob(
                job_id=job_id,
                db_name=dataset.db_name,
                dataset_id=dataset.dataset_id,
                dataset_version_id=version.version_id,
                artifact_output_name=dataset.name,
                dedupe_key=dedupe_key,
                dataset_branch=dataset.branch,
                artifact_key=version.artifact_key or "",
                mapping_spec_id=mapping_spec.mapping_spec_id,
                mapping_spec_version=spec.mapping_spec_version,
                target_class_id=mapping_spec.target_class_id,
                ontology_branch=options.get("ontology_branch"),
                max_rows=options.get("max_rows"),
                batch_size=options.get("batch_size"),
                allow_partial=bool(options.get("allow_partial")),
                options=options,
            )
            try:
                if self.objectify_job_queue:
                    await self.objectify_job_queue.publish(job, require_delivery=False)
                job_ids.append(job_id)
            except Exception as exc:
                logger.warning("Failed to enqueue relationship job %s: %s", job_id, exc)

        return job_ids

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
        if not self.dataset_registry:
            return self._empty_dataframe()
        resolved_db = str(db_name or "").strip()
        resolved_branch = str(branch or "").strip() or "main"
        resolved_name = str(dataset_name or "").strip()
        if not resolved_db or not resolved_name:
            return self._empty_dataframe()

        dataset = await self.dataset_registry.get_dataset_by_name(
            db_name=resolved_db,
            name=resolved_name,
            branch=resolved_branch,
        )
        if not dataset:
            return self._empty_dataframe()
        version = await self.dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
        if not version or not str(version.artifact_key or "").strip():
            return self._empty_dataframe()
        parsed = parse_s3_uri(str(version.artifact_key))
        if not parsed:
            raise ValueError(f"Invalid artifact_key for dataset {resolved_name}: {version.artifact_key}")
        bucket, key = parsed
        temp_dirs: List[str] = []
        try:
            return await self._load_artifact_dataframe(bucket, key, temp_dirs)
        finally:
            for temp_dir in temp_dirs:
                shutil.rmtree(temp_dir, ignore_errors=True)

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
        return {
            "node_id": node_id,
            "dataset_id": context.dataset.dataset_id,
            "dataset_name": context.dataset.name,
            "dataset_branch": context.resolved_branch,
            "requested_dataset_branch": context.requested_branch,
            "used_fallback": context.used_fallback,
            "lakefs_commit_id": lakefs_commit_id,
            "version_id": context.version.version_id,
            "artifact_key": context.version.artifact_key,
        }

    def _annotate_diff_snapshot(
        self,
        *,
        snapshot: Dict[str, Any],
        previous_commit_id: Optional[str],
        diff_ok: bool,
        diff_paths_count: int,
    ) -> None:
        snapshot.update(
            {
                "previous_commit_id": previous_commit_id,
                "diff_requested": True,
                "diff_ok": diff_ok,
                "diff_paths_count": diff_paths_count,
                "diff_empty": bool(diff_ok and diff_paths_count == 0),
            }
        )

    def _append_input_snapshot(
        self,
        *,
        input_snapshots: Optional[list[dict[str, Any]]],
        snapshot: Optional[dict[str, Any]],
    ) -> None:
        if input_snapshots is not None and snapshot is not None:
            input_snapshots.append(snapshot)

    async def _load_external_input_dataframe_with_snapshot(
        self,
        *,
        read_config: Dict[str, Any],
        node_id: str,
        temp_dirs: list[str],
        input_snapshots: Optional[list[dict[str, Any]]],
    ) -> DataFrame:
        read_mode = _resolve_external_read_mode(read_config=read_config)
        df = await self._run_spark(
            lambda: self._load_external_input_dataframe(read_config, node_id=node_id, temp_dirs=temp_dirs),
            label=f"external_input:{node_id}",
        )
        if input_snapshots is None:
            return df

        fmt = (
            str(
                read_config.get("format")
                or read_config.get("file_format")
                or read_config.get("fileFormat")
                or ""
            )
            .strip()
            .lower()
            or None
        )
        options = self._normalize_read_options(read_config)
        stream_trigger_mode: Optional[str] = None
        stream_timeout_seconds: Optional[int] = None
        if read_mode == "streaming":
            stream_trigger_mode = _resolve_streaming_trigger_mode(
                read_config=read_config,
                default_mode=self.spark_streaming_default_trigger,
            )
            stream_timeout_seconds = _resolve_streaming_timeout_seconds(
                read_config=read_config,
                default_seconds=self.spark_streaming_await_timeout_seconds,
            )
        input_snapshots.append(
            {
                "node_id": node_id,
                "source_type": "external",
                "read_mode": read_mode,
                "format": fmt,
                "stream_trigger_mode": stream_trigger_mode,
                "stream_timeout_seconds": stream_timeout_seconds,
                "options": self._mask_sensitive_options(options),
            }
        )
        return df

    async def _resolve_dataset_input_load_context(
        self,
        *,
        db_name: str,
        node_id: str,
        selection: Any,
        input_snapshots: Optional[list[dict[str, Any]]],
    ) -> Tuple[_DatasetInputLoadContext, Optional[dict[str, Any]]]:
        dataset_id = selection.dataset_id
        dataset_name = selection.dataset_name
        requested_branch = selection.requested_branch

        if not self.dataset_registry:
            raise RuntimeError("Dataset registry not initialized")
        if not dataset_id and not dataset_name:
            raise ValueError(f"Input node {node_id} is missing dataset selection")

        resolution = await resolve_dataset_version(
            self.dataset_registry,
            db_name=db_name,
            selection=selection,
        )
        dataset = resolution.dataset
        version = resolution.version
        resolved_branch = resolution.resolved_branch

        if not dataset:
            raise FileNotFoundError(
                f"Input node {node_id} dataset not found (datasetId={dataset_id} datasetName={dataset_name} requested_branch={requested_branch})"
            )
        if not version:
            raise RuntimeError(
                f"Input node {node_id} dataset has no versions in requested/fallback branches (datasetName={dataset.name} requested_branch={requested_branch})"
            )
        if not version.artifact_key:
            raise RuntimeError(
                f"Input node {node_id} dataset version has no artifact_key (dataset_id={dataset.dataset_id})"
            )

        parsed = parse_s3_uri(version.artifact_key)
        if not parsed:
            raise ValueError(
                f"Input node {node_id} artifact_key is not a valid s3:// URI: {version.artifact_key}"
            )
        bucket, key = parsed
        current_commit_id = str(version.lakefs_commit_id or "").strip()
        resolved_branch_value = resolved_branch or dataset.branch or requested_branch

        context = _DatasetInputLoadContext(
            dataset=dataset,
            version=version,
            requested_branch=requested_branch,
            resolved_branch=resolved_branch_value,
            used_fallback=resolution.used_fallback,
            bucket=bucket,
            key=key,
            current_commit_id=current_commit_id,
            artifact_prefix=self._strip_commit_prefix(key, current_commit_id),
        )
        snapshot = (
            self._build_dataset_input_snapshot(
                node_id=node_id,
                context=context,
                lakefs_commit_id=version.lakefs_commit_id,
            )
            if input_snapshots is not None
            else None
        )
        return context, snapshot

    async def _load_full_dataset_input_dataframe(
        self,
        *,
        context: _DatasetInputLoadContext,
        metadata: Dict[str, Any],
        temp_dirs: list[str],
        node_id: str,
    ) -> DataFrame:
        source_type = str(getattr(context.dataset, "source_type", "") or "").strip().lower()
        read_config = metadata.get("read") if isinstance(metadata.get("read"), dict) else {}
        if source_type == "media":
            df = await self._load_media_prefix_dataframe(context.bucket, context.key, node_id=node_id)
        else:
            df = await self._load_artifact_dataframe(
                context.bucket,
                context.key,
                temp_dirs,
                read_config=read_config,
            )
        return self._apply_schema_casts(df, dataset=context.dataset, version=context.version)

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
        if not bool(use_lakefs_diff and previous_commit_id and context.current_commit_id and self.lakefs_client):
            return None
        diff_paths: List[str] = []
        diff_ok = True
        dataset_branch_for_diff = safe_lakefs_ref(context.resolved_branch or "main")
        if previous_commit_id != context.current_commit_id:
            diff_paths, diff_ok = await self._list_lakefs_diff_paths(
                repository=context.bucket,
                ref=dataset_branch_for_diff,
                since=previous_commit_id,
                prefix=context.artifact_prefix,
                node_id=node_id,
            )
        diff_paths_count = len(diff_paths)
        parquet_paths = [path for path in diff_paths if path.endswith(".parquet")]
        if base_snapshot is not None:
            self._annotate_diff_snapshot(
                snapshot=base_snapshot,
                previous_commit_id=previous_commit_id,
                diff_ok=diff_ok,
                diff_paths_count=diff_paths_count,
            )
        if diff_ok and diff_paths_count == 0:
            self._append_input_snapshot(input_snapshots=input_snapshots, snapshot=base_snapshot)
            return self._empty_dataframe()
        if not parquet_paths:
            if diff_ok and diff_paths_count:
                logger.warning(
                    "lakeFS diff returned %s paths without parquet data for input node %s; falling back to full load",
                    diff_paths_count,
                    node_id,
                )
            return None
        df = await self._load_parquet_keys_dataframe(
            bucket=context.bucket,
            keys=[f"{context.current_commit_id}/{path.lstrip('/')}" for path in parquet_paths],
            temp_dirs=temp_dirs,
            prefix=f"{context.current_commit_id}/{context.artifact_prefix}".rstrip("/"),
        )
        df = self._apply_schema_casts(df, dataset=context.dataset, version=context.version)
        diff_snapshot = dict(base_snapshot) if base_snapshot is not None else None
        if diff_snapshot is not None:
            diff_snapshot["lakefs_commit_id"] = context.current_commit_id
            diff_snapshot["diff_used"] = True
            diff_snapshot["diff_parquet_paths"] = len(parquet_paths)
        df = await self._apply_input_watermark_and_snapshot(
            df=df,
            node_id=node_id,
            snapshot=diff_snapshot,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
            label_scope="diff",
            tolerate_max_errors=True,
            always_compute_watermark_max=False,
        )
        self._append_input_snapshot(input_snapshots=input_snapshots, snapshot=diff_snapshot)
        return df

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
        selection = normalize_dataset_selection(metadata, default_branch=branch or "main")
        dataset_id = selection.dataset_id
        dataset_name = selection.dataset_name
        read_config = metadata.get("read") if isinstance(metadata.get("read"), dict) else {}

        if not dataset_id and not dataset_name:
            return await self._load_external_input_dataframe_with_snapshot(
                read_config=read_config,
                node_id=node_id,
                temp_dirs=temp_dirs,
                input_snapshots=input_snapshots,
            )

        context, snapshot = await self._resolve_dataset_input_load_context(
            db_name=db_name,
            node_id=node_id,
            selection=selection,
            input_snapshots=input_snapshots,
        )
        diff_df = await self._try_load_diff_input_dataframe(
            context=context,
            node_id=node_id,
            temp_dirs=temp_dirs,
            input_snapshots=input_snapshots,
            base_snapshot=snapshot,
            previous_commit_id=previous_commit_id,
            use_lakefs_diff=use_lakefs_diff,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
        )
        if diff_df is not None:
            return diff_df

        df = await self._load_full_dataset_input_dataframe(
            context=context,
            metadata=metadata,
            temp_dirs=temp_dirs,
            node_id=node_id,
        )
        df = await self._apply_input_watermark_and_snapshot(
            df=df,
            node_id=node_id,
            snapshot=snapshot,
            watermark_column=watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
            label_scope=None,
            tolerate_max_errors=False,
            always_compute_watermark_max=True,
        )
        self._append_input_snapshot(input_snapshots=input_snapshots, snapshot=snapshot)
        return df

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
        metadata: Dict[str, Any] = {}
        if dataset_id:
            metadata["datasetId"] = dataset_id
        if dataset_name:
            metadata["datasetName"] = dataset_name
        if branch:
            metadata["datasetBranch"] = branch
        try:
            return await self._load_input_dataframe(
                db_name=db_name,
                metadata=metadata,
                temp_dirs=temp_dirs,
                branch=branch or "main",
                node_id=f"fk_ref_{dataset_name or dataset_id or 'dataset'}",
                input_snapshots=None,
                previous_commit_id=None,
                use_lakefs_diff=False,
            )
        except Exception as exc:
            logger.warning("Failed to load FK reference dataset: %s", exc)
            return None

    async def _evaluate_fk_expectations(
        self,
        *,
        expectations: List[Dict[str, Any]],
        output_df: DataFrame,
        db_name: str,
        branch: Optional[str],
        temp_dirs: list[str],
    ) -> List[str]:
        errors: List[str] = []
        for exp in expectations or []:
            if not isinstance(exp, dict):
                continue
            spec = self._parse_fk_expectation(exp, default_branch=branch)
            if not spec:
                continue
            columns = spec["columns"]
            ref_columns = spec["ref_columns"]
            if not columns or not ref_columns:
                errors.append("fk_exists missing columns or reference columns")
                continue
            if len(columns) != len(ref_columns):
                errors.append(f"fk_exists column count mismatch: {columns} vs {ref_columns}")
                continue
            missing_cols = [col for col in columns if col not in output_df.columns]
            if missing_cols:
                errors.append(f"fk_exists missing column(s): {', '.join(missing_cols)}")
                continue

            ref_df = await self._load_fk_reference_dataframe(
                db_name=db_name,
                dataset_id=spec["dataset_id"],
                dataset_name=spec["dataset_name"],
                branch=spec["branch"],
                temp_dirs=temp_dirs,
            )
            if ref_df is None:
                errors.append("fk_exists reference dataset not available")
                continue
            missing_ref_cols = [col for col in ref_columns if col not in ref_df.columns]
            if missing_ref_cols:
                errors.append(f"fk_exists reference missing column(s): {', '.join(missing_ref_cols)}")
                continue
            left = output_df.select(*columns)
            if spec["allow_nulls"]:
                for col in columns:
                    left = left.filter(F.col(col).isNotNull())
            right = ref_df.select(*ref_columns)
            for col in ref_columns:
                right = right.filter(F.col(col).isNotNull())
            right = right.dropDuplicates()
            if len(columns) == 1:
                join_cond = left[columns[0]] == right[ref_columns[0]]
            else:
                join_cond = reduce(
                    operator.and_,
                    [left[col] == right[ref_col] for col, ref_col in zip(columns, ref_columns)],
                )
            missing_count = await self._run_spark(
                lambda: left.join(right, join_cond, how="left_anti").count(),
                label="fk_exists:missing_count",
            )
            if missing_count:
                ref_label = spec["dataset_name"] or spec["dataset_id"] or "reference"
                errors.append(
                    f"fk_exists failed: {','.join(columns)} -> {ref_label}({','.join(ref_columns)}) missing={missing_count}"
                )
        return errors

    def _normalize_read_options(self, read_config: Dict[str, Any]) -> Dict[str, str]:
        options_raw = read_config.get("options") if isinstance(read_config.get("options"), dict) else {}
        options: Dict[str, str] = {}
        for k, v in options_raw.items():
            key = str(k or "").strip()
            if not key:
                continue
            if v is None:
                continue
            options[key] = str(v)

        # Option values can be sourced from environment variables to avoid embedding secrets in pipeline definitions.
        # Example: {"options_env": {"password": "JDBC_PASSWORD"}}.
        env_raw = read_config.get("options_env") or read_config.get("optionsEnv")
        if isinstance(env_raw, dict):
            for opt_key, env_name in env_raw.items():
                key = str(opt_key or "").strip()
                env_key = str(env_name or "").strip()
                if not key or not env_key:
                    continue
                env_val = os.environ.get(env_key)
                if env_val is not None:
                    options[key] = str(env_val)
        # Convenience aliases (so the planner doesn't need to remember Spark option keys).
        mode = read_config.get("mode")
        if mode is not None and "mode" not in options:
            options["mode"] = str(mode)
        corrupt_col = read_config.get("corrupt_record_column") or read_config.get("corruptRecordColumn")
        if corrupt_col is not None and "columnNameOfCorruptRecord" not in options:
            options["columnNameOfCorruptRecord"] = str(corrupt_col)
        if "header" not in options and "header" in read_config:
            options["header"] = "true" if bool(read_config.get("header")) else "false"
        if "inferSchema" not in options and "infer_schema" in read_config:
            options["inferSchema"] = "true" if bool(read_config.get("infer_schema")) else "false"
        return options

    def _mask_sensitive_options(self, options: Dict[str, str]) -> Dict[str, str]:
        masked: Dict[str, str] = {}
        sensitive_markers = (
            "password",
            "secret",
            "token",
            "apikey",
            "api_key",
            "access_key",
            "secret_key",
            "private_key",
            "client_secret",
        )
        for key, value in (options or {}).items():
            lower = str(key or "").lower()
            if any(marker in lower for marker in sensitive_markers):
                masked[str(key)] = "***"
            else:
                masked[str(key)] = str(value)
        return masked

    def _schema_ddl_from_read_config(self, read_config: Dict[str, Any]) -> Optional[str]:
        raw = read_config.get("schema")
        if raw is None:
            raw = read_config.get("schema_columns") or read_config.get("schemaColumns")
        if isinstance(raw, dict):
            raw = raw.get("columns") or raw.get("fields") or raw.get("schema")
        if not isinstance(raw, list):
            return None
        parts: list[str] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("column") or "").strip()
            if not name:
                continue
            raw_type = item.get("type") or item.get("data_type") or item.get("datatype") or "xsd:string"
            normalized = normalize_schema_type(raw_type) or str(raw_type or "").strip().lower()
            spark_type = xsd_to_spark_type(normalized) if normalized.startswith("xsd:") else str(normalized or "string")
            safe_name = name.replace("`", "``")
            parts.append(f"`{safe_name}` {spark_type}")
        ddl = ", ".join(parts).strip()
        return ddl or None

    def _resolve_read_format(self, *, path: str, read_config: Dict[str, Any]) -> str:
        fmt = str(read_config.get("format") or read_config.get("file_format") or read_config.get("fileFormat") or "").strip().lower()
        if fmt:
            return fmt
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            return "csv"
        if ext == ".parquet":
            return "parquet"
        if ext in {".xlsx", ".xlsm"}:
            return "excel"
        if ext == ".json":
            return "json"
        if ext == ".avro":
            return "avro"
        if ext == ".orc":
            return "orc"
        return ""

    def _resolve_streaming_checkpoint_location(self, *, read_config: Dict[str, Any], node_id: str) -> str:
        checkpoint = (
            read_config.get("checkpoint_location")
            or read_config.get("checkpointLocation")
            or read_config.get("stream_checkpoint")
            or read_config.get("streamCheckpoint")
            or ""
        )
        checkpoint_text = str(checkpoint or "").strip()
        if not checkpoint_text:
            raise ValueError(
                f"Input node {node_id} read.mode=streaming requires read.checkpoint_location"
            )
        return checkpoint_text

    def _resolve_kafka_value_format(self, *, read_config: Dict[str, Any]) -> str:
        raw_value = (
            read_config.get("value_format")
            or read_config.get("valueFormat")
            or read_config.get("kafka_value_format")
            or read_config.get("kafkaValueFormat")
            or "raw"
        )
        value_format = str(raw_value or "raw").strip().lower() or "raw"
        if value_format in {"none"}:
            value_format = "raw"
        if value_format not in {"raw", "json", "avro"}:
            raise ValueError("kafka value_format must be one of: raw|json|avro")
        return value_format

    def _resolve_kafka_schema_registry_headers(self, *, read_config: Dict[str, Any]) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        raw_headers = read_config.get("schema_registry_headers") or read_config.get("schemaRegistryHeaders")
        if isinstance(raw_headers, dict):
            for key, value in raw_headers.items():
                key_text = str(key or "").strip()
                value_text = str(value or "").strip()
                if key_text and value_text:
                    headers[key_text] = value_text

        if not any(key.lower() == "authorization" for key in headers):
            token = str(
                read_config.get("schema_registry_auth_token")
                or read_config.get("schemaRegistryAuthToken")
                or ""
            ).strip()
            if token:
                headers["Authorization"] = f"Bearer {token}"

        if not any(key.lower() == "authorization" for key in headers):
            options = self._normalize_read_options(read_config)
            basic_user_info = str(
                read_config.get("schema_registry_basic_auth")
                or read_config.get("schemaRegistryBasicAuth")
                or options.get("basic.auth.user.info")
                or options.get("schema.registry.basic.auth.user.info")
                or ""
            ).strip()
            username = str(
                read_config.get("schema_registry_username")
                or read_config.get("schemaRegistryUsername")
                or ""
            ).strip()
            password = str(
                read_config.get("schema_registry_password")
                or read_config.get("schemaRegistryPassword")
                or ""
            ).strip()
            if username and password:
                basic_user_info = f"{username}:{password}"
            if basic_user_info and ":" in basic_user_info:
                encoded = base64.b64encode(basic_user_info.encode("utf-8")).decode("ascii")
                headers["Authorization"] = f"Basic {encoded}"

        return headers

    def _resolve_kafka_avro_schema(
        self,
        *,
        read_config: Dict[str, Any],
        node_id: str,
    ) -> str:
        inline_schema = resolve_inline_avro_schema(read_config=read_config)
        if inline_schema:
            return inline_schema

        reference = resolve_kafka_avro_schema_registry_reference(
            read_config=read_config,
            node_id=node_id,
        )
        cached = self._kafka_avro_schema_cache.get(reference.cache_key)
        if cached:
            return cached

        headers = self._resolve_kafka_schema_registry_headers(read_config=read_config)
        try:
            schema_text = fetch_kafka_avro_schema_from_registry(
                reference=reference,
                timeout_seconds=float(self.kafka_schema_registry_timeout_seconds),
                headers=headers or None,
            )
        except Exception as exc:
            raise ValueError(
                f"Input node {node_id} kafka value_format=avro schema-registry lookup failed: {exc}"
            ) from exc
        self._kafka_avro_schema_cache[reference.cache_key] = schema_text
        return schema_text

    def _apply_kafka_value_parsing(
        self,
        *,
        df: DataFrame,
        read_config: Dict[str, Any],
        node_id: str,
    ) -> DataFrame:
        value_format = self._resolve_kafka_value_format(read_config=read_config)
        if value_format == "raw":
            return df

        parsed_column = str(
            read_config.get("parsed_value_column")
            or read_config.get("parsedValueColumn")
            or "value_parsed"
        ).strip() or "value_parsed"

        if value_format == "json":
            schema_ddl = self._schema_ddl_from_read_config(read_config)
            if not schema_ddl:
                raise ValueError(
                    f"Input node {node_id} kafka value_format=json requires read.schema/schema_columns"
                )
            parse_mode = str(
                read_config.get("json_parse_mode")
                or read_config.get("jsonParseMode")
                or "FAILFAST"
            ).strip().upper() or "FAILFAST"
            if parse_mode not in {"PERMISSIVE", "FAILFAST"}:
                raise ValueError("json parse mode must be PERMISSIVE or FAILFAST")
            return df.withColumn(
                parsed_column,
                F.from_json(
                    F.col("value").cast("string"),
                    schema_ddl,
                    {"mode": parse_mode},
                ),
            )

        avro_schema_text = self._resolve_kafka_avro_schema(
            read_config=read_config,
            node_id=node_id,
        )
        try:
            from pyspark.sql.avro.functions import from_avro  # type: ignore
        except Exception as exc:
            raise ValueError(
                "Spark avro functions are not available; install/enable spark-avro for kafka value_format=avro"
            ) from exc
        return df.withColumn(parsed_column, from_avro(F.col("value"), avro_schema_text))

    def _load_external_streaming_dataframe(
        self,
        *,
        read_config: Dict[str, Any],
        node_id: str,
        fmt: str,
        temp_dirs: list[str],
    ) -> DataFrame:
        if not self.spark_streaming_enabled:
            raise ValueError("Streaming external inputs are disabled by PIPELINE_SPARK_STREAMING_ENABLED")
        if fmt != "kafka":
            raise ValueError(
                f"Input node {node_id} read.mode=streaming currently supports only read.format=kafka"
            )

        options = self._normalize_read_options(read_config)
        stream_reader = self.spark.readStream.format("kafka")
        for key, value in options.items():
            stream_reader = stream_reader.option(key, value)
        stream_df = stream_reader.load()

        temp_root = tempfile.mkdtemp(prefix=f"pipeline-streaming-{node_id}-")
        temp_dirs.append(temp_root)
        sink_path = os.path.join(temp_root, "sink")
        checkpoint_path = self._resolve_streaming_checkpoint_location(read_config=read_config, node_id=node_id)
        os.makedirs(sink_path, exist_ok=True)
        if "://" not in checkpoint_path and not checkpoint_path.startswith("dbfs:/"):
            os.makedirs(checkpoint_path, exist_ok=True)

        trigger_mode = _resolve_streaming_trigger_mode(
            read_config=read_config,
            default_mode=self.spark_streaming_default_trigger,
        )
        timeout_seconds = _resolve_streaming_timeout_seconds(
            read_config=read_config,
            default_seconds=self.spark_streaming_await_timeout_seconds,
        )

        writer = (
            stream_df.writeStream.format("parquet")
            .option("path", sink_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
        )
        if trigger_mode == "once":
            writer = writer.trigger(once=True)
        else:
            writer = writer.trigger(availableNow=True)

        try:
            query = writer.start()
        except Exception as exc:
            if trigger_mode != "available_now":
                raise
            logger.warning(
                "Failed to start streaming query with available_now trigger; retrying once trigger (node_id=%s): %s",
                node_id,
                exc,
                exc_info=True,
            )
            writer = (
                stream_df.writeStream.format("parquet")
                .option("path", sink_path)
                .option("checkpointLocation", checkpoint_path)
                .outputMode("append")
                .trigger(once=True)
            )
            trigger_mode = "once"
            query = writer.start()
        terminated = query.awaitTermination(timeout=timeout_seconds)
        if not terminated:
            query.stop()
            raise TimeoutError(
                f"Input node {node_id} streaming query timed out after {timeout_seconds}s "
                f"(trigger={trigger_mode})"
            )
        query_exception = query.exception()
        if query_exception is not None:
            raise RuntimeError(f"Input node {node_id} streaming query failed: {query_exception}")

        reader = self.spark.read
        if fmt == "kafka":
            # Structured streaming source writes sink files as parquet snapshots.
            # When no rows arrive within the trigger window, the sink can be empty.
            try:
                parsed = reader.parquet(sink_path)
                return self._apply_kafka_value_parsing(
                    df=parsed,
                    read_config=read_config,
                    node_id=node_id,
                )
            except Exception:
                logger.warning(
                    "External streaming source produced no rows (node_id=%s trigger=%s)",
                    node_id,
                    trigger_mode,
                    exc_info=True,
                )
                return self.spark.createDataFrame([], schema=stream_df.schema)
        return reader.parquet(sink_path)

    def _load_external_input_dataframe(
        self,
        read_config: Dict[str, Any],
        *,
        node_id: str,
        temp_dirs: list[str],
    ) -> DataFrame:
        """
        Load an input DataFrame directly from Spark using metadata.read (no DatasetRegistry artifact).

        Supported (initial):
        - jdbc: requires options.url + options.dbtable (or options.query)
        - kafka (batch): requires Spark kafka options (e.g., kafka.bootstrap.servers + subscribe)
        - kafka (streaming snapshot): read.mode=streaming + available_now/once trigger
        - file formats: requires read.path/paths and a Spark data source for read.format
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")

        read_config = dict(read_config or {})
        read_mode = _resolve_external_read_mode(read_config=read_config)
        fmt = str(read_config.get("format") or read_config.get("file_format") or read_config.get("fileFormat") or "").strip().lower()
        if not fmt:
            raise ValueError(f"Input node {node_id} external source requires metadata.read.format")
        if read_mode == "streaming":
            return self._load_external_streaming_dataframe(
                read_config=read_config,
                node_id=node_id,
                fmt=fmt,
                temp_dirs=temp_dirs,
            )

        options = self._normalize_read_options(read_config)
        schema_ddl = self._schema_ddl_from_read_config(read_config)

        if fmt == "jdbc":
            opts = dict(options)
            # Spark JDBC prefers dbtable; translate query -> dbtable if provided.
            query = opts.pop("query", None)
            if query and "dbtable" not in opts:
                opts["dbtable"] = f"({query}) AS t"
            if not str(opts.get("url") or "").strip():
                raise ValueError(f"Input node {node_id} JDBC read requires options.url")
            if not str(opts.get("dbtable") or "").strip():
                raise ValueError(f"Input node {node_id} JDBC read requires options.dbtable (or options.query)")
            reader = self.spark.read.format("jdbc")
            for k, v in opts.items():
                reader = reader.option(k, v)
            return reader.load()

        reader = self.spark.read
        for k, v in options.items():
            reader = reader.option(k, v)
        if schema_ddl and fmt not in {"kafka"}:
            try:
                reader = reader.schema(schema_ddl)
            except Exception as exc:
                # Some sources ignore/forbid schema(); best-effort only.
                logger.warning("Failed to apply schema to source reader: %s", exc, exc_info=True)

        if fmt == "kafka":
            loaded = reader.format("kafka").load()
            return self._apply_kafka_value_parsing(
                df=loaded,
                read_config=read_config,
                node_id=node_id,
            )

        # File/data-source paths.
        path_value = read_config.get("path")
        if path_value is None:
            path_value = read_config.get("paths")
        if path_value is None:
            path_value = options.get("path")
        if path_value is None:
            path_value = read_config.get("uri")

        paths: list[str] = []
        if isinstance(path_value, list):
            paths = [str(p).strip() for p in path_value if str(p or "").strip()]
        else:
            text = str(path_value or "").strip()
            if text:
                paths = [text]

        if not paths:
            raise ValueError(f"Input node {node_id} external {fmt} read requires read.path/paths")

        if fmt == "csv":
            if "header" not in options:
                reader = reader.option("header", "true")
            return reader.csv(paths if len(paths) != 1 else paths[0])
        if fmt == "json":
            return reader.json(paths if len(paths) != 1 else paths[0])
        if fmt == "parquet":
            return reader.parquet(*paths)

        # Best-effort generic reader for formats provided by the Spark runtime (orc/text/avro/xml/etc).
        return reader.format(fmt).load(paths if len(paths) != 1 else paths[0])

    async def _load_artifact_dataframe(
        self,
        bucket: str,
        key: str,
        temp_dirs: list[str],
        *,
        read_config: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        read_config = dict(read_config or {})
        prefix = key.rstrip("/")
        has_extension = os.path.splitext(prefix)[1] != ""
        if not has_extension:
            return await self._load_prefix_dataframe(bucket, f"{prefix}/", temp_dirs, read_config=read_config)
        if key.endswith("/"):
            return await self._load_prefix_dataframe(bucket, key, temp_dirs, read_config=read_config)

        file_path = await self._download_object(bucket, key, temp_dirs)
        return await self._run_spark(
            lambda: self._read_local_file(file_path, read_config=read_config),
            label=f"read_local_file:{os.path.basename(file_path)}",
        )

    async def _collect_prefix_local_paths(
        self,
        *,
        bucket: str,
        prefix: str,
        temp_dirs: list[str],
    ) -> Tuple[Optional[str], List[str]]:
        objects = await self.storage.list_objects(bucket, prefix=prefix)
        keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
        data_keys = [key for key in keys if _is_data_object(key)]
        if not data_keys:
            return None, []

        temp_dir = tempfile.mkdtemp(prefix="pipeline-input-")
        temp_dirs.append(temp_dir)
        local_paths: List[str] = []
        normalized_prefix = str(prefix or "").lstrip("/").rstrip("/")
        if normalized_prefix:
            normalized_prefix = f"{normalized_prefix}/"
        for object_key in data_keys:
            key = str(object_key or "").lstrip("/")
            if not key:
                continue
            rel_path = (
                os.path.relpath(key, normalized_prefix)
                if normalized_prefix and key.startswith(normalized_prefix)
                else os.path.basename(key)
            )
            local_path = os.path.join(temp_dir, rel_path)
            await self._download_object_to_path(bucket, key, local_path)
            local_paths.append(local_path)
        return temp_dir, local_paths

    async def _read_prefix_dataframe_from_local_paths(
        self,
        *,
        temp_dir: str,
        local_paths: List[str],
        read_config: Dict[str, Any],
        bucket: str,
        prefix: str,
    ) -> DataFrame:
        reader = self.spark.read
        options = self._normalize_read_options(read_config)
        schema_ddl = self._schema_ddl_from_read_config(read_config)
        for k, v in options.items():
            reader = reader.option(k, v)
        if schema_ddl:
            reader = reader.schema(schema_ddl)

        forced_format = str(read_config.get("format") or "").strip().lower() or None
        has_parquet = any(path.endswith(".parquet") for path in local_paths)
        has_json = any(path.endswith(".json") for path in local_paths)
        has_csv = any(path.endswith(".csv") for path in local_paths)
        has_avro = any(path.endswith(".avro") for path in local_paths)
        has_orc = any(path.endswith(".orc") for path in local_paths)
        has_excel = any(path.endswith((".xlsx", ".xlsm")) for path in local_paths)

        if forced_format == "parquet" or (not forced_format and has_parquet):
            return await self._run_spark(lambda: reader.parquet(temp_dir), label="read_prefix:parquet")
        if forced_format == "json" or (not forced_format and has_json):
            return await self._run_spark(lambda: reader.json(temp_dir), label="read_prefix:json")
        if forced_format == "csv" or (not forced_format and has_csv):
            if "header" not in options:
                reader = reader.option("header", "true")
            return await self._run_spark(
                lambda: self._strip_bom_headers(reader.csv(temp_dir)),
                label="read_prefix:csv",
            )
        if forced_format == "avro" or (not forced_format and has_avro):
            return await self._run_spark(
                lambda: reader.format("avro").load(temp_dir),
                label="read_prefix:avro",
            )
        if forced_format == "orc" or (not forced_format and has_orc):
            return await self._run_spark(
                lambda: reader.orc(temp_dir),
                label="read_prefix:orc",
            )
        if forced_format in {"excel", "xlsx"} or (not forced_format and has_excel):
            return await self._run_spark(
                lambda: self._load_excel_path(
                    next(path for path in local_paths if path.endswith((".xlsx", ".xlsm")))
                ),
                label="read_prefix:excel",
            )
        if forced_format:
            # Best-effort generic reader for formats provided by the Spark runtime (orc/text/avro/etc).
            return await self._run_spark(
                lambda: reader.format(forced_format).load(temp_dir),
                label=f"read_prefix:{forced_format}",
            )

        extensions = sorted({os.path.splitext(path)[1] for path in local_paths if os.path.splitext(path)[1]})
        raise ValueError(
            f"Unsupported dataset artifact format in s3://{bucket}/{prefix} (extensions={','.join(extensions) or 'unknown'})"
        )

    async def _load_prefix_dataframe(
        self,
        bucket: str,
        prefix: str,
        temp_dirs: list[str],
        *,
        read_config: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        read_config = dict(read_config or {})
        temp_dir, local_paths = await self._collect_prefix_local_paths(
            bucket=bucket,
            prefix=prefix,
            temp_dirs=temp_dirs,
        )
        if not temp_dir or not local_paths:
            return self._empty_dataframe()
        return await self._read_prefix_dataframe_from_local_paths(
            temp_dir=temp_dir,
            local_paths=local_paths,
            read_config=read_config,
            bucket=bucket,
            prefix=prefix,
        )

    async def _download_object_to_path(self, bucket: str, key: str, local_path: str) -> None:
        if not self.storage:
            raise RuntimeError("Storage service not available")
        directory = os.path.dirname(local_path)

        def _download() -> None:
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(local_path, "wb") as handle:
                self.storage.client.download_fileobj(bucket, key, handle)

        await asyncio.to_thread(_download)

    async def _download_object(
        self,
        bucket: str,
        key: str,
        temp_dirs: list[str],
        *,
        temp_dir: Optional[str] = None,
    ) -> str:
        if temp_dir is None:
            temp_dir = tempfile.mkdtemp(prefix="pipeline-input-")
            temp_dirs.append(temp_dir)
        filename = os.path.basename(key) or f"artifact-{uuid4().hex}"
        local_path = os.path.join(temp_dir, filename)
        await self._download_object_to_path(bucket, key, local_path)
        return local_path

    def _read_local_file(self, path: str, *, read_config: Optional[Dict[str, Any]] = None) -> DataFrame:
        read_config = dict(read_config or {})
        fmt = self._resolve_read_format(path=path, read_config=read_config)
        options = self._normalize_read_options(read_config)
        schema_ddl = self._schema_ddl_from_read_config(read_config)

        reader = self.spark.read
        for k, v in options.items():
            reader = reader.option(k, v)
        if schema_ddl:
            reader = reader.schema(schema_ddl)

        if fmt == "csv":
            if "header" not in options:
                reader = reader.option("header", "true")
            return self._strip_bom_headers(reader.csv(path))
        if fmt == "parquet":
            return reader.parquet(path)
        if path.endswith((".xlsx", ".xlsm")):
            return self._load_excel_path(path)
        if fmt == "json":
            return self._load_json_path(path, reader=reader)
        if fmt:
            # Best-effort generic reader for formats provided by the Spark runtime (orc/text/avro/etc).
            return reader.format(fmt).load(path)
        raise ValueError(f"Unsupported dataset file type: {path}")

    def _strip_bom_headers(self, df: DataFrame) -> DataFrame:
        """
        Normalize UTF-8 BOM artifacts in CSV headers.

        Spark sometimes strips BOM from headers. Some upload/profiling code may preserve it.
        Creating BOM-prefixed *alias* columns is unsafe because it can introduce duplicate
        columns after joins (e.g., many datasets share a first column like order_id).

        Instead, we canonicalize to the BOM-stripped header and ensure uniqueness.
        """
        if not _PYSPARK_AVAILABLE:
            return df
        try:
            cols = list(df.columns)
        except Exception as exc:
            logger.warning("Failed to read dataframe columns for dedupe rename: %s", exc, exc_info=True)
            return df
        if not cols:
            return df

        new_cols: list[str] = []
        seen: set[str] = set()
        for col in cols:
            base = str(col).lstrip("\ufeff") or str(col)
            name = base
            if name in seen:
                suffix = 1
                while f"{base}__{suffix}" in seen:
                    suffix += 1
                name = f"{base}__{suffix}"
            seen.add(name)
            new_cols.append(name)
        if new_cols == cols:
            return df
        try:
            return df.toDF(*new_cols)
        except Exception as exc:
            logger.warning("Failed to rename duplicate dataframe columns: %s", exc, exc_info=True)
            return df

    def _load_excel_path(self, path: str) -> DataFrame:
        import pandas as pd

        frame = pd.read_excel(path)
        return self.spark.createDataFrame(frame)

    def _load_json_path(self, path: str, *, reader: Optional[Any] = None) -> DataFrame:
        if reader is None:
            reader = self.spark.read
        try:
            with open(path, "r", encoding="utf-8") as handle:
                head = handle.read(2048)
                handle.seek(0)
                if head.lstrip().startswith("{") and "\"rows\"" in head:
                    payload = json.load(handle)
                    rows = payload.get("rows") or [] if isinstance(payload, dict) else payload
                    if rows:
                        return self.spark.createDataFrame(rows)
                    return self._empty_dataframe()
        except Exception as exc:
            logger.warning("Fell back to Spark JSON reader for %s due to parser error: %s", path, exc, exc_info=True)
        return reader.json(path)

    def _empty_dataframe(self) -> DataFrame:
        return self.spark.createDataFrame([], schema=StructType([]))

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
