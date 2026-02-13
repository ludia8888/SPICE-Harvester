"""
Pipeline Worker (Spark/Flink-ready execution runtime).

Consumes Kafka pipeline-jobs and executes dataset transforms using Spark.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import reduce
import operator
from typing import Any, Callable, Dict, List, Optional, TypeVar
from uuid import UUID, uuid4

import httpx
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
from shared.models.event_envelope import EventEnvelope
from shared.models.pipeline_job import PipelineJob
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.kafka.processed_event_worker import HeartbeatOptions, ProcessedEventKafkaWorker, RegistryKey
from shared.services.kafka.dlq_publisher import DlqPublishSpec
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.storage.lakefs_storage_service import LakeFSStorageService
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
    OUTPUT_KIND_GEOTEMPORAL,
    OUTPUT_KIND_MEDIA,
    OUTPUT_KIND_ONTOLOGY,
    OUTPUT_KIND_VIRTUAL,
    normalize_output_kind,
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
from shared.services.pipeline.pipeline_parameter_utils import apply_parameters, normalize_parameters
from shared.services.pipeline.pipeline_definition_utils import (
    build_expectations_with_pk,
    normalize_expectation_columns,
    resolve_delete_column,
    resolve_incremental_config,
    resolve_pk_columns,
    resolve_pk_semantics,
    validate_pk_semantics,
    split_expectation_columns,
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
    normalize_union_mode,
    resolve_join_spec,
)
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.backing_source_adapter import MappingSpecResolver, is_oms_mapping_spec
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.pipeline.pipeline_lock import PipelineLock, PipelineLockError
from shared.services.storage.redis_service import RedisService, create_redis_service
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
    _list_part_files,
    _schema_diff,
    _schema_from_dataframe,
)

logger = logging.getLogger(__name__)

SUPPORTED_TRANSFORMS_SPARK = SUPPORTED_TRANSFORMS

T = TypeVar("T")


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
        self.http: Optional[httpx.AsyncClient] = None
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

        # MappingSpecResolver: OMS-first, PostgreSQL-fallback
        if self.http:
            oms_base = os.getenv("OMS_BASE_URL", "http://oms:8000").rstrip("/")
            oms_token = ""
            for key in ("OMS_CLIENT_TOKEN", "OMS_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
                oms_token = (os.getenv(key) or "").strip()
                if oms_token:
                    break
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

        token = self.bff_admin_token
        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Admin-Token"] = token
        self.http = httpx.AsyncClient(timeout=120.0, headers=headers)

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

    async def _execute_job(self, job: PipelineJob) -> None:
        if not self.pipeline_registry or not self.dataset_registry:
            raise RuntimeError("Registry services not available")
        if not self.spark:
            raise RuntimeError("Spark session not initialized")
        if not self.storage:
            raise RuntimeError("Storage service not available")

        definition = job.definition_json or {}
        # Apply per-pipeline Spark/cast configuration overrides before we record spark_conf in the run output.
        prev_spark_conf, prev_cast_mode = self._apply_job_overrides(definition)
        preview_meta = definition.get("__preview_meta__") or {}
        tables: Dict[str, DataFrame] = {}
        nodes = normalize_nodes(definition.get("nodes"))
        for _node in nodes.values():
            _node["metadata"] = normalize_transform_metadata(_node.get("metadata"))
        edges = normalize_edges(definition.get("edges"))
        order = topological_sort(nodes, edges, include_unordered=False)
        incoming = build_incoming(edges)
        parameters = normalize_parameters(definition.get("parameters"))
        input_snapshots: list[dict[str, Any]] = []
        input_sampling: dict[str, Any] = {}
        input_commit_payload: Optional[list[dict[str, Any]]] = None
        temp_dirs: list[str] = []
        persisted_dfs: list[DataFrame] = []
        output_nodes = {
            node_id: node for node_id, node in nodes.items() if str(node.get("type") or "") == "output"
        }

        execution_semantics = _resolve_execution_semantics(job=job, definition=definition)

        requested_node_error: Optional[str] = None
        target_node_ids: list[str]
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
                # Enterprise safety: scheduled/queued jobs can outlive the pipeline row
                # (e.g. smoke tests clean up immediately). This is non-retryable.
                logger.warning(
                    "Dropping stale pipeline job for missing pipeline (pipeline_id=%s job_id=%s mode=%s)",
                    resolved_pipeline_id,
                    job.job_id,
                    getattr(job, "mode", None),
                )
                return
        pipeline_ref = resolved_pipeline_id or str(job.pipeline_id)
        watermark_column = _resolve_watermark_column(
            incremental=resolve_incremental_config(definition),
            metadata=definition.get("settings") if isinstance(definition.get("settings"), dict) else {},
        )
        previous_watermark: Optional[Any] = None
        previous_watermark_keys: list[str] = []
        previous_input_commits: Dict[str, str] = {}
        if execution_semantics in {"incremental", "streaming"} and resolved_pipeline_id:
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

        lock: Optional[PipelineLock] = None
        spark_conf = self._collect_spark_conf()
        code_version = _resolve_code_version()
        pipeline_spec_commit_id = (
            str(job.definition_commit_id).strip() if job.definition_commit_id else None
        )
        pipeline_spec_hash = str(job.definition_hash).strip() if job.definition_hash else None
        declared_outputs = definition.get("outputs") if isinstance(definition.get("outputs"), list) else []

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

        artifact_id: Optional[str] = None
        run_id: Optional[str] = None

        async def record_artifact(
            *,
            status: str,
            outputs: Optional[List[Dict[str, Any]]] = None,
            inputs: Optional[Dict[str, Any]] = None,
            lakefs: Optional[Dict[str, Any]] = None,
            sampling_strategy: Optional[Dict[str, Any]] = None,
            errors: Optional[List[str]] = None,
        ) -> Optional[str]:
            nonlocal artifact_id, run_id
            if not resolved_pipeline_id or not self.pipeline_registry:
                return None
            if run_mode not in {"preview", "build"}:
                return None
            error_payload: Dict[str, Any] = {}
            if errors:
                error_payload["errors"] = errors
            lakefs = lakefs or {}
            artifact_id = await self.pipeline_registry.upsert_artifact(
                pipeline_id=resolved_pipeline_id,
                job_id=job.job_id,
                run_id=run_id,
                mode=run_mode,
                status=status,
                artifact_id=artifact_id,
                definition_hash=job.definition_hash,
                definition_commit_id=job.definition_commit_id,
                pipeline_spec_hash=pipeline_spec_hash,
                pipeline_spec_commit_id=pipeline_spec_commit_id,
                inputs=inputs,
                lakefs_repository=lakefs.get("repository"),
                lakefs_branch=lakefs.get("branch"),
                lakefs_commit_id=lakefs.get("commit_id"),
                outputs=outputs,
                declared_outputs=declared_outputs,
                sampling_strategy=sampling_strategy,
                error=error_payload,
            )
            return artifact_id

        job_mode = str(job.mode or "deploy").strip().lower()
        is_preview = job_mode == "preview"
        is_build = job_mode == "build"
        run_mode = "preview" if is_preview else "build" if is_build else "deploy"
        preview_sampling_seed = self._preview_sampling_seed(job.job_id)
        use_lakefs_diff = self.use_lakefs_diff

        async def emit_job_event(
            *,
            status: str,
            errors: Optional[list[str]] = None,
            lakefs: Optional[dict[str, Any]] = None,
            output: Optional[dict[str, Any]] = None,
        ) -> None:
            if run_mode not in {"build", "deploy"}:
                return
            event_type = "PIPELINE_JOB_SUCCEEDED" if status in {"SUCCESS", "DEPLOYED"} else "PIPELINE_JOB_FAILED"
            payload: dict[str, Any] = {
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
                    run_id = run_record.get("run_id")
            except Exception as exc:
                logger.debug("Failed to resolve run_id for job %s: %s", job.job_id, exc)

        run_ref = run_id or job.job_id
        await record_artifact(status="RUNNING")

        validation_errors = self._validate_definition(definition, require_output=not is_preview)
        if requested_node_error:
            validation_errors.append(requested_node_error)
        if execution_semantics in {"incremental", "streaming"} and not watermark_column:
            validation_errors.append(
                f"{execution_semantics} pipelines require settings.watermarkColumn to be configured"
            )
        validation_errors.extend(self._validate_required_subgraph(nodes, incoming, required_node_ids))
        if validation_errors:
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
            return

        if run_mode in {"build", "deploy"}:
            lock = await self._acquire_pipeline_lock(job)

        try:
            for node_id in order_run:
                node = nodes[node_id]
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
                        sampling_strategy = self._resolve_sampling_strategy(metadata, preview_meta)
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
                elif node_type == "output":
                    df = inputs[0] if inputs else self._empty_dataframe()
                else:
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
                    df = self._apply_transform(transform_metadata, inputs, parameters)

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
                    return

                tables[node_id] = df

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
            preview_limit = int(job.preview_limit or 200)
            preview_limit = max(1, min(500, preview_limit))

            if is_preview:
                primary_id = target_node_ids[0] if target_node_ids else None
                output_df = tables.get(primary_id) if primary_id else self._empty_dataframe()
                schema_columns = _schema_from_dataframe(output_df)
                row_count = int(await self._run_spark(lambda: output_df.count(), label=f"count:preview:{primary_id}"))
                schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
                output_ops = self._build_table_ops(output_df)
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
                return

            if is_build:
                if not self.storage or not self.lakefs_client:
                    raise RuntimeError("lakeFS services are not configured")
                if lock:
                    lock.raise_if_lost()
                artifact_repo = _resolve_lakefs_repository()
                await self.storage.create_bucket(artifact_repo)
                output_write_mode = "append" if execution_semantics in {"incremental", "streaming"} else "overwrite"
                base_branch = safe_lakefs_ref(job.branch or "main")
                output_work: List[Dict[str, Any]] = []
                has_output_rows = False
                no_op_candidate = execution_semantics in {"incremental", "streaming"} and diff_empty_inputs
                for node_id in target_node_ids:
                    if lock:
                        lock.raise_if_lost()
                    output_df = tables.get(node_id, self._empty_dataframe())
                    if _PYSPARK_AVAILABLE:
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
                        await self._run_spark(lambda: output_df.count(), label=f"count:build:{node_id}")
                    )
                    if delta_row_count > 0:
                        has_output_rows = True
                    schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
                    output_ops = self._build_table_ops(output_df)
                    contract_errors = await self._run_spark(
                        lambda: validate_schema_contract(output_ops, schema_contract),
                        label=f"schema_contract:build:{node_id}",
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
                            node_id=node_id,
                            context={"execution_semantics": execution_semantics},
                        )
                        await record_run(
                            job_id=job.job_id,
                            mode="build",
                            status="FAILED",
                            node_id=node_id,
                            row_count=delta_row_count,
                            output_json=contract_payload,
                            input_lakefs_commits=input_commit_payload,
                            finished_at=utcnow(),
                        )
                        await record_artifact(
                            status="FAILED",
                            errors=contract_errors,
                            inputs=inputs_payload,
                        )
                        await emit_job_event(status="FAILED", errors=contract_errors)
                        logger.error("Pipeline schema contract failed (build): %s", contract_errors)
                        return

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
                        label=f"expectations:build:{node_id}",
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
                        expectation_payload = self._build_error_payload(
                            message="Pipeline expectations failed",
                            errors=expectation_errors,
                            code=ErrorCode.REQUEST_VALIDATION_FAILED,
                            category=ErrorCategory.INPUT,
                            status_code=422,
                            external_code="PIPELINE_EXPECTATIONS_FAILED",
                            stage="expectations",
                            job=job,
                            node_id=node_id,
                            context={"execution_semantics": execution_semantics},
                        )
                        await record_run(
                            job_id=job.job_id,
                            mode="build",
                            status="FAILED",
                            node_id=node_id,
                            row_count=delta_row_count,
                            output_json=expectation_payload,
                            input_lakefs_commits=input_commit_payload,
                            finished_at=utcnow(),
                        )
                        await record_artifact(
                            status="FAILED",
                            errors=expectation_errors,
                            inputs=inputs_payload,
                        )
                        await emit_job_event(status="FAILED", errors=expectation_errors)
                        logger.error("Pipeline expectations failed (build): %s", expectation_errors)
                        return

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
                            available_columns=set(schema_columns or []),
                        )
                        if dataset_output_errors:
                            error_payload = self._build_error_payload(
                                message="Dataset output metadata invalid",
                                errors=dataset_output_errors,
                                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                                category=ErrorCategory.INPUT,
                                status_code=422,
                                external_code="PIPELINE_DATASET_OUTPUT_METADATA_INVALID",
                                stage="output_metadata",
                                job=job,
                                node_id=node_id,
                                context={
                                    "execution_semantics": execution_semantics,
                                    "output_name": dataset_name,
                                },
                            )
                            await record_run(
                                job_id=job.job_id,
                                mode="build",
                                status="FAILED",
                                node_id=node_id,
                                row_count=delta_row_count,
                                output_json=error_payload,
                                input_lakefs_commits=input_commit_payload,
                                finished_at=utcnow(),
                            )
                            await record_artifact(
                                status="FAILED",
                                errors=dataset_output_errors,
                                inputs=inputs_payload,
                            )
                            await emit_job_event(status="FAILED", errors=dataset_output_errors)
                            logger.error("Pipeline dataset output metadata invalid (build): %s", dataset_output_errors)
                            return
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
                        label=f"collect:build:{node_id}",
                    )
                    output_sample = [row.asDict(recursive=True) for row in sample_rows]
                    column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)

                    safe_name = dataset_name.replace(" ", "_")
                    artifact_prefix = f"pipelines/{job.db_name}/{pipeline_ref}/{safe_name}"
                    output_work.append(
                        {
                            "node_id": node_id,
                            "output_name": output_name,
                            "output_kind": output_kind,
                            "output_metadata": dict(metadata),
                            "dataset_name": dataset_name,
                            "output_df": output_df,
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
                            "artifact_prefix": artifact_prefix,
                            "schema_columns": schema_columns,
                            "schema_hash": schema_hash,
                            "delta_row_count": delta_row_count,
                            "output_sample": output_sample,
                            "column_stats": column_stats,
                        }
                    )

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

                build_branch = safe_lakefs_ref(f"build/{pipeline_ref}/{run_ref}")
                try:
                    await self.lakefs_client.create_branch(
                        repository=artifact_repo,
                        name=build_branch,
                        source=base_branch,
                    )
                except LakeFSConflictError:
                    build_branch = safe_lakefs_ref(f"build/{pipeline_ref}/{run_ref}/{uuid4().hex[:8]}")
                    await self.lakefs_client.create_branch(
                        repository=artifact_repo,
                        name=build_branch,
                        source=base_branch,
                    )

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
                        with contextlib.suppress(Exception):
                            output_df.unpersist(blocking=False)
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
                return

            if not self.storage or not self.lakefs_client:
                raise RuntimeError("lakeFS services are not configured")
            if lock:
                lock.raise_if_lost()

            artifact_repo = _resolve_lakefs_repository()
            await self.storage.create_bucket(artifact_repo)
            base_branch = safe_lakefs_ref(job.branch or "main")

            output_write_mode = "append" if execution_semantics in {"incremental", "streaming"} else "overwrite"
            output_work: List[Dict[str, Any]] = []
            has_output_rows = False
            no_op_candidate = execution_semantics in {"incremental", "streaming"} and diff_empty_inputs
            for node_id in target_node_ids:
                if lock:
                    lock.raise_if_lost()
                output_df = tables.get(node_id, self._empty_dataframe())
                schema_columns = _schema_from_dataframe(output_df)
                delta_row_count = int(
                    await self._run_spark(lambda: output_df.count(), label=f"count:deploy:{node_id}")
                )
                if delta_row_count > 0:
                    has_output_rows = True
                schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
                output_ops = self._build_table_ops(output_df)
                contract_errors = await self._run_spark(
                    lambda: validate_schema_contract(output_ops, schema_contract),
                    label=f"schema_contract:deploy:{node_id}",
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
                        node_id=node_id,
                        context={"execution_semantics": execution_semantics},
                    )
                    await record_build(
                        status="FAILED",
                        output_json=contract_payload,
                    )
                    await record_run(
                        job_id=job.job_id,
                        mode="deploy",
                        status="FAILED",
                        node_id=node_id,
                        row_count=delta_row_count,
                        output_json=contract_payload,
                        input_lakefs_commits=input_commit_payload,
                        finished_at=utcnow(),
                    )
                    await record_artifact(
                        status="FAILED",
                        errors=contract_errors,
                        inputs=inputs_payload,
                    )
                    await emit_job_event(status="FAILED", errors=contract_errors)
                    logger.error("Pipeline schema contract failed: %s", contract_errors)
                    return

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
                    label=f"expectations:deploy:{node_id}",
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
                    expectation_payload = self._build_error_payload(
                        message="Pipeline expectations failed",
                        errors=expectation_errors,
                        code=ErrorCode.REQUEST_VALIDATION_FAILED,
                        category=ErrorCategory.INPUT,
                        status_code=422,
                        external_code="PIPELINE_EXPECTATIONS_FAILED",
                        stage="expectations",
                        job=job,
                        node_id=node_id,
                        context={"execution_semantics": execution_semantics},
                    )
                    await record_build(
                        status="FAILED",
                        output_json=expectation_payload,
                    )
                    await record_run(
                        job_id=job.job_id,
                        mode="deploy",
                        status="FAILED",
                        node_id=node_id,
                        row_count=delta_row_count,
                        output_json=expectation_payload,
                        input_lakefs_commits=input_commit_payload,
                        finished_at=utcnow(),
                    )
                    await record_artifact(
                        status="FAILED",
                        errors=expectation_errors,
                        inputs=inputs_payload,
                    )
                    await emit_job_event(status="FAILED", errors=expectation_errors)
                    logger.error("Pipeline expectations failed: %s", expectation_errors)
                    return

                sample_rows = await self._run_spark(
                    lambda: output_df.limit(preview_limit).collect(),
                    label=f"collect:deploy:{node_id}",
                )
                output_sample = [row.asDict(recursive=True) for row in sample_rows]

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
                        available_columns=set(schema_columns or []),
                    )
                    if dataset_output_errors:
                        expectation_payload = self._build_error_payload(
                            message="Dataset output metadata invalid",
                            errors=dataset_output_errors,
                            code=ErrorCode.REQUEST_VALIDATION_FAILED,
                            category=ErrorCategory.INPUT,
                            status_code=422,
                            external_code="PIPELINE_DATASET_OUTPUT_METADATA_INVALID",
                            stage="output_metadata",
                            job=job,
                            node_id=node_id,
                            context={
                                "execution_semantics": execution_semantics,
                                "output_name": dataset_name,
                            },
                        )
                        await record_build(
                            status="FAILED",
                            output_json=expectation_payload,
                        )
                        await record_run(
                            job_id=job.job_id,
                            mode="deploy",
                            status="FAILED",
                            node_id=node_id,
                            row_count=delta_row_count,
                            output_json=expectation_payload,
                            input_lakefs_commits=input_commit_payload,
                            finished_at=utcnow(),
                        )
                        await record_artifact(
                            status="FAILED",
                            errors=dataset_output_errors,
                            inputs=inputs_payload,
                        )
                        await emit_job_event(status="FAILED", errors=dataset_output_errors)
                        logger.error("Pipeline dataset output metadata invalid: %s", dataset_output_errors)
                        return
                    output_format = dataset_policy.output_format
                    partition_cols = list(dataset_policy.partition_by)
                else:
                    output_format = _resolve_output_format(definition=definition, output_metadata=metadata)
                    partition_cols = _resolve_partition_columns(
                        definition=definition,
                        output_metadata=metadata,
                    )
                safe_name = dataset_name.replace(" ", "_")
                artifact_prefix = f"pipelines/{job.db_name}/{pipeline_ref}/{safe_name}"
                column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)
                output_work.append(
                    {
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
                        "row_count": delta_row_count,
                        "columns": schema_columns,
                        "rows": output_sample,
                        "sample_row_count": len(output_sample),
                        "column_stats": column_stats,
                    }
                )

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

            run_branch = safe_lakefs_ref(f"run/{pipeline_ref}/{run_ref}")
            try:
                await self.lakefs_client.create_branch(
                    repository=artifact_repo,
                    name=run_branch,
                    source=base_branch,
                )
            except LakeFSConflictError:
                run_branch = safe_lakefs_ref(f"run/{pipeline_ref}/{run_ref}/{uuid4().hex[:8]}")
                await self.lakefs_client.create_branch(
                    repository=artifact_repo,
                    name=run_branch,
                    source=base_branch,
                )

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

            if lock:
                lock.raise_if_lost()
            commit_id = await self.lakefs_client.commit(
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
            merge_commit_id = await self.lakefs_client.merge(
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
                await self.lakefs_client.delete_branch(repository=artifact_repo, name=run_branch)
            except Exception as exc:
                logger.info("Failed to delete run branch %s after merge: %s", run_branch, exc)

            build_outputs: List[Dict[str, Any]] = []
            for item in staged_outputs:
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
                    sample_json={
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
                        "incremental_inputs_have_additive_updates": item.get(
                            "incremental_inputs_have_additive_updates"
                        ),
                        "sample_row_count": len(output_sample),
                        "column_stats": item.get("column_stats") or {},
                    },
                    schema_json={"columns": schema_columns},
                )
                objectify_job_id = await self._maybe_enqueue_objectify_job(
                    dataset=dataset,
                    version=version,
                )
                relationship_job_ids = await self._maybe_enqueue_relationship_jobs(
                    dataset=dataset,
                    version=version,
                )

                build_outputs.append(
                    {
                        "node_id": item.get("node_id"),
                        "output_name": item.get("output_name"),
                        "output_kind": item.get("output_kind", OUTPUT_KIND_DATASET),
                        "output_metadata": item.get("output_metadata") if isinstance(item.get("output_metadata"), dict) else {},
                        "dataset_name": dataset_name,
                        "artifact_key": artifact_key,
                        "row_count": total_row_count,
                        "delta_row_count": delta_row_count if resolved_runtime_write_mode == "append" else None,
                        "write_mode_requested": item.get("write_mode_requested"),
                        "write_mode_resolved": item.get("write_mode_resolved"),
                        "runtime_write_mode": resolved_runtime_write_mode,
                        "pk_columns": item.get("pk_columns") or [],
                        "post_filtering_column": item.get("post_filtering_column"),
                        "write_policy_hash": item.get("write_policy_hash"),
                        "has_incremental_input": item.get("has_incremental_input"),
                        "incremental_inputs_have_additive_updates": item.get(
                            "incremental_inputs_have_additive_updates"
                        ),
                        "lakefs_commit_id": merge_commit_id,
                        "lakefs_branch": base_branch,
                        "objectify_job_id": objectify_job_id,
                        "relationship_job_ids": relationship_job_ids,
                    }
                )

                parsed = parse_s3_uri(artifact_key)
                if parsed:
                    bucket, key = parsed

                    async def _record_pipeline_output_lineage() -> None:
                        await self.lineage.record_link(  # type: ignore[union-attr]
                            from_node_id=LineageStore.node_aggregate("Pipeline", pipeline_ref),
                            to_node_id=LineageStore.node_artifact("s3", bucket, key),
                            edge_type="pipeline_output_stored",
                            occurred_at=utcnow(),
                            db_name=job.db_name,
                            edge_metadata={
                                "db_name": job.db_name,
                                "pipeline_id": pipeline_ref,
                                "artifact_key": artifact_key,
                                "dataset_name": dataset_name,
                                "node_id": item.get("node_id"),
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
                            "node_id": item.get("node_id"),
                        },
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

            watermark_update: Optional[Dict[str, Any]] = None
            if (
                execution_semantics in {"incremental", "streaming"}
                and resolved_pipeline_id
                and input_snapshots
            ):
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
                if (next_watermark is not None or input_commit_map) and self.pipeline_registry:
                    watermarks_payload: Dict[str, Any] = {}
                    if watermark_column:
                        watermarks_payload["watermark_column"] = watermark_column
                        if next_watermark is not None:
                            watermarks_payload["watermark_value"] = next_watermark
                        elif previous_watermark is not None:
                            watermarks_payload["watermark_value"] = previous_watermark
                        merged_keys: list[str] = []
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
                        if merged_keys:
                            deduped_keys: list[str] = []
                            seen_keys: set[str] = set()
                            for item in merged_keys:
                                if item in seen_keys:
                                    continue
                                seen_keys.add(item)
                                deduped_keys.append(item)
                            watermarks_payload["watermark_keys"] = deduped_keys
                    if input_commit_map:
                        watermarks_payload["input_commits"] = input_commit_map
                    try:
                        await self.pipeline_registry.upsert_watermarks(
                            pipeline_id=resolved_pipeline_id,
                            branch=job.branch or "main",
                            watermarks=watermarks_payload,
                        )
                        watermark_update = watermarks_payload
                    except Exception as exc:
                        logger.warning(
                            "Failed to persist pipeline watermarks (pipeline_id=%s): %s",
                            resolved_pipeline_id,
                            exc,
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
        except Exception as exc:
            execution_payload = self._build_error_payload(
                message="Pipeline execution failed",
                errors=[str(exc)],
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                status_code=500,
                external_code="PIPELINE_EXECUTION_FAILED",
                stage="execution",
                job=job,
                context={"exception_type": exc.__class__.__name__},
            )
            if is_preview:
                await record_preview(
                    status="FAILED",
                    row_count=0,
                    sample_json=execution_payload,
                    job_id=job.job_id,
                    node_id=job.node_id,
                )
            elif not is_build:
                await record_build(
                    status="FAILED",
                    output_json=execution_payload,
                )
            await record_run(
                job_id=job.job_id,
                mode=run_mode,
                status="FAILED",
                node_id=job.node_id,
                sample_json=execution_payload if is_preview else None,
                output_json=execution_payload if not is_preview else None,
                input_lakefs_commits=input_commit_payload,
                finished_at=utcnow(),
            )
            await emit_job_event(status="FAILED", errors=[str(exc)])
            logger.exception("Pipeline execution failed: %s", exc)
            raise
        finally:
            if lock:
                await lock.release()
            for path in temp_dirs:
                shutil.rmtree(path, ignore_errors=True)
            if _PYSPARK_AVAILABLE and persisted_dfs:
                for df in persisted_dfs:
                    with contextlib.suppress(Exception):
                        df.unpersist(blocking=False)
            # Restore any per-job overrides so the next run starts from the worker defaults.
            if self.spark and prev_spark_conf:
                for key, value in prev_spark_conf.items():
                    try:
                        if value is None:
                            self.spark.conf.unset(key)
                        else:
                            self.spark.conf.set(key, value)
                    except Exception as exc:
                        # Best-effort restore; some conf keys may be non-modifiable.
                        logger.warning(
                            "Failed to restore Spark conf key %s: %s",
                            key,
                            exc,
                            exc_info=True,
                        )
            self.cast_mode = prev_cast_mode

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
        normalized_kind = normalize_output_kind(output_kind)
        if normalized_kind == OUTPUT_KIND_DATASET:
            return await self._materialize_dataset_output(
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

        if normalized_kind == OUTPUT_KIND_GEOTEMPORAL:
            artifact_key = await self._materialize_geotemporal_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
            )
        elif normalized_kind == OUTPUT_KIND_MEDIA:
            artifact_key = await self._materialize_media_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
            )
        elif normalized_kind == OUTPUT_KIND_VIRTUAL:
            artifact_key = await self._materialize_virtual_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
                row_count_hint=base_row_count,
            )
        elif normalized_kind == OUTPUT_KIND_ONTOLOGY:
            artifact_key = await self._materialize_ontology_output(
                output_metadata=output_metadata,
                df=df,
                artifact_bucket=artifact_bucket,
                prefix=prefix,
                write_mode=write_mode,
                file_prefix=file_prefix,
                file_format=file_format,
                partition_cols=partition_cols,
            )
        else:
            raise ValueError(f"Unsupported output_kind for materialization: {normalized_kind}")

        return {
            "artifact_key": artifact_key,
            "delta_row_count": int(base_row_count or 0),
            "write_mode_requested": write_mode,
            "write_mode_resolved": write_mode,
            "runtime_write_mode": write_mode,
            "pk_columns": [],
            "write_policy_hash": None,
            "has_incremental_input": None,
            "incremental_inputs_have_additive_updates": None,
        }

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
        policy = resolve_dataset_write_policy(
            definition={},
            output_metadata=output_metadata,
            execution_semantics=execution_semantics,
            has_incremental_input=execution_semantics in {"incremental", "streaming"},
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
        )
        for warning in policy.warnings:
            logger.warning("Dataset output policy normalized for %s: %s", dataset_name or prefix, warning)
        output_metadata.clear()
        output_metadata.update(policy.normalized_metadata)

        validation_errors = validate_dataset_output_metadata(
            definition={},
            output_metadata=output_metadata,
            execution_semantics=execution_semantics,
            has_incremental_input=execution_semantics in {"incremental", "streaming"},
            incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
            available_columns=set(df.columns or []),
        )
        if validation_errors:
            raise ValueError("Invalid dataset output metadata: " + "; ".join(validation_errors))

        pk_columns = list(policy.primary_key_columns)

        existing_df = self._empty_dataframe()
        if policy.resolved_write_mode in {
            DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
            DatasetWriteMode.SNAPSHOT_DIFFERENCE,
            DatasetWriteMode.SNAPSHOT_REPLACE,
            DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
        }:
            existing_df = await self._load_existing_output_dataset(
                db_name=db_name,
                branch=branch,
                dataset_name=dataset_name,
            )

        if existing_df.columns and pk_columns:
            missing_existing = [column for column in pk_columns if column not in set(existing_df.columns)]
            if missing_existing:
                raise ValueError(
                    "Existing dataset missing primary_key_columns: " + ", ".join(missing_existing)
                )

        aligned_columns = list(df.columns or [])
        if policy.resolved_write_mode in {
            DatasetWriteMode.SNAPSHOT_REPLACE,
            DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
        } and existing_df.columns:
            existing_only = [column for column in (existing_df.columns or []) if column not in set(aligned_columns)]
            aligned_columns = [*aligned_columns, *existing_only]
        input_aligned = self._align_columns(df, aligned_columns) if aligned_columns else df
        existing_aligned = self._align_columns(existing_df, aligned_columns) if aligned_columns else existing_df

        if policy.resolved_write_mode in {
            DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
            DatasetWriteMode.CHANGELOG,
            DatasetWriteMode.SNAPSHOT_DIFFERENCE,
            DatasetWriteMode.SNAPSHOT_REPLACE,
            DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
        } and pk_columns:
            await self._assert_no_duplicate_primary_keys(
                df=input_aligned,
                pk_columns=pk_columns,
                write_mode=policy.resolved_write_mode.value,
                dataset_label=str(dataset_name or prefix or "output"),
            )

        materialized_df = input_aligned
        if policy.resolved_write_mode == DatasetWriteMode.ALWAYS_APPEND:
            materialized_df = input_aligned
        elif policy.resolved_write_mode == DatasetWriteMode.CHANGELOG:
            materialized_df = self._select_new_or_changed_rows(
                input_df=input_aligned,
                existing_df=existing_aligned,
                pk_columns=pk_columns,
                dedupe_input=False,
            )
        elif policy.resolved_write_mode == DatasetWriteMode.APPEND_ONLY_NEW_ROWS:
            if existing_aligned.columns and pk_columns:
                existing_keys = existing_aligned.select(*pk_columns).distinct()
                materialized_df = input_aligned.join(existing_keys, on=pk_columns, how="left_anti")
            else:
                materialized_df = input_aligned
        elif policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_DIFFERENCE:
            if existing_aligned.columns and pk_columns:
                existing_keys = existing_aligned.select(*pk_columns).distinct()
                # Foundry snapshot_difference keeps only newly seen PK rows in this transaction.
                # It intentionally does not deduplicate duplicates within the incoming transaction.
                materialized_df = input_aligned.join(existing_keys, on=pk_columns, how="left_anti")
            else:
                materialized_df = input_aligned
        elif policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_REPLACE:
            upsert_df = input_aligned
            if existing_aligned.columns and pk_columns:
                existing_dedup = existing_aligned.dropDuplicates(pk_columns)
                upsert_keys = upsert_df.select(*pk_columns).distinct()
                preserved_existing = existing_dedup.join(upsert_keys, on=pk_columns, how="left_anti")
                materialized_df = preserved_existing.unionByName(upsert_df, allowMissingColumns=True)
            else:
                materialized_df = upsert_df
        elif policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE:
            post_col = str(policy.post_filtering_column or "").strip()
            if not post_col:
                raise ValueError("post_filtering_column is required for snapshot_replace_and_remove")
            is_false_expr = self._post_filter_false_expr(post_filtering_column=post_col)
            upsert_input = input_aligned.filter(~is_false_expr)
            upsert_df = upsert_input
            false_keys = input_aligned.filter(is_false_expr).select(*pk_columns).distinct()
            if existing_aligned.columns and pk_columns:
                existing_dedup = existing_aligned.dropDuplicates(pk_columns)
                upsert_keys = upsert_df.select(*pk_columns).distinct()
                preserved_existing = existing_dedup.join(upsert_keys, on=pk_columns, how="left_anti")
                preserved_existing = preserved_existing.join(false_keys, on=pk_columns, how="left_anti")
                materialized_df = preserved_existing.unionByName(upsert_df, allowMissingColumns=True)
            else:
                materialized_df = upsert_df

        delta_row_count = int(
            await self._run_spark(
                lambda: materialized_df.count(),
                label=f"count:materialized:{dataset_name or prefix}",
            )
        )
        artifact_key = await self._materialize_output_dataframe(
            materialized_df,
            artifact_bucket=artifact_bucket,
            prefix=prefix,
            write_mode=policy.runtime_write_mode,
            file_prefix=file_prefix,
            file_format=policy.output_format or file_format,
            partition_cols=policy.partition_by or partition_cols,
        )
        return {
            "artifact_key": artifact_key,
            "delta_row_count": delta_row_count,
            "write_mode_requested": policy.requested_write_mode,
            "write_mode_resolved": policy.resolved_write_mode.value,
            "runtime_write_mode": policy.runtime_write_mode,
            "pk_columns": list(policy.primary_key_columns),
            "post_filtering_column": policy.post_filtering_column,
            "output_format": policy.output_format,
            "partition_by": list(policy.partition_by),
            "write_policy_hash": policy.policy_hash,
            "input_row_count": int(base_row_count or delta_row_count),
            "has_incremental_input": policy.has_incremental_input,
            "incremental_inputs_have_additive_updates": policy.incremental_inputs_have_additive_updates,
        }

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
        time_column = str(output_metadata.get("time_column") or output_metadata.get("timeColumn") or "").strip()
        geometry_column = str(output_metadata.get("geometry_column") or output_metadata.get("geometryColumn") or "").strip()
        geometry_format = str(output_metadata.get("geometry_format") or output_metadata.get("geometryFormat") or "").strip().lower()
        if not time_column or not geometry_column:
            raise ValueError("geotemporal output requires time_column and geometry_column")
        if geometry_format not in {"wkt", "geojson"}:
            raise ValueError("geotemporal geometry_format must be one of: wkt|geojson")
        self._ensure_output_columns_present(
            df=df,
            required_columns=[time_column, geometry_column],
            output_kind=OUTPUT_KIND_GEOTEMPORAL,
        )
        return await self._materialize_output_dataframe(
            df,
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
        media_uri_column = str(output_metadata.get("media_uri_column") or output_metadata.get("mediaUriColumn") or "").strip()
        media_type = str(output_metadata.get("media_type") or output_metadata.get("mediaType") or "").strip().lower()
        if not media_uri_column:
            raise ValueError("media output requires media_uri_column")
        if media_type not in {"image", "video", "audio", "document"}:
            raise ValueError("media_type must be one of: image|video|audio|document")
        self._ensure_output_columns_present(
            df=df,
            required_columns=[media_uri_column],
            output_kind=OUTPUT_KIND_MEDIA,
        )
        return await self._materialize_output_dataframe(
            df,
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
        query_sql = str(output_metadata.get("query_sql") or output_metadata.get("querySql") or "").strip()
        refresh_mode = str(output_metadata.get("refresh_mode") or output_metadata.get("refreshMode") or "").strip().lower()
        if not query_sql:
            raise ValueError("virtual output requires query_sql")
        if refresh_mode not in {"on_read", "scheduled"}:
            raise ValueError("virtual output refresh_mode must be one of: on_read|scheduled")
        dataset_style_keys = [
            key
            for key in (
                "write_mode",
                "writeMode",
                "primary_key_columns",
                "primaryKeyColumns",
                "post_filtering_column",
                "postFilteringColumn",
                "output_format",
                "outputFormat",
                "partition_by",
                "partitionBy",
            )
            if output_metadata.get(key) not in (None, "", [])
        ]
        if dataset_style_keys:
            raise ValueError(
                "virtual output does not support dataset write settings: "
                + ", ".join(sorted(set(dataset_style_keys)))
            )
        if not self.storage:
            raise RuntimeError("Storage service not available")
        await self.storage.create_bucket(artifact_bucket)

        normalized_prefix = (prefix or "").lstrip("/").rstrip("/")
        if not normalized_prefix:
            raise ValueError("prefix is required")

        resolved_write_mode = str(write_mode or "overwrite").strip().lower() or "overwrite"
        if resolved_write_mode not in {"overwrite", "append"}:
            raise ValueError("write_mode must be overwrite or append")

        if resolved_write_mode == "overwrite":
            await self.storage.delete_prefix(artifact_bucket, normalized_prefix)

        manifest_name = "virtual_manifest.json"
        if resolved_write_mode == "append":
            unique_prefix = str(file_prefix or "").strip().replace("/", "_") or uuid4().hex[:12]
            manifest_name = f"{unique_prefix}_virtual_manifest.json"

        manifest_key = f"{normalized_prefix}/{manifest_name}"
        resolved_row_count_hint = (
            int(row_count_hint)
            if row_count_hint is not None
            else int(await self._run_spark(lambda: df.count(), label=f"count:virtual:{normalized_prefix}"))
        )
        manifest = {
            "output_kind": OUTPUT_KIND_VIRTUAL,
            "query_sql": query_sql,
            "refresh_mode": refresh_mode,
            "generated_at": utcnow().isoformat(),
            "input_columns": list(df.columns or []),
            "row_count_hint": resolved_row_count_hint,
            "file_format_ignored": str(file_format or "parquet").strip().lower() or "parquet",
            "partition_by_ignored": [str(col).strip() for col in (partition_cols or []) if str(col).strip()],
        }
        payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        await self.storage.save_bytes(
            artifact_bucket,
            manifest_key,
            payload,
            content_type="application/json",
        )
        return build_s3_uri(artifact_bucket, manifest_key)

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
        source_key_column = str(output_metadata.get("source_key_column") or output_metadata.get("sourceKeyColumn") or "").strip()
        target_key_column = str(output_metadata.get("target_key_column") or output_metadata.get("targetKeyColumn") or "").strip()
        required_columns = [column for column in (source_key_column, target_key_column) if column]
        if required_columns:
            self._ensure_output_columns_present(
                df=df,
                required_columns=required_columns,
                output_kind=OUTPUT_KIND_ONTOLOGY,
            )
        return await self._materialize_output_dataframe(
            df,
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
        if not required_columns:
            return
        available_columns = set(df.columns or [])
        missing_columns = [column for column in required_columns if column not in available_columns]
        if missing_columns:
            raise ValueError(
                f"{output_kind} output columns missing from dataframe: {', '.join(missing_columns)}"
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

    async def _assert_no_duplicate_primary_keys(
        self,
        *,
        df: DataFrame,
        pk_columns: List[str],
        write_mode: str,
        dataset_label: str,
    ) -> None:
        if not pk_columns:
            return
        duplicate_candidates = df.groupBy(*[F.col(column) for column in pk_columns]).count().filter(F.col("count") > 1).limit(1)
        duplicate_rows = await self._run_spark(
            lambda: duplicate_candidates.collect(),
            label=f"check_duplicate_pk:{dataset_label}",
        )
        if not duplicate_rows:
            return
        sample_row = duplicate_rows[0].asDict(recursive=True)
        duplicate_count = int(sample_row.get("count") or 0)
        sample_pk = {column: sample_row.get(column) for column in pk_columns}
        raise ValueError(
            "Duplicate primary_key_columns detected for "
            f"write_mode={write_mode}: sample={sample_pk}, duplicate_count={duplicate_count}"
        )

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
        if not self.storage:
            raise RuntimeError("Storage service not available")
        await self.storage.create_bucket(artifact_bucket)
        normalized_prefix = (prefix or "").lstrip("/").rstrip("/")
        if not normalized_prefix:
            raise ValueError("prefix is required")
        resolved_write_mode = str(write_mode or "overwrite").strip().lower() or "overwrite"
        if resolved_write_mode not in {"overwrite", "append"}:
            raise ValueError("write_mode must be overwrite or append")
        resolved_format = str(file_format or "parquet").strip().lower() or "parquet"
        if resolved_format not in {"parquet", "json", "csv", "avro", "orc"}:
            raise ValueError("file_format must be parquet|json|csv|avro|orc")
        resolved_partition_cols = [col for col in (partition_cols or []) if str(col).strip()]
        format_constraint_errors = validate_dataset_output_format_constraints(
            output_format=resolved_format,
            partition_by=resolved_partition_cols,
        )
        if format_constraint_errors:
            raise ValueError("; ".join(format_constraint_errors))
        if resolved_partition_cols:
            missing = [col for col in resolved_partition_cols if col not in df.columns]
            if missing:
                raise ValueError(f"Partition columns missing from output: {', '.join(missing)}")
        # Use stable output paths in lakeFS branches:
        # - overwrite: delete old part files then write the new snapshot.
        # - append: keep old part files and upload new parts under unique keys.
        if resolved_write_mode == "overwrite":
            await self.storage.delete_prefix(artifact_bucket, normalized_prefix)
        temp_dir = tempfile.mkdtemp(prefix="pipeline-output-")
        try:
            output_path = os.path.join(temp_dir, "data")
            writer = df.write.mode("overwrite")
            if resolved_partition_cols:
                writer = writer.partitionBy(*resolved_partition_cols)
            if resolved_format == "parquet":
                await self._run_spark(
                    lambda: writer.parquet(output_path),
                    label=f"write_parquet:{normalized_prefix}",
                )
            elif resolved_format == "json":
                await self._run_spark(
                    lambda: writer.json(output_path),
                    label=f"write_json:{normalized_prefix}",
                )
            elif resolved_format == "csv":
                await self._run_spark(
                    lambda: writer.option("header", "true").csv(output_path),
                    label=f"write_csv:{normalized_prefix}",
                )
            elif resolved_format == "avro":
                await self._run_spark(
                    lambda: writer.format("avro").save(output_path),
                    label=f"write_avro:{normalized_prefix}",
                )
            else:
                await self._run_spark(
                    lambda: writer.orc(output_path),
                    label=f"write_orc:{normalized_prefix}",
                )
            extension_map = {
                "parquet": {".parquet"},
                "json": {".json"},
                "csv": {".csv"},
                "avro": {".avro"},
                "orc": {".orc"},
            }
            part_files = _list_part_files(
                output_path,
                extensions=extension_map.get(resolved_format, {f".{resolved_format}"}),
            )
            if not part_files:
                raise FileNotFoundError("Spark output part files not found")
            unique_prefix = str(file_prefix or "").strip().replace("/", "_")
            if not unique_prefix:
                unique_prefix = uuid4().hex[:12]
            for part_file in part_files:
                rel_path = os.path.relpath(part_file, output_path)
                base_name = os.path.basename(rel_path)
                dir_name = os.path.dirname(rel_path)
                if resolved_write_mode == "append":
                    base_name = f"{unique_prefix}_{base_name}"
                rel_path = os.path.join(dir_name, base_name) if dir_name else base_name
                target_key = f"{normalized_prefix}/{rel_path.replace(os.sep, '/')}"
                content_type = {
                    "parquet": "application/x-parquet",
                    "json": "application/json",
                    "csv": "text/csv",
                    "avro": "application/avro",
                    "orc": "application/orc",
                }.get(resolved_format, "application/octet-stream")
                with open(part_file, "rb") as handle:
                    await self.storage.save_bytes(
                        artifact_bucket,
                        target_key,
                        handle.read(),
                        content_type=content_type,
                    )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
        return build_s3_uri(artifact_bucket, normalized_prefix)

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
        if watermark_after is None:
            return df
        if not watermark_keys:
            return df.filter(F.col(watermark_column) >= F.lit(watermark_after))
        key_col = "__watermark_key"
        df = df.withColumn(key_col, self._row_hash_expr(df))
        df = df.filter(
            (F.col(watermark_column) > F.lit(watermark_after))
            | (
                (F.col(watermark_column) == F.lit(watermark_after))
                & (~F.col(key_col).isin(watermark_keys))
            )
        ).drop(key_col)
        return df

    def _collect_watermark_keys(
        self,
        df: DataFrame,
        *,
        watermark_column: str,
        watermark_value: Any,
    ) -> list[str]:
        if watermark_value is None:
            return []
        key_col = "__watermark_key"
        try:
            rows = (
                df.filter(F.col(watermark_column) == F.lit(watermark_value))
                .select(self._row_hash_expr(df).alias(key_col))
                .distinct()
                .collect()
            )
        except Exception as exc:
            logger.warning("Failed to collect changed row keys: %s", exc, exc_info=True)
            return []
        keys: list[str] = []
        for row in rows:
            value = row[key_col]
            if value:
                keys.append(str(value))
        return keys

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
        requested_branch = selection.requested_branch

        read_config = metadata.get("read") if isinstance(metadata.get("read"), dict) else {}

        # External inputs (no DatasetRegistry selection) are loaded directly via Spark read config.
        if not dataset_id and not dataset_name:
            read_mode = _resolve_external_read_mode(read_config=read_config)
            df = await self._run_spark(
                lambda: self._load_external_input_dataframe(read_config, node_id=node_id, temp_dirs=temp_dirs),
                label=f"external_input:{node_id}",
            )
            if input_snapshots is not None:
                fmt = str(read_config.get("format") or read_config.get("file_format") or read_config.get("fileFormat") or "").strip().lower() or None
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
            raise RuntimeError(f"Input node {node_id} dataset version has no artifact_key (dataset_id={dataset.dataset_id})")

        resolved_branch_value = resolved_branch or dataset.branch or requested_branch
        used_fallback = resolution.used_fallback

        snapshot: Optional[dict[str, Any]] = None
        if input_snapshots is not None:
            snapshot = {
                "node_id": node_id,
                "dataset_id": dataset.dataset_id,
                "dataset_name": dataset.name,
                "dataset_branch": resolved_branch_value,
                "requested_dataset_branch": requested_branch,
                "used_fallback": used_fallback,
                "lakefs_commit_id": version.lakefs_commit_id,
                "version_id": version.version_id,
                "artifact_key": version.artifact_key,
            }

        parsed = parse_s3_uri(version.artifact_key)
        if not parsed:
            raise ValueError(
                f"Input node {node_id} artifact_key is not a valid s3:// URI: {version.artifact_key}"
            )
        bucket, key = parsed
        current_commit_id = str(version.lakefs_commit_id or "").strip()
        artifact_prefix = self._strip_commit_prefix(key, current_commit_id)
        dataset_branch_for_diff = safe_lakefs_ref(resolved_branch_value or "main")
        diff_requested = bool(
            use_lakefs_diff and previous_commit_id and current_commit_id and self.lakefs_client
        )
        if diff_requested:
            diff_paths: List[str] = []
            diff_ok = True
            if previous_commit_id != current_commit_id:
                diff_paths, diff_ok = await self._list_lakefs_diff_paths(
                    repository=bucket,
                    ref=dataset_branch_for_diff,
                    since=previous_commit_id,
                    prefix=artifact_prefix,
                    node_id=node_id,
                )
            diff_paths_count = len(diff_paths)
            parquet_paths = [path for path in diff_paths if path.endswith(".parquet")]
            if snapshot is not None:
                snapshot.update(
                    {
                        "previous_commit_id": previous_commit_id,
                        "diff_requested": True,
                        "diff_ok": diff_ok,
                        "diff_paths_count": diff_paths_count,
                        "diff_empty": bool(diff_ok and diff_paths_count == 0),
                    }
                )
            if diff_ok and diff_paths_count == 0:
                if snapshot is not None:
                    input_snapshots.append(snapshot)
                return self._empty_dataframe()
            if parquet_paths:
                df = await self._load_parquet_keys_dataframe(
                    bucket=bucket,
                    keys=[f"{current_commit_id}/{path.lstrip('/')}" for path in parquet_paths],
                    temp_dirs=temp_dirs,
                    prefix=f"{current_commit_id}/{artifact_prefix}".rstrip("/"),
                )
                df = self._apply_schema_casts(df, dataset=dataset, version=version)
                if snapshot is not None:
                    snapshot["diff_used"] = True
                    snapshot["diff_parquet_paths"] = len(parquet_paths)
                resolved_watermark_column = str(watermark_column or "").strip()
                if resolved_watermark_column:
                    if resolved_watermark_column not in df.columns:
                        raise ValueError(
                            f"Input node {node_id} is missing watermark column '{resolved_watermark_column}'"
                        )
                    df = self._apply_watermark_filter(
                        df,
                        watermark_column=resolved_watermark_column,
                        watermark_after=watermark_after,
                        watermark_keys=watermark_keys,
                    )
                    if input_snapshots is not None:
                        snapshot = {
                            "node_id": node_id,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "dataset_branch": resolved_branch_value,
                            "requested_dataset_branch": requested_branch,
                            "used_fallback": used_fallback,
                            "lakefs_commit_id": current_commit_id,
                            "previous_commit_id": previous_commit_id,
                            "version_id": version.version_id,
                            "artifact_key": version.artifact_key,
                            "diff_requested": True,
                            "diff_ok": diff_ok,
                            "diff_paths_count": diff_paths_count,
                            "diff_empty": bool(diff_ok and diff_paths_count == 0),
                            "diff_used": True,
                            "diff_parquet_paths": len(parquet_paths),
                        }
                        snapshot["watermark_column"] = resolved_watermark_column
                        if watermark_after is not None:
                            snapshot["watermark_after"] = watermark_after
                        try:
                            watermark_max = await self._run_spark(
                                lambda: df.agg(F.max(F.col(resolved_watermark_column)).alias("watermark_max"))
                                .collect()[0]["watermark_max"],
                                label=f"watermark_max:diff:{node_id}",
                            )
                        except Exception as exc:
                            logger.warning(
                                "Failed to compute watermark max for node %s (column=%s): %s",
                                node_id,
                                resolved_watermark_column,
                                exc,
                                exc_info=True,
                            )
                            watermark_max = None
                        snapshot["watermark_max"] = watermark_max
                        if watermark_max is not None:
                            snapshot["watermark_keys"] = await self._run_spark(
                                lambda: self._collect_watermark_keys(
                                    df,
                                    watermark_column=resolved_watermark_column,
                                    watermark_value=watermark_max,
                                ),
                                label=f"watermark_keys:diff:{node_id}",
                            )
                        input_snapshots.append(snapshot)
                elif input_snapshots is not None:
                    input_snapshots.append(
                        {
                            "node_id": node_id,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "dataset_branch": resolved_branch_value,
                            "requested_dataset_branch": requested_branch,
                            "used_fallback": used_fallback,
                            "lakefs_commit_id": current_commit_id,
                            "previous_commit_id": previous_commit_id,
                            "version_id": version.version_id,
                            "artifact_key": version.artifact_key,
                            "diff_requested": True,
                            "diff_ok": diff_ok,
                            "diff_paths_count": diff_paths_count,
                            "diff_empty": bool(diff_ok and diff_paths_count == 0),
                            "diff_used": True,
                            "diff_parquet_paths": len(parquet_paths),
                        }
                    )
                return df
            if diff_ok and diff_paths_count:
                logger.warning(
                    "lakeFS diff returned %s paths without parquet data for input node %s; falling back to full load",
                    diff_paths_count,
                    node_id,
                )

        source_type = str(getattr(dataset, "source_type", "") or "").strip().lower()
        read_config = metadata.get("read") if isinstance(metadata.get("read"), dict) else {}
        if source_type == "media":
            df = await self._load_media_prefix_dataframe(bucket, key, node_id=node_id)
        else:
            df = await self._load_artifact_dataframe(bucket, key, temp_dirs, read_config=read_config)
        df = self._apply_schema_casts(df, dataset=dataset, version=version)

        resolved_watermark_column = str(watermark_column or "").strip()
        if resolved_watermark_column:
            if resolved_watermark_column not in df.columns:
                raise ValueError(
                    f"Input node {node_id} is missing watermark column '{resolved_watermark_column}'"
                )
            if snapshot is not None:
                snapshot["watermark_column"] = resolved_watermark_column
                if watermark_after is not None:
                    snapshot["watermark_after"] = watermark_after
            df = self._apply_watermark_filter(
                df,
                watermark_column=resolved_watermark_column,
                watermark_after=watermark_after,
                watermark_keys=watermark_keys,
            )
            watermark_max = await self._run_spark(
                lambda: df.agg(F.max(F.col(resolved_watermark_column)).alias("watermark_max"))
                .collect()[0]["watermark_max"],
                label=f"watermark_max:{node_id}",
            )
            if snapshot is not None:
                snapshot["watermark_max"] = watermark_max
                if watermark_max is not None:
                    snapshot["watermark_keys"] = await self._run_spark(
                        lambda: self._collect_watermark_keys(
                            df,
                            watermark_column=resolved_watermark_column,
                            watermark_value=watermark_max,
                        ),
                        label=f"watermark_keys:{node_id}",
                    )

        if snapshot is not None:
            input_snapshots.append(snapshot)
        return df

    def _preview_sampling_seed(self, job_id: str) -> int:
        seed_raw = abs(hash(str(job_id)))
        return seed_raw % 2_147_483_647

    def _resolve_sampling_strategy(
        self,
        metadata: Dict[str, Any],
        preview_meta: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        raw = metadata.get("samplingStrategy") or metadata.get("sampling_strategy")
        if raw is None:
            raw = preview_meta.get("samplingStrategy") or preview_meta.get("sampling_strategy")
        if raw is None:
            return None
        if isinstance(raw, str):
            return {"type": raw}
        if isinstance(raw, dict):
            return raw
        raise ValueError("sampling_strategy must be an object")

    def _attach_sampling_snapshot(
        self,
        input_snapshots: list[dict[str, Any]],
        *,
        node_id: str,
        sampling_strategy: Dict[str, Any],
    ) -> None:
        for snapshot in reversed(input_snapshots):
            if snapshot.get("node_id") == node_id:
                snapshot["sampling_strategy"] = sampling_strategy
                break

    def _normalize_sampling_fraction(self, value: Any, *, field: str) -> float:
        try:
            fraction = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"sampling_strategy.{field} must be a number")
        if fraction <= 0 or fraction > 1:
            raise ValueError(f"sampling_strategy.{field} must be within (0, 1]")
        return fraction

    def _apply_sampling_strategy(
        self,
        df: DataFrame,
        sampling_strategy: Dict[str, Any],
        *,
        node_id: str,
        seed: Optional[int],
    ) -> DataFrame:
        strategy = sampling_strategy or {}
        strategy_type = str(strategy.get("type") or strategy.get("mode") or "").strip().lower()
        if not strategy_type:
            raise ValueError(f"sampling_strategy.type is required for input node {node_id}")
        if strategy_type in {"random", "sample", "bernoulli", "tablesample"}:
            fraction = strategy.get("fraction")
            if fraction is None:
                percent = strategy.get("percent")
                if percent is not None:
                    fraction = float(percent) / 100.0
            if fraction is None:
                raise ValueError(f"sampling_strategy.fraction is required for input node {node_id}")
            resolved_fraction = self._normalize_sampling_fraction(fraction, field="fraction")
            with_replacement = bool(strategy.get("with_replacement") or strategy.get("withReplacement") or False)
            return df.sample(withReplacement=with_replacement, fraction=resolved_fraction, seed=seed)
        if strategy_type in {"stratified", "sampleby"}:
            column = str(strategy.get("column") or "").strip()
            fractions_raw = strategy.get("fractions")
            if not column:
                raise ValueError(f"sampling_strategy.column is required for input node {node_id}")
            if not isinstance(fractions_raw, dict) or not fractions_raw:
                raise ValueError(f"sampling_strategy.fractions is required for input node {node_id}")
            fractions: Dict[Any, float] = {}
            for key, value in fractions_raw.items():
                fractions[key] = self._normalize_sampling_fraction(value, field="fractions")
            return df.sampleBy(column, fractions, seed=seed)
        if strategy_type in {"limit", "head"}:
            limit = strategy.get("limit") or strategy.get("rows")
            try:
                limit_value = int(limit)
            except (TypeError, ValueError):
                raise ValueError(f"sampling_strategy.limit must be an integer for input node {node_id}")
            if limit_value <= 0:
                raise ValueError(f"sampling_strategy.limit must be positive for input node {node_id}")
            return df.limit(limit_value)
        raise ValueError(f"Unsupported sampling_strategy.type '{strategy_type}' for input node {node_id}")

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
        errors: List[str] = []
        if not required_node_ids:
            return errors
        for node_id in required_node_ids:
            node = nodes.get(node_id) or {}
            node_type = str(node.get("type") or "transform")
            if node_type in {"transform", "output"} and not incoming.get(node_id):
                errors.append(f"{node_type} node {node_id} has no input")
        return errors

    def _validate_definition(self, definition: Dict[str, Any], *, require_output: bool = True) -> List[str]:
        policy = PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS_SPARK,
            require_output=require_output,
            normalize_metadata=True,
            require_udf_reference=self._udf_require_reference,
            require_udf_version_pinning=self._udf_require_version_pinning,
        )
        validation = validate_pipeline_definition(definition, policy=policy)
        errors: List[str] = list(validation.errors)
        nodes = validation.nodes

        for node_id, node in nodes.items():
            if node.get("type") != "transform":
                continue
            metadata = node.get("metadata") or {}

            checks = metadata.get("schemaChecks") or []
            if checks:
                if not isinstance(checks, list):
                    errors.append(f"schemaChecks must be a list on node {node_id}")
                else:
                    for check in checks:
                        if not isinstance(check, dict):
                            errors.append(f"schema check invalid on node {node_id}")
                            continue
                        rule = str(check.get("rule") or "").strip()
                        column = str(check.get("column") or "").strip()
                        if not rule:
                            errors.append(f"schema check missing rule on node {node_id}")
                        if not column:
                            errors.append(f"schema check missing column on node {node_id}")

        expectations = definition.get("expectations") or []
        if expectations:
            if not isinstance(expectations, list):
                errors.append("expectations must be a list")
            else:
                for exp in expectations:
                    if not isinstance(exp, dict):
                        errors.append("expectation entry must be an object")
                        continue
                    rule = str(exp.get("rule") or "").strip()
                    if not rule:
                        errors.append("expectation missing rule")
                        continue
                    column = str(exp.get("column") or "").strip()
                    if rule not in {"row_count_min", "row_count_max"} and not column:
                        errors.append(f"expectation {rule} missing column")

        contract = definition.get("schemaContract") or definition.get("schema_contract") or []
        if contract:
            if not isinstance(contract, list):
                errors.append("schemaContract must be a list")
            else:
                for item in contract:
                    if not isinstance(item, dict):
                        errors.append("schemaContract entry must be an object")
                        continue
                    column = str(item.get("column") or "").strip()
                    if not column:
                        errors.append("schemaContract missing column")

        dependencies = definition.get("dependencies") or []
        if dependencies:
            if not isinstance(dependencies, list):
                errors.append("dependencies must be a list")
            else:
                for dep in dependencies:
                    if not isinstance(dep, dict):
                        errors.append("dependency entry must be an object")
                        continue
                    pipeline_id = dep.get("pipelineId") or dep.get("pipeline_id")
                    if not pipeline_id:
                        errors.append("dependency missing pipeline_id")

        return errors

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

    def _clean_string_column(self, column: "Column") -> "Column":
        trimmed = F.trim(column.cast("string"))
        return F.when((trimmed.isNull()) | (F.length(trimmed) == 0), F.lit(None)).otherwise(trimmed)

    def _try_cast_column(self, column: str, spark_type: str) -> "Column":
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

    def _safe_cast_column(self, column: str, target_type: str) -> "Column":
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

    async def _load_prefix_dataframe(
        self,
        bucket: str,
        prefix: str,
        temp_dirs: list[str],
        *,
        read_config: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        read_config = dict(read_config or {})
        objects = await self.storage.list_objects(bucket, prefix=prefix)
        keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
        data_keys = [key for key in keys if _is_data_object(key)]
        if not data_keys:
            return self._empty_dataframe()

        temp_dir = tempfile.mkdtemp(prefix="pipeline-input-")
        temp_dirs.append(temp_dir)
        local_paths: list[str] = []
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
