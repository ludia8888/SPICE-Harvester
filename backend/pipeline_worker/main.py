"""
Pipeline Worker (Spark/Flink-ready execution runtime).

Consumes Kafka pipeline-jobs and executes dataset transforms using Spark.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import httpx
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.models.pipeline_job import PipelineJob
from shared.services.dataset_registry import DatasetRegistry
from shared.services.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.lakefs_storage_service import LakeFSStorageService
from shared.services.lineage_store import LineageStore
from shared.services.pipeline_profiler import compute_column_stats
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_control_plane_events import emit_pipeline_control_plane_event
from shared.services.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline_parameter_utils import apply_parameters, normalize_parameters
from shared.services.pipeline_schema_utils import (
    normalize_expectations,
    normalize_number,
    normalize_schema_checks,
    normalize_schema_contract,
    normalize_schema_type,
    normalize_value_list,
)
from shared.services.pipeline_transform_spec import (
    SUPPORTED_TRANSFORMS,
    normalize_operation,
    normalize_union_mode,
    resolve_join_spec,
)
from shared.services.objectify_registry import ObjectifyRegistry
from shared.services.objectify_job_queue import ObjectifyJobQueue
from shared.models.objectify_job import ObjectifyJob
from shared.services.processed_event_registry import ProcessedEventRegistry, ClaimDecision
from shared.services.redis_service import RedisService, create_redis_service_legacy
from shared.services.storage_service import StorageService
from shared.security.auth_utils import get_expected_token
from shared.utils.env_utils import parse_bool_env, parse_int_env
from shared.utils.path_utils import safe_lakefs_ref
from shared.utils.s3_uri import build_s3_uri, parse_s3_uri
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)

_SENSITIVE_CONF_TOKENS = (
    "secret",
    "password",
    "token",
    "access.key",
    "secret.key",
    "session.token",
    "aws_access",
    "aws_secret",
    "credentials",
)

_BFF_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")


def _resolve_code_version() -> Optional[str]:
    for key in ("CODE_SHA", "GIT_SHA", "COMMIT_SHA"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _is_sensitive_conf_key(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(token in lowered for token in _SENSITIVE_CONF_TOKENS)


def _resolve_lakefs_repository() -> str:
    repo = (os.getenv("LAKEFS_ARTIFACTS_REPOSITORY") or "").strip()
    if repo:
        return repo
    return "pipeline-artifacts"


class PipelineLockError(RuntimeError):
    pass


class PipelineLock:
    def __init__(
        self,
        *,
        redis_client: Any,
        key: str,
        token: str,
        ttl_seconds: int,
        renew_seconds: int,
    ) -> None:
        self._redis = redis_client
        self._key = key
        self._token = token
        self._ttl_seconds = ttl_seconds
        self._renew_seconds = max(0, int(renew_seconds))
        self._renew_task: Optional[asyncio.Task] = None
        self._lost = False

    async def start(self) -> None:
        if self._renew_seconds <= 0:
            return
        if self._renew_task:
            return
        self._renew_task = asyncio.create_task(self._renew_loop())

    def raise_if_lost(self) -> None:
        if self._lost:
            raise PipelineLockError(f"Pipeline lock lost: {self._key}")

    async def release(self) -> None:
        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('DEL', KEYS[1]) else return 0 end"
        )
        try:
            await self._redis.eval(script, 1, self._key, self._token)
        except Exception as exc:
            logger.warning("Failed to release pipeline lock (key=%s): %s", self._key, exc)

    async def _renew_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._renew_seconds)
                ok = await self._extend()
                if not ok:
                    self._lost = True
                    logger.error("Pipeline lock lost during renewal (key=%s)", self._key)
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._lost = True
                logger.error("Pipeline lock renewal failed (key=%s): %s", self._key, exc)
                return

    async def _extend(self) -> bool:
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
        )
        result = await self._redis.eval(script, 1, self._key, self._token, self._ttl_seconds)
        return bool(result)


def _resolve_pipeline_execution_mode(*, pipeline_type: str, definition: Dict[str, Any]) -> str:
    """
    Normalize execution semantics into one of:
    - snapshot (default): full recompute + overwrite output prefix
    - incremental: watermark-based delta processing + append output prefix
    - streaming: job-group semantics (implemented as micro-batch + append for now)
    """

    raw_type = str(pipeline_type or "").strip().lower()

    raw_mode = ""
    for key in ("execution_mode", "executionMode", "run_mode", "runMode", "batch_mode", "batchMode"):
        value = definition.get(key)
        if isinstance(value, str) and value.strip():
            raw_mode = value.strip().lower()
            break
    if not raw_mode:
        settings = definition.get("settings")
        if isinstance(settings, dict):
            engine = settings.get("engine")
            if isinstance(engine, str) and engine.strip():
                raw_mode = engine.strip().lower()

    if raw_mode in {"incremental", "increment", "append"}:
        return "incremental"
    if raw_mode in {"stream", "streaming"}:
        return "streaming"
    if raw_type in {"stream", "streaming"}:
        return "streaming"
    if raw_type in {"incremental", "increment", "inc"}:
        # Default incremental semantics for pipelines explicitly marked incremental.
        return "incremental"
    return "snapshot"


def _resolve_incremental_config(definition: Dict[str, Any]) -> Dict[str, Any]:
    raw = definition.get("incremental") or definition.get("incremental_config") or definition.get("incrementalConfig")
    return dict(raw) if isinstance(raw, dict) else {}


def _resolve_watermark_column(*, incremental: Dict[str, Any], metadata: Dict[str, Any]) -> Optional[str]:
    for key in ("watermark_column", "watermarkColumn", "watermark"):
        value = metadata.get(key) if isinstance(metadata, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()
        value = incremental.get(key) if isinstance(incremental, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _max_watermark_from_snapshots(
    input_snapshots: list[dict[str, Any]], *, watermark_column: str
) -> Optional[Any]:
    resolved_column = str(watermark_column or "").strip()
    if not resolved_column:
        return None
    max_value: Optional[Any] = None
    for snapshot in input_snapshots or []:
        if not isinstance(snapshot, dict):
            continue
        if str(snapshot.get("watermark_column") or "").strip() != resolved_column:
            continue
        candidate = snapshot.get("watermark_max")
        if candidate is None:
            continue
        if max_value is None:
            max_value = candidate
            continue
        try:
            if candidate > max_value:
                max_value = candidate
        except TypeError:
            try:
                if float(candidate) > float(max_value):
                    max_value = candidate
            except Exception:
                if str(candidate) > str(max_value):
                    max_value = candidate
    return max_value


def _watermark_values_match(left: Any, right: Any) -> bool:
    if left == right:
        return True
    try:
        return float(left) == float(right)
    except Exception:
        return str(left) == str(right)


def _collect_watermark_keys_from_snapshots(
    input_snapshots: list[dict[str, Any]], *, watermark_column: str, watermark_value: Any
) -> list[str]:
    resolved_column = str(watermark_column or "").strip()
    if not resolved_column or watermark_value is None:
        return []
    keys: list[str] = []
    for snapshot in input_snapshots or []:
        if not isinstance(snapshot, dict):
            continue
        if str(snapshot.get("watermark_column") or "").strip() != resolved_column:
            continue
        candidate_value = snapshot.get("watermark_max")
        if candidate_value is None or not _watermark_values_match(candidate_value, watermark_value):
            continue
        raw_keys = snapshot.get("watermark_keys") or snapshot.get("watermarkKeys")
        if not isinstance(raw_keys, list):
            continue
        for item in raw_keys:
            item_str = str(item or "").strip()
            if item_str:
                keys.append(item_str)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in keys:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _collect_input_commit_map(input_snapshots: list[dict[str, Any]]) -> Dict[str, str]:
    commits: Dict[str, str] = {}
    for snapshot in input_snapshots or []:
        if not isinstance(snapshot, dict):
            continue
        node_id = str(snapshot.get("node_id") or "").strip()
        commit_id = str(snapshot.get("lakefs_commit_id") or "").strip()
        if node_id and commit_id:
            commits[node_id] = commit_id
    return commits


def _inputs_diff_empty(input_snapshots: list[dict[str, Any]]) -> bool:
    diff_snapshots = [
        snapshot
        for snapshot in input_snapshots or []
        if isinstance(snapshot, dict) and snapshot.get("diff_requested")
    ]
    if not diff_snapshots:
        return False
    return all(bool(snapshot.get("diff_ok")) and bool(snapshot.get("diff_empty")) for snapshot in diff_snapshots)


def _resolve_execution_semantics(*, job: PipelineJob, definition: Dict[str, Any]) -> str:
    return _resolve_pipeline_execution_mode(pipeline_type=str(job.pipeline_type or ""), definition=definition)


def _resolve_output_format(*, definition: Dict[str, Any], output_metadata: Dict[str, Any]) -> str:
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    for key in ("outputFormat", "output_format", "format"):
        raw = output_metadata.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
        raw = settings.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
        raw = definition.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
    return "parquet"


def _resolve_partition_columns(
    *,
    definition: Dict[str, Any],
    output_metadata: Dict[str, Any],
) -> List[str]:
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    raw = (
        output_metadata.get("partitionBy")
        or output_metadata.get("partition_by")
        or settings.get("partitionBy")
        or settings.get("partition_by")
        or definition.get("partitionBy")
        or definition.get("partition_by")
    )
    if raw is None:
        return []
    if isinstance(raw, str):
        values = [item.strip() for item in raw.split(",") if item.strip()]
    elif isinstance(raw, list):
        values = [str(item).strip() for item in raw if str(item).strip()]
    else:
        return []
    seen: set[str] = set()
    deduped: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


class PipelineWorker:
    def __init__(self) -> None:
        self.running = False
        self.topic = (os.getenv("PIPELINE_JOBS_TOPIC") or "pipeline-jobs").strip() or "pipeline-jobs"
        self.dlq_topic = (os.getenv("PIPELINE_JOBS_DLQ_TOPIC") or "pipeline-jobs-dlq").strip() or "pipeline-jobs-dlq"
        self.group_id = (os.getenv("PIPELINE_JOBS_GROUP") or "pipeline-worker-group").strip()
        self.handler = (os.getenv("PIPELINE_WORKER_HANDLER") or "pipeline_worker").strip()
        self.pipeline_label = (os.getenv("PIPELINE_WORKER_NAME") or "pipeline_worker").strip()

        self.max_retries = parse_int_env("PIPELINE_JOBS_MAX_RETRIES", 5, min_value=1, max_value=100)
        self.backoff_base = parse_int_env("PIPELINE_JOBS_BACKOFF_BASE_SECONDS", 2, min_value=0, max_value=300)
        self.backoff_max = parse_int_env("PIPELINE_JOBS_BACKOFF_MAX_SECONDS", 60, min_value=1, max_value=3600)

        self.consumer: Optional[Consumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.dataset_registry: Optional[DatasetRegistry] = None
        self.pipeline_registry: Optional[PipelineRegistry] = None
        self.objectify_registry: Optional[ObjectifyRegistry] = None
        self.objectify_job_queue: Optional[ObjectifyJobQueue] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.lineage: Optional[LineageStore] = None
        self.storage: Optional[StorageService] = None
        self.lakefs_client: Optional[LakeFSClient] = None
        self.sheets: Optional[GoogleSheetsService] = None
        self.http: Optional[httpx.AsyncClient] = None
        self.spark: Optional[SparkSession] = None
        self.redis: Optional[RedisService] = None
        self.lock_enabled = parse_bool_env("PIPELINE_LOCKS_ENABLED", True)
        self.lock_required = parse_bool_env("PIPELINE_LOCKS_REQUIRED", True)
        self.lock_ttl_seconds = parse_int_env(
            "PIPELINE_LOCK_TTL_SECONDS", 3600, min_value=60, max_value=86_400
        )
        self.lock_renew_seconds = parse_int_env(
            "PIPELINE_LOCK_RENEW_SECONDS", 300, min_value=10, max_value=3_600
        )
        self.lock_retry_seconds = parse_int_env(
            "PIPELINE_LOCK_RETRY_SECONDS", 5, min_value=1, max_value=600
        )
        self.lock_acquire_timeout_seconds = parse_int_env(
            "PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS", 3600, min_value=30, max_value=86_400
        )

    async def initialize(self) -> None:
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

        self.processed = ProcessedEventRegistry()
        await self.processed.initialize()

        self.lineage = LineageStore()
        try:
            await self.lineage.initialize()
        except Exception as exc:
            logger.warning("LineageStore unavailable: %s", exc)
            self.lineage = None

        self.storage = await self.pipeline_registry.get_lakefs_storage()
        self.lakefs_client = await self.pipeline_registry.get_lakefs_client()

        api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_SHEETS_API_KEY") or "").strip() or None
        self.sheets = GoogleSheetsService(api_key=api_key)

        token = get_expected_token(_BFF_TOKEN_ENV_KEYS)
        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Admin-Token"] = token
        self.http = httpx.AsyncClient(timeout=120.0, headers=headers)

        if self.lock_enabled:
            self.redis = create_redis_service_legacy()
            try:
                await self.redis.connect()
            except Exception as exc:
                if self.lock_required:
                    raise
                logger.warning("Pipeline locks disabled (redis unavailable): %s", exc)
                self.redis = None
                self.lock_enabled = False

        self.spark = (
            SparkSession.builder.appName("spice-pipeline-worker")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.sql.ansi.enabled",
                "true"
                if (os.getenv("PIPELINE_SPARK_ANSI_ENABLED") or "true").strip().lower() in {"1", "true", "yes", "on"}
                else "false",
            )
            .getOrCreate()
        )

        self.consumer = Consumer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 300000,
            }
        )
        self.consumer.subscribe([self.topic])
        logger.info("PipelineWorker initialized (topic=%s)", self.topic)

        self.dlq_producer = Producer(
            {
                "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                "client.id": os.getenv("SERVICE_NAME") or "pipeline-worker-dlq",
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 100,
                "linger.ms": 20,
                "compression.type": "snappy",
            }
        )

    async def close(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.dlq_producer:
            try:
                self.dlq_producer.flush(5)
            except Exception:
                pass
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

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        try:
            while self.running:
                msg = self.consumer.poll(1.0) if self.consumer else None
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                raw_value: Optional[bytes] = None
                raw_text: Optional[str] = None
                payload: Optional[Dict[str, Any]] = None

                try:
                    raw_value = msg.value()
                    raw_text = raw_value.decode("utf-8")
                except Exception as exc:
                    await self._send_to_dlq(
                        msg=msg,
                        stage="decode",
                        error=str(exc),
                        payload_text=None,
                        payload_obj=None,
                        job=None,
                        attempt_count=None,
                    )
                    if self.consumer:
                        self.consumer.commit(msg)
                    continue

                try:
                    payload = json.loads(raw_text)
                except Exception as exc:
                    await self._send_to_dlq(
                        msg=msg,
                        stage="json",
                        error=str(exc),
                        payload_text=raw_text,
                        payload_obj=None,
                        job=None,
                        attempt_count=None,
                    )
                    if self.consumer:
                        self.consumer.commit(msg)
                    continue

                job: Optional[PipelineJob] = None
                try:
                    job = PipelineJob.model_validate(payload)
                except Exception as exc:
                    await self._send_to_dlq(
                        msg=msg,
                        stage="validate",
                        error=str(exc),
                        payload_text=raw_text,
                        payload_obj=payload,
                        job=None,
                        attempt_count=None,
                    )
                    try:
                        await self._best_effort_record_invalid_job(payload, error=str(exc))
                    except Exception:
                        pass
                    if self.consumer:
                        self.consumer.commit(msg)
                    continue

                claim = None
                heartbeat_task: Optional[asyncio.Task] = None
                try:
                    if not self.processed:
                        raise RuntimeError("ProcessedEventRegistry not available")

                    claim = await self.processed.claim(
                        handler=self.handler,
                        event_id=job.job_id,
                        aggregate_id=job.pipeline_id,
                    )

                    if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                        if self.consumer:
                            self.consumer.commit(msg)
                        continue

                    if claim.decision == ClaimDecision.IN_PROGRESS:
                        await asyncio.sleep(2)
                        if self.consumer:
                            self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                        continue

                    heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(handler=self.handler, event_id=job.job_id)
                    )

                    await self._execute_job(job)
                    await self.processed.mark_done(handler=self.handler, event_id=job.job_id)
                    if self.consumer:
                        self.consumer.commit(msg)
                except Exception as exc:
                    err = str(exc)
                    attempt_count = int(getattr(claim, "attempt_count", 1) or 1)
                    if self.processed and job:
                        try:
                            await self.processed.mark_failed(handler=self.handler, event_id=job.job_id, error=err)
                        except Exception as mark_err:
                            logger.warning("Failed to mark pipeline job failed: %s", mark_err)

                    if attempt_count >= self.max_retries:
                        logger.error(
                            "Pipeline job max retries exceeded; sending to DLQ (job_id=%s attempt=%s)",
                            getattr(job, "job_id", None),
                            attempt_count,
                        )
                        await self._send_to_dlq(
                            msg=msg,
                            stage="execute",
                            error=err,
                            payload_text=raw_text,
                            payload_obj=payload,
                            job=job,
                            attempt_count=attempt_count,
                        )
                        if self.consumer:
                            self.consumer.commit(msg)
                        continue

                    backoff_s = min(self.backoff_max, int(self.backoff_base * (2 ** max(0, attempt_count - 1))))
                    logger.warning(
                        "Pipeline job failed; will retry (job_id=%s attempt=%s backoff=%ss): %s",
                        getattr(job, "job_id", None),
                        attempt_count,
                        backoff_s,
                        err,
                    )
                    await asyncio.sleep(backoff_s)
                    if self.consumer:
                        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                finally:
                    if heartbeat_task:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass
        finally:
            await self.close()

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        if not self.processed:
            return
        interval = parse_int_env("PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS", 30, min_value=1, max_value=3600)
        while True:
            try:
                await asyncio.sleep(interval)
                await self.processed.heartbeat(handler=handler, event_id=event_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Heartbeat failed (handler=%s event_id=%s): %s", handler, event_id, exc)

    async def _send_to_dlq(
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
        if not self.dlq_producer:
            logger.error("DLQ producer not configured; dropping message (stage=%s error=%s)", stage, error)
            return

        try:
            original_value = payload_text if payload_text is not None else msg.value().decode("utf-8", errors="replace")
        except Exception:
            original_value = "<unavailable>"

        dlq_message: Dict[str, Any] = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "original_timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "original_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
            "original_value": original_value,
            "stage": stage,
            "error": error,
            "attempt_count": attempt_count,
            "worker": self.pipeline_label,
            "timestamp": utcnow().isoformat(),
        }
        if payload_obj is not None:
            dlq_message["parsed_payload"] = payload_obj
        if job is not None:
            dlq_message["job"] = job.model_dump(mode="json")

        try:
            key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}".encode("utf-8")
            value = json.dumps(dlq_message, ensure_ascii=False, default=str).encode("utf-8")
            self.dlq_producer.produce(self.dlq_topic, key=key, value=value)
            self.dlq_producer.flush(10)
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

        payload_obj = {"errors": [error], "stage": "validate"}
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
        except Exception:
            pass
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
        preview_meta = definition.get("__preview_meta__") or {}
        tables: Dict[str, DataFrame] = {}
        nodes = normalize_nodes(definition.get("nodes"))
        edges = normalize_edges(definition.get("edges"))
        order = topological_sort(nodes, edges, include_unordered=False)
        incoming = build_incoming(edges)
        parameters = normalize_parameters(definition.get("parameters"))
        input_snapshots: list[dict[str, Any]] = []
        input_sampling: dict[str, Any] = {}
        input_commit_payload: Optional[list[dict[str, Any]]] = None
        temp_dirs: list[str] = []
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
        pipeline_ref = resolved_pipeline_id or str(job.pipeline_id)
        watermark_column = _resolve_watermark_column(
            incremental=_resolve_incremental_config(definition),
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
        use_lakefs_diff = parse_bool_env("PIPELINE_LAKEFS_DIFF_ENABLED", True)

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
            if is_preview:
                await record_preview(
                    status="FAILED",
                    row_count=0,
                    sample_json={"job_id": job.job_id, "errors": validation_errors},
                    job_id=job.job_id,
                    node_id=job.node_id,
                )
            elif not is_build:
                await record_build(
                    status="FAILED",
                    output_json={"job_id": job.job_id, "errors": validation_errors},
                )
            await record_run(
                job_id=job.job_id,
                mode=run_mode,
                status="FAILED",
                node_id=job.node_id,
                sample_json={"errors": validation_errors} if is_preview else None,
                output_json={"errors": validation_errors} if not is_preview else None,
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
                    df = self._apply_transform(metadata, inputs, parameters)

                schema_errors = self._validate_schema_checks(df, metadata.get("schemaChecks") or [], node_id)
                if schema_errors:
                    if is_preview:
                        await record_preview(
                            status="FAILED",
                            row_count=0,
                            sample_json={"job_id": job.job_id, "errors": schema_errors},
                            job_id=job.job_id,
                            node_id=job.node_id,
                        )
                    elif not is_build:
                        await record_build(
                            status="FAILED",
                            output_json={"job_id": job.job_id, "errors": schema_errors},
                        )
                    await record_run(
                        job_id=job.job_id,
                        mode=run_mode,
                        status="FAILED",
                        node_id=job.node_id,
                        sample_json={"errors": schema_errors} if is_preview else None,
                        output_json={"errors": schema_errors} if not is_preview else None,
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
            preview_limit = int(job.preview_limit or 200)
            preview_limit = max(1, min(500, preview_limit))

            if is_preview:
                primary_id = target_node_ids[0] if target_node_ids else None
                output_df = tables.get(primary_id) if primary_id else self._empty_dataframe()
                schema_columns = _schema_from_dataframe(output_df)
                row_count = int(output_df.count())
                schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
                contract_errors = self._validate_schema_contract(output_df, schema_contract)
                if contract_errors:
                    await record_preview(
                        status="FAILED",
                        row_count=row_count,
                        sample_json={"job_id": job.job_id, "errors": contract_errors},
                        job_id=job.job_id,
                        node_id=job.node_id,
                    )
                    await record_run(
                        job_id=job.job_id,
                        mode="preview",
                        status="FAILED",
                        node_id=job.node_id,
                        row_count=row_count,
                        sample_json={"errors": contract_errors},
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
                pk_semantics = self._resolve_pk_semantics(
                    execution_semantics=execution_semantics,
                    definition=definition,
                    output_metadata=output_metadata,
                )
                delete_column = self._resolve_delete_column(
                    definition=definition,
                    output_metadata=output_metadata,
                )
                pk_columns = self._resolve_pk_columns(
                    definition=definition,
                    output_metadata=output_metadata,
                    output_name=output_name,
                    output_node_id=primary_id,
                    declared_outputs=declared_outputs,
                )
                available_columns = set(output_df.columns)
                pk_semantic_errors = self._validate_pk_semantics(
                    output_df,
                    pk_semantics=pk_semantics,
                    pk_columns=pk_columns,
                    delete_column=delete_column,
                )
                expectation_errors = pk_semantic_errors + self._validate_expectations(
                    output_df,
                    self._build_expectations_with_pk(
                        definition=definition,
                        output_metadata=output_metadata,
                        output_name=output_name,
                        output_node_id=primary_id,
                        declared_outputs=declared_outputs,
                        pk_semantics=pk_semantics,
                        delete_column=delete_column,
                        pk_columns=pk_columns,
                        available_columns=available_columns,
                    ),
                )
                sample_rows = output_df.limit(preview_limit).collect()
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
                    schema_columns = _schema_from_dataframe(output_df)
                    delta_row_count = int(output_df.count())
                    if delta_row_count > 0:
                        has_output_rows = True
                    schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
                    contract_errors = self._validate_schema_contract(output_df, schema_contract)
                    if contract_errors:
                        await record_run(
                            job_id=job.job_id,
                            mode="build",
                            status="FAILED",
                            node_id=node_id,
                            row_count=delta_row_count,
                            output_json={"errors": contract_errors},
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
                    pk_semantics = self._resolve_pk_semantics(
                        execution_semantics=execution_semantics,
                        definition=definition,
                        output_metadata=metadata,
                    )
                    delete_column = self._resolve_delete_column(
                        definition=definition,
                        output_metadata=metadata,
                    )
                    pk_columns = self._resolve_pk_columns(
                        definition=definition,
                        output_metadata=metadata,
                        output_name=output_name,
                        output_node_id=node_id,
                        declared_outputs=declared_outputs,
                    )
                    available_columns = set(output_df.columns)
                    pk_semantic_errors = self._validate_pk_semantics(
                        output_df,
                        pk_semantics=pk_semantics,
                        pk_columns=pk_columns,
                        delete_column=delete_column,
                    )
                    expectation_errors = pk_semantic_errors + self._validate_expectations(
                        output_df,
                        self._build_expectations_with_pk(
                            definition=definition,
                            output_metadata=metadata,
                            output_name=output_name,
                            output_node_id=node_id,
                            declared_outputs=declared_outputs,
                            pk_semantics=pk_semantics,
                            delete_column=delete_column,
                            pk_columns=pk_columns,
                            available_columns=available_columns,
                        ),
                    )
                    if expectation_errors:
                        await record_run(
                            job_id=job.job_id,
                            mode="build",
                            status="FAILED",
                            node_id=node_id,
                            row_count=delta_row_count,
                            output_json={"errors": expectation_errors},
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

                    output_format = _resolve_output_format(definition=definition, output_metadata=metadata)
                    partition_cols = _resolve_partition_columns(
                        definition=definition,
                        output_metadata=metadata,
                    )
                    dataset_name = str(output_name or job.output_dataset_name)
                    schema_hash = _hash_schema_columns(schema_columns)
                    sample_rows = output_df.limit(preview_limit).collect()
                    output_sample = [row.asDict(recursive=True) for row in sample_rows]
                    column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)

                    safe_name = dataset_name.replace(" ", "_")
                    artifact_prefix = f"pipelines/{job.db_name}/{pipeline_ref}/{safe_name}"
                    output_work.append(
                        {
                            "node_id": node_id,
                            "output_name": output_name,
                            "dataset_name": dataset_name,
                            "output_df": output_df,
                            "output_format": output_format,
                            "partition_columns": partition_cols,
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
                    artifact_key = await self._materialize_output_dataframe(
                        output_df,
                        artifact_bucket=artifact_repo,
                        prefix=f"{build_branch}/{item['artifact_prefix']}",
                        write_mode=output_write_mode,
                        file_prefix=job.job_id,
                        file_format=item["output_format"],
                        partition_cols=item["partition_columns"],
                    )
                    row_count = int(item["delta_row_count"])
                    if output_write_mode == "append":
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
                                    row_count = int(existing_version.row_count) + int(item["delta_row_count"])
                        except Exception as exc:
                            logger.debug("Failed to resolve previous row_count for incremental build: %s", exc)
                    build_outputs.append(
                        {
                            "node_id": item["node_id"],
                            "output_name": item["output_name"],
                            "dataset_name": item["dataset_name"],
                            "artifact_key": artifact_key,
                            "artifact_prefix": item["artifact_prefix"],
                            "output_format": item["output_format"],
                            "partition_columns": item["partition_columns"],
                            "row_count": row_count,
                            "delta_row_count": item["delta_row_count"] if output_write_mode == "append" else None,
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
                delta_row_count = int(output_df.count())
                if delta_row_count > 0:
                    has_output_rows = True
                schema_contract = definition.get("schemaContract") or definition.get("schema_contract") or []
                contract_errors = self._validate_schema_contract(output_df, schema_contract)
                if contract_errors:
                    await record_build(
                        status="FAILED",
                        output_json={"job_id": job.job_id, "errors": contract_errors, "node_id": node_id},
                    )
                    await record_run(
                        job_id=job.job_id,
                        mode="deploy",
                        status="FAILED",
                        node_id=node_id,
                        row_count=delta_row_count,
                        output_json={"errors": contract_errors},
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
                pk_semantics = self._resolve_pk_semantics(
                    execution_semantics=execution_semantics,
                    definition=definition,
                    output_metadata=metadata,
                )
                delete_column = self._resolve_delete_column(
                    definition=definition,
                    output_metadata=metadata,
                )
                pk_columns = self._resolve_pk_columns(
                    definition=definition,
                    output_metadata=metadata,
                    output_name=output_name,
                    output_node_id=node_id,
                    declared_outputs=declared_outputs,
                )
                available_columns = set(output_df.columns)
                pk_semantic_errors = self._validate_pk_semantics(
                    output_df,
                    pk_semantics=pk_semantics,
                    pk_columns=pk_columns,
                    delete_column=delete_column,
                )
                expectation_errors = pk_semantic_errors + self._validate_expectations(
                    output_df,
                    self._build_expectations_with_pk(
                        definition=definition,
                        output_metadata=metadata,
                        output_name=output_name,
                        output_node_id=node_id,
                        declared_outputs=declared_outputs,
                        pk_semantics=pk_semantics,
                        delete_column=delete_column,
                        pk_columns=pk_columns,
                        available_columns=available_columns,
                    ),
                )
                if expectation_errors:
                    await record_build(
                        status="FAILED",
                        output_json={"job_id": job.job_id, "errors": expectation_errors, "node_id": node_id},
                    )
                    await record_run(
                        job_id=job.job_id,
                        mode="deploy",
                        status="FAILED",
                        node_id=node_id,
                        row_count=delta_row_count,
                        output_json={"errors": expectation_errors},
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

                sample_rows = output_df.limit(preview_limit).collect()
                output_sample = [row.asDict(recursive=True) for row in sample_rows]

                output_format = _resolve_output_format(definition=definition, output_metadata=metadata)
                partition_cols = _resolve_partition_columns(
                    definition=definition,
                    output_metadata=metadata,
                )
                dataset_name = str(output_name or job.output_dataset_name)
                safe_name = dataset_name.replace(" ", "_")
                artifact_prefix = f"pipelines/{job.db_name}/{pipeline_ref}/{safe_name}"
                column_stats = compute_column_stats(rows=output_sample, columns=schema_columns)
                output_work.append(
                    {
                        "node_id": node_id,
                        "dataset_name": dataset_name,
                        "output_df": output_df,
                        "artifact_prefix": artifact_prefix,
                        "output_format": output_format,
                        "partition_columns": partition_cols,
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
                await self._materialize_output_dataframe(
                    item["output_df"],
                    artifact_bucket=artifact_repo,
                    prefix=branch_prefix,
                    write_mode=output_write_mode,
                    file_prefix=job.job_id,
                    file_format=item["output_format"],
                    partition_cols=item["partition_columns"],
                )
                staged_outputs.append(
                    {
                        "node_id": item["node_id"],
                        "dataset_name": item["dataset_name"],
                        "artifact_prefix": item["artifact_prefix"],
                        "output_format": item["output_format"],
                        "partition_columns": item["partition_columns"],
                        "row_count": item["row_count"],
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
                total_row_count = delta_row_count
                if output_write_mode == "append" and dataset:
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
                        "delta_row_count": delta_row_count if output_write_mode == "append" else None,
                        "sample_row_count": len(output_sample),
                        "column_stats": item.get("column_stats") or {},
                    },
                    schema_json={"columns": schema_columns},
                )
                objectify_job_id = await self._maybe_enqueue_objectify_job(
                    dataset=dataset,
                    version=version,
                )

                build_outputs.append(
                    {
                        "node_id": item.get("node_id"),
                        "dataset_name": dataset_name,
                        "artifact_key": artifact_key,
                        "row_count": total_row_count,
                        "delta_row_count": delta_row_count if output_write_mode == "append" else None,
                        "lakefs_commit_id": merge_commit_id,
                        "lakefs_branch": base_branch,
                        "objectify_job_id": objectify_job_id,
                    }
                )

                if self.lineage:
                    parsed = parse_s3_uri(artifact_key)
                    if parsed:
                        bucket, key = parsed
                        try:
                            await self.lineage.record_link(
                                from_node_id=self.lineage.node_aggregate("Pipeline", pipeline_ref),
                                to_node_id=self.lineage.node_artifact("s3", bucket, key),
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
                        except Exception as exc:
                            logger.warning("Lineage record_link failed (deploy): %s", exc)

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
            if is_preview:
                await record_preview(
                    status="FAILED",
                    row_count=0,
                    sample_json={"job_id": job.job_id, "errors": [str(exc)]},
                    job_id=job.job_id,
                    node_id=job.node_id,
                )
            elif not is_build:
                await record_build(
                    status="FAILED",
                    output_json={"job_id": job.job_id, "errors": [str(exc)]},
                )
            await record_run(
                job_id=job.job_id,
                mode=run_mode,
                status="FAILED",
                node_id=job.node_id,
                sample_json={"errors": [str(exc)]} if is_preview else None,
                output_json={"errors": [str(exc)]} if not is_preview else None,
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

    async def _maybe_enqueue_objectify_job(self, *, dataset, version) -> Optional[str]:
        if not self.objectify_registry:
            return None
        if not getattr(version, "artifact_key", None):
            return None
        resolved_output_name = getattr(dataset, "name", None)
        schema_hash = compute_schema_hash(version.sample_json.get("columns")) if isinstance(getattr(version, "sample_json", None), dict) else None
        if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
            schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])
        mapping_spec = await self.objectify_registry.get_active_mapping_spec(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=resolved_output_name,
            schema_hash=schema_hash,
        )
        if not mapping_spec or not mapping_spec.auto_sync:
            return None
        dedupe_key = self.objectify_registry.build_dedupe_key(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
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
        job = ObjectifyJob(
            job_id=job_id,
            db_name=dataset.db_name,
            dataset_id=dataset.dataset_id,
            dataset_version_id=version.version_id,
            artifact_output_name=resolved_output_name,
            dedupe_key=dedupe_key,
            dataset_branch=dataset.branch,
            artifact_key=version.artifact_key or "",
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
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
        if resolved_format not in {"parquet", "json"}:
            raise ValueError("file_format must be parquet or json")
        resolved_partition_cols = [col for col in (partition_cols or []) if str(col).strip()]
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
                writer.parquet(output_path)
            else:
                writer.json(output_path)
            part_files = _list_part_files(
                output_path,
                extensions={".parquet"} if resolved_format == "parquet" else {".json"},
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
                content_type = (
                    "application/x-parquet" if resolved_format == "parquet" else "application/json"
                )
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
        except Exception:
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
        if not self.dataset_registry:
            raise RuntimeError("Dataset registry not initialized")
        dataset_id = metadata.get("datasetId") or metadata.get("dataset_id")
        dataset_name = metadata.get("datasetName") or metadata.get("dataset_name")
        requested_branch = str(metadata.get("datasetBranch") or metadata.get("dataset_branch") or branch or "main")
        dataset_id = str(dataset_id).strip() if dataset_id else None
        dataset_name = str(dataset_name).strip() if dataset_name else None

        if not dataset_id and not dataset_name:
            raise ValueError(f"Input node {node_id} is missing dataset selection")

        fallback_raw = os.getenv("PIPELINE_FALLBACK_BRANCHES", "main")
        fallback_candidates = [b.strip() for b in fallback_raw.split(",") if b.strip()]
        if "main" not in fallback_candidates:
            fallback_candidates.append("main")
        candidates: list[str] = []
        for candidate in [requested_branch, *fallback_candidates]:
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        dataset = None
        resolved_branch: Optional[str] = None
        version = None

        if dataset_id:
            dataset = await self.dataset_registry.get_dataset(dataset_id=str(dataset_id))
            if dataset:
                resolved_branch = dataset.branch
                dataset_name = dataset_name or dataset.name
                version = await self.dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)

        if (not dataset or not version) and dataset_name:
            dataset = None
            version = None
            resolved_branch = None
            for candidate_branch in candidates:
                found = await self.dataset_registry.get_dataset_by_name(
                    db_name=db_name,
                    name=str(dataset_name),
                    branch=candidate_branch,
                )
                if not found:
                    continue
                found_version = await self.dataset_registry.get_latest_version(dataset_id=found.dataset_id)
                if not found_version:
                    continue
                dataset = found
                version = found_version
                resolved_branch = candidate_branch
                break

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

        snapshot: Optional[dict[str, Any]] = None
        if input_snapshots is not None:
            snapshot = {
                "node_id": node_id,
                "dataset_id": dataset.dataset_id,
                "dataset_name": dataset.name,
                "dataset_branch": resolved_branch or dataset.branch or requested_branch,
                "requested_dataset_branch": requested_branch,
                "used_fallback": bool((resolved_branch or dataset.branch or requested_branch) != requested_branch),
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
        dataset_branch_for_diff = safe_lakefs_ref(resolved_branch or dataset.branch or requested_branch or "main")
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
                            "dataset_branch": resolved_branch or dataset.branch or requested_branch,
                            "requested_dataset_branch": requested_branch,
                            "used_fallback": bool(
                                (resolved_branch or dataset.branch or requested_branch) != requested_branch
                            ),
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
                            watermark_max = df.agg(
                                F.max(F.col(resolved_watermark_column)).alias("watermark_max")
                            ).collect()[0]["watermark_max"]
                        except Exception:
                            watermark_max = None
                        snapshot["watermark_max"] = watermark_max
                        if watermark_max is not None:
                            snapshot["watermark_keys"] = self._collect_watermark_keys(
                                df,
                                watermark_column=resolved_watermark_column,
                                watermark_value=watermark_max,
                            )
                        input_snapshots.append(snapshot)
                elif input_snapshots is not None:
                    input_snapshots.append(
                        {
                            "node_id": node_id,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "dataset_branch": resolved_branch or dataset.branch or requested_branch,
                            "requested_dataset_branch": requested_branch,
                            "used_fallback": bool(
                                (resolved_branch or dataset.branch or requested_branch) != requested_branch
                            ),
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
        if source_type == "media":
            df = await self._load_media_prefix_dataframe(bucket, key, node_id=node_id)
        else:
            df = await self._load_artifact_dataframe(bucket, key, temp_dirs)

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
            watermark_max = df.agg(F.max(F.col(resolved_watermark_column)).alias("watermark_max")).collect()[0][
                "watermark_max"
            ]
            if snapshot is not None:
                snapshot["watermark_max"] = watermark_max
                if watermark_max is not None:
                    snapshot["watermark_keys"] = self._collect_watermark_keys(
                        df,
                        watermark_column=resolved_watermark_column,
                        watermark_value=watermark_max,
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
        return reader.parquet(*local_paths)

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
            except Exception:
                pass
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
        return self.spark.createDataFrame(rows)

    async def _resolve_pipeline_id(self, job: PipelineJob) -> Optional[str]:
        if not self.pipeline_registry:
            return None
        pipeline_id = str(job.pipeline_id)
        try:
            UUID(pipeline_id)
            return pipeline_id
        except Exception:
            pass
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
        errors: List[str] = []
        nodes_raw = definition.get("nodes")
        if not isinstance(nodes_raw, list) or not nodes_raw:
            errors.append("Pipeline has no nodes")
            return errors
        nodes = normalize_nodes(nodes_raw)
        edges = normalize_edges(definition.get("edges"))
        node_ids = set(nodes.keys())
        for edge in edges:
            if edge["from"] not in node_ids or edge["to"] not in node_ids:
                errors.append(f"Pipeline edge references missing node: {edge['from']}->{edge['to']}")
        has_output = any(node.get("type") == "output" for node in nodes.values())
        if require_output and not has_output:
            errors.append("Pipeline has no output node")

        incoming = build_incoming(edges)
        supported_ops = SUPPORTED_TRANSFORMS
        for node_id, node in nodes.items():
            if node.get("type") != "transform":
                continue
            metadata = node.get("metadata") or {}
            operation = normalize_operation(metadata.get("operation"))
            if not operation:
                if len(incoming.get(node_id, [])) >= 2:
                    errors.append(f"transform node {node_id} has multiple inputs but no operation")
                continue
            if operation not in supported_ops:
                errors.append(f"Unsupported operation '{operation}' on node {node_id}")
                continue
            if operation in {"filter", "compute"} and not str(metadata.get("expression") or "").strip():
                errors.append(f"{operation} missing expression on node {node_id}")
            if operation in {"select", "drop", "sort", "dedupe", "explode"}:
                columns = metadata.get("columns") or []
                if not columns:
                    errors.append(f"{operation} missing columns on node {node_id}")
            if operation == "rename":
                rename_map = metadata.get("rename") or {}
                if not rename_map:
                    errors.append(f"rename missing mapping on node {node_id}")
            if operation == "cast":
                casts = metadata.get("casts") or []
                if not casts:
                    errors.append(f"cast missing columns on node {node_id}")
            if operation in {"groupBy", "aggregate"}:
                aggregates = metadata.get("aggregates") or []
                if not isinstance(aggregates, list) or not any(
                    isinstance(item, dict) and item.get("column") and item.get("op")
                    for item in aggregates
                ):
                    errors.append(f"{operation} missing aggregates on node {node_id}")
            if operation == "join":
                if len(incoming.get(node_id, [])) < 2:
                    errors.append(f"join requires two inputs on node {node_id}")
                join_spec = resolve_join_spec(metadata)
                if not join_spec.allow_cross_join and not (join_spec.left_key and join_spec.right_key):
                    errors.append(f"join requires leftKey/rightKey (or joinKey) on node {node_id}")
                if join_spec.allow_cross_join and not (join_spec.left_key and join_spec.right_key):
                    if join_spec.join_type != "cross":
                        errors.append(f"join allowCrossJoin requires joinType='cross' on node {node_id}")
            if operation == "union":
                if len(incoming.get(node_id, [])) < 2:
                    errors.append(f"union requires two inputs on node {node_id}")
                union_mode = normalize_union_mode(metadata)
                if union_mode not in {"strict", "common_only", "pad_missing_nulls", "pad"}:
                    errors.append(f"union has invalid unionMode '{union_mode}' on node {node_id}")
            if operation == "pivot":
                pivot_meta = metadata.get("pivot") or {}
                index_cols = pivot_meta.get("index") or []
                columns_col = pivot_meta.get("columns")
                values_col = pivot_meta.get("values")
                if not index_cols or not columns_col or not values_col:
                    errors.append(f"pivot missing fields on node {node_id}")
            if operation == "window":
                window_meta = metadata.get("window") or {}
                order_by = window_meta.get("orderBy") or []
                if not order_by:
                    errors.append(f"window missing orderBy on node {node_id}")

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

    @staticmethod
    def _split_expectation_columns(column: str) -> List[str]:
        return [part.strip() for part in str(column or "").split(",") if part.strip()]

    def _coerce_pk_columns(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        if isinstance(value, list):
            columns: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    columns.extend(
                        self._coerce_pk_columns(
                            item.get("column")
                            or item.get("name")
                            or item.get("key")
                            or item.get("field")
                        )
                    )
                else:
                    columns.extend(self._coerce_pk_columns(item))
            return columns
        if isinstance(value, dict):
            columns: List[str] = []
            for key in (
                "columns",
                "column",
                "keys",
                "key",
                "fields",
                "field",
                "propertyKeys",
                "property_keys",
                "primaryKey",
                "primary_key",
                "primaryKeys",
                "primary_keys",
            ):
                columns.extend(self._coerce_pk_columns(value.get(key)))
            return columns
        return []

    def _collect_pk_columns(self, *candidates: Any) -> List[str]:
        seen: set[str] = set()
        output: List[str] = []
        for candidate in candidates:
            for column in self._coerce_pk_columns(candidate):
                if column and column not in seen:
                    seen.add(column)
                    output.append(column)
        return output

    @staticmethod
    def _match_output_declaration(
        output: Dict[str, Any],
        *,
        node_id: Optional[str],
        output_name: Optional[str],
    ) -> bool:
        if node_id:
            declared_node_id = str(output.get("node_id") or output.get("nodeId") or "").strip()
            if declared_node_id and declared_node_id == node_id:
                return True
        if output_name:
            declared_name = (
                output.get("output_name")
                or output.get("outputName")
                or output.get("dataset_name")
                or output.get("datasetName")
                or output.get("name")
            )
            if declared_name and str(declared_name).strip() == output_name:
                return True
        return False

    @staticmethod
    def _normalize_pk_semantics(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        raw = str(value).strip().lower()
        if not raw:
            return None
        if raw in {"snapshot", "overwrite", "full", "full_refresh", "replace"}:
            return "snapshot"
        if raw in {"append", "append_log", "log", "event", "history", "audit"}:
            return "append_log"
        if raw in {"state", "append_state", "latest", "upsert", "merge"}:
            return "append_state"
        if raw in {"remove", "delete", "tombstone"}:
            return "remove"
        return raw

    @staticmethod
    def _is_truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return False

    def _resolve_pk_semantics(
        self,
        *,
        execution_semantics: str,
        definition: Dict[str, Any],
        output_metadata: Dict[str, Any],
    ) -> str:
        settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
        incremental = _resolve_incremental_config(definition)
        raw: Optional[str] = None
        for key in ("pkSemantics", "pk_semantics", "pkMode", "pk_mode", "loadSemantics", "load_semantics"):
            value = output_metadata.get(key) or incremental.get(key) or settings.get(key) or definition.get(key)
            if isinstance(value, str) and value.strip():
                raw = value
                break
        if raw is None:
            remove_flag = (
                output_metadata.get("remove")
                or output_metadata.get("delete")
                or incremental.get("remove")
                or incremental.get("delete")
            )
            if self._is_truthy(remove_flag):
                raw = "remove"

        normalized = self._normalize_pk_semantics(raw)
        if execution_semantics == "snapshot":
            return "snapshot"
        if normalized:
            return normalized
        if execution_semantics in {"incremental", "streaming"}:
            return "append_state"
        return "snapshot"

    def _resolve_delete_column(
        self,
        *,
        definition: Dict[str, Any],
        output_metadata: Dict[str, Any],
    ) -> Optional[str]:
        settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
        incremental = _resolve_incremental_config(definition)
        for key in ("deleteColumn", "delete_column", "removeColumn", "remove_column", "is_deleted", "isDeleted"):
            value = output_metadata.get(key) or incremental.get(key) or settings.get(key) or definition.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _validate_pk_semantics(
        df: DataFrame,
        *,
        pk_semantics: str,
        pk_columns: List[str],
        delete_column: Optional[str],
    ) -> List[str]:
        errors: List[str] = []
        available = set(df.columns)
        missing_pk = [col for col in pk_columns if col not in available]
        if missing_pk:
            errors.append(f"pk columns missing: {', '.join(missing_pk)}")
        if pk_semantics == "remove":
            if not delete_column:
                errors.append("remove semantics requires deleteColumn (e.g. is_deleted)")
            elif delete_column not in available:
                errors.append(f"delete column missing: {delete_column}")
        return errors

    def _resolve_pk_columns(
        self,
        *,
        definition: Dict[str, Any],
        output_metadata: Dict[str, Any],
        output_name: Optional[str],
        output_node_id: Optional[str],
        declared_outputs: List[Dict[str, Any]],
    ) -> List[str]:
        definition_pk = self._collect_pk_columns(
            definition.get("pk_spec") or definition.get("pkSpec"),
            definition.get("primary_key")
            or definition.get("primaryKey")
            or definition.get("primary_keys")
            or definition.get("primaryKeys"),
        )
        output_pk = self._collect_pk_columns(
            output_metadata.get("pk_spec") or output_metadata.get("pkSpec"),
            output_metadata.get("primary_key")
            or output_metadata.get("primaryKey")
            or output_metadata.get("primary_keys")
            or output_metadata.get("primaryKeys")
            or output_metadata.get("pkColumns")
            or output_metadata.get("pk_columns"),
        )
        declared_pk: List[str] = []
        for item in declared_outputs:
            if not isinstance(item, dict):
                continue
            if not self._match_output_declaration(item, node_id=output_node_id, output_name=output_name):
                continue
            declared_pk.extend(
                self._collect_pk_columns(
                    item.get("pk_spec") or item.get("pkSpec"),
                    item.get("primary_key")
                    or item.get("primaryKey")
                    or item.get("primary_keys")
                    or item.get("primaryKeys")
                    or item.get("pkColumns")
                    or item.get("pk_columns"),
                )
            )
        return self._collect_pk_columns(definition_pk, output_pk, declared_pk)

    def _build_expectations_with_pk(
        self,
        *,
        definition: Dict[str, Any],
        output_metadata: Dict[str, Any],
        output_name: Optional[str],
        output_node_id: Optional[str],
        declared_outputs: List[Dict[str, Any]],
        pk_semantics: str,
        delete_column: Optional[str],
        pk_columns: Optional[List[str]] = None,
        available_columns: Optional[set[str]] = None,
    ) -> List[Dict[str, Any]]:
        base_expectations = definition.get("expectations") if isinstance(definition.get("expectations"), list) else []
        pk_columns = pk_columns or self._resolve_pk_columns(
            definition=definition,
            output_metadata=output_metadata,
            output_name=output_name,
            output_node_id=output_node_id,
            declared_outputs=declared_outputs,
        )
        if available_columns is not None:
            pk_columns = [col for col in pk_columns if col in available_columns]
        if not pk_columns and pk_semantics != "remove":
            return list(base_expectations)

        existing_not_null: set[str] = set()
        existing_unique: set[str] = set()
        for exp in normalize_expectations(base_expectations):
            if exp.rule in {"not_null", "non_null"}:
                for col in self._split_expectation_columns(exp.column):
                    if col:
                        existing_not_null.add(col)
            if exp.rule == "unique":
                cols = self._split_expectation_columns(exp.column)
                if cols:
                    existing_unique.add(",".join(cols))

        injected: List[Dict[str, Any]] = []
        enforce_unique = pk_semantics in {"snapshot", "append_state"}
        for col in pk_columns:
            if col not in existing_not_null:
                injected.append({"rule": "not_null", "column": col})
        if enforce_unique and pk_columns:
            composite_key = ",".join(pk_columns)
            if composite_key not in existing_unique:
                injected.append({"rule": "unique", "column": composite_key})
        if pk_semantics == "remove" and delete_column:
            if available_columns is None or delete_column in available_columns:
                if delete_column not in existing_not_null:
                    injected.append({"rule": "not_null", "column": delete_column})

        return list(base_expectations) + injected

    def _validate_expectations(self, df: DataFrame, expectations: List[Dict[str, Any]]) -> List[str]:
        errors: List[str] = []
        specs = normalize_expectations(expectations)
        if not specs:
            return errors
        total_count: Optional[int] = None

        def get_total() -> int:
            nonlocal total_count
            if total_count is None:
                total_count = int(df.count())
            return total_count

        for exp in specs:
            rule = exp.rule
            columns = self._split_expectation_columns(exp.column)
            value = exp.value
            if rule == "row_count_min" and value is not None:
                if get_total() < int(value):
                    errors.append(f"row_count_min failed: {value}")
            if rule == "row_count_max" and value is not None:
                if get_total() > int(value):
                    errors.append(f"row_count_max failed: {value}")
            if rule in {"not_null", "non_null"} and columns:
                for column in columns:
                    if df.filter(F.col(column).isNull()).limit(1).count() > 0:
                        errors.append(f"not_null failed: {column}")
            if rule == "non_empty" and len(columns) == 1:
                column = columns[0]
                if df.filter(F.col(column).isNull() | (F.trim(F.col(column)) == "")).limit(1).count() > 0:
                    errors.append(f"non_empty failed: {column}")
            if rule == "unique" and columns:
                total = get_total()
                if len(columns) == 1:
                    unique = df.select(columns[0]).distinct().count()
                else:
                    unique = df.select(*columns).distinct().count()
                if unique != total:
                    errors.append(f"unique failed: {','.join(columns)}")
            if rule in {"min", "max"} and len(columns) == 1:
                column = columns[0]
                threshold = normalize_number(value)
                if threshold is None:
                    continue
                result = df.agg(F.min(F.col(column)).alias("min"), F.max(F.col(column)).alias("max")).collect()[0]
                if rule == "min" and result["min"] is not None and float(result["min"]) < threshold:
                    errors.append(f"min failed: {column} < {threshold}")
                if rule == "max" and result["max"] is not None and float(result["max"]) > threshold:
                    errors.append(f"max failed: {column} > {threshold}")
            if rule == "regex" and len(columns) == 1 and value:
                column = columns[0]
                pattern = str(value)
                if df.filter(~F.col(column).rlike(pattern)).limit(1).count() > 0:
                    errors.append(f"regex failed: {column}")
            if rule == "in_set" and len(columns) == 1:
                column = columns[0]
                allowed = normalize_value_list(value)
                if allowed:
                    if df.filter(~F.col(column).isin(allowed)).limit(1).count() > 0:
                        errors.append(f"in_set failed: {column}")
        return errors

    def _validate_schema_contract(self, df: DataFrame, contract: Any) -> List[str]:
        specs = normalize_schema_contract(contract)
        if not specs:
            return []
        type_map = {field.name: _spark_type_to_xsd(field.dataType) for field in df.schema.fields}
        errors: List[str] = []
        for item in specs:
            column = item.column
            if column not in type_map:
                if item.required:
                    errors.append(f"schema contract missing column: {column}")
                continue
            if item.expected_type:
                actual = normalize_schema_type(type_map.get(column))
                if actual and actual != item.expected_type:
                    errors.append(f"schema contract type mismatch: {column} {type_map.get(column)} != {item.expected_type}")
        return errors

    async def _load_artifact_dataframe(self, bucket: str, key: str, temp_dirs: list[str]) -> DataFrame:
        prefix = key.rstrip("/")
        has_extension = os.path.splitext(prefix)[1] != ""
        if not has_extension:
            return await self._load_prefix_dataframe(bucket, f"{prefix}/", temp_dirs)
        if key.endswith("/"):
            return await self._load_prefix_dataframe(bucket, key, temp_dirs)

        file_path = await self._download_object(bucket, key, temp_dirs)
        return self._read_local_file(file_path)

    async def _load_prefix_dataframe(self, bucket: str, prefix: str, temp_dirs: list[str]) -> DataFrame:
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

        if any(path.endswith(".parquet") for path in local_paths):
            return self.spark.read.parquet(temp_dir)
        if any(path.endswith(".json") for path in local_paths):
            return self.spark.read.json(temp_dir)
        if any(path.endswith(".csv") for path in local_paths):
            return self.spark.read.option("header", "true").csv(temp_dir)
        if any(path.endswith((".xlsx", ".xlsm")) for path in local_paths):
            return self._load_excel_path(next(path for path in local_paths if path.endswith((".xlsx", ".xlsm"))))

        extensions = sorted({os.path.splitext(path)[1] for path in local_paths if os.path.splitext(path)[1]})
        raise ValueError(
            f"Unsupported dataset artifact format in s3://{bucket}/{prefix} (extensions={','.join(extensions) or 'unknown'})"
        )

    async def _download_object_to_path(self, bucket: str, key: str, local_path: str) -> None:
        if not self.storage:
            raise RuntimeError("Storage service not available")
        directory = os.path.dirname(local_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(local_path, "wb") as handle:
            self.storage.client.download_fileobj(bucket, key, handle)

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

    def _read_local_file(self, path: str) -> DataFrame:
        if path.endswith(".csv"):
            return self.spark.read.option("header", "true").csv(path)
        if path.endswith(".parquet"):
            return self.spark.read.parquet(path)
        if path.endswith((".xlsx", ".xlsm")):
            return self._load_excel_path(path)
        if path.endswith(".json"):
            return self._load_json_path(path)
        raise ValueError(f"Unsupported dataset file type: {path}")

    def _load_excel_path(self, path: str) -> DataFrame:
        import pandas as pd

        frame = pd.read_excel(path)
        return self.spark.createDataFrame(frame)

    def _load_json_path(self, path: str) -> DataFrame:
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
        except Exception:
            pass
        return self.spark.read.json(path)

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
        operation = normalize_operation(metadata.get("operation"))
        if operation == "join" and len(inputs) >= 2:
            join_spec = resolve_join_spec(metadata)
            join_type = join_spec.join_type
            allow_cross_join = join_spec.allow_cross_join
            left_key = join_spec.left_key
            right_key = join_spec.right_key
            left = inputs[0]
            right = inputs[1]
            if left_key and right_key:
                if left_key == right_key:
                    return left.join(right, on=[left_key], how=join_type)
                return left.join(right, left[left_key] == right[right_key], how=join_type)
            if allow_cross_join:
                if join_type != "cross":
                    logger.warning("allowCrossJoin enabled but joinType=%s; forcing cross join", join_type)
                try:
                    return left.crossJoin(right)
                except Exception:
                    return left.join(right, how="cross")
            raise ValueError("Join requires leftKey/rightKey (or joinKey). Cross join requires allowCrossJoin=true.")
        if operation == "filter":
            expr = apply_parameters(str(metadata.get("expression") or ""), parameters)
            if expr:
                return inputs[0].filter(expr)
        if operation == "compute":
            expr = apply_parameters(str(metadata.get("expression") or ""), parameters)
            if expr:
                if "=" in expr:
                    target, formula = [part.strip() for part in expr.split("=", 1)]
                    return inputs[0].withColumn(target, F.expr(formula))
                return inputs[0].withColumn("computed", F.expr(expr))
        if operation == "explode":
            columns = metadata.get("columns") or []
            if columns:
                column = str(columns[0]).strip()
                if column:
                    return inputs[0].withColumn(column, F.explode(F.col(column)))
        if operation == "select":
            columns = metadata.get("columns") or []
            if columns:
                return inputs[0].select(*[F.col(col) for col in columns])
        if operation == "drop":
            columns = metadata.get("columns") or []
            if columns:
                return inputs[0].drop(*columns)
        if operation == "rename":
            rename_map = metadata.get("rename") or {}
            if rename_map:
                df = inputs[0]
                for key, value in rename_map.items():
                    df = df.withColumnRenamed(str(key), str(value))
                return df
        if operation == "cast":
            casts = metadata.get("casts") or []
            if casts:
                df = inputs[0]
                for cast in casts:
                    column = cast.get("column")
                    data_type = cast.get("type")
                    if column and data_type:
                        df = df.withColumn(str(column), F.col(str(column)).cast(str(data_type)))
                return df
        if operation == "dedupe":
            subset = metadata.get("columns") or []
            if subset:
                return inputs[0].dropDuplicates(subset)
            return inputs[0].dropDuplicates()
        if operation == "sort":
            columns = metadata.get("columns") or []
            if columns:
                return inputs[0].sort(*columns)
        if operation == "union" and len(inputs) >= 2:
            union_mode = normalize_union_mode(metadata)
            left = inputs[0]
            right = inputs[1]
            left_cols = list(left.columns)
            right_cols = list(right.columns)
            left_set = set(left_cols)
            right_set = set(right_cols)
            if union_mode == "strict":
                if left_set != right_set:
                    missing_left = sorted(right_set - left_set)
                    missing_right = sorted(left_set - right_set)
                    raise ValueError(
                        "union schema mismatch (strict): "
                        f"missing_in_left={missing_left} missing_in_right={missing_right}"
                    )
                return left.unionByName(right)
            if union_mode == "common_only":
                common = [col for col in left_cols if col in right_set]
                if not common:
                    raise ValueError("union has no common columns")
                return left.select(*common).unionByName(right.select(*common))
            if union_mode in {"pad_missing_nulls", "pad"}:
                all_cols = left_cols + [col for col in right_cols if col not in left_set]

                def align(df: DataFrame, present: set[str]) -> DataFrame:
                    return df.select(
                        *[
                            (F.col(col) if col in present else F.lit(None).alias(col))
                            for col in all_cols
                        ]
                    )

                return align(left, left_set).unionByName(align(right, right_set))
            raise ValueError(f"Invalid unionMode: {union_mode}")
        if operation in {"groupBy", "aggregate"}:
            group_by = metadata.get("groupBy") or []
            aggregates = metadata.get("aggregates") or []
            group_cols = [F.col(col) for col in group_by] if group_by else []
            agg_exprs = []
            for agg in aggregates:
                col_name = agg.get("column")
                op = str(agg.get("op") or "").lower()
                alias = agg.get("alias") or f"{op}_{col_name}"
                if not col_name or not op:
                    continue
                base_col = F.col(col_name)
                if op == "count":
                    agg_exprs.append(F.count(base_col).alias(alias))
                elif op == "sum":
                    agg_exprs.append(F.sum(base_col).alias(alias))
                elif op == "avg":
                    agg_exprs.append(F.avg(base_col).alias(alias))
                elif op == "min":
                    agg_exprs.append(F.min(base_col).alias(alias))
                elif op == "max":
                    agg_exprs.append(F.max(base_col).alias(alias))
            if group_cols and agg_exprs:
                return inputs[0].groupBy(*group_cols).agg(*agg_exprs)
            if agg_exprs:
                return inputs[0].agg(*agg_exprs)
        if operation == "pivot":
            pivot_meta = metadata.get("pivot") or {}
            index_cols = pivot_meta.get("index") or []
            columns_col = pivot_meta.get("columns")
            values_col = pivot_meta.get("values")
            agg = str(pivot_meta.get("agg") or "sum").lower()
            if index_cols and columns_col and values_col:
                base = inputs[0].groupBy(*[F.col(c) for c in index_cols]).pivot(columns_col)
                if agg == "count":
                    return base.count()
                if agg == "avg":
                    return base.avg(values_col)
                if agg == "min":
                    return base.min(values_col)
                if agg == "max":
                    return base.max(values_col)
                return base.sum(values_col)
        if operation == "window":
            window_meta = metadata.get("window") or {}
            partition_by = window_meta.get("partitionBy") or []
            order_by = window_meta.get("orderBy") or []
            if order_by:
                if partition_by:
                    window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)
                else:
                    window_spec = Window.orderBy(*order_by)
                return inputs[0].withColumn("row_number", F.row_number().over(window_spec))
        return inputs[0]

    def _validate_schema_checks(
        self,
        df: DataFrame,
        checks: List[Dict[str, Any]],
        node_id: str,
    ) -> List[str]:
        specs = normalize_schema_checks(checks)
        if not specs:
            return []
        type_map = {field.name: _spark_type_to_xsd(field.dataType) for field in df.schema.fields}
        errors: List[str] = []

        for check in specs:
            rule = check.rule
            column = check.column
            value = check.value
            if rule in {"required", "exists"}:
                if column not in type_map:
                    errors.append(f"schema check failed ({node_id}): missing column {column}")
            if rule in {"type", "dtype"}:
                expected = normalize_schema_type(value)
                actual = normalize_schema_type(type_map.get(column))
                if column not in type_map:
                    errors.append(f"schema check failed ({node_id}): missing column {column}")
                elif expected and actual != expected:
                    errors.append(
                        f"schema check failed ({node_id}): {column} type {type_map.get(column)} != {expected}"
                    )
            if rule in {"not_null", "non_null"}:
                if column not in type_map:
                    errors.append(f"schema check failed ({node_id}): missing column {column}")
                elif df.filter(F.col(column).isNull()).limit(1).count() > 0:
                    errors.append(f"schema check failed ({node_id}): {column} has nulls")
            if rule == "min":
                threshold = normalize_number(value)
                if threshold is not None:
                    result = df.agg(F.min(F.col(column)).alias("min")).collect()[0]["min"]
                    if result is not None and float(result) < threshold:
                        errors.append(f"schema check failed ({node_id}): {column} min < {threshold}")
            if rule == "max":
                threshold = normalize_number(value)
                if threshold is not None:
                    result = df.agg(F.max(F.col(column)).alias("max")).collect()[0]["max"]
                    if result is not None and float(result) > threshold:
                        errors.append(f"schema check failed ({node_id}): {column} max > {threshold}")
            if rule == "regex":
                pattern = str(value or "").strip()
                if pattern and df.filter(~F.col(column).rlike(pattern)).limit(1).count() > 0:
                    errors.append(f"schema check failed ({node_id}): {column} regex mismatch")
        return errors


def _is_data_object(key: str) -> bool:
    if not key:
        return False
    base = os.path.basename(key)
    if base.startswith("_"):
        return False
    if base.startswith("."):
        return False
    return bool(os.path.splitext(base)[1])


def _schema_from_dataframe(frame: DataFrame) -> List[Dict[str, str]]:
    columns: List[Dict[str, str]] = []
    schema = frame.schema
    for field in schema.fields:
        columns.append({"name": field.name, "type": _spark_type_to_xsd(field.dataType)})
    return columns


def _hash_schema_columns(columns: List[Dict[str, Any]]) -> str:
    return compute_schema_hash(columns)


def _spark_type_to_xsd(data_type: Any) -> str:
    if isinstance(data_type, BooleanType):
        return "xsd:boolean"
    if isinstance(data_type, (IntegerType, LongType)):
        return "xsd:integer"
    if isinstance(data_type, (FloatType, DoubleType, DecimalType)):
        return "xsd:decimal"
    if isinstance(data_type, (DateType, TimestampType)):
        return "xsd:dateTime"
    return "xsd:string"


def _list_part_files(path: str, *, extensions: Optional[set[str]] = None) -> List[str]:
    if extensions is None:
        extensions = {".json", ".parquet"}
    normalized = {ext.lower() for ext in extensions}
    part_files: List[str] = []
    for root, _, files in os.walk(path):
        for name in files:
            if not name.startswith("part-"):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext in normalized:
                part_files.append(os.path.join(root, name))
    return part_files


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    worker = PipelineWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
