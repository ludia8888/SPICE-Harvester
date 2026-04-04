from __future__ import annotations

import asyncio
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, TypeVar
from uuid import UUID

from data_connector.google_sheets.service import GoogleSheetsService

from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import RuntimeZone, log_exception_rate_limited
from shared.models.pipeline_job import PipelineJob
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.grpc.oms_gateway_client import OMSGrpcHttpCompatClient
from shared.services.kafka.processed_event_worker import HeartbeatOptions, RegistryKey
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.registries.backing_source_adapter import MappingSpecResolver
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.lineage_store import LineageStore
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.services.storage.redis_service import create_redis_service
from shared.utils.time_utils import utcnow

from pipeline_worker.worker_helpers import _is_sensitive_conf_key
from shared.services.pipeline.pipeline_type_utils import normalize_cast_mode

try:
    from pyspark.sql import SparkSession  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    SparkSession = Any  # type: ignore[assignment,misc]

logger = logging.getLogger("pipeline_worker.main")

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


class PipelineWorkerRuntimeMixin:
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

        # MappingSpecResolver: OMS-first, PostgreSQL-fallback (gRPC bridge transport).
        # For OMSGrpcHttpCompatClient, URLs are treated as already-normalized API paths,
        # so oms_base_url intentionally stays empty.
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
        spark_session_cls = getattr(sys.modules.get("pipeline_worker.main"), "SparkSession", SparkSession)
        builder = (
            spark_session_cls.builder.appName("spice-pipeline-worker")
            .config("spark.driver.memory", self.spark_driver_memory)
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
            builder = builder.config("spark.ui.showConsoleProgress", "false").config("spark.ui.enabled", "false")

        return builder.getOrCreate()

    def _cleanup_spark_gateway(self, spark: Optional[SparkSession]) -> None:
        if spark is None:
            return

        spark_context = getattr(spark, "sparkContext", None)
        gateway = getattr(spark_context, "_gateway", None)
        if gateway is None:
            return

        for method_name in ("shutdown_callback_server", "shutdown"):
            method = getattr(gateway, method_name, None)
            if not callable(method):
                continue
            try:
                method()
            except Exception as exc:
                logger.debug("Spark gateway %s cleanup failed: %s", method_name, exc, exc_info=True)

        proc = getattr(gateway, "proc", None) or getattr(gateway, "java_process", None)
        if proc is None:
            return

        try:
            if proc.poll() is not None:
                return
        except Exception as exc:
            logger.debug("Failed to poll Spark gateway process state: %s", exc, exc_info=True)

        try:
            proc.terminate()
        except Exception as exc:
            logger.debug("Failed to terminate Spark gateway process: %s", exc, exc_info=True)

        wait = getattr(proc, "wait", None)
        if callable(wait):
            try:
                wait(timeout=5)
                return
            except Exception as exc:
                logger.debug("Timed out waiting for Spark gateway process termination: %s", exc, exc_info=True)

        try:
            proc.kill()
        except Exception as exc:
            logger.debug("Failed to kill Spark gateway process: %s", exc, exc_info=True)

        if callable(wait):
            try:
                wait(timeout=5)
            except Exception as exc:
                logger.debug("Failed waiting for Spark gateway process exit: %s", exc, exc_info=True)

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
                    logger.warning(
                        "Failed to read existing Spark conf %s before override: %s",
                        conf_key,
                        exc,
                        exc_info=True,
                    )
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
            old_spark = self.spark
            try:
                old_spark.stop()
            except Exception as exc:
                logger.warning("Failed to stop Spark session: %s", exc)
            self._cleanup_spark_gateway(old_spark)
            self.spark = None
        try:
            from pyspark import SparkContext

            spark_session_cls = getattr(sys.modules.get("pipeline_worker.main"), "SparkSession", SparkSession)

            # Best-effort reset of PySpark globals so builder.getOrCreate() doesn't
            # try to reuse a dead gateway/JVM after Py4JNetworkError/ConnectionRefusedError.
            for attr in (
                "_instantiatedContext",
                "_activeSession",
                "_defaultSession",
            ):
                if hasattr(spark_session_cls, attr):
                    setattr(spark_session_cls, attr, None)
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
