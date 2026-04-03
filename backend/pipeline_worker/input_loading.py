from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import logging
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from shared.services.pipeline.pipeline_kafka_avro import (
    fetch_kafka_avro_schema_from_registry,
    resolve_inline_avro_schema,
    resolve_kafka_avro_schema_registry_reference,
)
from shared.services.pipeline.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.utils.path_utils import safe_lakefs_ref
from shared.utils.s3_uri import parse_s3_uri
from shared.services.pipeline.pipeline_type_utils import xsd_to_spark_type
from pipeline_worker.spark_schema_helpers import _is_data_object

from pipeline_worker.worker_helpers import (
    _resolve_external_read_mode,
    _resolve_streaming_timeout_seconds,
    _resolve_streaming_trigger_mode,
)

try:  # pragma: no cover - import guard for unit tests without Spark
    from pyspark.sql import DataFrame  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.types import StructType  # type: ignore
    _PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment,misc]
    F = None  # type: ignore[assignment]
    StructType = Any  # type: ignore[assignment,misc]
    _PYSPARK_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetInputLoadContext:
    dataset: Any
    version: Any
    requested_branch: Optional[str]
    resolved_branch: Optional[str]
    used_fallback: bool
    bucket: str
    key: str
    current_commit_id: str
    artifact_prefix: str


def build_dataset_input_snapshot(
    worker: Any,
    *,
    node_id: str,
    context: DatasetInputLoadContext,
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


def annotate_diff_snapshot(
    worker: Any,
    *,
    snapshot: Dict[str, Any],
    previous_commit_id: Optional[str],
    diff_ok: bool,
    diff_paths_count: int,
) -> None:
    _ = worker
    snapshot.update(
        {
            "previous_commit_id": previous_commit_id,
            "diff_requested": True,
            "diff_ok": diff_ok,
            "diff_paths_count": diff_paths_count,
            "diff_empty": bool(diff_ok and diff_paths_count == 0),
        }
    )


def append_input_snapshot(
    worker: Any,
    *,
    input_snapshots: Optional[list[dict[str, Any]]],
    snapshot: Optional[dict[str, Any]],
) -> None:
    _ = worker
    if input_snapshots is not None and snapshot is not None:
        input_snapshots.append(snapshot)


async def load_external_input_dataframe_with_snapshot(
    worker: Any,
    *,
    read_config: Dict[str, Any],
    node_id: str,
    temp_dirs: list[str],
    input_snapshots: Optional[list[dict[str, Any]]],
) -> DataFrame:
    read_mode = _resolve_external_read_mode(read_config=read_config)
    df = await worker._run_spark(
        lambda: worker._load_external_input_dataframe(read_config, node_id=node_id, temp_dirs=temp_dirs),
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
    options = worker._normalize_read_options(read_config)
    stream_trigger_mode: Optional[str] = None
    stream_timeout_seconds: Optional[int] = None
    if read_mode == "streaming":
        stream_trigger_mode = _resolve_streaming_trigger_mode(
            read_config=read_config,
            default_mode=worker.spark_streaming_default_trigger,
        )
        stream_timeout_seconds = _resolve_streaming_timeout_seconds(
            read_config=read_config,
            default_seconds=worker.spark_streaming_await_timeout_seconds,
        )
    input_snapshots.append(
        {
            "node_id": node_id,
            "source_type": "external",
            "read_mode": read_mode,
            "format": fmt,
            "stream_trigger_mode": stream_trigger_mode,
            "stream_timeout_seconds": stream_timeout_seconds,
            "options": worker._mask_sensitive_options(options),
        }
    )
    return df


async def resolve_dataset_input_load_context(
    worker: Any,
    *,
    db_name: str,
    node_id: str,
    selection: Any,
    input_snapshots: Optional[list[dict[str, Any]]],
) -> Tuple[DatasetInputLoadContext, Optional[dict[str, Any]]]:
    dataset_id = selection.dataset_id
    dataset_name = selection.dataset_name
    requested_branch = selection.requested_branch

    if not worker.dataset_registry:
        raise RuntimeError("Dataset registry not initialized")
    if not dataset_id and not dataset_name:
        raise ValueError(f"Input node {node_id} is missing dataset selection")

    resolution = await resolve_dataset_version(
        worker.dataset_registry,
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

    context = DatasetInputLoadContext(
        dataset=dataset,
        version=version,
        requested_branch=requested_branch,
        resolved_branch=resolved_branch_value,
        used_fallback=resolution.used_fallback,
        bucket=bucket,
        key=key,
        current_commit_id=current_commit_id,
        artifact_prefix=worker._strip_commit_prefix(key, current_commit_id),
    )
    snapshot = (
        build_dataset_input_snapshot(
            worker,
            node_id=node_id,
            context=context,
            lakefs_commit_id=version.lakefs_commit_id,
        )
        if input_snapshots is not None
        else None
    )
    return context, snapshot


async def load_full_dataset_input_dataframe(
    worker: Any,
    *,
    context: DatasetInputLoadContext,
    metadata: Dict[str, Any],
    temp_dirs: list[str],
    node_id: str,
) -> DataFrame:
    source_type = str(getattr(context.dataset, "source_type", "") or "").strip().lower()
    read_config = metadata.get("read") if isinstance(metadata.get("read"), dict) else {}
    if source_type == "media":
        df = await worker._load_media_prefix_dataframe(context.bucket, context.key, node_id=node_id)
    else:
        df = await worker._load_artifact_dataframe(
            context.bucket,
            context.key,
            temp_dirs,
            read_config=read_config,
        )
    return worker._apply_schema_casts(df, dataset=context.dataset, version=context.version)


async def try_load_diff_input_dataframe(
    worker: Any,
    *,
    context: DatasetInputLoadContext,
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
    if not bool(use_lakefs_diff and previous_commit_id and context.current_commit_id and worker.lakefs_client):
        return None
    diff_paths: List[str] = []
    diff_ok = True
    dataset_branch_for_diff = safe_lakefs_ref(context.resolved_branch or "main")
    if previous_commit_id != context.current_commit_id:
        diff_paths, diff_ok = await worker._list_lakefs_diff_paths(
            repository=context.bucket,
            ref=dataset_branch_for_diff,
            since=previous_commit_id,
            prefix=context.artifact_prefix,
            node_id=node_id,
        )
    diff_paths_count = len(diff_paths)
    parquet_paths = [path for path in diff_paths if path.endswith(".parquet")]
    if base_snapshot is not None:
        annotate_diff_snapshot(
            worker,
            snapshot=base_snapshot,
            previous_commit_id=previous_commit_id,
            diff_ok=diff_ok,
            diff_paths_count=diff_paths_count,
        )
    if diff_ok and diff_paths_count == 0:
        append_input_snapshot(worker, input_snapshots=input_snapshots, snapshot=base_snapshot)
        return worker._empty_dataframe()
    if not parquet_paths:
        if diff_ok and diff_paths_count:
            logger.warning(
                "lakeFS diff returned %s paths without parquet data for input node %s; falling back to full load",
                diff_paths_count,
                node_id,
            )
        return None
    df = await worker._load_parquet_keys_dataframe(
        bucket=context.bucket,
        keys=[f"{context.current_commit_id}/{path.lstrip('/')}" for path in parquet_paths],
        temp_dirs=temp_dirs,
        prefix=f"{context.current_commit_id}/{context.artifact_prefix}".rstrip("/"),
    )
    df = worker._apply_schema_casts(df, dataset=context.dataset, version=context.version)
    diff_snapshot = dict(base_snapshot) if base_snapshot is not None else None
    if diff_snapshot is not None:
        diff_snapshot["lakefs_commit_id"] = context.current_commit_id
        diff_snapshot["diff_used"] = True
        diff_snapshot["diff_parquet_paths"] = len(parquet_paths)
    df = await worker._apply_input_watermark_and_snapshot(
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
    append_input_snapshot(worker, input_snapshots=input_snapshots, snapshot=diff_snapshot)
    return df


async def load_input_dataframe(
    worker: Any,
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
        return await load_external_input_dataframe_with_snapshot(
            worker,
            read_config=read_config,
            node_id=node_id,
            temp_dirs=temp_dirs,
            input_snapshots=input_snapshots,
        )

    context, snapshot = await resolve_dataset_input_load_context(
        worker,
        db_name=db_name,
        node_id=node_id,
        selection=selection,
        input_snapshots=input_snapshots,
    )
    diff_df = await try_load_diff_input_dataframe(
        worker,
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

    df = await load_full_dataset_input_dataframe(
        worker,
        context=context,
        metadata=metadata,
        temp_dirs=temp_dirs,
        node_id=node_id,
    )
    df = await worker._apply_input_watermark_and_snapshot(
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
    append_input_snapshot(worker, input_snapshots=input_snapshots, snapshot=snapshot)
    return df


async def load_existing_output_dataset(
    worker: Any,
    *,
    db_name: Optional[str],
    branch: Optional[str],
    dataset_name: Optional[str],
) -> DataFrame:
    if not worker.dataset_registry:
        return worker._empty_dataframe()
    resolved_db = str(db_name or "").strip()
    resolved_branch = str(branch or "").strip() or "main"
    resolved_name = str(dataset_name or "").strip()
    if not resolved_db or not resolved_name:
        return worker._empty_dataframe()

    dataset = await worker.dataset_registry.get_dataset_by_name(
        db_name=resolved_db,
        name=resolved_name,
        branch=resolved_branch,
    )
    if not dataset:
        return worker._empty_dataframe()
    version = await worker.dataset_registry.get_latest_version(dataset_id=dataset.dataset_id)
    if not version or not str(version.artifact_key or "").strip():
        return worker._empty_dataframe()
    parsed = parse_s3_uri(str(version.artifact_key))
    if not parsed:
        raise ValueError(f"Invalid artifact_key for dataset {resolved_name}: {version.artifact_key}")
    bucket, key = parsed
    temp_dirs: List[str] = []
    try:
        return await worker._load_artifact_dataframe(bucket, key, temp_dirs)
    finally:
        for temp_dir in temp_dirs:
            shutil.rmtree(temp_dir, ignore_errors=True)


def normalize_read_options(worker: Any, read_config: Dict[str, Any]) -> Dict[str, str]:
    options_raw = read_config.get("options") if isinstance(read_config.get("options"), dict) else {}
    options: Dict[str, str] = {}
    for k, v in options_raw.items():
        key = str(k or "").strip()
        if not key or v is None:
            continue
        options[key] = str(v)

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


def mask_sensitive_options(worker: Any, options: Dict[str, str]) -> Dict[str, str]:
    _ = worker
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
        masked[str(key)] = "***" if any(marker in lower for marker in sensitive_markers) else str(value)
    return masked


def schema_ddl_from_read_config(worker: Any, read_config: Dict[str, Any]) -> Optional[str]:
    _ = worker
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


def resolve_read_format(worker: Any, *, path: str, read_config: Dict[str, Any]) -> str:
    _ = worker
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


def resolve_streaming_checkpoint_location(worker: Any, *, read_config: Dict[str, Any], node_id: str) -> str:
    _ = worker
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


def resolve_kafka_value_format(worker: Any, *, read_config: Dict[str, Any]) -> str:
    _ = worker
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


def resolve_kafka_schema_registry_headers(worker: Any, *, read_config: Dict[str, Any]) -> Dict[str, str]:
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
        options = normalize_read_options(worker, read_config)
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


def resolve_kafka_avro_schema(worker: Any, *, read_config: Dict[str, Any], node_id: str) -> str:
    inline_schema = resolve_inline_avro_schema(read_config=read_config)
    if inline_schema:
        return inline_schema

    reference = resolve_kafka_avro_schema_registry_reference(
        read_config=read_config,
        node_id=node_id,
    )
    cached = worker._kafka_avro_schema_cache.get(reference.cache_key)
    if cached:
        return cached

    headers = resolve_kafka_schema_registry_headers(worker, read_config=read_config)
    fetcher = getattr(worker, "_fetch_kafka_avro_schema_from_registry", fetch_kafka_avro_schema_from_registry)
    try:
        schema_text = fetcher(
            reference=reference,
            timeout_seconds=float(worker.kafka_schema_registry_timeout_seconds),
            headers=headers or None,
        )
    except Exception as exc:
        raise ValueError(
            f"Input node {node_id} kafka value_format=avro schema-registry lookup failed: {exc}"
        ) from exc
    worker._kafka_avro_schema_cache[reference.cache_key] = schema_text
    return schema_text


def apply_kafka_value_parsing(worker: Any, *, df: DataFrame, read_config: Dict[str, Any], node_id: str) -> DataFrame:
    value_format = resolve_kafka_value_format(worker, read_config=read_config)
    if value_format == "raw":
        return df

    parsed_column = str(
        read_config.get("parsed_value_column")
        or read_config.get("parsedValueColumn")
        or "value_parsed"
    ).strip() or "value_parsed"

    if value_format == "json":
        schema_ddl = schema_ddl_from_read_config(worker, read_config)
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

    avro_schema_text = resolve_kafka_avro_schema(worker, read_config=read_config, node_id=node_id)
    try:
        from pyspark.sql.avro.functions import from_avro  # type: ignore
    except Exception as exc:
        raise ValueError(
            "Spark avro functions are not available; install/enable spark-avro for kafka value_format=avro"
        ) from exc
    return df.withColumn(parsed_column, from_avro(F.col("value"), avro_schema_text))


def load_external_streaming_dataframe(
    worker: Any,
    *,
    read_config: Dict[str, Any],
    node_id: str,
    fmt: str,
    temp_dirs: list[str],
) -> DataFrame:
    if not worker.spark_streaming_enabled:
        raise ValueError("Streaming external inputs are disabled by PIPELINE_SPARK_STREAMING_ENABLED")
    if fmt != "kafka":
        raise ValueError(
            f"Input node {node_id} read.mode=streaming currently supports only read.format=kafka"
        )

    options = normalize_read_options(worker, read_config)
    stream_reader = worker.spark.readStream.format("kafka")
    for key, value in options.items():
        stream_reader = stream_reader.option(key, value)
    stream_df = stream_reader.load()

    temp_root = tempfile.mkdtemp(prefix=f"pipeline-streaming-{node_id}-")
    temp_dirs.append(temp_root)
    sink_path = os.path.join(temp_root, "sink")
    checkpoint_path = resolve_streaming_checkpoint_location(worker, read_config=read_config, node_id=node_id)
    os.makedirs(sink_path, exist_ok=True)
    if "://" not in checkpoint_path and not checkpoint_path.startswith("dbfs:/"):
        os.makedirs(checkpoint_path, exist_ok=True)

    trigger_mode = _resolve_streaming_trigger_mode(
        read_config=read_config,
        default_mode=worker.spark_streaming_default_trigger,
    )
    timeout_seconds = _resolve_streaming_timeout_seconds(
        read_config=read_config,
        default_seconds=worker.spark_streaming_await_timeout_seconds,
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

    reader = worker.spark.read
    if fmt == "kafka":
        try:
            parsed = reader.parquet(sink_path)
            return apply_kafka_value_parsing(
                worker,
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
            return worker.spark.createDataFrame([], schema=stream_df.schema)
    return reader.parquet(sink_path)


def load_external_input_dataframe(
    worker: Any,
    read_config: Dict[str, Any],
    *,
    node_id: str,
    temp_dirs: list[str],
) -> DataFrame:
    if not worker.spark:
        raise RuntimeError("Spark session not initialized")

    read_config = dict(read_config or {})
    read_mode = _resolve_external_read_mode(read_config=read_config)
    fmt = str(read_config.get("format") or read_config.get("file_format") or read_config.get("fileFormat") or "").strip().lower()
    if not fmt:
        raise ValueError(f"Input node {node_id} external source requires metadata.read.format")
    if read_mode == "streaming":
        return load_external_streaming_dataframe(
            worker,
            read_config=read_config,
            node_id=node_id,
            fmt=fmt,
            temp_dirs=temp_dirs,
        )

    options = normalize_read_options(worker, read_config)
    schema_ddl = schema_ddl_from_read_config(worker, read_config)

    if fmt == "jdbc":
        opts = dict(options)
        query = opts.pop("query", None)
        if query and "dbtable" not in opts:
            opts["dbtable"] = f"({query}) AS t"
        if not str(opts.get("url") or "").strip():
            raise ValueError(f"Input node {node_id} JDBC read requires options.url")
        if not str(opts.get("dbtable") or "").strip():
            raise ValueError(f"Input node {node_id} JDBC read requires options.dbtable (or options.query)")
        reader = worker.spark.read.format("jdbc")
        for k, v in opts.items():
            reader = reader.option(k, v)
        return reader.load()

    reader = worker.spark.read
    for k, v in options.items():
        reader = reader.option(k, v)
    if schema_ddl and fmt not in {"kafka"}:
        try:
            reader = reader.schema(schema_ddl)
        except Exception as exc:
            logger.warning("Failed to apply schema to source reader: %s", exc, exc_info=True)

    if fmt == "kafka":
        loaded = reader.format("kafka").load()
        return apply_kafka_value_parsing(
            worker,
            df=loaded,
            read_config=read_config,
            node_id=node_id,
        )

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

    return reader.format(fmt).load(paths if len(paths) != 1 else paths[0])


async def load_artifact_dataframe(
    worker: Any,
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
        return await load_prefix_dataframe(worker, bucket, f"{prefix}/", temp_dirs, read_config=read_config)
    if key.endswith("/"):
        return await load_prefix_dataframe(worker, bucket, key, temp_dirs, read_config=read_config)

    file_path = await download_object(worker, bucket, key, temp_dirs)
    return await worker._run_spark(
        lambda: read_local_file(worker, file_path, read_config=read_config),
        label=f"read_local_file:{os.path.basename(file_path)}",
    )


async def collect_prefix_local_paths(
    worker: Any,
    *,
    bucket: str,
    prefix: str,
    temp_dirs: list[str],
) -> Tuple[Optional[str], List[str]]:
    objects = await worker.storage.list_objects(bucket, prefix=prefix)
    keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
    data_keys = [key for key in keys if key and _is_data_object(key)]
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
        await download_object_to_path(worker, bucket, key, local_path)
        local_paths.append(local_path)
    return temp_dir, local_paths


async def read_prefix_dataframe_from_local_paths(
    worker: Any,
    *,
    temp_dir: str,
    local_paths: List[str],
    read_config: Dict[str, Any],
    bucket: str,
    prefix: str,
) -> DataFrame:
    reader = worker.spark.read
    options = normalize_read_options(worker, read_config)
    schema_ddl = schema_ddl_from_read_config(worker, read_config)
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
        return await worker._run_spark(lambda: reader.parquet(temp_dir), label="read_prefix:parquet")
    if forced_format == "json" or (not forced_format and has_json):
        return await worker._run_spark(lambda: reader.json(temp_dir), label="read_prefix:json")
    if forced_format == "csv" or (not forced_format and has_csv):
        if "header" not in options:
            reader = reader.option("header", "true")
        return await worker._run_spark(
            lambda: strip_bom_headers(worker, reader.csv(temp_dir)),
            label="read_prefix:csv",
        )
    if forced_format == "avro" or (not forced_format and has_avro):
        return await worker._run_spark(
            lambda: reader.format("avro").load(temp_dir),
            label="read_prefix:avro",
        )
    if forced_format == "orc" or (not forced_format and has_orc):
        return await worker._run_spark(
            lambda: reader.orc(temp_dir),
            label="read_prefix:orc",
        )
    if forced_format in {"excel", "xlsx"} or (not forced_format and has_excel):
        return await worker._run_spark(
            lambda: load_excel_path(
                worker,
                next(path for path in local_paths if path.endswith((".xlsx", ".xlsm")))
            ),
            label="read_prefix:excel",
        )
    if forced_format:
        return await worker._run_spark(
            lambda: reader.format(forced_format).load(temp_dir),
            label=f"read_prefix:{forced_format}",
        )

    extensions = sorted({os.path.splitext(path)[1] for path in local_paths if os.path.splitext(path)[1]})
    raise ValueError(
        f"Unsupported dataset artifact format in s3://{bucket}/{prefix} (extensions={','.join(extensions) or 'unknown'})"
    )


async def load_prefix_dataframe(
    worker: Any,
    bucket: str,
    prefix: str,
    temp_dirs: list[str],
    *,
    read_config: Optional[Dict[str, Any]] = None,
) -> DataFrame:
    read_config = dict(read_config or {})
    temp_dir, local_paths = await collect_prefix_local_paths(
        worker,
        bucket=bucket,
        prefix=prefix,
        temp_dirs=temp_dirs,
    )
    if not temp_dir or not local_paths:
        return empty_dataframe(worker)
    return await read_prefix_dataframe_from_local_paths(
        worker,
        temp_dir=temp_dir,
        local_paths=local_paths,
        read_config=read_config,
        bucket=bucket,
        prefix=prefix,
    )


async def download_object_to_path(worker: Any, bucket: str, key: str, local_path: str) -> None:
    if not worker.storage:
        raise RuntimeError("Storage service not available")
    directory = os.path.dirname(local_path)

    def _download() -> None:
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(local_path, "wb") as handle:
            worker.storage.client.download_fileobj(bucket, key, handle)

    await asyncio.to_thread(_download)


async def download_object(
    worker: Any,
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
    await download_object_to_path(worker, bucket, key, local_path)
    return local_path


def read_local_file(worker: Any, path: str, *, read_config: Optional[Dict[str, Any]] = None) -> DataFrame:
    read_config = dict(read_config or {})
    fmt = resolve_read_format(worker, path=path, read_config=read_config)
    options = normalize_read_options(worker, read_config)
    schema_ddl = schema_ddl_from_read_config(worker, read_config)

    reader = worker.spark.read
    for k, v in options.items():
        reader = reader.option(k, v)
    if schema_ddl:
        reader = reader.schema(schema_ddl)

    if fmt == "csv":
        if "header" not in options:
            reader = reader.option("header", "true")
        return strip_bom_headers(worker, reader.csv(path))
    if fmt == "parquet":
        return reader.parquet(path)
    if path.endswith((".xlsx", ".xlsm")):
        return load_excel_path(worker, path)
    if fmt == "json":
        return load_json_path(worker, path, reader=reader)
    if fmt:
        return reader.format(fmt).load(path)
    raise ValueError(f"Unsupported dataset file type: {path}")


def strip_bom_headers(worker: Any, df: DataFrame) -> DataFrame:
    _ = worker
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


def load_excel_path(worker: Any, path: str) -> DataFrame:
    import pandas as pd

    frame = pd.read_excel(path)
    return worker.spark.createDataFrame(frame)


def load_json_path(worker: Any, path: str, *, reader: Optional[Any] = None) -> DataFrame:
    if reader is None:
        reader = worker.spark.read
    try:
        with open(path, "r", encoding="utf-8") as handle:
            head = handle.read(2048)
            handle.seek(0)
            if head.lstrip().startswith("{") and "\"rows\"" in head:
                payload = json.load(handle)
                rows = payload.get("rows") or [] if isinstance(payload, dict) else payload
                if rows:
                    return worker.spark.createDataFrame(rows)
                return empty_dataframe(worker)
    except Exception as exc:
        logger.warning("Fell back to Spark JSON reader for %s due to parser error: %s", path, exc, exc_info=True)
    return reader.json(path)


def empty_dataframe(worker: Any) -> DataFrame:
    return worker.spark.createDataFrame([], schema=StructType([]))
