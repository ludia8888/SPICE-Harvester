from __future__ import annotations
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from pipeline_worker.main import (
    PipelineWorker,
    _collect_input_commit_map,
    _resolve_external_read_mode,
    _collect_watermark_keys_from_snapshots,
    _inputs_diff_empty,
    _is_sensitive_conf_key,
    _max_watermark_from_snapshots,
    _resolve_code_version,
    _resolve_lakefs_repository,
    _resolve_output_format,
    _resolve_partition_columns,
    _resolve_streaming_timeout_seconds,
    _resolve_streaming_trigger_mode,
    _resolve_watermark_column,
    _watermark_values_match,
)
from pipeline_worker.runtime_mixin import _PipelinePayloadParseError


class _MsgWithHeaders:
    def headers(self):
        return [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]


def test_resolve_code_version_and_sensitive_keys(monkeypatch) -> None:
    monkeypatch.setenv("CODE_SHA", "abc")
    assert _resolve_code_version() == "abc"
    assert _is_sensitive_conf_key("aws_secret") is True
    assert _is_sensitive_conf_key("regular") is False


def test_resolve_lakefs_repository(monkeypatch) -> None:
    monkeypatch.setenv("LAKEFS_ARTIFACTS_REPOSITORY", "repo")
    assert _resolve_lakefs_repository() == "repo"


def test_watermark_snapshot_helpers() -> None:
    snapshots = [
        {"node_id": "n1", "lakefs_commit_id": "c1", "diff_requested": True, "diff_ok": True, "diff_empty": True},
        {"node_id": "n2", "lakefs_commit_id": "c2"},
    ]
    assert _collect_input_commit_map(snapshots) == {"n1": "c1", "n2": "c2"}
    assert _inputs_diff_empty(snapshots) is True

    wm = _max_watermark_from_snapshots(
        [
            {"watermark_column": "ts", "watermark_max": 1},
            {"watermark_column": "ts", "watermark_max": 2},
        ],
        watermark_column="ts",
    )
    assert wm == 2

    keys = _collect_watermark_keys_from_snapshots(
        [{"watermark_column": "ts", "watermark_max": 1, "watermark_keys": ["a"]}],
        watermark_column="ts",
        watermark_value=1,
    )
    assert keys == ["a"]
    assert _watermark_values_match("1", 1.0) is True


def test_resolve_output_format_and_partitions() -> None:
    definition = {"settings": {"outputFormat": "json"}}
    output_metadata = {}
    assert _resolve_output_format(definition=definition, output_metadata=output_metadata) == "json"

    partitions = _resolve_partition_columns(
        definition={"partitionBy": "a,b"},
        output_metadata={},
    )
    assert partitions == ["a", "b"]

    assert _resolve_watermark_column(incremental={"watermark": "ts"}, metadata={}) == "ts"


def test_resolve_external_read_mode_streaming_aliases() -> None:
    assert _resolve_external_read_mode(read_config={"mode": "stream"}) == "streaming"
    assert _resolve_external_read_mode(read_config={"readMode": "streaming"}) == "streaming"
    assert _resolve_external_read_mode(read_config={"mode": "microbatch"}) == "streaming"
    assert _resolve_external_read_mode(read_config={}) == "batch"


def test_resolve_streaming_trigger_mode_and_timeout() -> None:
    assert _resolve_streaming_trigger_mode(read_config={}, default_mode="available_now") == "available_now"
    assert _resolve_streaming_trigger_mode(read_config={"trigger": "once"}, default_mode="available_now") == "once"
    assert (
        _resolve_streaming_trigger_mode(
            read_config={"trigger": {"mode": "availableNow"}},
            default_mode="once",
        )
        == "available_now"
    )
    assert _resolve_streaming_timeout_seconds(read_config={}, default_seconds=90) == 90
    assert _resolve_streaming_timeout_seconds(read_config={"timeout_seconds": "30"}, default_seconds=90) == 30


def test_streaming_external_source_requires_kafka_format() -> None:
    worker = PipelineWorker()
    worker.spark = object()
    try:
        with pytest.raises(ValueError):
            worker._load_external_streaming_dataframe(
                read_config={"mode": "streaming"},
                node_id="in_stream",
                fmt="json",
                temp_dirs=[],
            )
    finally:
        worker.spark = None


def test_streaming_external_source_respects_global_toggle() -> None:
    worker = PipelineWorker()
    worker.spark = object()
    worker.spark_streaming_enabled = False
    try:
        with pytest.raises(ValueError):
            worker._load_external_streaming_dataframe(
                read_config={"mode": "streaming"},
                node_id="in_stream",
                fmt="kafka",
                temp_dirs=[],
            )
    finally:
        worker.spark = None


def test_resolve_kafka_value_format_and_checkpoint_policy() -> None:
    worker = PipelineWorker()
    assert worker._resolve_kafka_value_format(read_config={}) == "raw"
    assert worker._resolve_kafka_value_format(read_config={"value_format": "json"}) == "json"
    with pytest.raises(ValueError):
        worker._resolve_kafka_value_format(read_config={"value_format": "xml"})
    with pytest.raises(ValueError):
        worker._resolve_streaming_checkpoint_location(read_config={}, node_id="in_stream")


def test_resolve_kafka_avro_schema_accepts_schema_registry_reference(monkeypatch) -> None:
    worker = PipelineWorker()
    fetch_calls: list[str] = []

    def _fake_fetch(**kwargs):  # noqa: ANN003
        reference = kwargs["reference"]
        fetch_calls.append(reference.request_url)
        return '{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}'

    monkeypatch.setattr("pipeline_worker.main.fetch_kafka_avro_schema_from_registry", _fake_fetch)
    read_config = {
        "value_format": "avro",
        "schema_registry": {
            "url": "https://registry.example",
            "subject": "orders-value",
            "version": 3,
        },
    }
    schema_text = worker._resolve_kafka_avro_schema(read_config=read_config, node_id="in")
    assert "Order" in schema_text
    # Cached on repeated lookups.
    assert worker._resolve_kafka_avro_schema(read_config=read_config, node_id="in") == schema_text
    assert len(fetch_calls) == 1


def test_resolve_kafka_avro_schema_rejects_incomplete_schema_registry_reference() -> None:
    worker = PipelineWorker()
    with pytest.raises(ValueError) as exc_info:
        worker._resolve_kafka_avro_schema(
            read_config={
                "value_format": "avro",
                "schema_registry": {"url": "https://registry.example", "subject": "orders-value"},
            },
            node_id="in",
        )
    assert "schema registry requires version" in str(exc_info.value)


def test_resolve_kafka_avro_schema_rejects_latest_registry_version() -> None:
    worker = PipelineWorker()
    with pytest.raises(ValueError) as exc_info:
        worker._resolve_kafka_avro_schema(
            read_config={
                "value_format": "avro",
                "schema_registry": {
                    "url": "https://registry.example",
                    "subject": "orders-value",
                    "version": "latest",
                },
            },
            node_id="in",
        )
    assert "schema registry version=latest is not allowed" in str(exc_info.value)


def test_preview_helpers_use_foundry_defaults() -> None:
    worker = PipelineWorker()

    assert worker._resolve_preview_limit(preview_limit=None, preview_meta=None) == 500
    assert worker._resolve_preview_limit(preview_limit=None, preview_meta={"sample_limit": "250"}) == 250
    assert worker._resolve_preview_limit(preview_limit=None, preview_meta={"sampleLimit": 700}) == 500

    assert worker._resolve_preview_flag(
        {"skip_production_checks": "true"},
        snake_case_key="skip_production_checks",
        camel_case_key="skipProductionChecks",
        default=False,
    )
    assert not worker._resolve_preview_flag(
        {"skipOutputRecording": "false"},
        snake_case_key="skip_output_recording",
        camel_case_key="skipOutputRecording",
        default=True,
    )


def test_sampling_strategy_defaults_to_limit_for_preview_inputs() -> None:
    worker = PipelineWorker()

    strategy = worker._resolve_sampling_strategy(
        metadata={},
        preview_meta={},
        preview_limit=500,
    )
    assert strategy == {"type": "limit", "limit": 500}

    explicit_limit = worker._resolve_sampling_strategy(
        metadata={"samplingStrategy": {"type": "limit"}},
        preview_meta={},
        preview_limit=500,
    )
    assert explicit_limit == {"type": "limit", "limit": 500}


def test_restart_spark_session_terminates_stale_gateway_process(monkeypatch) -> None:
    pytest.importorskip("pyspark")
    import pipeline_worker.main as pipeline_main
    import pyspark

    worker = PipelineWorker()

    class _FakeProc:
        def __init__(self) -> None:
            self.terminate_calls = 0
            self.kill_calls = 0
            self.wait_calls: list[int] = []

        def poll(self):
            return None

        def terminate(self) -> None:
            self.terminate_calls += 1

        def wait(self, timeout=None):
            self.wait_calls.append(timeout)
            return 0

        def kill(self) -> None:
            self.kill_calls += 1

    class _FakeGateway:
        def __init__(self, proc: _FakeProc) -> None:
            self.proc = proc
            self.shutdown_calls = 0
            self.callback_shutdown_calls = 0

        def shutdown(self) -> None:
            self.shutdown_calls += 1

        def shutdown_callback_server(self) -> None:
            self.callback_shutdown_calls += 1

    proc = _FakeProc()
    gateway = _FakeGateway(proc)
    stale_spark = SimpleNamespace(
        sparkContext=SimpleNamespace(_gateway=gateway),
        stop=Mock(side_effect=ConnectionRefusedError("gateway already dead")),
    )
    new_spark = object()

    class _FakeSparkSession:
        _instantiatedContext = object()
        _activeSession = object()
        _defaultSession = object()

    class _FakeSparkContext:
        _active_spark_context = object()
        _gateway = object()
        _jvm = object()

    worker.spark = stale_spark
    monkeypatch.setattr(worker, "_create_spark_session", lambda: new_spark)
    monkeypatch.setattr(pipeline_main, "SparkSession", _FakeSparkSession)
    monkeypatch.setattr(pyspark, "SparkContext", _FakeSparkContext, raising=False)

    worker._restart_spark_session()

    stale_spark.stop.assert_called_once()
    assert gateway.callback_shutdown_calls == 1
    assert gateway.shutdown_calls == 1
    assert proc.terminate_calls == 1
    assert proc.kill_calls == 0
    assert proc.wait_calls == [5]
    assert worker.spark is new_spark
    assert _FakeSparkSession._instantiatedContext is None
    assert _FakeSparkSession._activeSession is None
    assert _FakeSparkSession._defaultSession is None
    assert _FakeSparkContext._active_spark_context is None
    assert _FakeSparkContext._gateway is None
    assert _FakeSparkContext._jvm is None


@pytest.mark.asyncio
async def test_pipeline_parse_error_uses_shared_flow_and_records_invalid_job() -> None:
    worker = PipelineWorker()
    worker._publish_to_dlq = AsyncMock()
    worker._best_effort_record_invalid_job = AsyncMock()

    payload_obj = {
        "job_id": "job-1",
        "pipeline_id": "pipeline-1",
        "db_name": "demo",
        "branch": "main",
        "mode": "preview",
    }
    error = _PipelinePayloadParseError(
        stage="validate",
        payload_text='{"job_id":"job-1"}',
        payload_obj=payload_obj,
        fallback_metadata=None,
        cause=ValueError("bad payload"),
    )

    await worker._on_parse_error(
        msg=_MsgWithHeaders(),
        raw_payload='{"job_id":"job-1","pipeline_id":"pipeline-1","db_name":"demo","branch":"main","mode":"preview"}',
        error=error,
    )

    worker._publish_to_dlq.assert_awaited_once_with(
        msg=ANY,
        stage="validate",
        error="bad payload",
        attempt_count=None,
        payload_text='{"job_id":"job-1"}',
        payload_obj=payload_obj,
    )
    worker._best_effort_record_invalid_job.assert_awaited_once_with(
        payload_obj,
        error="bad payload",
    )
