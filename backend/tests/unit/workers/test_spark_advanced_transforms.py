from __future__ import annotations

import importlib.util
import json
import os
import sys

import pytest

try:  # pragma: no cover
    _pyspark_spec = importlib.util.find_spec("pyspark")
except ValueError:  # pragma: no cover
    _pyspark_spec = None

if _pyspark_spec is None:  # pragma: no cover
    pytest.skip("pyspark is not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from pipeline_worker.main import PipelineWorker  # noqa: E402


def _resolve_java_home() -> str:
    explicit = os.environ.get("JAVA_HOME")
    if explicit:
        return explicit
    for candidate in (
        "/opt/homebrew/opt/openjdk@17",
        "/opt/homebrew/opt/openjdk@21",
        "/opt/homebrew/opt/openjdk",
    ):
        if os.path.exists(candidate):
            return candidate
    return ""


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    java_home = _resolve_java_home()
    if java_home and os.path.exists(java_home):
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ.setdefault("PATH", f"{java_home}/bin:" + os.environ.get("PATH", ""))
    backend_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    current_pythonpath = os.environ.get("PYTHONPATH", "")
    pythonpath_entries = [entry for entry in current_pythonpath.split(os.pathsep) if entry]
    if backend_root not in pythonpath_entries:
        os.environ["PYTHONPATH"] = (
            backend_root
            if not current_pythonpath
            else backend_root + os.pathsep + current_pythonpath
        )
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-worker-advanced-transform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", ""))
    )
    try:
        session = builder.getOrCreate()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Spark runtime is not available in this environment: {exc}")
    yield session
    session.stop()


@pytest.fixture()
def worker(spark: SparkSession) -> PipelineWorker:
    instance = PipelineWorker()
    instance.spark = spark
    return instance


@pytest.mark.unit
def test_split_transform(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame([(1, 2), (2, 8), (3, 11)], ["id", "amount"])
    out = worker._apply_transform(
        {
            "operation": "split",
            "expression": "amount > 5",
        },
        [df],
        {},
    )
    assert [row["id"] for row in out.orderBy("id").collect()] == [2, 3]


@pytest.mark.unit
def test_geospatial_point_and_distance(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame(
        [
            (1, 37.5665, 126.9780, 37.5651, 126.98955),
            (2, None, 126.9780, 37.5651, 126.98955),
        ],
        ["id", "lat", "lon", "lat2", "lon2"],
    )

    points = worker._apply_transform(
        {
            "operation": "geospatial",
            "geospatial": {
                "mode": "point",
                "latColumn": "lat",
                "lonColumn": "lon",
                "outputColumn": "point_wkt",
            },
        },
        [df],
        {},
    )
    rows = points.orderBy("id").collect()
    assert rows[0]["point_wkt"].startswith("POINT(")
    assert rows[1]["point_wkt"] is None

    distances = worker._apply_transform(
        {
            "operation": "geospatial",
            "geospatial": {
                "mode": "distance",
                "lat1Column": "lat",
                "lon1Column": "lon",
                "lat2Column": "lat2",
                "lon2Column": "lon2",
                "outputColumn": "distance_km",
            },
        },
        [df],
        {},
    )
    distance_rows = distances.orderBy("id").collect()
    assert distance_rows[0]["distance_km"] is not None
    assert distance_rows[0]["distance_km"] > 0
    assert distance_rows[1]["distance_km"] is None


@pytest.mark.unit
def test_geospatial_geohash(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame([(37.5665, 126.9780)], ["lat", "lon"])
    out = worker._apply_transform(
        {
            "operation": "geospatial",
            "geospatial": {
                "mode": "geohash",
                "latColumn": "lat",
                "lonColumn": "lon",
                "outputColumn": "gh",
                "precision": 7,
            },
        },
        [df],
        {},
    )
    row = out.collect()[0]
    assert isinstance(row["gh"], str)
    assert len(row["gh"]) == 7


@pytest.mark.unit
def test_pattern_mining_contains_and_extract(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame(
        [(1, "ERR-100 failed"), (2, "ok")],
        ["id", "message"],
    )

    contains_df = worker._apply_transform(
        {
            "operation": "patternMining",
            "patternMining": {
                "sourceColumn": "message",
                "pattern": "ERR-(\\d+)",
                "outputColumn": "has_error",
                "matchMode": "contains",
            },
        },
        [df],
        {},
    )
    contains_rows = contains_df.orderBy("id").collect()
    assert contains_rows[0]["has_error"] is True
    assert contains_rows[1]["has_error"] is False

    extract_df = worker._apply_transform(
        {
            "operation": "patternMining",
            "patternMining": {
                "sourceColumn": "message",
                "pattern": "ERR-(\\d+)",
                "outputColumn": "error_code",
                "matchMode": "extract",
            },
        },
        [df],
        {},
    )
    extract_rows = extract_df.orderBy("id").collect()
    assert extract_rows[0]["error_code"] == "100"
    assert extract_rows[1]["error_code"] is None

    count_df = worker._apply_transform(
        {
            "operation": "patternMining",
            "patternMining": {
                "sourceColumn": "message",
                "pattern": "ERR-(\\d+)",
                "outputColumn": "error_count",
                "matchMode": "count",
            },
        },
        [df],
        {},
    )
    count_rows = count_df.orderBy("id").collect()
    assert count_rows[0]["error_count"] == 1
    assert count_rows[1]["error_count"] == 0


@pytest.mark.unit
def test_stream_join_transform(worker: PipelineWorker) -> None:
    left = worker.spark.createDataFrame(
        [(1, "left-a", "2026-01-01T00:00:00Z"), (2, "left-b", "2026-01-01T00:10:00Z")],
        ["id", "left_val", "left_event_time"],
    )
    right = worker.spark.createDataFrame(
        [(1, "right-a", "2025-12-31T23:59:40Z"), (2, "right-b", "2026-01-01T00:20:00Z")],
        ["id", "right_val", "right_event_time"],
    )

    out = worker._apply_transform(
        {
            "operation": "streamJoin",
            "joinType": "inner",
            "leftKeys": ["id"],
            "rightKeys": ["id"],
            "streamJoin": {
                "strategy": "dynamic",
                "leftEventTimeColumn": "left_event_time",
                "rightEventTimeColumn": "right_event_time",
                "allowedLatenessSeconds": 60,
                "leftCacheExpirationSeconds": 300,
                "rightCacheExpirationSeconds": 300,
            },
        },
        [left, right],
        {},
    )
    rows = out.orderBy("id").collect()
    assert len(rows) == 3
    assert any(row["id"] == 1 and row["left_val"] == "left-a" and row["right_val"] == "right-a" for row in rows)
    assert any(row["id"] == 2 and row["left_val"] == "left-b" and row["right_val"] is None for row in rows)
    assert any(row["id"] == 2 and row["left_val"] is None and row["right_val"] == "right-b" for row in rows)


@pytest.mark.unit
def test_stream_join_transform_respects_cache_expiration(worker: PipelineWorker) -> None:
    left = worker.spark.createDataFrame(
        [(1, "left-a", "2026-01-01T00:05:00Z")],
        ["id", "left_val", "left_event_time"],
    )
    right = worker.spark.createDataFrame(
        [(1, "right-a", "2026-01-01T00:03:00Z")],
        ["id", "right_val", "right_event_time"],
    )

    out = worker._apply_transform(
        {
            "operation": "streamJoin",
            "joinType": "inner",
            "leftKeys": ["id"],
            "rightKeys": ["id"],
            "streamJoin": {
                "strategy": "dynamic",
                "leftEventTimeColumn": "left_event_time",
                "rightEventTimeColumn": "right_event_time",
                "allowedLatenessSeconds": 300,
                "leftCacheExpirationSeconds": 60,
                "rightCacheExpirationSeconds": 60,
            },
        },
        [left, right],
        {},
    )
    rows = out.collect()
    assert len(rows) == 2
    assert any(row["left_val"] == "left-a" and row["right_val"] is None for row in rows)
    assert any(row["left_val"] is None and row["right_val"] == "right-a" for row in rows)


@pytest.mark.unit
def test_stream_join_transform_dynamic_selects_single_best_match_per_left_row(worker: PipelineWorker) -> None:
    left = worker.spark.createDataFrame(
        [(1, "left-a", "2026-01-01T00:00:30Z")],
        ["id", "left_val", "left_event_time"],
    )
    right = worker.spark.createDataFrame(
        [
            (1, "right-old", "2026-01-01T00:00:10Z"),
            (1, "right-latest", "2026-01-01T00:00:25Z"),
        ],
        ["id", "right_val", "right_event_time"],
    )

    out = worker._apply_transform(
        {
            "operation": "streamJoin",
            "joinType": "inner",
            "leftKeys": ["id"],
            "rightKeys": ["id"],
            "streamJoin": {
                "strategy": "dynamic",
                "timeDirection": "backward",
                "leftEventTimeColumn": "left_event_time",
                "rightEventTimeColumn": "right_event_time",
                "allowedLatenessSeconds": 60,
                "leftCacheExpirationSeconds": 300,
                "rightCacheExpirationSeconds": 300,
            },
        },
        [left, right],
        {},
    )
    rows = out.collect()
    matched_rows = [row for row in rows if row["left_val"] is not None and row["right_val"] is not None]
    assert len(matched_rows) == 1
    assert matched_rows[0]["right_val"] == "right-latest"


@pytest.mark.unit
def test_stream_join_left_lookup_selects_single_latest_right_row_without_event_time(worker: PipelineWorker) -> None:
    left = worker.spark.createDataFrame(
        [(1, "left-a")],
        ["id", "left_val"],
    )
    right = worker.spark.createDataFrame(
        [
            (1, "right-old"),
            (1, "right-latest"),
        ],
        ["id", "right_val"],
    )

    out = worker._apply_transform(
        {
            "operation": "streamJoin",
            "joinType": "left",
            "leftKeys": ["id"],
            "rightKeys": ["id"],
            "streamJoin": {"strategy": "left_lookup"},
        },
        [left, right],
        {},
    )
    rows = out.collect()
    matched_rows = [row for row in rows if row["left_val"] is not None]
    assert len(matched_rows) == 1
    assert matched_rows[0]["right_val"] == "right-latest"


@pytest.mark.unit
def test_stream_join_static_defaults_to_left_join_semantics(worker: PipelineWorker) -> None:
    left = worker.spark.createDataFrame(
        [(1, "left-a"), (2, "left-b")],
        ["id", "left_val"],
    )
    right = worker.spark.createDataFrame(
        [(1, "right-a"), (3, "right-c")],
        ["id", "right_val"],
    )

    out = worker._apply_transform(
        {
            "operation": "streamJoin",
            "joinType": "inner",
            "leftKeys": ["id"],
            "rightKeys": ["id"],
            "streamJoin": {"strategy": "static"},
        },
        [left, right],
        {},
    )
    rows = out.orderBy("id").collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["right_val"] == "right-a"
    assert rows[1]["id"] == 2 and rows[1]["right_val"] is None


class _StorageStub:
    def __init__(self) -> None:
        self.created_buckets: list[str] = []
        self.deleted_prefixes: list[tuple[str, str]] = []
        self.saved_bytes: list[tuple[str, str, bytes, str]] = []

    async def create_bucket(self, bucket_name: str) -> bool:
        self.created_buckets.append(bucket_name)
        return True

    async def delete_prefix(self, bucket: str, prefix: str) -> bool:
        self.deleted_prefixes.append((bucket, prefix))
        return True

    async def save_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> str:
        _ = metadata
        self.saved_bytes.append((bucket, key, data, content_type))
        return "checksum"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_output_dataframe_rejects_partitioned_json(worker: PipelineWorker) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]
    df = worker.spark.createDataFrame([(1, "2026-02-13")], ["id", "ds"])

    with pytest.raises(ValueError) as exc_info:
        await worker._materialize_output_dataframe(
            df,
            artifact_bucket="bucket-dataset",
            prefix="pipeline/dataset-output",
            write_mode="overwrite",
            file_format="json",
            partition_cols=["ds"],
        )
    assert "output_format=json does not support partition_by" in str(exc_info.value)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_dataset_output_deduplicates_duplicate_primary_keys_for_append_only_new_rows(
    worker: PipelineWorker,
) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]
    df = worker.spark.createDataFrame(
        [
            (1, "first"),
            (1, "second"),
        ],
        ["id", "name"],
    )

    result = await worker._materialize_dataset_output(
        output_metadata={
            "write_mode": "append_only_new_rows",
            "primary_key_columns": ["id"],
            "output_format": "parquet",
        },
        df=df,
        artifact_bucket="bucket-dataset",
        prefix="pipeline/dataset-output",
        db_name=None,
        branch=None,
        dataset_name="dataset_output",
        execution_semantics="streaming",
        incremental_inputs_have_additive_updates=True,
        write_mode="append",
        file_prefix=None,
        file_format="parquet",
        partition_cols=None,
        base_row_count=2,
    )

    assert result["delta_row_count"] == 1
    assert result["runtime_write_mode"] == "append"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_dataset_output_changelog_appends_only_new_or_changed_rows(worker: PipelineWorker) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]

    incoming = worker.spark.createDataFrame(
        [
            (1, "same"),
            (2, "changed"),
            (3, "new"),
        ],
        ["id", "name"],
    )
    existing = worker.spark.createDataFrame(
        [
            (1, "same"),
            (2, "before"),
        ],
        ["id", "name"],
    )

    async def _fake_load_existing_output_dataset(**kwargs):  # noqa: ANN003
        _ = kwargs
        return existing

    worker._load_existing_output_dataset = _fake_load_existing_output_dataset  # type: ignore[assignment]

    result = await worker._materialize_dataset_output(
        output_metadata={
            "write_mode": "changelog",
            "primary_key_columns": ["id"],
            "output_format": "parquet",
        },
        df=incoming,
        artifact_bucket="bucket-dataset",
        prefix="pipeline/dataset-output",
        db_name="demo",
        branch="main",
        dataset_name="dataset_output",
        execution_semantics="incremental",
        incremental_inputs_have_additive_updates=True,
        write_mode="append",
        file_prefix=None,
        file_format="parquet",
        partition_cols=None,
        base_row_count=3,
    )
    assert result["delta_row_count"] == 2
    assert result["runtime_write_mode"] == "append"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_dataset_output_snapshot_difference_keeps_current_transaction_duplicates(
    worker: PipelineWorker,
) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]

    incoming = worker.spark.createDataFrame(
        [
            (1, "changed-but-existing"),
            (2, "new-a"),
            (2, "new-b"),
        ],
        ["id", "name"],
    )
    existing = worker.spark.createDataFrame(
        [
            (1, "before"),
        ],
        ["id", "name"],
    )

    async def _fake_load_existing_output_dataset(**kwargs):  # noqa: ANN003
        _ = kwargs
        return existing

    worker._load_existing_output_dataset = _fake_load_existing_output_dataset  # type: ignore[assignment]

    result = await worker._materialize_dataset_output(
        output_metadata={
            "write_mode": "snapshot_difference",
            "primary_key_columns": ["id"],
            "output_format": "parquet",
        },
        df=incoming,
        artifact_bucket="bucket-dataset",
        prefix="pipeline/dataset-output",
        db_name="demo",
        branch="main",
        dataset_name="dataset_output",
        execution_semantics="incremental",
        incremental_inputs_have_additive_updates=True,
        write_mode="append",
        file_prefix=None,
        file_format="parquet",
        partition_cols=None,
        base_row_count=3,
    )
    assert result["delta_row_count"] == 2
    assert result["runtime_write_mode"] == "overwrite"
    assert storage.deleted_prefixes == [("bucket-dataset", "pipeline/dataset-output")]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_dataset_output_default_incremental_without_additive_signal_uses_snapshot_runtime(
    worker: PipelineWorker,
) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]
    df = worker.spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    result = await worker._materialize_dataset_output(
        output_metadata={
            "primary_key_columns": ["id"],
            "output_format": "parquet",
        },
        df=df,
        artifact_bucket="bucket-dataset",
        prefix="pipeline/dataset-output",
        db_name="demo",
        branch="main",
        dataset_name="dataset_output",
        execution_semantics="incremental",
        incremental_inputs_have_additive_updates=None,
        write_mode="append",
        file_prefix=None,
        file_format="parquet",
        partition_cols=None,
        base_row_count=2,
    )

    assert result["write_mode_requested"] == "default"
    assert result["write_mode_resolved"] == "default"
    assert result["runtime_write_mode"] == "overwrite"
    assert storage.deleted_prefixes == [("bucket-dataset", "pipeline/dataset-output")]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_dataset_output_default_incremental_additive_updates_uses_append_runtime(
    worker: PipelineWorker,
) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]
    df = worker.spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    result = await worker._materialize_dataset_output(
        output_metadata={
            "primary_key_columns": ["id"],
            "output_format": "parquet",
        },
        df=df,
        artifact_bucket="bucket-dataset",
        prefix="pipeline/dataset-output",
        db_name="demo",
        branch="main",
        dataset_name="dataset_output",
        execution_semantics="incremental",
        incremental_inputs_have_additive_updates=True,
        write_mode="append",
        file_prefix=None,
        file_format="parquet",
        partition_cols=None,
        base_row_count=2,
    )

    assert result["write_mode_requested"] == "default"
    assert result["write_mode_resolved"] == "default"
    assert result["runtime_write_mode"] == "append"
    assert storage.deleted_prefixes == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_virtual_output_writes_manifest_artifact(worker: PipelineWorker) -> None:
    storage = _StorageStub()
    worker.storage = storage  # type: ignore[assignment]
    df = worker.spark.createDataFrame([(1, "a")], ["id", "name"])

    artifact_key = await worker._materialize_virtual_output(
        output_metadata={
            "query_sql": "select id, name from source",
            "refresh_mode": "scheduled",
        },
        df=df,
        artifact_bucket="bucket-virtual",
        prefix="pipeline/virtual-output",
        write_mode="overwrite",
        file_prefix=None,
        file_format="parquet",
        partition_cols=["id"],
        row_count_hint=1,
    )

    assert artifact_key == "s3://bucket-virtual/pipeline/virtual-output/virtual_manifest.json"
    assert storage.created_buckets == ["bucket-virtual"]
    assert storage.deleted_prefixes == [("bucket-virtual", "pipeline/virtual-output")]
    assert len(storage.saved_bytes) == 1
    bucket, key, payload, content_type = storage.saved_bytes[0]
    assert bucket == "bucket-virtual"
    assert key == "pipeline/virtual-output/virtual_manifest.json"
    assert content_type == "application/json"

    manifest = json.loads(payload.decode("utf-8"))
    assert manifest["output_kind"] == "virtual"
    assert manifest["query_sql"] == "select id, name from source"
    assert manifest["refresh_mode"] == "scheduled"
    assert manifest["input_columns"] == ["id", "name"]
    assert manifest["row_count_hint"] == 1
    assert manifest["file_format_ignored"] == "parquet"
    assert manifest["partition_by_ignored"] == ["id"]


@pytest.mark.unit
def test_select_new_or_changed_rows_returns_new_and_changed(worker: PipelineWorker) -> None:
    incoming = worker.spark.createDataFrame(
        [
            (1, "same", 10),
            (2, "changed", 20),
            (3, "new", 30),
        ],
        ["id", "name", "score"],
    )
    existing = worker.spark.createDataFrame(
        [
            (1, "same", 10),
            (2, "before", 20),
        ],
        ["id", "name", "score"],
    )

    out = worker._select_new_or_changed_rows(
        input_df=incoming,
        existing_df=existing,
        pk_columns=["id"],
    )
    ids = [row["id"] for row in out.orderBy("id").collect()]
    assert ids == [2, 3]


@pytest.mark.unit
def test_select_new_or_changed_rows_without_pk_returns_input(worker: PipelineWorker) -> None:
    incoming = worker.spark.createDataFrame(
        [
            (1, "a"),
            (2, "b"),
        ],
        ["id", "name"],
    )
    existing = worker.spark.createDataFrame(
        [
            (1, "a"),
        ],
        ["id", "name"],
    )

    out = worker._select_new_or_changed_rows(
        input_df=incoming,
        existing_df=existing,
        pk_columns=[],
    )
    assert out.count() == 2


@pytest.mark.unit
def test_select_new_or_changed_rows_can_preserve_input_duplicates_for_changelog(worker: PipelineWorker) -> None:
    incoming = worker.spark.createDataFrame(
        [
            (1, "v2"),
            (1, "v3"),
            (2, "new"),
        ],
        ["id", "name"],
    )
    existing = worker.spark.createDataFrame(
        [
            (1, "v1"),
        ],
        ["id", "name"],
    )

    out = worker._select_new_or_changed_rows(
        input_df=incoming,
        existing_df=existing,
        pk_columns=["id"],
        dedupe_input=False,
    )
    rows = out.orderBy("id", "name").collect()
    assert len(rows) == 3
    assert [row["name"] for row in rows] == ["v2", "v3", "new"]


@pytest.mark.unit
def test_udf_transform_applies_resolved_code(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    code = """
def transform(row):
    return {
        "id": row["id"],
        "value": row["value"],
        "value_upper": str(row["value"]).upper(),
    }
"""
    out = worker._apply_transform(
        {
            "operation": "udf",
            "__resolved_udf_code": code,
        },
        [df],
        {},
    )
    rows = out.orderBy("id").collect()
    assert rows[0]["value_upper"] == "A"
    assert rows[1]["value_upper"] == "B"


@pytest.mark.unit
def test_udf_transform_supports_flat_map_rows(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame([(1,), (2,)], ["id"])
    code = """
def transform(row):
    base = int(row["id"])
    return [
        {"id": base, "variant": "left"},
        {"id": base, "variant": "right"},
    ]
"""
    out = worker._apply_transform(
        {
            "operation": "udf",
            "__resolved_udf_code": code,
        },
        [df],
        {},
    )
    rows = out.orderBy("id", "variant").collect()
    assert len(rows) == 4
    assert rows[0]["variant"] == "left"
    assert rows[1]["variant"] == "right"


@pytest.mark.unit
def test_udf_transform_rejects_schema_drift(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame([(1,), (2,)], ["id"])
    code = """
def transform(row):
    if row["id"] == 1:
        return {"id": row["id"], "left_only": "x"}
    return {"id": row["id"], "right_only": "y"}
"""
    with pytest.raises(ValueError, match="consistent output schema"):
        worker._apply_transform(
            {
                "operation": "udf",
                "__resolved_udf_code": code,
            },
            [df],
            {},
        )
