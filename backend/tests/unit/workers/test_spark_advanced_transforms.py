from __future__ import annotations

import importlib.util
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


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    java_home = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk")
    if java_home and os.path.exists(java_home):
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ.setdefault("PATH", f"{java_home}/bin:" + os.environ.get("PATH", ""))
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-worker-advanced-transform-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
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
