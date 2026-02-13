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
        [(1, "right-a", "2026-01-01T00:00:20Z"), (2, "right-b", "2026-01-01T00:20:00Z")],
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
            },
        },
        [left, right],
        {},
    )
    rows = out.orderBy("id").collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["left_val"] == "left-a"
    assert rows[0]["right_val"] == "right-a"
