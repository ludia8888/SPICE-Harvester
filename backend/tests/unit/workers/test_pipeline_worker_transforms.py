from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

from pipeline_worker.main import (
    PipelineWorker,
    _hash_schema_columns,
    _is_data_object,
    _list_part_files,
    _schema_from_dataframe,
)


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
        .appName("pipeline-worker-tests")
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


def test_apply_transform_basic_ops(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame(
        [(1, 10, ["a", "b"]), (2, 20, ["c"])], ["a", "b", "tags"]
    )

    filtered = worker._apply_transform({"operation": "filter", "expression": "a > 1"}, [df], {})
    assert filtered.count() == 1

    computed = worker._apply_transform({"operation": "compute", "expression": "c = a + b"}, [df], {})
    assert "c" in computed.columns

    exploded = worker._apply_transform({"operation": "explode", "columns": ["tags"]}, [df], {})
    assert exploded.count() == 3

    selected = worker._apply_transform({"operation": "select", "columns": ["a"]}, [df], {})
    assert selected.columns == ["a"]

    dropped = worker._apply_transform({"operation": "drop", "columns": ["b"]}, [df], {})
    assert "b" not in dropped.columns

    renamed = worker._apply_transform({"operation": "rename", "rename": {"a": "a1"}}, [df], {})
    assert "a1" in renamed.columns

    casted = worker._apply_transform({"operation": "cast", "casts": [{"column": "a", "type": "string"}]}, [df], {})
    assert dict(casted.dtypes)["a"] == "string"

    deduped = worker._apply_transform({"operation": "dedupe", "columns": ["a"]}, [df], {})
    assert deduped.count() == 2

    sorted_df = worker._apply_transform({"operation": "sort", "columns": ["a"]}, [df], {})
    assert [row["a"] for row in sorted_df.collect()] == [1, 2]


def test_apply_transform_join_union_groupby_pivot_window(worker: PipelineWorker) -> None:
    left = worker.spark.createDataFrame([(1, "x"), (2, "y")], ["id", "val"])
    right = worker.spark.createDataFrame([(1, "a"), (3, "b")], ["id", "name"])

    joined = worker._apply_transform({"operation": "join", "leftKey": "id", "rightKey": "id"}, [left, right], {})
    assert joined.count() == 1

    unioned = worker._apply_transform({"operation": "union", "unionMode": "pad"}, [left, right], {})
    assert set(unioned.columns) == {"id", "val", "name"}

    with pytest.raises(ValueError):
        worker._apply_transform({"operation": "union", "unionMode": "strict"}, [left, right], {})

    grouped = worker._apply_transform(
        {
            "operation": "groupBy",
            "groupBy": ["id"],
            "aggregates": [{"column": "id", "op": "count", "alias": "cnt"}],
        },
        [left],
        {},
    )
    assert "cnt" in grouped.columns

    pivot_df = worker.spark.createDataFrame(
        [("A", "k1", 1), ("A", "k2", 2)], ["group", "key", "value"]
    )
    pivoted = worker._apply_transform(
        {"operation": "pivot", "pivot": {"index": ["group"], "columns": "key", "values": "value", "agg": "sum"}},
        [pivot_df],
        {},
    )
    assert "k1" in pivoted.columns

    windowed = worker._apply_transform(
        {"operation": "window", "window": {"partitionBy": [], "orderBy": ["id"]}},
        [left],
        {},
    )
    assert "row_number" in windowed.columns


def test_watermark_helpers(worker: PipelineWorker) -> None:
    df = worker.spark.createDataFrame([(1, "a"), (2, "b")], ["ts", "value"])

    keys = worker._collect_watermark_keys(df, watermark_column="ts", watermark_value=1)
    assert keys

    filtered = worker._apply_watermark_filter(df, watermark_column="ts", watermark_after=1, watermark_keys=[])
    assert filtered.count() == 2

    filtered_dedup = worker._apply_watermark_filter(df, watermark_column="ts", watermark_after=1, watermark_keys=keys)
    assert filtered_dedup.count() == 1


def test_pipeline_worker_file_helpers(worker: PipelineWorker, tmp_path) -> None:
    assert _is_data_object("part-0000.json") is True
    assert _is_data_object("_temporary") is False

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "part-0000.json").write_text("{}", encoding="utf-8")
    (data_dir / "part-0001.parquet").write_text("data", encoding="utf-8")
    (data_dir / "ignore.txt").write_text("x", encoding="utf-8")

    files = _list_part_files(str(data_dir))
    assert len(files) == 2

    df = worker.spark.createDataFrame([(1, "x")], ["id", "name"])
    schema = _schema_from_dataframe(df)
    schema_hash = _hash_schema_columns(schema)
    assert schema_hash
