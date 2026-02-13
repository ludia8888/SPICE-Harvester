from __future__ import annotations

import importlib.util
import os
import sys

import pytest

from shared.tools.foundry_functions_compat import (
    default_snapshot_path,
    filter_functions,
    load_foundry_functions_snapshot,
)

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
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-functions-spark-tests")
        .config("spark.ui.enabled", "false")
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


def _run_group_aggregate(worker: PipelineWorker, op: str) -> dict[str, float | int]:
    df = worker.spark.createDataFrame(
        [("A", 1), ("A", 3), ("B", 10)],
        ["group", "amount"],
    )
    alias = f"{op}_amount"
    out = worker._apply_transform(
        {
            "operation": "groupBy",
            "groupBy": ["group"],
            "aggregates": [{"column": "amount", "op": op, "alias": alias}],
        },
        [df],
        {},
    )
    rows = out.collect()
    return {str(row["group"]): row[alias] for row in rows}


@pytest.mark.unit
def test_functions_spark_supported_matrix_contract(worker: PipelineWorker) -> None:
    entries = load_foundry_functions_snapshot(default_snapshot_path())
    spark_supported = filter_functions(entries, engine="spark", status="supported")

    assert spark_supported, "snapshot must include at least one spark-supported function"

    checks = {
        "sum": {"A": 4, "B": 10},
        "count": {"A": 2, "B": 1},
        "avg": {"A": 2.0, "B": 10.0},
        "min": {"A": 1, "B": 10},
        "max": {"A": 3, "B": 10},
    }

    missing = [entry.name for entry in spark_supported if entry.name not in checks]
    assert not missing, f"missing spark compatibility assertions for: {missing}"

    for entry in spark_supported:
        actual = _run_group_aggregate(worker, entry.name)
        expected = checks[entry.name]
        assert actual == expected, f"spark compatibility mismatch for {entry.name}: {actual} != {expected}"
