from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


def _ensure_pyspark_stub() -> None:
    if importlib.util.find_spec("pyspark") is not None:
        return
    if "pyspark" in sys.modules:
        return

    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_functions = types.ModuleType("pyspark.sql.functions")
    fake_window = types.ModuleType("pyspark.sql.window")
    fake_types = types.ModuleType("pyspark.sql.types")

    class _SparkSession:  # pragma: no cover - placeholder
        pass

    class _DataFrame:  # pragma: no cover - placeholder
        pass

    class _Window:  # pragma: no cover - placeholder
        pass

    class _StructType:  # pragma: no cover - placeholder
        pass

    fake_sql.SparkSession = _SparkSession
    fake_sql.DataFrame = _DataFrame
    fake_sql.functions = fake_functions
    fake_window.Window = _Window
    fake_types.StructType = _StructType
    fake_sql.window = fake_window
    fake_sql.types = fake_types

    sys.modules["pyspark"] = fake_pyspark
    sys.modules["pyspark.sql"] = fake_sql
    sys.modules["pyspark.sql.functions"] = fake_functions
    sys.modules["pyspark.sql.window"] = fake_window
    sys.modules["pyspark.sql.types"] = fake_types


_ensure_pyspark_stub()

from pipeline_worker.main import PipelineWorker  # noqa: E402


class _FakeObjectifyRegistry:
    def __init__(self, *, mapping_spec, existing_job=None, mismatched_specs=None):
        self._mapping_spec = mapping_spec
        self._existing_job = existing_job
        self._mismatched_specs = mismatched_specs or []

    async def get_active_mapping_spec(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return self._mapping_spec

    async def list_mapping_specs(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return self._mismatched_specs

    def build_dedupe_key(self, **kwargs):  # noqa: ANN003
        return "dedupe-key"

    async def get_objectify_job_by_dedupe_key(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return self._existing_job


class _FakeObjectifyJobQueue:
    def __init__(self):
        self.jobs = []

    async def publish(self, job, *, require_delivery=False):  # noqa: ANN003
        self.jobs.append(job)


class _FakeDatasetRegistry:
    def __init__(self):
        self.recorded = None
        self._backing_version = SimpleNamespace(
            version_id="backing-ver-1",
            dataset_version_id="ver-expected",
            schema_hash="hash-expected",
        )
        self._expected_version = SimpleNamespace(
            version_id="ver-expected",
            sample_json={"columns": [{"name": "id", "type": "xsd:string"}]},
            schema_json={"columns": [{"name": "id", "type": "xsd:string"}]},
        )

    async def record_gate_result(self, **kwargs):  # noqa: ANN003
        self.recorded = kwargs

    async def get_backing_datasource_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._backing_version.version_id:
            return self._backing_version
        return None

    async def get_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._expected_version.version_id:
            return self._expected_version
        return None


@pytest.mark.asyncio
async def test_pipeline_worker_enqueues_objectify_job_without_pyspark():
    worker = PipelineWorker()
    mapping_spec = SimpleNamespace(
        mapping_spec_id="map-1",
        version=2,
        auto_sync=True,
        target_class_id="Account",
        options={"ontology_branch": "main"},
    )
    worker.objectify_registry = _FakeObjectifyRegistry(mapping_spec=mapping_spec)
    worker.objectify_job_queue = _FakeObjectifyJobQueue()

    dataset = SimpleNamespace(
        dataset_id="ds-1",
        db_name="test_db",
        name="accounts",
        branch="main",
        schema_json={"columns": [{"name": "id", "type": "xsd:string"}]},
    )
    version = SimpleNamespace(
        version_id="ver-1",
        artifact_key="s3://bucket/accounts",
        sample_json={"columns": [{"name": "id", "type": "xsd:string"}]},
    )

    job_id = await worker._maybe_enqueue_objectify_job(dataset=dataset, version=version)

    assert job_id is not None
    assert worker.objectify_job_queue.jobs
    job = worker.objectify_job_queue.jobs[0]
    assert job.dataset_version_id == "ver-1"
    assert job.mapping_spec_id == "map-1"
    assert job.mapping_spec_version == 2


@pytest.mark.asyncio
async def test_pipeline_worker_schema_mismatch_records_gate_without_pyspark():
    worker = PipelineWorker()
    mismatched_spec = SimpleNamespace(
        mapping_spec_id="map-old",
        version=1,
        auto_sync=False,
        target_class_id="Account",
        artifact_output_name="accounts",
        schema_hash="hash-old",
        backing_datasource_version_id="backing-ver-1",
        created_at=datetime.now(timezone.utc),
    )
    worker.objectify_registry = _FakeObjectifyRegistry(
        mapping_spec=None,
        mismatched_specs=[mismatched_spec],
    )
    dataset_registry = _FakeDatasetRegistry()
    worker.dataset_registry = dataset_registry

    dataset = SimpleNamespace(
        dataset_id="ds-1",
        db_name="test_db",
        name="accounts",
        branch="main",
        schema_json={"columns": [{"name": "id", "type": "xsd:string"}, {"name": "email", "type": "xsd:string"}]},
    )
    version = SimpleNamespace(
        version_id="ver-new",
        artifact_key="s3://bucket/accounts",
        sample_json={"columns": [{"name": "id", "type": "xsd:string"}, {"name": "email", "type": "xsd:string"}]},
    )

    job_id = await worker._maybe_enqueue_objectify_job(dataset=dataset, version=version)

    assert job_id is None
    recorded = dataset_registry.recorded
    assert recorded is not None
    assert recorded["scope"] == "objectify_schema"
    assert recorded["status"] == "FAIL"
    details = recorded["details"]
    assert details["observed_schema_hash"]
    assert details["expected_schema_hashes"] == ["hash-old"]
    assert details["schema_diff"]["added_columns"] == ["email"]
