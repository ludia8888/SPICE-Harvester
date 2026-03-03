from types import SimpleNamespace

import pytest

from bff.routers import pipeline_datasets, pipeline_ops, pipeline_proposals


def test_pipeline_helpers_normalize_inputs(monkeypatch):
    monkeypatch.setenv("PIPELINE_PROTECTED_BRANCHES", "main, release")
    monkeypatch.setenv("PIPELINE_REQUIRE_PROPOSALS", "true")

    assert pipeline_ops._resolve_pipeline_protected_branches() == {"main", "main", "release"}
    assert pipeline_ops._pipeline_requires_proposal("main") is True
    assert pipeline_ops._pipeline_requires_proposal("feature") is False

    assert pipeline_proposals._normalize_mapping_spec_ids(None) == []
    assert pipeline_proposals._normalize_mapping_spec_ids("a,b") == ["a", "b"]
    assert pipeline_proposals._normalize_mapping_spec_ids(["a", "a", "b"]) == ["a", "b"]

    metadata = pipeline_datasets._sanitize_s3_metadata({"hello": "world", "label": "안녕"})
    assert metadata["hello"] == "world"
    assert metadata["label"] != "안녕"

    assert pipeline_datasets._detect_csv_delimiter("a,b,c") == ","
    columns, preview_rows, total_rows = pipeline_datasets._parse_csv_content(
        "id,name\n1,Ada\n2,Ben\n",
        delimiter=",",
        has_header=True,
        preview_limit=1,
    )
    assert columns == ["id", "name"]
    assert preview_rows == [["1", "Ada"]]
    assert total_rows == 2


@pytest.mark.asyncio
async def test_maybe_enqueue_objectify_job():
    mapping_spec = SimpleNamespace(
        mapping_spec_id="map-1",
        version=1,
        options={"ontology_branch": "main"},
        auto_sync=True,
        target_class_id="class-1",
    )

    class FakeObjectifyRegistry:
        async def get_active_mapping_spec(self, **kwargs):
            return mapping_spec

        def build_dedupe_key(self, **kwargs):
            return "dedupe-key"

        async def get_objectify_job_by_dedupe_key(self, **kwargs):
            return None

        async def enqueue_objectify_job(self, **kwargs):
            return None

    class FakeObjectifyJobQueue:
        def __init__(self):
            self.jobs = []

        async def publish(self, job, *, require_delivery: bool = True):
            self.jobs.append(job)

    class FakeDatasetRegistry:
        async def record_gate_result(self, **kwargs):
            return None

    dataset = SimpleNamespace(
        dataset_id="ds-1",
        db_name="core-db",
        name="orders",
        branch="main",
        schema_json={"columns": [{"name": "id"}]},
    )
    version = SimpleNamespace(
        version_id="ver-1",
        artifact_key="s3://raw/commit/object.csv",
        sample_json={"columns": ["id"]},
    )
    registry = FakeObjectifyRegistry()
    queue = FakeObjectifyJobQueue()
    dataset_registry = FakeDatasetRegistry()

    job_id = await pipeline_datasets._maybe_enqueue_objectify_job(
        dataset=dataset,
        version=version,
        objectify_registry=registry,
        job_queue=queue,
        dataset_registry=dataset_registry,
        actor_user_id="user-1",
    )

    assert job_id is not None
    assert queue.jobs
