from types import SimpleNamespace

import pytest

from objectify_worker.main import ObjectifyWorker
from shared.models.objectify_job import ObjectifyJob


class _FakeLineageStore:
    def __init__(self):
        self.links = []

    def node_aggregate(self, kind, aggregate_id):  # noqa: ANN003
        return f"{kind}:{aggregate_id}"

    async def record_link(self, **kwargs):  # noqa: ANN003
        self.links.append(kwargs)


@pytest.mark.asyncio
async def test_instance_lineage_records_dataset_version():
    worker = ObjectifyWorker()
    worker.lineage_store = _FakeLineageStore()

    job = ObjectifyJob(
        job_id="job-1",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="ver-1",
        artifact_output_name="accounts",
        dedupe_key="dedupe",
        dataset_branch="main",
        artifact_key="s3://bucket/accounts",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        target_class_id="Account",
    )

    await worker._record_instance_lineage(
        job=job,
        job_node_id="ObjectifyJob:job-1",
        instance_ids=["acc-1"],
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        ontology_version={},
        limit_remaining=10,
        input_type="dataset_version",
        artifact_output_name="accounts",
    )

    assert worker.lineage_store.links
    dataset_links = [
        link for link in worker.lineage_store.links if link.get("edge_type") == "dataset_version_objectified"
    ]
    assert dataset_links
    metadata = dataset_links[0]["edge_metadata"]
    assert metadata["dataset_version_id"] == "ver-1"
    assert metadata["dataset_id"] == "ds-1"
