from types import SimpleNamespace

import pytest

from objectify_worker.main import ObjectifyNonRetryableError, ObjectifyWorker
from shared.models.objectify_job import ObjectifyJob


class _StubObjectifyRegistry:
    def __init__(self, *, mapping_spec):
        self._mapping_spec = mapping_spec
        self.last_status = None

    async def get_objectify_job(self, *, job_id):  # noqa: ANN003
        _ = job_id
        return None

    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        if mapping_spec_id == self._mapping_spec.mapping_spec_id:
            return self._mapping_spec
        return None

    async def update_objectify_job_status(self, **kwargs):  # noqa: ANN003
        self.last_status = kwargs


class _StubDatasetRegistry:
    def __init__(self, *, dataset, version):
        self._dataset = dataset
        self._version = version

    async def get_dataset(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._dataset
        return None

    async def get_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._version.version_id:
            return self._version
        return None


class _PKDuplicateWorker(ObjectifyWorker):
    def __init__(self, *, rows, mapping_spec, dataset, version):
        super().__init__()
        self._rows = rows
        self.objectify_registry = _StubObjectifyRegistry(mapping_spec=mapping_spec)
        self.dataset_registry = _StubDatasetRegistry(dataset=dataset, version=version)

    async def _iter_dataset_batches(self, **kwargs):  # noqa: ANN003
        yield ["account_id"], self._rows, 0

    async def _fetch_class_schema(self, job):  # noqa: ANN003
        return {"data": {"properties": [{"name": "account_id", "type": "xsd:string"}]}}

    async def _fetch_object_type_contract(self, job):  # noqa: ANN003
        return {
            "data": {
                "spec": {
                    "status": "ACTIVE",
                    "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                }
            }
        }

    async def _fetch_ontology_version(self, job):  # noqa: ANN003
        return {}

    async def _fetch_value_type_defs(self, job, value_type_refs):  # noqa: ANN003
        return {}, set()

    async def _record_lineage_header(self, **kwargs):  # noqa: ANN003
        return "node"

    async def _validate_batches(self, **kwargs):  # noqa: ANN003
        raise AssertionError("validate_batches should not run when PK scan fails")

    async def _bulk_update_instances(self, **kwargs):  # noqa: ANN003
        raise AssertionError("bulk_update_instances should not run when PK scan fails")

    async def _record_gate_result(self, **kwargs):  # noqa: ANN003
        return None


@pytest.mark.asyncio
async def test_pk_duplicates_fail_before_writes():
    dataset = SimpleNamespace(dataset_id="ds-1", db_name="test_db")
    version = SimpleNamespace(version_id="ver-1", dataset_id="ds-1", artifact_key="s3://bucket/key")
    mapping_spec = SimpleNamespace(
        mapping_spec_id="map-1",
        version=1,
        dataset_id="ds-1",
        target_class_id="Account",
        mappings=[{"source_field": "account_id", "target_field": "account_id"}],
        target_field_types={"account_id": "xsd:string"},
        options={},
        backing_datasource_version_id=None,
    )
    job = ObjectifyJob(
        job_id="job-1",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="ver-1",
        artifact_output_name="accounts",
        dedupe_key="dedupe",
        dataset_branch="main",
        artifact_key="s3://bucket/key",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        target_class_id="Account",
    )

    worker = _PKDuplicateWorker(rows=[["1"], ["1"]], mapping_spec=mapping_spec, dataset=dataset, version=version)

    with pytest.raises(ObjectifyNonRetryableError):
        await worker._process_job(job)

    assert worker.objectify_registry.last_status["status"] == "FAILED"
    stats = worker.objectify_registry.last_status["report"]["stats"]
    assert stats["pk_duplicates"] == 1
