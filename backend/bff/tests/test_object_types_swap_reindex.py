from types import SimpleNamespace

import pytest
from starlette.requests import Request

from bff.routers import object_types as object_types_router


class _FakeDatasetRegistry:
    def __init__(self, *, dataset, backing, backing_version, version):
        self._dataset = dataset
        self._backing = backing
        self._backing_version = backing_version
        self._version = version

    async def get_dataset(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._dataset
        return None

    async def get_backing_datasource(self, *, backing_id):  # noqa: ANN003
        if backing_id == self._backing.backing_id:
            return self._backing
        return None

    async def get_backing_datasource_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._backing_version.version_id:
            return self._backing_version
        return None

    async def get_latest_version(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._version
        return None

    async def get_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._version.version_id:
            return self._version
        return None

    async def get_or_create_backing_datasource(self, *, dataset, source_type, source_ref):  # noqa: ANN003
        _ = dataset, source_type, source_ref
        return self._backing

    async def get_or_create_backing_datasource_version(self, *, backing_id, dataset_version_id, schema_hash, metadata):  # noqa: ANN003
        _ = backing_id, dataset_version_id, schema_hash, metadata
        return self._backing_version

    async def record_gate_result(self, **kwargs):  # noqa: ANN003
        return None

    async def create_schema_migration_plan(self, **kwargs):  # noqa: ANN003
        return None


class _FakeOMSClient:
    def __init__(self, *, existing_spec):
        self._spec = existing_spec

    async def get_ontology_resource(self, db_name, resource_type, resource_id, branch):  # noqa: ANN003
        _ = db_name, resource_type, resource_id, branch
        return {"data": {"id": resource_id, "label": resource_id, "spec": self._spec}}

    async def get_ontology(self, db_name, class_id, branch):  # noqa: ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": [{"name": "account_id"}]}}

    async def update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit):  # noqa: ANN003
        _ = db_name, resource_type, resource_id, branch, expected_head_commit
        return {"data": payload}


class _FakeObjectifyRegistry:
    def __init__(self, *, mapping_spec):
        self._mapping_spec = mapping_spec
        self.enqueued = None

    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        if mapping_spec_id == self._mapping_spec.mapping_spec_id:
            return self._mapping_spec
        return None

    def build_dedupe_key(  # noqa: ANN003
        self,
        *,
        dataset_id,
        dataset_branch,
        mapping_spec_id,
        mapping_spec_version,
        dataset_version_id,
        artifact_id,
        artifact_output_name,
    ):
        return f"{dataset_id}:{dataset_branch}:{mapping_spec_id}:{mapping_spec_version}:{dataset_version_id}"

    async def get_objectify_job_by_dedupe_key(self, *, dedupe_key):  # noqa: ANN003
        _ = dedupe_key
        return None

    async def enqueue_objectify_job(self, *, job):  # noqa: ANN003
        self.enqueued = job


@pytest.mark.asyncio
async def test_object_type_swap_enqueues_reindex():
    dataset = SimpleNamespace(
        dataset_id="ds-new",
        db_name="test_db",
        name="accounts",
        branch="main",
        source_type="lakefs",
        source_ref=None,
        schema_json={"columns": [{"name": "account_id", "type": "xsd:string"}]},
    )
    version = SimpleNamespace(
        version_id="ver-new",
        dataset_id="ds-new",
        artifact_key="s3://bucket/accounts",
        sample_json={"columns": [{"name": "account_id", "type": "xsd:string"}]},
    )
    backing = SimpleNamespace(backing_id="backing-new", dataset_id="ds-new", db_name="test_db")
    backing_version = SimpleNamespace(version_id="backing-ver-new", backing_id="backing-new", dataset_version_id="ver-new", schema_hash="hash-new")
    mapping_spec = SimpleNamespace(
        mapping_spec_id="map-1",
        version=1,
        dataset_id="ds-new",
        dataset_branch="main",
        target_class_id="Account",
        options={},
    )
    existing_spec = {
        "backing_source": {"ref": "backing-old", "schema_hash": "hash-old", "version_id": "backing-ver-old"},
        "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
        "mapping_spec": {"mapping_spec_id": "map-1", "mapping_spec_version": 1},
        "status": "ACTIVE",
    }

    dataset_registry = _FakeDatasetRegistry(
        dataset=dataset,
        backing=backing,
        backing_version=backing_version,
        version=version,
    )
    oms_client = _FakeOMSClient(existing_spec=existing_spec)
    objectify_registry = _FakeObjectifyRegistry(mapping_spec=mapping_spec)

    request = Request({"type": "http", "headers": []})
    body = object_types_router.ObjectTypeContractUpdate(
        backing_dataset_id="ds-new",
        migration={"approved": True},
    )

    response = await object_types_router.update_object_type_contract(
        db_name="test_db",
        class_id="Account",
        body=body,
        request=request,
        branch="main",
        expected_head_commit="head",
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )

    assert response.data["reindex_job_id"]
    assert objectify_registry.enqueued is not None
    assert objectify_registry.enqueued.mapping_spec_id == "map-1"
