from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.routers import object_types as object_types_router


class _FakeDatasetRegistry:
    def __init__(self, *, dataset, backing, backing_version, version):
        self._dataset = dataset
        self._backing = backing
        self._backing_version = backing_version
        self._version = version
        self.last_gate = None
        self.last_plan = None

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

    async def get_or_create_backing_datasource_version(  # noqa: ANN003
        self,
        *,
        backing_id,
        dataset_version_id,
        schema_hash,
        metadata,
    ):
        _ = backing_id, dataset_version_id, schema_hash, metadata
        return self._backing_version

    async def record_gate_result(self, **kwargs):  # noqa: ANN003
        self.last_gate = kwargs

    async def create_schema_migration_plan(self, **kwargs):  # noqa: ANN003
        self.last_plan = kwargs

    async def count_instance_edits(self, **kwargs):  # noqa: ANN003
        return 0

    async def get_instance_edit_field_stats(self, **kwargs):  # noqa: ANN003
        return {"total": 0}


class _FakeOMSClient:
    def __init__(self, *, existing_spec):
        self._spec = existing_spec

    async def get_ontology_resource(self, db_name, resource_type, resource_id, branch):  # noqa: ANN003
        _ = db_name, resource_type, resource_id, branch
        return {"data": {"id": resource_id, "label": resource_id, "spec": self._spec}}

    async def update_ontology_resource(  # noqa: ANN003
        self,
        db_name,
        resource_type,
        resource_id,
        payload,
        branch,
        expected_head_commit,
    ):
        _ = db_name, resource_type, resource_id, branch, expected_head_commit
        return {"data": payload}

    async def get_ontology(self, db_name, class_id, branch):  # noqa: ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": [{"name": "account_id", "type": "xsd:string"}]}}


class _FakeObjectifyRegistry:
    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        _ = mapping_spec_id
        return None


def _build_context(status_value="ACTIVE"):
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
    backing_version = SimpleNamespace(
        version_id="backing-ver-new",
        backing_id="backing-new",
        dataset_version_id="ver-new",
        schema_hash="hash-new",
    )
    existing_spec = {
        "backing_source": {"ref": "backing-old", "schema_hash": "hash-old", "version_id": "backing-ver-old"},
        "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
        "mapping_spec": {"mapping_spec_id": "map-1", "mapping_spec_version": 1},
        "status": status_value,
    }
    dataset_registry = _FakeDatasetRegistry(
        dataset=dataset,
        backing=backing,
        backing_version=backing_version,
        version=version,
    )
    oms_client = _FakeOMSClient(existing_spec=existing_spec)
    return dataset_registry, oms_client


@pytest.mark.asyncio
async def test_object_type_migration_requires_approval():
    dataset_registry, oms_client = _build_context()
    objectify_registry = _FakeObjectifyRegistry()
    request = Request({"type": "http", "headers": []})
    body = object_types_router.ObjectTypeContractUpdate(
        backing_dataset_id="ds-new",
    )

    with pytest.raises(HTTPException) as exc_info:
        await object_types_router.update_object_type_contract(
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

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "OBJECT_TYPE_MIGRATION_REQUIRED"
    assert dataset_registry.last_gate["status"] == "FAIL"
    assert dataset_registry.last_gate["details"]["backing_changed"] is True


@pytest.mark.asyncio
async def test_object_type_migration_plan_is_recorded():
    dataset_registry, oms_client = _build_context(status_value="INACTIVE")
    objectify_registry = _FakeObjectifyRegistry()
    request = Request({"type": "http", "headers": []})
    body = object_types_router.ObjectTypeContractUpdate(
        backing_dataset_id="ds-new",
        migration={"approved": True, "note": "schema upgrade"},
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

    assert response.data["object_type"]["spec"]["backing_source"]["ref"] == "backing-new"
    assert dataset_registry.last_plan is not None
    assert dataset_registry.last_plan["status"] == "APPROVED"
    assert dataset_registry.last_plan["plan"]["backing_changed"] is True
    assert dataset_registry.last_plan["plan"]["pk_changed"] is False
