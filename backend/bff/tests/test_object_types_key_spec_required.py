from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.routers import object_types as object_types_router


class _FakeDatasetRegistry:
    def __init__(self, *, dataset, version, backing, backing_version):
        self._dataset = dataset
        self._version = version
        self._backing = backing
        self._backing_version = backing_version

    async def get_dataset(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._dataset
        return None

    async def get_latest_version(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._version
        return None

    async def get_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._version.version_id:
            return self._version
        return None

    async def get_backing_datasource(self, *, backing_id):  # noqa: ANN003
        if backing_id == self._backing.backing_id:
            return self._backing
        return None

    async def get_backing_datasource_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._backing_version.version_id:
            return self._backing_version
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


class _FakeOMSClient:
    async def get_ontology(self, db_name, class_id, branch):  # noqa: ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": [{"name": "account_id", "type": "xsd:string"}]}}

    async def create_ontology_resource(  # noqa: ANN003
        self,
        db_name,
        resource_type,
        payload,
        branch,
        expected_head_commit,
    ):
        _ = db_name, resource_type, branch, expected_head_commit
        return {"data": payload}


class _FakeObjectifyRegistry:
    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        _ = mapping_spec_id
        return None


def _build_registry():
    dataset = SimpleNamespace(
        dataset_id="ds-1",
        db_name="test_db",
        name="accounts",
        branch="main",
        source_type="dataset",
        source_ref=None,
        schema_json={"columns": [{"name": "account_id", "type": "xsd:string"}]},
    )
    version = SimpleNamespace(
        version_id="ver-1",
        dataset_id="ds-1",
        artifact_key="s3://bucket/accounts",
        sample_json={"columns": [{"name": "account_id", "type": "xsd:string"}]},
    )
    backing = SimpleNamespace(
        backing_id="backing-1",
        dataset_id="ds-1",
        db_name="test_db",
        name="accounts",
        description=None,
        source_type="dataset",
        source_ref=None,
        branch="main",
        status="ACTIVE",
    )
    backing_version = SimpleNamespace(
        version_id="backing-ver-1",
        backing_id="backing-1",
        dataset_version_id="ver-1",
        schema_hash="hash-1",
        artifact_key=None,
        metadata={},
        status="ACTIVE",
        created_at=None,
    )
    return _FakeDatasetRegistry(
        dataset=dataset,
        version=version,
        backing=backing,
        backing_version=backing_version,
    )


@pytest.mark.asyncio
async def test_object_type_requires_primary_key():
    registry = _build_registry()
    oms_client = _FakeOMSClient()
    objectify_registry = _FakeObjectifyRegistry()
    request = Request({"type": "http", "headers": []})

    original_enforce = object_types_router.enforce_database_role

    async def _noop_enforce_database_role(**kwargs):
        return None

    object_types_router.enforce_database_role = _noop_enforce_database_role
    try:
        body = object_types_router.ObjectTypeContractRequest(
            class_id="Account",
            backing_dataset_id="ds-1",
            dataset_version_id="ver-1",
            pk_spec={"title_key": ["account_id"]},
            status="ACTIVE",
        )
        with pytest.raises(HTTPException) as exc_info:
            await object_types_router.create_object_type_contract(
                db_name="test_db",
                body=body,
                request=request,
                branch="main",
                expected_head_commit="head",
                oms_client=oms_client,
                dataset_registry=registry,
                objectify_registry=objectify_registry,
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "pk_spec.primary_key is required"
    finally:
        object_types_router.enforce_database_role = original_enforce


@pytest.mark.asyncio
async def test_object_type_requires_title_key():
    registry = _build_registry()
    oms_client = _FakeOMSClient()
    objectify_registry = _FakeObjectifyRegistry()
    request = Request({"type": "http", "headers": []})

    original_enforce = object_types_router.enforce_database_role

    async def _noop_enforce_database_role(**kwargs):
        return None

    object_types_router.enforce_database_role = _noop_enforce_database_role
    try:
        body = object_types_router.ObjectTypeContractRequest(
            class_id="Account",
            backing_dataset_id="ds-1",
            dataset_version_id="ver-1",
            pk_spec={"primary_key": ["account_id"]},
            status="ACTIVE",
        )
        with pytest.raises(HTTPException) as exc_info:
            await object_types_router.create_object_type_contract(
                db_name="test_db",
                body=body,
                request=request,
                branch="main",
                expected_head_commit="head",
                oms_client=oms_client,
                dataset_registry=registry,
                objectify_registry=objectify_registry,
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "pk_spec.title_key is required"
    finally:
        object_types_router.enforce_database_role = original_enforce
