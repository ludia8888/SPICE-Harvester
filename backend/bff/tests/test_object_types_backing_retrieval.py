from types import SimpleNamespace

import pytest
from starlette.requests import Request

from bff.routers import object_types as object_types_router


class _FakeOMSClient:
    async def get_ontology_resource(self, db_name, resource_type, resource_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, resource_type, branch
        return {
            "data": {
                "id": resource_id,
                "spec": {
                    "backing_source": {"ref": "backing-1", "version_id": "backing-ver-1"},
                },
            }
        }


class _FakeDatasetRegistry:
    async def get_backing_datasource(self, *, backing_id):  # noqa: ANN003
        if backing_id != "backing-1":
            return None
        return SimpleNamespace(
            backing_id="backing-1",
            dataset_id="ds-1",
            db_name="test_db",
            name="backing",
            description=None,
            source_type="dataset",
            source_ref=None,
            branch="main",
            status="ACTIVE",
        )

    async def get_backing_datasource_version(self, *, version_id):  # noqa: ANN003
        if version_id != "backing-ver-1":
            return None
        return SimpleNamespace(
            version_id="backing-ver-1",
            backing_id="backing-1",
            dataset_version_id="ver-1",
            schema_hash="hash-1",
            metadata={},
            status="ACTIVE",
        )


class _FakeObjectifyRegistry:
    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        _ = mapping_spec_id
        return None


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


@pytest.mark.asyncio
async def test_object_type_retrieval_includes_backing_datasource():
    request = Request({"type": "http", "headers": []})
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        response = await object_types_router.get_object_type_contract(
            db_name="test_db",
            class_id="Account",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
            dataset_registry=_FakeDatasetRegistry(),
            objectify_registry=_FakeObjectifyRegistry(),
        )
    finally:
        object_types_router._require_domain_role = original_require

    data = response.data
    assert data["backing_datasource"]["backing_id"] == "backing-1"
    assert data["backing_datasource_version"]["version_id"] == "backing-ver-1"
