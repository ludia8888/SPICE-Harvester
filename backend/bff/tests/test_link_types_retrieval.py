from types import SimpleNamespace

import pytest
from starlette.requests import Request

from bff.routers import link_types as link_types_router


class _FakeDatasetRegistry:
    def __init__(self, spec):
        self._spec = spec

    async def get_relationship_spec(self, *, link_type_id):  # noqa: ANN003
        if link_type_id == self._spec.link_type_id:
            return self._spec
        return None


class _FakeOMSClient:
    def __init__(self, resources):
        self._resources = resources

    async def list_ontology_resources(self, db_name, resource_type, branch="main"):  # noqa: ANN003
        _ = db_name, resource_type, branch
        return {"resources": self._resources}

    async def get_ontology_resource(self, db_name, resource_type, resource_id, branch="main"):  # noqa: ANN003
        _ = db_name, resource_type, branch
        for res in self._resources:
            if res.get("id") == resource_id:
                return {"data": res}
        return None


@pytest.mark.asyncio
async def test_list_link_types_includes_relationship_spec_status():
    relationship_spec = SimpleNamespace(
        link_type_id="link-1",
        last_index_status="SUCCESS",
        last_index_stats={"link_count": 1},
    )
    dataset_registry = _FakeDatasetRegistry(relationship_spec)
    oms_client = _FakeOMSClient([{"id": "link-1", "label": "Link"}])

    response = await link_types_router.list_link_types(
        db_name="test_db",
        branch="main",
        oms_client=oms_client,
        dataset_registry=dataset_registry,
    )

    items = response.data["link_types"]
    assert items[0]["relationship_spec"]["last_index_status"] == "SUCCESS"
    assert items[0]["relationship_spec"]["last_index_stats"] == {"link_count": 1}


@pytest.mark.asyncio
async def test_get_link_type_includes_relationship_spec_status():
    relationship_spec = SimpleNamespace(
        link_type_id="link-1",
        last_index_status="FAILED",
        last_index_stats={"dangling_missing_targets": 2},
    )
    dataset_registry = _FakeDatasetRegistry(relationship_spec)
    oms_client = _FakeOMSClient([{"id": "link-1", "label": "Link"}])
    request = Request({"type": "http", "headers": []})

    original_enforce = link_types_router.enforce_database_role

    async def _noop_enforce_database_role(**kwargs):
        return None

    link_types_router.enforce_database_role = _noop_enforce_database_role
    try:
        response = await link_types_router.get_link_type(
            db_name="test_db",
            link_type_id="link-1",
            request=request,
            branch="main",
            oms_client=oms_client,
            dataset_registry=dataset_registry,
        )
    finally:
        link_types_router.enforce_database_role = original_enforce

    spec = response.data["relationship_spec"]
    assert spec["last_index_status"] == "FAILED"
    assert spec["last_index_stats"] == {"dangling_missing_targets": 2}
