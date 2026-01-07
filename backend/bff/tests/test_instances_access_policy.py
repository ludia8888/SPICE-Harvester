from types import SimpleNamespace

from fastapi.testclient import TestClient

from bff.dependencies import get_elasticsearch_service, get_oms_client
from bff.main import app
from bff.routers import instances as instances_router


class _FakeElasticsearch:
    def __init__(self, *, hits):
        self._hits = hits

    async def search(self, *, index, query, size, from_=0, sort=None):  # noqa: ANN003
        return {"total": len(self._hits), "hits": self._hits}


class _FakeDatasetRegistry:
    def __init__(self, *, policy):
        self._policy = policy

    async def get_access_policy(self, *, db_name, scope, subject_type, subject_id):  # noqa: ANN003
        if not self._policy:
            return None
        return SimpleNamespace(policy=self._policy)


class _FakeOMSClient:
    async def get_instance(self, *, db_name, instance_id, class_id):  # noqa: ANN003
        return {"status": "success", "data": {"instance_id": instance_id, "class_id": class_id}}


def _client_with_overrides(*, registry, es, oms=None):
    app.dependency_overrides[instances_router.get_dataset_registry] = lambda: registry
    app.dependency_overrides[get_elasticsearch_service] = lambda: es
    app.dependency_overrides[get_oms_client] = lambda: oms or _FakeOMSClient()
    client = TestClient(app)
    return client


def test_instances_list_masks_fields_with_access_policy():
    registry = _FakeDatasetRegistry(policy={"mask_columns": ["email"], "mask_value": "REDACTED"})
    es = _FakeElasticsearch(hits=[{"instance_id": "u1", "class_id": "User", "email": "u1@example.com"}])
    client = _client_with_overrides(registry=registry, es=es)
    try:
        resp = client.get("/api/v1/databases/test_db/class/User/instances")
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["instances"][0]["email"] == "REDACTED"


def test_instance_get_hides_rows_blocked_by_access_policy():
    policy = {
        "row_filters": [{"field": "status", "op": "eq", "value": "active"}],
        "filter_mode": "allow",
    }
    registry = _FakeDatasetRegistry(policy=policy)
    es = _FakeElasticsearch(hits=[{"instance_id": "u1", "class_id": "User", "status": "inactive"}])
    client = _client_with_overrides(registry=registry, es=es)
    try:
        resp = client.get("/api/v1/databases/test_db/class/User/instance/u1")
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 404
