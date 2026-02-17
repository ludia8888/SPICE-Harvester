from __future__ import annotations

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app
from bff.routers import foundry_ontology_v2 as router_v2


class _FakeOMSClient:
    async def list_databases(self):  # noqa: ANN201
        return {"data": {"databases": [{"name": "test_db"}]}}

    async def get_ontology_resource(self, db_name, *, resource_type, resource_id, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if resource_type == "object_type" and resource_id == "User":
            return {
                "data": {
                    "id": "User",
                    "spec": {"pk_spec": {"primary_key": ["user_id"]}},
                }
            }
        return {"data": None}

    async def get_ontology(self, db_name, class_id, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": [{"name": "user_id", "type": "string"}]}}

    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        if path.endswith("/search"):
            payload = kwargs.get("json") if isinstance(kwargs, dict) else {}
            where = payload.get("where") if isinstance(payload, dict) else None
            if isinstance(where, dict) and where.get("field") == "user_id":
                value = str(where.get("value") or "").strip()
                if value == "u-missing":
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {"data": [{"user_id": value, "email": "masked@example.com"}], "nextPageToken": None, "totalCount": "1"}
            return {"data": [{"user_id": "u1", "email": "masked@example.com"}], "nextPageToken": None, "totalCount": "1"}
        return {}


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


def _client_with_overrides():
    app.dependency_overrides[get_oms_client] = lambda: _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    client = TestClient(app)
    return client, original_require


def test_v2_list_objects_returns_foundry_shape():
    client, original_require = _client_with_overrides()
    try:
        resp = client.get("/api/v2/ontologies/test_db/objects/User")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload.get("data"), list)
    assert payload["data"][0]["email"] == "masked@example.com"


def test_v2_get_object_not_found_uses_foundry_error_envelope():
    client, original_require = _client_with_overrides()
    try:
        resp = client.get("/api/v2/ontologies/test_db/objects/User/u-missing")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert resp.status_code == 404
    payload = resp.json()
    assert payload["errorName"] == "ObjectNotFound"
