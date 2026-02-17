"""Security tests for information leakage prevention on Foundry v2 object reads."""

from __future__ import annotations

import httpx
from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app
from bff.routers import foundry_ontology_v2 as router_v2


class _FakeOMSClient:
    def __init__(self, *, fail_search: bool = False) -> None:
        self._fail_search = fail_search

    async def list_databases(self):  # noqa: ANN201
        return {"data": {"databases": [{"name": "test_db"}]}}

    async def get_ontology_resource(self, db_name, *, resource_type, resource_id, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if resource_type == "object_type" and resource_id == "TestClass":
            return {"data": {"id": "TestClass", "spec": {"pk_spec": {"primary_key": ["instance_id"]}}}}
        return {"data": None}

    async def get_ontology(self, db_name, class_id, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": [{"name": "instance_id", "type": "string"}]}}

    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        if self._fail_search:
            request = httpx.Request("POST", "http://oms.local" + str(path))
            response = httpx.Response(503, request=request, json={"detail": "upstream unavailable"})
            raise httpx.HTTPStatusError("upstream unavailable", request=request, response=response)

        payload = kwargs.get("json") if isinstance(kwargs, dict) else {}
        where = payload.get("where") if isinstance(payload, dict) else None
        if isinstance(where, dict) and where.get("field") == "instance_id":
            value = str(where.get("value") or "").strip()
            if value == "nonexistent":
                return {"data": [], "nextPageToken": None, "totalCount": "0"}
            return {
                "data": [{"instance_id": value, "class_id": "TestClass", "status": "ACTIVE"}],
                "nextPageToken": None,
                "totalCount": "1",
            }
        return {
            "data": [
                {"instance_id": "inst1", "class_id": "TestClass"},
                {"instance_id": "inst2", "class_id": "TestClass"},
            ],
            "nextPageToken": None,
            "totalCount": "2",
        }


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


def _client(*, fail_search: bool = False):
    app.dependency_overrides[get_oms_client] = lambda: _FakeOMSClient(fail_search=fail_search)
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    client = TestClient(app)
    return client, original_require


def test_v2_get_object_success_no_source_leak():
    client, original_require = _client()
    try:
        response = client.get("/api/v2/ontologies/test_db/objects/TestClass/test_instance")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert "source" not in response_data
    assert "elasticsearch" not in str(response_data).lower()
    assert "terminus" not in str(response_data).lower()
    assert response_data["instance_id"] == "test_instance"


def test_v2_list_objects_success_no_source_leak():
    client, original_require = _client()
    try:
        response = client.get("/api/v2/ontologies/test_db/objects/TestClass")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert response.status_code == 200
    response_data = response.json()
    assert "source" not in str(response_data).lower()
    assert "elasticsearch" not in str(response_data).lower()
    assert "terminus" not in str(response_data).lower()
    assert isinstance(response_data.get("data"), list)
    assert len(response_data["data"]) == 2


def test_v2_not_found_error_uses_foundry_envelope_without_internal_leak():
    client, original_require = _client()
    try:
        response = client.get("/api/v2/ontologies/test_db/objects/TestClass/nonexistent")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["errorName"] == "ObjectNotFound"
    assert "elasticsearch" not in str(response_data).lower()
    assert "terminus" not in str(response_data).lower()
    assert "oms" not in str(response_data).lower()


def test_v2_upstream_error_does_not_leak_backend_source_details():
    client, original_require = _client(fail_search=True)
    try:
        response = client.get("/api/v2/ontologies/test_db/objects/TestClass/test_instance")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert response.status_code == 503
    response_data = response.json()
    assert "elasticsearch" not in str(response_data).lower()
    assert "terminus" not in str(response_data).lower()
