from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_oms_client
from bff.routers import foundry_ontology_v2 as router_v2


def test_bff_health_message_localizes_by_lang_param():
    fake_oms = AsyncMock()
    fake_oms.check_health.return_value = True

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    client = TestClient(app)
    try:
        ko = client.get("/api/v1/health")
        en = client.get("/api/v1/health?lang=en")
    finally:
        app.dependency_overrides.clear()

    assert ko.status_code == 200
    assert ko.json()["message"] == "서비스가 정상입니다."

    assert en.status_code == 200
    assert en.json()["message"] == "Service is healthy"


def test_foundry_v2_error_envelope_stable_with_lang_param():
    mock_oms = AsyncMock()
    mock_oms.list_databases.return_value = {"data": {"databases": [{"name": "test_db"}]}}
    mock_oms.get_ontology_resource.return_value = {"data": {"id": "TestClass", "spec": {"pk_spec": {"primary_key": ["instance_id"]}}}}
    mock_oms.get_ontology.return_value = {"data": {"properties": [{"name": "instance_id", "type": "string"}]}}
    mock_oms.post.return_value = {"data": [], "nextPageToken": None, "totalCount": "0"}

    async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
        _ = request, db_name
        return None

    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    app.dependency_overrides[get_oms_client] = lambda: mock_oms

    client = TestClient(app)
    try:
        ko = client.get("/api/v2/ontologies/test_db/objects/TestClass/nonexistent")
        en = client.get("/api/v2/ontologies/test_db/objects/TestClass/nonexistent?lang=en")
    finally:
        router_v2._require_domain_role = original_require
        app.dependency_overrides.clear()

    assert ko.status_code == 404
    assert ko.json()["errorName"] == "ObjectNotFound"

    assert en.status_code == 404
    assert en.json()["errorName"] == "ObjectNotFound"
