from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_elasticsearch_service, get_oms_client
from elasticsearch.exceptions import ConnectionError as ESConnectionError


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


def test_bff_http_exception_detail_localizes_by_lang_param():
    # Force ES + OMS failures so BFF returns 404 with a safe message.
    mock_es = AsyncMock()
    mock_es.search.side_effect = ESConnectionError("Connection failed")

    mock_oms = AsyncMock()
    mock_oms.get_instance.side_effect = Exception("OMS connection failed")

    app.dependency_overrides[get_elasticsearch_service] = lambda: mock_es
    app.dependency_overrides[get_oms_client] = lambda: mock_oms

    client = TestClient(app)
    try:
        ko = client.get("/api/v1/databases/test_db/class/TestClass/instance/nonexistent")
        en = client.get("/api/v1/databases/test_db/class/TestClass/instance/nonexistent?lang=en")
    finally:
        app.dependency_overrides.clear()

    assert ko.status_code == 404
    assert "찾을 수 없습니다" in ko.json()["detail"]

    assert en.status_code == 404
    assert "was not found" in en.json()["detail"]
