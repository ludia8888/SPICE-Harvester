from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_oms_client


def test_ontology_validate_create_proxies_to_oms():
    fake_oms = AsyncMock()
    fake_oms.validate_ontology_create.return_value = {
        "status": "success",
        "message": "ok",
        "data": {"lint_report": {"ok": True}},
    }

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    client = TestClient(app)
    try:
        res = client.post(
            "/api/v1/databases/demo_db/ontology/validate",
            params={"branch": "main"},
            json={
                "id": "Product",
                "label": "Product",
                "properties": [{"name": "product_id", "type": "xsd:string", "label": "Product ID"}],
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    assert res.json()["status"] == "success"
    fake_oms.validate_ontology_create.assert_awaited_once()


def test_ontology_validate_update_endpoint_removed():
    fake_oms = AsyncMock()

    app.dependency_overrides[get_oms_client] = lambda: fake_oms

    client = TestClient(app)
    try:
        res = client.post(
            "/api/v1/databases/demo_db/ontology/Product/validate",
            params={"branch": "main"},
            json={"label": "Product v2"},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 404
    fake_oms.validate_ontology_update.assert_not_awaited()
