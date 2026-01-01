from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_label_mapper, get_oms_client


class _FakeLabelMapper:
    async def get_class_id(self, db_name: str, label: str, lang: str = "ko"):
        assert db_name == "demo_db"
        assert label == "Product"
        return "Product"

    async def update_mappings(self, db_name: str, update_data: dict):
        return None


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


def test_ontology_validate_update_resolves_label_and_proxies_to_oms():
    fake_oms = AsyncMock()
    fake_oms.validate_ontology_update.return_value = {
        "status": "success",
        "message": "ok",
        "data": {"diff_lint": {"ok": True}},
    }

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    app.dependency_overrides[get_label_mapper] = lambda: _FakeLabelMapper()

    client = TestClient(app)
    try:
        res = client.post(
            "/api/v1/databases/demo_db/ontology/Product/validate",
            params={"branch": "main"},
            json={"label": "Product v2"},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "success"

    called = fake_oms.validate_ontology_update.await_args
    assert called.args[0] == "demo_db"
    assert called.args[1] == "Product"
    assert called.kwargs["branch"] == "main"
