from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_label_mapper, get_oms_client


class _FakeLabelMapper:
    async def get_class_id(self, db_name: str, label: str, lang: str = "ko"):
        assert db_name == "demo_db"
        assert label == "Product"
        return "Product"

    async def get_property_id(self, db_name: str, class_id: str, label: str, lang: str = "ko"):
        assert db_name == "demo_db"
        assert class_id == "Product"
        mapping = {"Product ID": "product_id", "Name": "name"}
        return mapping.get(label)


def test_instance_create_allows_label_keys_with_spaces():
    fake_oms = AsyncMock()
    fake_oms.post.return_value = {
        "command_id": "123e4567-e89b-12d3-a456-426614174000",
        "status": "PENDING",
        "result": {"message": "accepted"},
        "error": None,
        "completed_at": None,
        "retry_count": 0,
    }

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    app.dependency_overrides[get_label_mapper] = lambda: _FakeLabelMapper()

    client = TestClient(app)
    try:
        res = client.post(
            "/api/v1/database/demo_db/instances/Product/create",
            params={"branch": "main"},
            json={"data": {"Product ID": "PROD-1", "Name": "Apple"}, "metadata": {}},
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 202
    payload = res.json()
    assert payload["command_id"] == "123e4567-e89b-12d3-a456-426614174000"

    # Ensure we called OMS with converted property ids (not label keys)
    called = fake_oms.post.await_args
    assert called.args[0] == "/api/v1/instances/demo_db/async/Product/create"
    assert called.kwargs["json"]["data"] == {"product_id": "PROD-1", "name": "Apple"}
