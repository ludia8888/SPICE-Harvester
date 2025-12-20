import json
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.main import app
from bff.dependencies import get_oms_client


class _FakeFunnelClient:
    google_result = {}
    excel_result = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def google_sheets_to_structure_preview(self, **kwargs):
        return dict(self.google_result)

    async def excel_to_structure_preview(self, **kwargs):
        return dict(self.excel_result)


def test_google_sheets_import_commit_submits_to_oms(monkeypatch):
    import bff.services.funnel_client as funnel_client_module

    # Unit tests should be hermetic: enforce a local auth token and present it.
    monkeypatch.setenv("BFF_ADMIN_TOKEN", "testtoken")

    _FakeFunnelClient.google_result = {
        "table": {"id": "t1", "mode": "table", "bbox": {"top": 1, "left": 1, "bottom": 3, "right": 1}},
        "preview": {"columns": ["Name"], "sample_data": [["Alice"], ["Bob"]]},
        "structure": {"tables": [{"id": "t1"}]},
    }
    monkeypatch.setattr(funnel_client_module, "FunnelClient", _FakeFunnelClient)

    fake_oms = AsyncMock()
    fake_oms.post.return_value = {
        "command_id": "123e4567-e89b-12d3-a456-426614174000",
        "status": "PENDING",
        "result": {"message": "accepted"},
    }
    app.dependency_overrides[get_oms_client] = lambda: fake_oms

    client = TestClient(app)
    try:
        res = client.post(
            "/api/v1/database/testdb/import-from-google-sheets/commit",
            headers={"X-Admin-Token": "testtoken"},
            json={
                "sheet_url": "https://docs.google.com/spreadsheets/d/example",
                "worksheet_name": "Sheet1",
                "target_class_id": "Customer",
                "target_schema": [{"name": "name", "type": "xsd:string"}],
                "mappings": [{"source_field": "Name", "target_field": "name"}],
                "batch_size": 500,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert "OMS disabled" not in body.get("message", "")
    assert body["write"]["commands"][0]["status_url"] == "/api/v1/commands/123e4567-e89b-12d3-a456-426614174000/status"
    fake_oms.post.assert_awaited_once()
    called_path = fake_oms.post.await_args.args[0]
    assert called_path == "/api/v1/instances/testdb/async/Customer/bulk-create"


def test_excel_import_commit_submits_to_oms(monkeypatch):
    import bff.services.funnel_client as funnel_client_module

    # Unit tests should be hermetic: enforce a local auth token and present it.
    monkeypatch.setenv("BFF_ADMIN_TOKEN", "testtoken")

    _FakeFunnelClient.excel_result = {
        "table": {"id": "t1", "mode": "table", "bbox": {"top": 1, "left": 1, "bottom": 3, "right": 1}},
        "preview": {"columns": ["Name"], "sample_data": [["Alice"]]},
        "structure": {"tables": [{"id": "t1"}]},
    }
    monkeypatch.setattr(funnel_client_module, "FunnelClient", _FakeFunnelClient)

    fake_oms = AsyncMock()
    fake_oms.post.return_value = {
        "command_id": "123e4567-e89b-12d3-a456-426614174000",
        "status": "PENDING",
        "result": {"message": "accepted"},
    }
    app.dependency_overrides[get_oms_client] = lambda: fake_oms

    client = TestClient(app)
    try:
        res = client.post(
            "/api/v1/database/testdb/import-from-excel/commit",
            headers={"X-Admin-Token": "testtoken"},
            data={
                "target_class_id": "Customer",
                "target_schema_json": json.dumps([{"name": "name", "type": "xsd:string"}]),
                "mappings_json": json.dumps([{"source_field": "Name", "target_field": "name"}]),
            },
            files={
                "file": (
                    "test.xlsx",
                    b"fake-xlsx-bytes",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert "OMS disabled" not in body.get("message", "")
    assert body["write"]["commands"][0]["status_url"] == "/api/v1/commands/123e4567-e89b-12d3-a456-426614174000/status"
    fake_oms.post.assert_awaited_once()
    called_path = fake_oms.post.await_args.args[0]
    assert called_path == "/api/v1/instances/testdb/async/Customer/bulk-create"
