import httpx
import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_google_sheets_preview_sets_explicit_timeout(monkeypatch):
    from funnel.services.data_processor import FunnelDataProcessor
    import funnel.services.data_processor as module

    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            captured["raise_for_status_called"] = True

        def json(self):
            return {
                "sheet_id": "sheet-1",
                "sheet_title": "Demo",
                "worksheet_title": "Sheet1",
                "columns": ["id"],
                "sample_rows": [["1"]],
                "total_rows": 1,
                "total_columns": 1,
            }

    class FakeClient:
        def __init__(self, **kwargs):
            captured["timeout"] = kwargs.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            captured["url"] = url
            captured["payload"] = json
            return FakeResponse()

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)
    monkeypatch.setenv("BFF_BASE_URL", "http://bff")

    processor = FunnelDataProcessor()
    result = await processor.process_google_sheets_preview(
        sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ4567890abcdef/edit#gid=0",
        infer_types=False,
    )

    timeout = captured["timeout"]
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.connect == 5.0
    assert timeout.read == 30.0
    assert timeout.write == 30.0
    assert timeout.pool == 30.0
    assert captured["raise_for_status_called"] is True
    assert captured["url"].startswith("http://bff/")
    assert result.preview_rows == 1
