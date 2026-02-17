from types import SimpleNamespace

import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_google_sheets_preview_uses_direct_google_sheets_service(monkeypatch):
    from funnel.services.data_processor import FunnelDataProcessor
    import funnel.services.data_processor as module

    captured = {"preview_calls": []}

    class FakeGoogleSheetsService:
        def __init__(self, api_key=None):  # noqa: ANN001
            captured["init_api_key"] = api_key

        async def preview_sheet(self, sheet_url, **kwargs):  # noqa: ANN001
            captured["preview_calls"].append({"sheet_url": sheet_url, **kwargs})
            return SimpleNamespace(
                sheet_id="sheet-1",
                sheet_title="Demo",
                worksheet_title="Sheet1",
                columns=["id"],
                sample_rows=[["1"]],
                total_rows=1,
                total_columns=1,
            )

    async def fake_resolve_access_token(*, connection_id):  # noqa: ANN001
        captured["connection_id"] = connection_id
        return "token-1"

    monkeypatch.setattr(module, "GoogleSheetsService", FakeGoogleSheetsService)

    processor = FunnelDataProcessor()
    monkeypatch.setattr(processor, "resolve_optional_access_token", fake_resolve_access_token)
    result = await processor.process_google_sheets_preview(
        sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ4567890abcdef/edit#gid=0",
        worksheet_name="Sheet1",
        api_key="api-key",
        connection_id="conn-1",
        infer_types=False,
    )

    assert captured["init_api_key"] == "api-key"
    assert captured["connection_id"] == "conn-1"
    assert len(captured["preview_calls"]) == 1
    assert captured["preview_calls"][0]["worksheet_name"] == "Sheet1"
    assert captured["preview_calls"][0]["access_token"] == "token-1"
    assert result.preview_rows == 1
