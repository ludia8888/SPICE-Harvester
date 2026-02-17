from __future__ import annotations

import pytest
from types import SimpleNamespace

from funnel.services.data_processor import FunnelDataProcessor
from shared.models.type_inference import ColumnAnalysisResult, DatasetAnalysisRequest, TypeInferenceResult


@pytest.mark.asyncio
async def test_data_processor_analyze_dataset_metadata() -> None:
    processor = FunnelDataProcessor()
    request = DatasetAnalysisRequest(
        data=[[1, "Alice"], [2, "Bob"]],
        columns=["id", "name"],
        sample_size=10,
        include_complex_types=False,
    )

    response = await processor.analyze_dataset(request)

    assert response.analysis_metadata["total_columns"] == 2
    assert response.analysis_metadata["analyzed_rows"] == 2


def test_generate_schema_suggestion_handles_confidence() -> None:
    processor = FunnelDataProcessor()
    high_conf = ColumnAnalysisResult(
        column_name="amount",
        inferred_type=TypeInferenceResult(type="xsd:decimal", confidence=0.9, reason="ok"),
        total_count=2,
        non_empty_count=2,
        sample_values=["1.2", "3.4"],
        null_count=0,
        unique_count=2,
        null_ratio=0.0,
        unique_ratio=1.0,
    )
    low_conf = ColumnAnalysisResult(
        column_name="notes",
        inferred_type=TypeInferenceResult(type="xsd:integer", confidence=0.1, reason="low"),
        total_count=1,
        non_empty_count=1,
        sample_values=["x"],
        null_count=0,
        unique_count=1,
        null_ratio=0.0,
        unique_ratio=1.0,
    )

    schema = processor.generate_schema_suggestion([high_conf, low_conf], class_name="Order")

    assert schema["id"] == "Order"
    props = {prop["name"]: prop["type"] for prop in schema["properties"]}
    assert props["amount"] == "xsd:decimal"
    assert props["notes"] == "xsd:string"


@pytest.mark.asyncio
async def test_process_google_sheets_preview_success(monkeypatch: pytest.MonkeyPatch) -> None:
    processor = FunnelDataProcessor()

    class _FakeGoogleSheetsService:
        def __init__(self, api_key=None):  # noqa: ANN001
            self.api_key = api_key

        async def preview_sheet(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return SimpleNamespace(
                sheet_id="sheet-1",
                sheet_title="Demo",
                worksheet_title="Sheet1",
                columns=["id", "name"],
                sample_rows=[["1", "Alice"]],
                total_rows=1,
                total_columns=2,
            )

    async def _fake_resolve_optional_access_token(*, connection_id):  # noqa: ANN001
        assert connection_id is None
        return None

    monkeypatch.setattr("funnel.services.data_processor.GoogleSheetsService", _FakeGoogleSheetsService)
    monkeypatch.setattr(processor, "resolve_optional_access_token", _fake_resolve_optional_access_token)

    response = await processor.process_google_sheets_preview(
        sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ456def789GHI012jklMNOP3456789/edit",
        infer_types=False,
    )

    assert response.source_metadata["sheet_id"] == "sheet-1"
    assert response.preview_rows == 1


@pytest.mark.asyncio
async def test_process_google_sheets_preview_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    processor = FunnelDataProcessor()

    class _FailingGoogleSheetsService:
        def __init__(self, api_key=None):  # noqa: ANN001
            self.api_key = api_key

        async def preview_sheet(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("preview failure")

    async def _fake_resolve_optional_access_token(*, connection_id):  # noqa: ANN001
        assert connection_id is None
        return None

    monkeypatch.setattr("funnel.services.data_processor.GoogleSheetsService", _FailingGoogleSheetsService)
    monkeypatch.setattr(processor, "resolve_optional_access_token", _fake_resolve_optional_access_token)

    with pytest.raises(Exception):
        await processor.process_google_sheets_preview(
            sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ456def789GHI012jklMNOP3456789/edit",
            infer_types=False,
        )
