from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from funnel.routers import type_inference_router as router
from shared.models.sheet_grid import GoogleSheetStructureAnalysisRequest, SheetGrid
from shared.models.structure_analysis import SheetStructureAnalysisRequest
from shared.models.structure_patch import SheetStructurePatch, SheetStructurePatchOp
from shared.models.type_inference import (
    ColumnAnalysisResult,
    DatasetAnalysisRequest,
    DatasetAnalysisResponse,
    TabularPreviewResponse,
    TypeInferenceResult,
)


class _FakeProcessor:
    async def analyze_dataset(self, request: DatasetAnalysisRequest) -> DatasetAnalysisResponse:
        result = ColumnAnalysisResult(
            column_name=request.columns[0],
            inferred_type=TypeInferenceResult(type="xsd:string", confidence=0.9, reason="ok"),
            total_count=len(request.data),
            non_empty_count=len(request.data),
            sample_values=request.data[:1],
            null_count=0,
            unique_count=len(request.data),
            null_ratio=0.0,
            unique_ratio=1.0,
        )
        return DatasetAnalysisResponse(columns=[result], analysis_metadata={"total_columns": len(request.columns)})

    async def process_google_sheets_preview(self, **kwargs) -> TabularPreviewResponse:  # noqa: ANN003
        return TabularPreviewResponse(
            source_metadata={"type": "google_sheets"},
            columns=["id"],
            sample_data=[[1]],
            inferred_schema=None,
            total_rows=1,
            preview_rows=1,
        )

    async def resolve_optional_access_token(self, *, connection_id):  # noqa: ANN001
        _ = connection_id
        return None

    def generate_schema_suggestion(self, analysis_results, class_name=None):  # noqa: ANN001, ANN002
        return {"id": class_name or "Default", "properties": [], "relationships": []}


class _FailingProcessor:
    async def analyze_dataset(self, request: DatasetAnalysisRequest):  # noqa: ANN001
        raise RuntimeError("boom")

    async def process_google_sheets_preview(self, **kwargs):  # noqa: ANN003
        raise RuntimeError("boom")

    async def resolve_optional_access_token(self, *, connection_id):  # noqa: ANN001
        raise RuntimeError("boom")

    def generate_schema_suggestion(self, analysis_results, class_name=None):  # noqa: ANN001, ANN002
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_analyze_dataset_success_and_error() -> None:
    request = DatasetAnalysisRequest(data=[["a"]], columns=["col1"])
    response = await router.analyze_dataset(request, processor=_FakeProcessor())
    assert response.analysis_metadata["total_columns"] == 1

    with pytest.raises(HTTPException):
        await router.analyze_dataset(request, processor=_FailingProcessor())


@pytest.mark.asyncio
async def test_analyze_sheet_structure_applies_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    analysis = SimpleNamespace(
        tables=[],
        key_values=[],
        metadata={"sheet_signature": "sig-1"},
        warnings=[],
    )

    monkeypatch.setattr(router.FunnelStructureAnalyzer, "analyze", lambda *args, **kwargs: analysis)
    monkeypatch.setattr(router, "get_patch", lambda sig: {"ops": [{"op": "noop"}]})  # noqa: ARG005

    def _apply_patch(*args, **kwargs):  # noqa: ANN002, ANN003
        return SimpleNamespace(
            tables=[],
            key_values=[],
            metadata={"sheet_signature": "sig-1", "patched": True},
            warnings=["patched"],
        )

    monkeypatch.setattr(router, "apply_structure_patch", _apply_patch)

    request = SheetStructureAnalysisRequest(grid=[["id"], ["1"]], options={"apply_patches": True})
    result = await router.analyze_sheet_structure(request)
    assert result.metadata["patched"] is True


@pytest.mark.asyncio
async def test_analyze_excel_structure_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    sheet_grid = SheetGrid(
        source="excel",
        sheet_name="Sheet1",
        grid=[["id"], ["1"]],
        merged_cells=[],
        cell_style_hints=None,
        metadata={},
        warnings=[],
    )
    analysis = SimpleNamespace(tables=[], key_values=[], metadata={}, warnings=[])

    monkeypatch.setattr(router.SheetGridParser, "from_excel_bytes", lambda *args, **kwargs: sheet_grid)
    monkeypatch.setattr(router.FunnelStructureAnalyzer, "analyze", lambda *args, **kwargs: analysis)
    monkeypatch.setattr(router, "get_patch", lambda sig: None)  # noqa: ARG005

    upload = UploadFile(file=io.BytesIO(b"fake"), filename="sample.xlsx")

    response = await router.analyze_excel_structure(file=upload, options_json="{}")
    assert response.metadata["source"] == "excel"


@pytest.mark.asyncio
async def test_analyze_excel_structure_errors() -> None:
    bad_upload = UploadFile(file=io.BytesIO(b"fake"), filename="sample.csv")
    with pytest.raises(HTTPException):
        await router.analyze_excel_structure(file=bad_upload)

    good_upload = UploadFile(file=io.BytesIO(b"fake"), filename="sample.xlsx")
    with pytest.raises(HTTPException):
        await router.analyze_excel_structure(file=good_upload, options_json="not-json")


@pytest.mark.asyncio
async def test_analyze_google_sheets_structure(monkeypatch: pytest.MonkeyPatch) -> None:
    sheet_grid = SheetGrid(grid=[["id"], ["1"]], merged_cells=[], metadata={}, warnings=[])
    analysis = SimpleNamespace(tables=[], key_values=[], metadata={}, warnings=[])

    class _FakeMetadata:
        title = "Demo"

        def model_dump(self):  # noqa: D401
            return {}

    class _FakeGoogleSheetsService:
        def __init__(self, api_key=None):  # noqa: ANN001
            self.api_key = api_key

        async def fetch_sheet_values(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return "sheet-1", _FakeMetadata(), "Sheet1", 0, [["id"], ["1"]]

    monkeypatch.setattr(router, "GoogleSheetsService", _FakeGoogleSheetsService)
    monkeypatch.setattr(router.SheetGridParser, "from_google_sheets_values", lambda *args, **kwargs: sheet_grid)
    monkeypatch.setattr(router.SheetGridParser, "merged_cells_from_google_metadata", lambda *args, **kwargs: [])
    monkeypatch.setattr(router, "get_patch", lambda sig: None)  # noqa: ARG005
    monkeypatch.setattr(router.FunnelStructureAnalyzer, "analyze", lambda *args, **kwargs: analysis)

    request = GoogleSheetStructureAnalysisRequest(
        sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ456def789GHI012jklMNOP3456789/edit",
        worksheet_name=None,
        api_key=None,
        connection_id=None,
        max_rows=10,
        max_cols=10,
        trim_trailing_empty=True,
        include_complex_types=True,
        max_tables=5,
        options={},
    )
    response = await router.analyze_google_sheets_structure(request, processor=_FakeProcessor())
    assert response.metadata["source"] == "google_sheets"


@pytest.mark.asyncio
async def test_structure_patch_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    patch = SheetStructurePatch(
        sheet_signature="sig-1",
        ops=[SheetStructurePatchOp(op="remove_table", table_index=0)],
    )

    monkeypatch.setattr(router, "upsert_patch", lambda p: p)
    monkeypatch.setattr(router, "get_patch", lambda sig: patch)  # noqa: ARG005
    monkeypatch.setattr(router, "delete_patch", lambda sig: True)  # noqa: ARG005

    assert await router.upsert_structure_patch(patch) == patch
    assert await router.get_structure_patch("sig-1") == patch
    deleted = await router.delete_structure_patch("sig-1")
    assert deleted["deleted"] is True

    with pytest.raises(HTTPException):
        await router.upsert_structure_patch(SheetStructurePatch(sheet_signature="sig-2", ops=[]))


@pytest.mark.asyncio
async def test_preview_and_suggest_schema() -> None:
    processor = _FakeProcessor()
    preview = await router.preview_google_sheets_with_inference(
        sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ456def789GHI012jklMNOP3456789/edit",
        processor=processor,
    )
    assert preview.preview_rows == 1

    analysis = DatasetAnalysisResponse(
        columns=[
            ColumnAnalysisResult(
                column_name="id",
                inferred_type=TypeInferenceResult(type="xsd:string", confidence=0.9, reason="ok"),
            )
        ]
    )
    schema = await router.suggest_schema(analysis, class_name="Demo", processor=processor)
    assert schema["id"] == "Demo"

    with pytest.raises(HTTPException):
        await router.preview_google_sheets_with_inference(
            sheet_url="https://docs.google.com/spreadsheets/d/1abc123XYZ456def789GHI012jklMNOP3456789/edit",
            processor=_FailingProcessor(),
        )

    with pytest.raises(HTTPException):
        await router.suggest_schema(analysis, class_name="Demo", processor=_FailingProcessor())


@pytest.mark.asyncio
async def test_router_health_check() -> None:
    response = await router.health_check()
    assert response["status"] == "healthy"
