"""Funnel Structure Analysis Router.

Provides sheet structure analysis endpoints (Data Island detection, multi-table
separation, orientation detection).

NOTE: Legacy type inference endpoints (/analyze, /suggest-schema,
/preview/google-sheets) have been removed. Palantir Foundry style: all columns
default to xsd:string. Type coercion at objectify/write time via coerce_value().
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, File, UploadFile, status

from funnel.services.structure_analysis import FunnelStructureAnalyzer
from shared.services.core.sheet_grid_parser import SheetGridParseOptions, SheetGridParser
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.sheet_grid import GoogleSheetStructureAnalysisRequest
from shared.models.structure_patch import SheetStructurePatch
from shared.models.structure_analysis import (
    SheetStructureAnalysisRequest,
    SheetStructureAnalysisResponse,
)
from shared.utils.app_logger import get_logger
from funnel.services.structure_patch import apply_structure_patch
from funnel.services.structure_patch_store import delete_patch, get_patch, upsert_patch

logger = get_logger(__name__)

router = APIRouter(prefix="/funnel", tags=["funnel"])


@router.post("/structure/analyze", response_model=SheetStructureAnalysisResponse)
async def analyze_sheet_structure(
    request: SheetStructureAnalysisRequest,
) -> SheetStructureAnalysisResponse:
    """
    Raw sheet grid(엑셀/스프레드시트)의 구조를 분석합니다.

    - 데이터 섬(Data Island) 탐지 + 멀티 테이블 분리
    - 방향성/모드 판별(일반 표 / 전치 표 / Key-Value 폼)
    - (옵션) 병합 셀 해체 + 채우기
    - 표 밖 메타데이터 Key-Value 추출
    """
    import asyncio

    try:
        logger.info("Analyzing sheet structure...")
        analysis = await asyncio.to_thread(
            FunnelStructureAnalyzer.analyze,
            request.grid,
            include_complex_types=request.include_complex_types,
            merged_cells=request.merged_cells,
            cell_style_hints=request.cell_style_hints,
            max_tables=request.max_tables,
            options=request.options,
        )
        if bool((request.options or {}).get("apply_patches", True)):
            sig = (analysis.metadata or {}).get("sheet_signature")
            if isinstance(sig, str) and sig:
                patch = get_patch(sig)
                if patch:
                    analysis = apply_structure_patch(
                        analysis,
                        patch=patch,
                        grid=request.grid,
                        merged_cells=request.merged_cells,
                        cell_style_hints=request.cell_style_hints,
                        include_complex_types=request.include_complex_types,
                        options=request.options,
                    )
        return analysis
    except Exception as e:
        logger.error(f"Sheet structure analysis failed: {str(e)}")
        raise classified_http_exception(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sheet structure analysis failed: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/structure/analyze/excel", response_model=SheetStructureAnalysisResponse)
async def analyze_excel_structure(
    file: UploadFile = File(...),
    sheet_name: str | None = None,
    include_complex_types: bool = True,
    max_tables: int = 5,
    max_rows: int | None = None,
    max_cols: int | None = None,
    options_json: str | None = None,
) -> SheetStructureAnalysisResponse:
    """
    Excel(.xlsx/.xlsm) 파일을 업로드 받아 grid + merged_cells로 파싱한 뒤,
    구조 분석(데이터 섬/방향성/병합셀/키-값)을 수행합니다.
    """
    import asyncio
    import json

    filename = file.filename or ""
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        raise classified_http_exception(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .xlsx/.xlsm files are supported",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    content = await file.read()
    if not content:
        raise classified_http_exception(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty file",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    try:
        opts = json.loads(options_json) if options_json else {}
        if not isinstance(opts, dict):
            raise ValueError("options_json must be an object")
    except Exception as e:
        raise classified_http_exception(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid options_json: {e}",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    try:
        sheet_grid = await asyncio.to_thread(
            SheetGridParser.from_excel_bytes,
            content,
            sheet_name=sheet_name,
            options=SheetGridParseOptions(
                max_rows=max_rows,
                max_cols=max_cols,
                excel_include_style_hints=True,
            ),
        )
    except RuntimeError as e:
        # openpyxl missing or similar optional dependency issue
        raise classified_http_exception(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=str(e),
            code=ErrorCode.FEATURE_NOT_IMPLEMENTED,
        )
    except Exception as e:
        raise classified_http_exception(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse Excel: {e}",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    try:
        analysis = await asyncio.to_thread(
            FunnelStructureAnalyzer.analyze,
            sheet_grid.grid,
            include_complex_types=include_complex_types,
            merged_cells=sheet_grid.merged_cells,
            cell_style_hints=sheet_grid.cell_style_hints,
            max_tables=max_tables,
            options=opts,
        )
    except Exception as e:
        raise classified_http_exception(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sheet structure analysis failed: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )

    if bool(opts.get("apply_patches", True)):
        sig = (analysis.metadata or {}).get("sheet_signature")
        if isinstance(sig, str) and sig:
            patch = get_patch(sig)
            if patch:
                analysis = apply_structure_patch(
                    analysis,
                    patch=patch,
                    grid=sheet_grid.grid,
                    merged_cells=sheet_grid.merged_cells,
                    cell_style_hints=sheet_grid.cell_style_hints,
                    include_complex_types=include_complex_types,
                    options=opts,
                )

    return SheetStructureAnalysisResponse(
        tables=analysis.tables,
        key_values=analysis.key_values,
        metadata={
            **(analysis.metadata or {}),
            "source": "excel",
            "sheet_name": sheet_grid.sheet_name,
            **(sheet_grid.metadata or {}),
        },
        warnings=[*(analysis.warnings or []), *(sheet_grid.warnings or [])],
    )


@router.post("/structure/analyze/google-sheets", response_model=SheetStructureAnalysisResponse)
async def analyze_google_sheets_structure(
    request: GoogleSheetStructureAnalysisRequest,
) -> SheetStructureAnalysisResponse:
    """
    Google Sheets URL → direct connector fetch(values+metadata+merges) → grid/merged_cells → 구조 분석
    """
    import asyncio

    from data_connector.google_sheets.service import GoogleSheetsService

    try:
        access_token = await _resolve_optional_access_token(
            connection_id=request.connection_id,
        )
        google_sheets_service = GoogleSheetsService(api_key=request.api_key)
        sheet_id, metadata, worksheet_title, worksheet_sheet_id, values = await google_sheets_service.fetch_sheet_values(
            str(request.sheet_url),
            worksheet_name=request.worksheet_name,
            api_key=request.api_key,
            access_token=access_token,
        )
        merges = SheetGridParser.merged_cells_from_google_metadata(
            metadata.model_dump(),
            worksheet_name=worksheet_title,
            sheet_id=worksheet_sheet_id,
        )
        sheet_grid = SheetGridParser.from_google_sheets_values(
            values,
            merged_cells=merges,
            sheet_name=worksheet_title,
            options=SheetGridParseOptions(
                trim_trailing_empty=bool(request.trim_trailing_empty),
                max_rows=request.max_rows,
                max_cols=request.max_cols,
            ),
            metadata={
                "sheet_id": sheet_id,
                "sheet_title": metadata.title,
                "worksheet_title": worksheet_title,
                "sheet_url": str(request.sheet_url),
            },
        )
    except ValueError as e:
        raise classified_http_exception(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to fetch Google Sheets grid: {str(e)}",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    except Exception as e:
        raise classified_http_exception(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch Google Sheets grid: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )

    try:
        analysis = await asyncio.to_thread(
            FunnelStructureAnalyzer.analyze,
            sheet_grid.grid,
            include_complex_types=request.include_complex_types,
            merged_cells=sheet_grid.merged_cells,
            cell_style_hints=getattr(sheet_grid, "cell_style_hints", None),
            max_tables=request.max_tables,
            options=request.options,
        )
    except Exception as e:
        raise classified_http_exception(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sheet structure analysis failed: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )

    if bool((request.options or {}).get("apply_patches", True)):
        sig = (analysis.metadata or {}).get("sheet_signature")
        if isinstance(sig, str) and sig:
            patch = get_patch(sig)
            if patch:
                analysis = apply_structure_patch(
                    analysis,
                    patch=patch,
                    grid=sheet_grid.grid,
                    merged_cells=sheet_grid.merged_cells,
                    cell_style_hints=getattr(sheet_grid, "cell_style_hints", None),
                    include_complex_types=request.include_complex_types,
                    options=request.options,
                )

    return SheetStructureAnalysisResponse(
        tables=analysis.tables,
        key_values=analysis.key_values,
        metadata={
            **(analysis.metadata or {}),
            "source": "google_sheets",
            **(sheet_grid.metadata or {}),
        },
        warnings=[*(analysis.warnings or []), *(sheet_grid.warnings or [])],
    )


@router.post("/structure/patch", response_model=SheetStructurePatch)
async def upsert_structure_patch(patch: SheetStructurePatch) -> SheetStructurePatch:
    """Store/update a structure-analysis patch for a given sheet_signature."""
    if not patch.ops:
        raise classified_http_exception(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Patch ops must not be empty",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    return upsert_patch(patch)


@router.get("/structure/patch/{sheet_signature}", response_model=SheetStructurePatch)
async def get_structure_patch(sheet_signature: str) -> SheetStructurePatch:
    patch = get_patch(sheet_signature)
    if patch is None:
        raise classified_http_exception(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Patch not found",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )
    return patch


@router.delete("/structure/patch/{sheet_signature}")
async def delete_structure_patch(sheet_signature: str) -> Dict[str, Any]:
    deleted = delete_patch(sheet_signature)
    return {"deleted": bool(deleted), "sheet_signature": sheet_signature}


# NOTE: Legacy endpoints removed:
# - POST /analyze (type inference)
# - POST /suggest-schema (schema suggestion)
# - POST /preview/google-sheets (preview with type inference)
# Palantir Foundry style: all columns default to xsd:string.


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """Funnel 서비스 상태 확인"""
    return {"status": "healthy", "service": "funnel", "version": "0.1.0"}


async def _resolve_optional_access_token(
    *,
    connection_id: Optional[str],
) -> Optional[str]:
    """Resolve OAuth access token for a Google Sheets connection (if configured)."""
    resolved_connection_id = str(connection_id or "").strip()
    if not resolved_connection_id:
        return None

    from shared.services.registries.connector_registry import ConnectorRegistry
    from data_connector.google_sheets.oauth import GoogleOAuth2Client

    connector_registry = ConnectorRegistry()
    await connector_registry.initialize()
    try:
        source = await connector_registry.get_source(
            source_type="google_sheets_connection",
            source_id=resolved_connection_id,
        )
        if not source or not source.enabled:
            raise ValueError("Connection not found")

        config = dict(source.config_json or {})
        token = str(config.get("access_token") or "").strip() or None
        expires_at = config.get("expires_at")
        refresh_token = config.get("refresh_token")
        oauth_client = GoogleOAuth2Client()

        if token and expires_at:
            try:
                expires_at_float = float(expires_at)
            except (TypeError, ValueError):
                expires_at_float = None
            if expires_at_float and oauth_client.is_token_expired(expires_at_float):
                token = None

        if not token and refresh_token:
            refreshed = await oauth_client.refresh_access_token(str(refresh_token))
            config.update(
                {
                    "access_token": refreshed.get("access_token"),
                    "expires_at": refreshed.get("expires_at"),
                    "refresh_token": refreshed.get("refresh_token", refresh_token),
                }
            )
            await connector_registry.upsert_source(
                source_type=source.source_type,
                source_id=source.source_id,
                enabled=True,
                config_json=config,
            )
            token = str(config.get("access_token") or "").strip() or None

        return token
    finally:
        await connector_registry.close()
