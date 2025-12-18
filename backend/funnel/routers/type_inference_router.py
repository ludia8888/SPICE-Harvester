"""
🔥 THINK ULTRA! Funnel Type Inference Router
타입 추론 및 스키마 제안 API 엔드포인트
"""

from typing import Any, Dict

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from funnel.services.data_processor import FunnelDataProcessor
from funnel.services.structure_analysis import FunnelStructureAnalyzer
from shared.services.sheet_grid_parser import SheetGridParseOptions, SheetGridParser
from shared.models.sheet_grid import GoogleSheetStructureAnalysisRequest, SheetGrid
from shared.models.structure_analysis import (
    SheetStructureAnalysisRequest,
    SheetStructureAnalysisResponse,
)
from shared.models.type_inference import (
    DatasetAnalysisRequest,
    DatasetAnalysisResponse,
    FunnelPreviewResponse,
)
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/funnel", tags=["funnel"])


def get_data_processor() -> FunnelDataProcessor:
    """데이터 프로세서 의존성"""
    return FunnelDataProcessor()


@router.post("/analyze", response_model=DatasetAnalysisResponse)
async def analyze_dataset(
    request: DatasetAnalysisRequest, processor: FunnelDataProcessor = Depends(get_data_processor)
) -> DatasetAnalysisResponse:
    """
    데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.

    - 기본 타입 감지: STRING, INTEGER, DECIMAL, BOOLEAN, DATE
    - 복합 타입 감지 (선택사항): EMAIL, PHONE, URL, MONEY 등
    - 신뢰도 점수와 상세한 이유 제공
    """
    try:
        logger.info(f"Analyzing dataset with {len(request.columns)} columns")

        result = await processor.analyze_dataset(request)

        logger.info(
            f"Analysis completed. Detected types: {[col.inferred_type.type for col in result.columns]}"
        )

        return result

    except Exception as e:
        logger.error(f"Dataset analysis failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dataset analysis failed: {str(e)}",
        )


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
        return await asyncio.to_thread(
            FunnelStructureAnalyzer.analyze,
            request.grid,
            include_complex_types=request.include_complex_types,
            merged_cells=request.merged_cells,
            max_tables=request.max_tables,
            options=request.options,
        )
    except Exception as e:
        logger.error(f"Sheet structure analysis failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sheet structure analysis failed: {str(e)}",
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .xlsx/.xlsm files are supported",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

    try:
        opts = json.loads(options_json) if options_json else {}
        if not isinstance(opts, dict):
            raise ValueError("options_json must be an object")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid options_json: {e}")

    try:
        sheet_grid = await asyncio.to_thread(
            SheetGridParser.from_excel_bytes,
            content,
            sheet_name=sheet_name,
            options=SheetGridParseOptions(max_rows=max_rows, max_cols=max_cols),
        )
    except RuntimeError as e:
        # openpyxl missing or similar optional dependency issue
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Failed to parse Excel: {e}")

    try:
        analysis = await asyncio.to_thread(
            FunnelStructureAnalyzer.analyze,
            sheet_grid.grid,
            include_complex_types=include_complex_types,
            merged_cells=sheet_grid.merged_cells,
            max_tables=max_tables,
            options=opts,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sheet structure analysis failed: {str(e)}",
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
    Google Sheets URL → (BFF에서 values+metadata(merges) 가져오기) → grid/merged_cells → 구조 분석
    """
    import asyncio

    import httpx

    from shared.config.service_config import ServiceConfig

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{ServiceConfig.get_bff_url()}/api/v1/data-connectors/google-sheets/grid",
                json={
                    "sheet_url": str(request.sheet_url),
                    "worksheet_name": request.worksheet_name,
                    "api_key": request.api_key,
                    "max_rows": request.max_rows,
                    "max_cols": request.max_cols,
                    "trim_trailing_empty": request.trim_trailing_empty,
                },
            )
            resp.raise_for_status()
            sheet_grid = SheetGrid(**resp.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Failed to fetch Google Sheets grid: {e.response.text}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch Google Sheets grid: {str(e)}",
        )

    try:
        analysis = await asyncio.to_thread(
            FunnelStructureAnalyzer.analyze,
            sheet_grid.grid,
            include_complex_types=request.include_complex_types,
            merged_cells=sheet_grid.merged_cells,
            max_tables=request.max_tables,
            options=request.options,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sheet structure analysis failed: {str(e)}",
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


@router.post("/preview/google-sheets", response_model=FunnelPreviewResponse)
async def preview_google_sheets_with_inference(
    sheet_url: str,
    worksheet_name: str = None,
    api_key: str = None,
    infer_types: bool = True,
    include_complex_types: bool = False,
    processor: FunnelDataProcessor = Depends(get_data_processor),
) -> FunnelPreviewResponse:
    """
    Google Sheets 데이터를 미리보기하고 타입을 추론합니다.

    - Google Sheets connector를 통해 데이터 수집
    - 각 컬럼의 타입 자동 추론
    - 스키마 제안을 위한 메타데이터 포함
    """
    try:
        logger.info(f"Processing Google Sheets preview with type inference: {sheet_url}")

        result = await processor.process_google_sheets_preview(
            sheet_url=sheet_url,
            worksheet_name=worksheet_name,
            api_key=api_key,
            infer_types=infer_types,
            include_complex_types=include_complex_types,
        )

        if result.inferred_schema:
            logger.info(f"Type inference completed for {len(result.columns)} columns")

        return result

    except Exception as e:
        logger.error(f"Google Sheets preview failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets preview failed: {str(e)}",
        )


@router.post("/suggest-schema")
async def suggest_schema(
    analysis_results: DatasetAnalysisResponse,
    class_name: str = None,
    processor: FunnelDataProcessor = Depends(get_data_processor),
) -> Dict[str, Any]:
    """
    분석 결과를 기반으로 OMS 스키마를 제안합니다.

    - 타입 추론 결과를 OMS 온톨로지 형식으로 변환
    - 높은 신뢰도의 타입만 사용 (0.7 이상)
    - 필수 필드, 제약조건 등 자동 감지
    """
    try:
        logger.info(f"Generating schema suggestion for {len(analysis_results.columns)} columns")

        schema = processor.generate_schema_suggestion(analysis_results.columns, class_name)

        logger.info(f"Schema suggestion generated: {schema['id']}")

        return schema

    except Exception as e:
        logger.error(f"Schema suggestion failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema suggestion failed: {str(e)}",
        )


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """Funnel 서비스 상태 확인"""
    return {"status": "healthy", "service": "funnel", "version": "0.1.0"}
