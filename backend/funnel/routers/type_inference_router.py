"""
🔥 THINK ULTRA! Funnel Type Inference Router
타입 추론 및 스키마 제안 API 엔드포인트
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from funnel.models import (
    DatasetAnalysisRequest,
    DatasetAnalysisResponse,
    FunnelPreviewRequest,
    FunnelPreviewResponse
)
from funnel.services.data_processor import FunnelDataProcessor
from shared.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/funnel", tags=["funnel"])


def get_data_processor() -> FunnelDataProcessor:
    """데이터 프로세서 의존성"""
    return FunnelDataProcessor()


@router.post("/analyze", response_model=DatasetAnalysisResponse)
async def analyze_dataset(
    request: DatasetAnalysisRequest,
    processor: FunnelDataProcessor = Depends(get_data_processor)
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
        
        logger.info(f"Analysis completed. Detected types: {[col.inferred_type.type for col in result.columns]}")
        
        return result
        
    except Exception as e:
        logger.error(f"Dataset analysis failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dataset analysis failed: {str(e)}"
        )


@router.post("/preview/google-sheets", response_model=FunnelPreviewResponse)
async def preview_google_sheets_with_inference(
    sheet_url: str,
    worksheet_name: str = None,
    api_key: str = None,
    infer_types: bool = True,
    include_complex_types: bool = False,
    processor: FunnelDataProcessor = Depends(get_data_processor)
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
            include_complex_types=include_complex_types
        )
        
        if result.inferred_schema:
            logger.info(f"Type inference completed for {len(result.columns)} columns")
        
        return result
        
    except Exception as e:
        logger.error(f"Google Sheets preview failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets preview failed: {str(e)}"
        )


@router.post("/suggest-schema")
async def suggest_schema(
    analysis_results: DatasetAnalysisResponse,
    class_name: str = None,
    processor: FunnelDataProcessor = Depends(get_data_processor)
) -> Dict[str, Any]:
    """
    분석 결과를 기반으로 OMS 스키마를 제안합니다.
    
    - 타입 추론 결과를 OMS 온톨로지 형식으로 변환
    - 높은 신뢰도의 타입만 사용 (0.7 이상)
    - 필수 필드, 제약조건 등 자동 감지
    """
    try:
        logger.info(f"Generating schema suggestion for {len(analysis_results.columns)} columns")
        
        schema = processor.generate_schema_suggestion(
            analysis_results.columns,
            class_name
        )
        
        logger.info(f"Schema suggestion generated: {schema['id']}")
        
        return schema
        
    except Exception as e:
        logger.error(f"Schema suggestion failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema suggestion failed: {str(e)}"
        )


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """Funnel 서비스 상태 확인"""
    return {
        "status": "healthy",
        "service": "funnel",
        "version": "0.1.0"
    }