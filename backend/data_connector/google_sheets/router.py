"""
Google Sheets Connector - FastAPI Router
"""

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
import logging

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from .models import (
    GoogleSheetPreviewRequest,
    GoogleSheetPreviewResponse,
    GoogleSheetRegisterRequest,
    GoogleSheetRegisterResponse,
    GoogleSheetError,
    RegisteredSheet
)
from .service import GoogleSheetsService
from funnel.services.type_inference import FunnelTypeInferenceService
from funnel.models import ColumnAnalysisResult


logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/data-connectors/google-sheets",
    tags=["Google Sheets Data Connector"],
    responses={
        403: {"model": GoogleSheetError},
        404: {"model": GoogleSheetError},
        500: {"model": GoogleSheetError}
    }
)

# Service instance (in production, use dependency injection)
sheets_service = GoogleSheetsService()


async def get_sheets_service() -> GoogleSheetsService:
    """의존성 주입을 위한 서비스 getter"""
    return sheets_service


@router.post(
    "/preview",
    response_model=GoogleSheetPreviewResponse,
    summary="Google Sheet 미리보기",
    description="Google Sheet URL을 통해 데이터 미리보기를 가져옵니다."
)
async def preview_sheet(
    request: GoogleSheetPreviewRequest,
    service: GoogleSheetsService = Depends(get_sheets_service)
) -> GoogleSheetPreviewResponse:
    """
    Google Sheet 데이터 미리보기
    
    - Sheet는 'Anyone with the link can view' 권한이 필요합니다
    - 최대 5개의 샘플 행을 반환합니다
    - 컬럼 이름은 첫 번째 행에서 추출됩니다
    """
    try:
        logger.info(f"Previewing sheet: {request.sheet_url}")
        result = await service.preview_sheet(str(request.sheet_url))
        return result
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=GoogleSheetError(
                error_code="INVALID_URL",
                message="Invalid Google Sheets URL format",
                detail=str(e)
            ).dict()
        )
    
    except Exception as e:
        logger.error(f"Failed to preview sheet: {e}")
        
        # Check if it's an access error
        if "403" in str(e) or "permission" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=GoogleSheetError(
                    error_code="SHEET_NOT_ACCESSIBLE",
                    message="Cannot access the Google Sheet. Please ensure it's shared publicly.",
                    detail=str(e)
                ).dict()
            )
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=GoogleSheetError(
                error_code="PREVIEW_FAILED",
                message="Failed to preview Google Sheet",
                detail=str(e)
            ).dict()
        )


@router.post(
    "/register",
    response_model=GoogleSheetRegisterResponse,
    summary="Google Sheet 등록",
    description="주기적인 데이터 동기화를 위해 Google Sheet를 등록합니다."
)
async def register_sheet(
    request: GoogleSheetRegisterRequest,
    service: GoogleSheetsService = Depends(get_sheets_service)
) -> GoogleSheetRegisterResponse:
    """
    Google Sheet 등록 (폴링용)
    
    - 등록된 시트는 지정된 간격으로 자동 폴링됩니다
    - 데이터 변경 시 알림을 받을 수 있습니다
    - 폴링 간격은 60초에서 3600초(1시간) 사이여야 합니다
    """
    try:
        logger.info(f"Registering sheet: {request.sheet_url}")
        result = await service.register_sheet(
            str(request.sheet_url),
            request.worksheet_name,
            request.polling_interval
        )
        return result
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=GoogleSheetError(
                error_code="INVALID_REQUEST",
                message="Invalid registration request",
                detail=str(e)
            ).dict()
        )
    
    except Exception as e:
        logger.error(f"Failed to register sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=GoogleSheetError(
                error_code="REGISTRATION_FAILED",
                message="Failed to register Google Sheet",
                detail=str(e)
            ).dict()
        )


@router.delete(
    "/register/{sheet_id}",
    summary="Google Sheet 등록 해제",
    description="등록된 Google Sheet의 폴링을 중지합니다."
)
async def unregister_sheet(
    sheet_id: str,
    service: GoogleSheetsService = Depends(get_sheets_service)
) -> dict:
    """
    Google Sheet 등록 해제
    
    - 폴링이 즉시 중지됩니다
    - 이미 수집된 데이터는 영향받지 않습니다
    """
    try:
        success = await service.unregister_sheet(sheet_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=GoogleSheetError(
                    error_code="SHEET_NOT_FOUND",
                    message=f"Registered sheet not found: {sheet_id}",
                    detail=None
                ).dict()
            )
        
        return {
            "status": "unregistered",
            "sheet_id": sheet_id,
            "message": "Sheet successfully unregistered"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unregister sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=GoogleSheetError(
                error_code="UNREGISTRATION_FAILED",
                message="Failed to unregister Google Sheet",
                detail=str(e)
            ).dict()
        )


@router.get(
    "/registered",
    response_model=List[RegisteredSheet],
    summary="등록된 시트 목록",
    description="현재 등록되어 폴링 중인 Google Sheet 목록을 반환합니다."
)
async def list_registered_sheets(
    service: GoogleSheetsService = Depends(get_sheets_service)
) -> List[RegisteredSheet]:
    """
    등록된 Google Sheet 목록 조회
    
    - 각 시트의 폴링 상태를 확인할 수 있습니다
    - 마지막 폴링 시간과 데이터 해시값이 포함됩니다
    """
    try:
        sheets = await service.get_registered_sheets()
        return sheets
        
    except Exception as e:
        logger.error(f"Failed to list registered sheets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=GoogleSheetError(
                error_code="LIST_FAILED",
                message="Failed to list registered sheets",
                detail=str(e)
            ).dict()
        )


@router.post(
    "/analyze-types",
    response_model=Dict[str, ColumnAnalysisResult],
    summary="Google Sheet 타입 분석",
    description="Google Sheet의 각 컬럼 데이터 타입을 자동으로 추론합니다."
)
async def analyze_sheet_types(
    request: GoogleSheetPreviewRequest,
    include_complex_types: bool = False,
    service: GoogleSheetsService = Depends(get_sheets_service)
) -> Dict[str, ColumnAnalysisResult]:
    """
    Google Sheet 데이터 타입 분석
    
    - 각 컬럼의 데이터를 분석하여 적절한 타입을 추론합니다
    - 신뢰도 점수와 추론 이유를 포함합니다
    - 기본 타입: STRING, INTEGER, DECIMAL, BOOLEAN, DATE
    - 복합 타입 (선택사항): EMAIL, PHONE, URL, MONEY 등
    
    Returns:
        컬럼 이름을 키로 하는 분석 결과 딕셔너리
    """
    try:
        logger.info(f"Analyzing types for sheet: {request.sheet_url}")
        
        # 1. 먼저 데이터 미리보기를 가져옴
        preview_result = await service.preview_sheet(str(request.sheet_url))
        
        # 2. Funnel 서비스를 사용하여 타입 추론
        type_inference = FunnelTypeInferenceService()
        analysis_results = type_inference.analyze_dataset(
            data=preview_result.sample_rows,
            columns=preview_result.columns,
            include_complex_types=include_complex_types
        )
        
        # 3. 결과를 딕셔너리로 변환
        result_dict = {}
        for analysis in analysis_results:
            result_dict[analysis.column_name] = analysis
        
        logger.info(f"Type analysis completed for {len(preview_result.columns)} columns")
        return result_dict
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=GoogleSheetError(
                error_code="INVALID_URL",
                message="Invalid Google Sheets URL format",
                detail=str(e)
            ).dict()
        )
    
    except Exception as e:
        logger.error(f"Failed to analyze sheet types: {e}")
        
        if "403" in str(e) or "permission" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=GoogleSheetError(
                    error_code="SHEET_NOT_ACCESSIBLE",
                    message="Cannot access the Google Sheet. Please ensure it's shared publicly.",
                    detail=str(e)
                ).dict()
            )
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=GoogleSheetError(
                error_code="TYPE_ANALYSIS_FAILED",
                message="Failed to analyze Google Sheet types",
                detail=str(e)
            ).dict()
        )


@router.get(
    "/health",
    summary="헬스 체크",
    description="Google Sheets 커넥터 상태를 확인합니다."
)
async def health_check() -> dict:
    """
    커넥터 헬스 체크
    
    - API 키 설정 여부
    - 서비스 상태
    """
    try:
        has_api_key = bool(sheets_service.api_key)
        
        return {
            "status": "healthy",
            "service": "google_sheets_connector",
            "api_key_configured": has_api_key,
            "message": "Google Sheets connector is operational"
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "service": "google_sheets_connector",
                "error": str(e)
            }
        )


# Cleanup on shutdown
@router.on_event("shutdown")
async def shutdown_event():
    """서비스 종료 시 정리"""
    logger.info("Shutting down Google Sheets connector")
    await sheets_service.close()