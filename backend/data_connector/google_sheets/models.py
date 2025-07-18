"""
Google Sheets Connector - Request/Response Models
"""

from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl, validator
import re


class GoogleSheetPreviewRequest(BaseModel):
    """Google Sheet 미리보기 요청 모델"""
    sheet_url: HttpUrl = Field(
        ...,
        description="Google Sheets URL",
        example="https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"
    )
    
    @validator('sheet_url')
    def validate_google_sheet_url(cls, v):
        """Google Sheets URL 형식 검증"""
        url_str = str(v)
        if not re.match(r'https://docs\.google\.com/spreadsheets/d/[a-zA-Z0-9-_]+', url_str):
            raise ValueError("Invalid Google Sheets URL format")
        return v


class GoogleSheetPreviewResponse(BaseModel):
    """Google Sheet 미리보기 응답 모델"""
    sheet_id: str = Field(..., description="Google Sheets ID")
    sheet_title: str = Field(..., description="스프레드시트 제목")
    worksheet_title: str = Field(default="Sheet1", description="워크시트 이름")
    columns: List[str] = Field(..., description="컬럼 이름 목록")
    sample_rows: List[List[str]] = Field(
        ..., 
        description="샘플 데이터 행 (최대 5개)"
    )
    total_rows: int = Field(..., description="전체 행 수")
    total_columns: int = Field(..., description="전체 열 수")


class GoogleSheetRegisterRequest(BaseModel):
    """Google Sheet 등록 요청 모델"""
    sheet_url: HttpUrl = Field(
        ...,
        description="Google Sheets URL",
        example="https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"
    )
    worksheet_name: Optional[str] = Field(
        default="Sheet1",
        description="워크시트 이름 (기본값: Sheet1)"
    )
    polling_interval: Optional[int] = Field(
        default=300,
        ge=60,
        le=3600,
        description="폴링 간격 (초 단위, 60-3600)"
    )
    
    @validator('sheet_url')
    def validate_google_sheet_url(cls, v):
        """Google Sheets URL 형식 검증"""
        url_str = str(v)
        if not re.match(r'https://docs\.google\.com/spreadsheets/d/[a-zA-Z0-9-_]+', url_str):
            raise ValueError("Invalid Google Sheets URL format")
        return v


class GoogleSheetRegisterResponse(BaseModel):
    """Google Sheet 등록 응답 모델"""
    status: str = Field(default="registered", description="등록 상태")
    sheet_id: str = Field(..., description="Google Sheets ID")
    worksheet_name: str = Field(..., description="워크시트 이름")
    polling_interval: int = Field(..., description="폴링 간격 (초)")
    registered_at: str = Field(..., description="등록 시간 (ISO 8601)")


class GoogleSheetDataUpdate(BaseModel):
    """Google Sheet 데이터 업데이트 알림 모델"""
    sheet_id: str = Field(..., description="Google Sheets ID")
    worksheet_name: str = Field(..., description="워크시트 이름")
    changed_rows: int = Field(..., description="변경된 행 수")
    changed_columns: Optional[List[str]] = Field(
        None, 
        description="변경된 컬럼 목록"
    )
    timestamp: str = Field(..., description="변경 감지 시간 (ISO 8601)")


class GoogleSheetError(BaseModel):
    """Google Sheet 오류 응답 모델"""
    error_code: str = Field(..., description="오류 코드")
    message: str = Field(..., description="오류 메시지")
    detail: Optional[str] = Field(None, description="상세 오류 정보")
    
    class Config:
        schema_extra = {
            "example": {
                "error_code": "SHEET_NOT_ACCESSIBLE",
                "message": "Cannot access the Google Sheet. Please ensure it's shared publicly.",
                "detail": "403 Forbidden: The caller does not have permission"
            }
        }


class RegisteredSheet(BaseModel):
    """등록된 Google Sheet 정보"""
    sheet_id: str
    sheet_url: str
    worksheet_name: str
    polling_interval: int
    last_polled: Optional[str] = None
    last_hash: Optional[str] = None
    is_active: bool = True
    registered_at: str
    
    class Config:
        orm_mode = True


class SheetMetadata(BaseModel):
    """Google Sheet 메타데이터"""
    sheet_id: str
    title: str
    locale: str = "ko_KR"
    time_zone: str = "Asia/Seoul"
    auto_recalc: str = "ON_CHANGE"
    default_format: dict = Field(default_factory=dict)
    sheets: List[dict] = Field(
        default_factory=list,
        description="워크시트 목록"
    )