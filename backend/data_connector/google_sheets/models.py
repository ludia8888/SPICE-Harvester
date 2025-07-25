"""
Google Sheets Connector - Request/Response Models
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

# Import shared models


class GoogleSheetDataUpdate(BaseModel):
    """Google Sheet 데이터 업데이트 알림 모델"""

    sheet_id: str = Field(..., description="Google Sheets ID")
    worksheet_name: str = Field(..., description="워크시트 이름")
    changed_rows: int = Field(..., description="변경된 행 수")
    changed_columns: Optional[List[str]] = Field(None, description="변경된 컬럼 목록")
    timestamp: str = Field(..., description="변경 감지 시간 (ISO 8601)")


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

    model_config = ConfigDict(from_attributes=True)


class SheetMetadata(BaseModel):
    """Google Sheet 메타데이터"""

    sheet_id: str
    title: str
    locale: str = "ko_KR"
    time_zone: str = "Asia/Seoul"
    auto_recalc: str = "ON_CHANGE"
    default_format: dict = Field(default_factory=dict)
    sheets: List[dict] = Field(default_factory=list, description="워크시트 목록")


class GoogleSheetPreviewRequest(BaseModel):
    """Google Sheet 미리보기 요청"""

    sheet_url: str = Field(..., description="Google Sheets URL")
    worksheet_name: Optional[str] = Field(None, description="워크시트 이름")


class GoogleSheetPreviewResponse(BaseModel):
    """Google Sheet 미리보기 응답"""

    sheet_id: str = Field(..., description="Google Sheets ID")
    sheet_url: str = Field(description="Google Sheets URL")
    sheet_title: str = Field(description="시트 제목")
    worksheet_title: str = Field(default="", description="워크시트 제목")
    worksheet_name: str = Field(default="", description="워크시트 이름")
    metadata: Optional[SheetMetadata] = Field(None, description="시트 메타데이터")
    columns: List[str] = Field(default_factory=list, description="컬럼 목록")
    sample_rows: List[Dict[str, Any]] = Field(default_factory=list, description="샘플 행 데이터")
    preview_data: List[List[str]] = Field(default_factory=list, description="미리보기 데이터")
    total_rows: int = Field(0, description="전체 행 수")
    total_columns: int = Field(0, description="전체 컬럼 수")


class GoogleSheetRegisterRequest(BaseModel):
    """Google Sheet 등록 요청"""

    sheet_url: str = Field(..., description="Google Sheets URL")
    worksheet_name: str = Field(..., description="워크시트 이름")
    polling_interval: int = Field(default=300, description="폴링 간격 (초)")


class GoogleSheetRegisterResponse(BaseModel):
    """Google Sheet 등록 응답"""

    sheet_id: str = Field(..., description="Google Sheets ID")
    status: str = Field(..., description="등록 상태")
    message: str = Field(..., description="응답 메시지")
    registered_sheet: Optional[RegisteredSheet] = Field(None, description="등록된 시트 정보")
