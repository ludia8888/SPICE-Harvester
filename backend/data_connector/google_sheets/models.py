"""
Google Sheets Connector - Request/Response Models
"""

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# Import shared models


class RegisteredSheet(BaseModel):
    """등록된 Google Sheet 정보"""

    sheet_id: str
    sheet_url: str
    sheet_title: Optional[str] = None
    worksheet_name: str
    polling_interval: int
    # Optional pipeline config (maps source → ontology; auto-import requires confirmed mapping)
    database_name: Optional[str] = None
    branch: str = "main"
    class_label: Optional[str] = None
    auto_import: bool = False
    max_import_rows: Optional[int] = None
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
    sample_rows: List[List[Any]] = Field(default_factory=list, description="샘플 행 데이터")
    preview_data: List[List[Any]] = Field(default_factory=list, description="미리보기 데이터")
    total_rows: int = Field(0, description="전체 행 수")
    total_columns: int = Field(0, description="전체 컬럼 수")


class GoogleSheetRegisterRequest(BaseModel):
    """Google Sheet 등록 요청"""

    sheet_url: str = Field(..., description="Google Sheets URL")
    worksheet_name: str = Field(..., description="워크시트 이름")
    polling_interval: int = Field(default=300, description="폴링 간격 (초)")
    # Optional: auto-import config (best-effort)
    database_name: Optional[str] = Field(default=None, description="Target database (optional)")
    branch: str = Field(default="main", description="Target branch (default: main)")
    class_label: Optional[str] = Field(default=None, description="Target class label (optional)")
    auto_import: bool = Field(default=False, description="Enable auto-import on change (default: false)")
    max_import_rows: Optional[int] = Field(default=None, description="Max rows to import per run (optional)")


class GoogleSheetRegisterResponse(BaseModel):
    """Google Sheet 등록 응답"""

    sheet_id: str = Field(..., description="Google Sheets ID")
    status: str = Field(..., description="등록 상태")
    message: str = Field(..., description="응답 메시지")
    registered_sheet: Optional[RegisteredSheet] = Field(None, description="등록된 시트 정보")
