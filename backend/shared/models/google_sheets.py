"""
Google Sheets models for SPICE HARVESTER
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class GoogleSheetPreviewRequest(BaseModel):
    """Google Sheet preview request model"""

    sheet_url: HttpUrl = Field(
        ...,
        description="Google Sheets URL",
        examples=["https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"],
    )
    worksheet_name: Optional[str] = Field(
        default=None, description="Optional worksheet title; falls back to gid/first sheet"
    )
    api_key: Optional[str] = Field(None, description="Google API key for authentication")
    connection_id: Optional[str] = Field(
        default=None, description="Optional OAuth connection id"
    )

    @field_validator("sheet_url")
    @classmethod
    def validate_google_sheet_url(cls, v):
        """Validate Google Sheets URL format"""
        from ..validators import get_validator

        validator = get_validator("google_sheets_url")
        if validator:
            result = validator.validate(str(v))
            if not result.is_valid:
                raise ValueError(result.message)
        return v


class GoogleSheetPreviewResponse(BaseModel):
    """Google Sheet preview response model"""

    sheet_id: str = Field(..., description="Google Sheets ID")
    sheet_title: str = Field(..., description="Spreadsheet title")
    worksheet_title: str = Field(..., description="Worksheet title")
    columns: List[str] = Field(..., description="Column names")
    sample_rows: List[List[str]] = Field(..., description="Sample data rows")
    total_rows: int = Field(..., description="Total number of rows")
    total_columns: int = Field(..., description="Total number of columns")

    @field_validator("sheet_id")
    @classmethod
    def validate_sheet_id(cls, v):
        """Validate sheet ID format"""
        if not v or not isinstance(v, str):
            raise ValueError("Sheet ID must be a non-empty string")
        return v

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, v):
        """Validate columns"""
        if not isinstance(v, list):
            raise ValueError("Columns must be a list")
        return v

    @field_validator("sample_rows")
    @classmethod
    def validate_sample_rows(cls, v):
        """Validate sample rows"""
        if not isinstance(v, list):
            raise ValueError("Sample rows must be a list")
        return v

    @field_validator("total_rows")
    @classmethod
    def validate_total_rows(cls, v):
        """Validate total rows"""
        if not isinstance(v, int) or v < 0:
            raise ValueError("Total rows must be a non-negative integer")
        return v

    @field_validator("total_columns")
    @classmethod
    def validate_total_columns(cls, v):
        """Validate total columns"""
        if not isinstance(v, int) or v < 0:
            raise ValueError("Total columns must be a non-negative integer")
        return v


class GoogleSheetError(BaseModel):
    """Google Sheet error response model"""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error_code": "SHEET_NOT_ACCESSIBLE",
                "message": "Cannot access the Google Sheet. Please ensure it's shared publicly.",
                "detail": "403 Forbidden: The caller does not have permission",
            }
        }
    )

    error_code: str = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")


class GoogleSheetRegisterRequest(BaseModel):
    """Google Sheet registration request model"""

    sheet_url: HttpUrl = Field(
        ...,
        description="Google Sheets URL",
        examples=["https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"],
    )
    worksheet_name: Optional[str] = Field(
        default="Sheet1", description="Worksheet name (default: Sheet1)"
    )
    polling_interval: Optional[int] = Field(
        default=300, ge=60, le=3600, description="Polling interval in seconds (60-3600)"
    )

    @field_validator("sheet_url")
    @classmethod
    def validate_google_sheet_url(cls, v):
        """Validate Google Sheets URL format"""
        from ..validators import get_validator

        validator = get_validator("google_sheets_url")
        if validator:
            result = validator.validate(str(v))
            if not result.is_valid:
                raise ValueError(result.message)
        return v


class GoogleSheetRegisterResponse(BaseModel):
    """Google Sheet registration response model"""

    status: str = Field(default="registered", description="Registration status")
    sheet_id: str = Field(..., description="Google Sheets ID")
    worksheet_name: str = Field(..., description="Worksheet name")
    polling_interval: int = Field(..., description="Polling interval (seconds)")
    registered_at: str = Field(..., description="Registration time (ISO 8601)")
