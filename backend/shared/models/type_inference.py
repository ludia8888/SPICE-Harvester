"""
Funnel Service Models
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from shared.models.common import DataType


def _default_risk_policy() -> Dict[str, Any]:
    return {"stage": "funnel", "suggestion_only": True, "hard_gate": False}


class TypeInferenceResult(BaseModel):
    """Type inference result with confidence and reasoning"""

    type: str = Field(..., description="Inferred data type")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0.0-1.0)")
    reason: str = Field(..., description="Reasoning for the inference")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")


class FunnelRiskItem(BaseModel):
    """Risk signal emitted by Funnel (suggestion-only)."""

    code: str = Field(..., description="Machine-readable risk identifier")
    severity: str = Field(..., description="info | warning (suggestion-only)")
    message: str = Field(..., description="Human-readable risk summary")
    column: Optional[str] = Field(default=None, description="Column name when applicable")
    evidence: Dict[str, Any] = Field(default_factory=dict, description="Structured context")
    suggested_actions: List[str] = Field(
        default_factory=list, description="Recommended follow-up actions"
    )
    is_suggestion: bool = Field(default=True, description="Funnel signals are suggestions only")
    stage: str = Field(default="funnel", description="Origin stage for the risk signal")


class ColumnProfile(BaseModel):
    """Lightweight column profiling summary (sample-based)."""

    length_stats: Optional[Dict[str, float]] = Field(default=None, description="String length stats")
    numeric_stats: Optional[Dict[str, float]] = Field(default=None, description="Numeric stats")
    format_stats: Optional[Dict[str, Any]] = Field(default=None, description="Format-related hints")


class ColumnAnalysisResult(BaseModel):
    """Analysis result for a single column"""

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "column_name": "order_date",
                "inferred_type": {
                    "type": "xsd:date",
                    "confidence": 0.93,
                    "reason": "47/50 values (94%) match date pattern YYYY-MM-DD",
                    "metadata": {"detected_format": "YYYY-MM-DD"},
                },
                "total_count": 50,
                "non_empty_count": 47,
                "sample_values": ["2023-01-15", "2023-02-20", "2023-03-10"],
                "null_count": 3,
                "unique_count": 47,
                "null_ratio": 0.06,
                "unique_ratio": 1.0,
            }
        },
    )

    column_name: str
    inferred_type: TypeInferenceResult
    total_count: int = Field(default=0, ge=0, description="Total analyzed values (incl. null/empty)")
    non_empty_count: int = Field(default=0, ge=0, description="Non-empty analyzed values")
    sample_values: List[Any] = Field(
        default_factory=list, description="Sample values used for analysis"
    )
    null_count: int = Field(default=0, description="Number of null/empty values")
    unique_count: int = Field(default=0, description="Number of unique values")
    null_ratio: float = Field(default=0.0, ge=0.0, le=1.0, description="Null/total ratio")
    unique_ratio: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Unique/non-empty ratio"
    )
    profile: Optional[ColumnProfile] = Field(default=None, description="Sample-based profile")
    risk_flags: List[FunnelRiskItem] = Field(
        default_factory=list, description="Suggestion-only risk signals"
    )


class FunnelAnalysisPayload(BaseModel):
    """Funnel analysis payload (suggestion-only)."""

    columns: List[ColumnAnalysisResult] = Field(default_factory=list)
    risk_summary: List[FunnelRiskItem] = Field(default_factory=list)
    risk_policy: Dict[str, Any] = Field(default_factory=_default_risk_policy)


class DatasetAnalysisRequest(BaseModel):
    """Request for dataset type analysis"""

    data: List[List[Any]] = Field(..., description="Dataset rows")
    columns: List[str] = Field(..., description="Column names")
    sample_size: Optional[int] = Field(default=1000, description="Number of rows to analyze")
    include_complex_types: bool = Field(default=False, description="Include complex type detection")


class DatasetAnalysisResponse(BaseModel):
    """Response for dataset type analysis"""

    columns: List[ColumnAnalysisResult]
    analysis_metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    risk_summary: List[FunnelRiskItem] = Field(
        default_factory=list, description="Dataset-level risk signals"
    )
    risk_policy: Dict[str, Any] = Field(
        default_factory=_default_risk_policy, description="Risk signal policy metadata"
    )


class SchemaGenerationRequest(BaseModel):
    """Request for schema generation based on analysis"""

    analysis_id: str = Field(..., description="Analysis result ID")
    class_name: Optional[str] = Field(default=None, description="Suggested class name")
    include_relationships: bool = Field(
        default=False, description="Detect and include relationships"
    )


class SchemaGenerationResponse(BaseModel):
    """Generated schema based on type analysis"""

    class_id: str
    properties: List[Dict[str, Any]]
    suggested_relationships: Optional[List[Dict[str, Any]]] = None
    confidence_score: float


class FunnelPreviewRequest(BaseModel):
    """Request for data preview with type inference"""

    source_type: str = Field(..., description="Type of data source (google_sheets, csv, etc.)")
    source_config: Dict[str, Any] = Field(..., description="Source-specific configuration")
    preview_rows: int = Field(default=100, ge=1, le=1000, description="Number of rows to preview")
    infer_types: bool = Field(default=True, description="Whether to infer column types")


class FunnelPreviewResponse(BaseModel):
    """Preview response with inferred types"""

    source_metadata: Dict[str, Any]
    columns: List[str]
    sample_data: List[List[Any]]
    inferred_schema: Optional[List[ColumnAnalysisResult]] = None
    total_rows: int
    preview_rows: int
    risk_summary: List[FunnelRiskItem] = Field(
        default_factory=list, description="Dataset-level risk signals"
    )
    risk_policy: Dict[str, Any] = Field(
        default_factory=_default_risk_policy, description="Risk signal policy metadata"
    )


class TypeMappingRequest(BaseModel):
    """Request for mapping inferred types to target schema"""

    inferred_types: Dict[str, str] = Field(..., description="Column name to inferred type mapping")
    target_system: str = Field(default="terminus", description="Target system for type mapping")
    custom_mappings: Optional[Dict[str, str]] = None


class TypeMappingResponse(BaseModel):
    """Response with mapped types for target system"""

    mappings: Dict[str, Dict[str, Any]]
    warnings: List[str] = Field(default_factory=list)
    requires_transformation: List[str] = Field(default_factory=list)
