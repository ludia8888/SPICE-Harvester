"""
Funnel Service Models
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from shared.models.common import DataType


class TypeInferenceResult(BaseModel):
    """Type inference result with confidence and reasoning"""
    type: str = Field(..., description="Inferred data type")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0.0-1.0)")
    reason: str = Field(..., description="Reasoning for the inference")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")


class ColumnAnalysisResult(BaseModel):
    """Analysis result for a single column"""
    column_name: str
    inferred_type: TypeInferenceResult
    sample_values: List[Any] = Field(default_factory=list, description="Sample values used for analysis")
    null_count: int = Field(default=0, description="Number of null/empty values")
    unique_count: int = Field(default=0, description="Number of unique values")
    
    class Config:
        json_schema_extra = {
            "example": {
                "column_name": "order_date",
                "inferred_type": {
                    "type": "xsd:date",
                    "confidence": 0.93,
                    "reason": "47/50 values (94%) match date pattern YYYY-MM-DD",
                    "metadata": {"detected_format": "YYYY-MM-DD"}
                },
                "sample_values": ["2023-01-15", "2023-02-20", "2023-03-10"],
                "null_count": 3,
                "unique_count": 47
            }
        }


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
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SchemaGenerationRequest(BaseModel):
    """Request for schema generation based on analysis"""
    analysis_id: str = Field(..., description="Analysis result ID")
    class_name: Optional[str] = Field(default=None, description="Suggested class name")
    include_relationships: bool = Field(default=False, description="Detect and include relationships")


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