"""
Schema changes request/response schemas (BFF).

Extracted from `bff.routers.schema_changes` to keep routers thin and reusable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SchemaChangeItem(BaseModel):
    drift_id: str
    subject_type: str
    subject_id: str
    db_name: str
    previous_hash: Optional[str]
    current_hash: str
    drift_type: str
    severity: str
    changes: List[Dict[str, Any]]
    detected_at: datetime
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None


class SubscriptionCreateRequest(BaseModel):
    subject_type: str = Field(..., pattern="^(dataset|mapping_spec|object_type)$")
    subject_id: str
    db_name: str
    severity_filter: List[str] = Field(default=["warning", "breaking"])
    notification_channels: List[str] = Field(default=["websocket"])


class SubscriptionResponse(BaseModel):
    subscription_id: str
    user_id: str
    subject_type: str
    subject_id: str
    db_name: str
    severity_filter: List[str]
    notification_channels: List[str]
    status: str
    created_at: datetime


class CompatibilityCheckRequest(BaseModel):
    dataset_version_id: Optional[str] = None


class CompatibilityCheckResponse(BaseModel):
    is_compatible: bool
    has_drift: bool
    drift_type: Optional[str] = None
    severity: Optional[str] = None
    changes: List[Dict[str, Any]] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)


class AcknowledgeRequest(BaseModel):
    acknowledged_by: str
