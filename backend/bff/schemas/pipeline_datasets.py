"""
Pipeline datasets schemas (BFF).

These response models are used by dataset ingest-analysis endpoints.
Keeping them in `bff.schemas` avoids bloating router modules and supports
router composition (Composite pattern).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from shared.models.type_inference import FunnelAnalysisPayload


class FunnelAnalysisData(BaseModel):
    dataset: Optional[Dict[str, Any]] = Field(default=None)
    ingest_request: Optional[Dict[str, Any]] = Field(default=None)
    version: Optional[Dict[str, Any]] = Field(default=None)
    funnel_analysis: FunnelAnalysisPayload


class FunnelAnalysisApiResponse(BaseModel):
    status: str
    message: str
    data: FunnelAnalysisData
    errors: Optional[list[str]] = None

