"""
Pipeline task spec (intent/scope contract).

This is the "front-door" contract used to prevent overreach:
- Report-only requests (e.g. null checks) must not trigger plan compilation.
- Pipeline requests may compile/preview/repair within the declared scope.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field


class PipelineTaskScope(str, Enum):
    report_only = "report_only"
    pipeline = "pipeline"


class PipelineTaskIntent(str, Enum):
    null_check = "null_check"
    profile = "profile"
    cleanse = "cleanse"
    integrate = "integrate"
    prepare_mapping = "prepare_mapping"
    unknown = "unknown"


class PipelineTaskSpec(BaseModel):
    """
    Typed task spec returned by the task classifier.

    Notes:
    - allow_write is always false for now (control-plane only).
    - questions uses a loose dict shape to avoid importing BFF-only models.
    """

    model_config = ConfigDict(extra="ignore")

    scope: PipelineTaskScope
    intent: PipelineTaskIntent = Field(default=PipelineTaskIntent.unknown)

    allow_join: bool = Field(default=False)
    allow_cleansing: bool = Field(default=False)
    allow_advanced_transforms: bool = Field(default=False)
    allow_specs: bool = Field(default=False)
    allow_write: bool = Field(default=False)

    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    questions: List[Dict[str, Any]] = Field(default_factory=list)

