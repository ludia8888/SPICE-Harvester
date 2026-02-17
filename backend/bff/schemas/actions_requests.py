"""
Action-related request schemas (BFF).

These schemas are shared across action submission + simulation endpoints.
Keeping them in one module reduces router bloat and supports router composition.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ActionUndoRequest(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=2000)
    correlation_id: Optional[str] = Field(default=None)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    base_branch: str = Field(default="main")
    overlay_branch: Optional[str] = Field(default=None)
