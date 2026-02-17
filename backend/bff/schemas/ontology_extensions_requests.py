"""Ontology extension request schemas (BFF).

These models back the "ontology extensions" resource endpoints.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class OntologyResourceRequest(BaseModel):
    id: Optional[str] = Field(None, description="Resource id (optional)")
    label: Any = Field(..., description="Resource label")
    description: Optional[Any] = Field(None, description="Resource description")
    spec: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")

