"""
Ontology linting models (domain-neutral).

These are used to expose "Block / Warn / Require Proof" style guardrails
without coupling to any specific business domain.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class LintSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class LintIssue(BaseModel):
    severity: LintSeverity = Field(..., description="Issue severity")
    rule_id: str = Field(..., description="Stable rule identifier (e.g., ONT001)")
    message: str = Field(..., description="Human-readable issue message")
    path: Optional[str] = Field(
        default=None,
        description="JSON-path-ish pointer to the problematic field (e.g., properties[0].name)",
    )
    suggestion: Optional[str] = Field(default=None, description="Suggested fix (if applicable)")
    rationale: Optional[str] = Field(default=None, description="Why this matters (short)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra structured context")


class LintReport(BaseModel):
    ok: bool = Field(..., description="True when there are no ERROR issues")
    risk_score: float = Field(..., ge=0.0, le=100.0, description="0-100, higher is riskier")
    risk_level: str = Field(..., description="low|medium|high|critical")
    errors: List[LintIssue] = Field(default_factory=list)
    warnings: List[LintIssue] = Field(default_factory=list)
    infos: List[LintIssue] = Field(default_factory=list)

