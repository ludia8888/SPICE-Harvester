from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from shared.models.agent_plan_report import PlanCompilationReport
from shared.models.pipeline_plan import PipelinePlan
from shared.services.agent.llm_gateway import LLMCallMeta


class PipelineClarificationQuestion(BaseModel):
    id: str = Field(..., min_length=1, max_length=100)
    question: str = Field(..., min_length=1, max_length=2000)
    required: bool = Field(default=True)
    type: str = Field(default="string", description="string|enum|boolean|number|object")
    options: Optional[List[str]] = None
    default: Optional[Any] = None

    @model_validator(mode="before")
    @classmethod
    def _accept_key_as_id(cls, data: Any) -> Any:
        """LLM sometimes returns ``key`` instead of ``id``."""
        if isinstance(data, dict) and "key" in data and "id" not in data:
            data["id"] = data.pop("key")
        return data


@dataclass(frozen=True)
class PipelinePlanCompileResult:
    status: str  # success|clarification_required|error
    plan_id: str
    plan: Optional[PipelinePlan]
    validation_errors: List[str]
    validation_warnings: List[str]
    questions: List[PipelineClarificationQuestion]
    compilation_report: Optional[PlanCompilationReport] = None
    llm_meta: Optional[LLMCallMeta] = None
    planner_confidence: Optional[float] = None
    planner_notes: Optional[List[str]] = None
    preflight: Optional[Dict[str, Any]] = None

