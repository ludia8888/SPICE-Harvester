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
        """LLM sometimes returns ``key`` or ``name`` instead of ``id``,
        and ``description`` instead of ``question``."""
        if not isinstance(data, dict):
            return data
        # Alias: key / name → id
        if "id" not in data:
            if "key" in data:
                data["id"] = data.pop("key")
            elif "name" in data:
                data["id"] = data.pop("name")
        # Alias: description / text / label → question
        if "question" not in data:
            for alt in ("description", "text", "label"):
                if alt in data:
                    data["question"] = data.pop(alt)
                    break
        # Fallback: generate id from question hash if still missing
        if "id" not in data and "question" in data:
            import hashlib
            data["id"] = hashlib.md5(str(data["question"]).encode()).hexdigest()[:12]
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

