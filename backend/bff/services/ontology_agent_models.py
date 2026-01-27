"""
Ontology Agent Models

Pydantic models for the autonomous ontology agent that enables
natural language ontology schema creation/mapping.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class OntologyClarificationQuestion(BaseModel):
    """A clarification question for the user."""
    id: str = Field(..., description="Unique question ID")
    question: str = Field(..., description="The question text")
    required: bool = Field(default=True, description="Whether an answer is required")
    type: Literal["string", "boolean", "choice"] = Field(default="string", description="Answer type")
    options: Optional[List[str]] = Field(default=None, description="Options for choice type")


class AutonomousOntologyAgentToolCall(BaseModel):
    """A single tool call in the agent decision."""
    tool: str = Field(default="", max_length=200)
    args: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("args", mode="before")
    @classmethod
    def _coerce_args(cls, v):
        return {} if v is None else v


class AutonomousOntologyAgentDecision(BaseModel):
    """The LLM's decision at each step of the agent loop."""
    action: Literal["call_tool", "finish", "clarify"] = Field(default="call_tool")
    tool_calls: List[AutonomousOntologyAgentToolCall] = Field(default_factory=list)
    # Legacy single-call shape (kept for backward compatibility)
    tool: Optional[str] = Field(default=None, max_length=200)
    args: Dict[str, Any] = Field(default_factory=dict)
    questions: List[OntologyClarificationQuestion] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("args", mode="before")
    @classmethod
    def _coerce_args(cls, v):
        return {} if v is None else v

    @field_validator("tool_calls", mode="before")
    @classmethod
    def _coerce_tool_calls(cls, v):
        return [] if v is None else v

    @field_validator("questions", mode="before")
    @classmethod
    def _coerce_questions(cls, v):
        if v is None:
            return []
        if not isinstance(v, list):
            return v
        out: List[Any] = []
        for idx, item in enumerate(v):
            if isinstance(item, str):
                text = item.strip()
                if text:
                    out.append({
                        "id": f"q{idx + 1}",
                        "question": text,
                        "required": True,
                        "type": "string",
                    })
                continue
            out.append(item)
        return out

    @field_validator("notes", "warnings", mode="before")
    @classmethod
    def _coerce_lists(cls, v):
        return [] if v is None else v

    @model_validator(mode="after")
    def _validate_action(self) -> "AutonomousOntologyAgentDecision":
        if self.action == "call_tool":
            has_batch = bool(self.tool_calls)
            has_single = bool(str(self.tool or "").strip())
            if not (has_batch or has_single):
                raise ValueError("tool_calls or tool is required when action=call_tool")
            for idx, call in enumerate(self.tool_calls or []):
                if not str(call.tool or "").strip():
                    raise ValueError(f"tool_calls[{idx}].tool is required when action=call_tool")
        if self.action == "clarify":
            if not self.questions:
                raise ValueError("questions is required when action=clarify")
        return self


class OntologyAgentRunRequest(BaseModel):
    """Request model for POST /api/v1/agent/ontology-runs."""
    goal: str = Field(..., description="Natural language goal (e.g., 'Customer 클래스 만들어줘')")
    db_name: str = Field(..., description="Target database name")
    branch: str = Field(default="main", description="Branch name")
    target_class_id: Optional[str] = Field(
        default=None,
        description="Target class ID for updates/mappings"
    )
    dataset_sample: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Sample data for schema inference: {columns: [], data: [[]]}"
    )
    answers: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Answers to clarification questions from a previous run"
    )
    selected_model: Optional[str] = Field(
        default=None,
        description="Preferred LLM model ID"
    )

    @field_validator("goal")
    @classmethod
    def validate_goal(cls, v):
        if not v or not str(v).strip():
            raise ValueError("goal must be a non-empty string")
        return str(v).strip()

    @field_validator("db_name")
    @classmethod
    def validate_db_name(cls, v):
        if not v or not str(v).strip():
            raise ValueError("db_name must be a non-empty string")
        return str(v).strip()


class OntologyAgentRunResponse(BaseModel):
    """Response model for POST /api/v1/agent/ontology-runs."""
    run_id: str = Field(..., description="Unique run ID")
    status: Literal["success", "clarification_required", "partial", "failed"] = Field(
        ..., description="Run status"
    )
    ontology: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Generated/updated ontology schema"
    )
    questions: List[OntologyClarificationQuestion] = Field(
        default_factory=list,
        description="Clarification questions if status=clarification_required"
    )
    validation_errors: List[str] = Field(
        default_factory=list,
        description="Validation errors if any"
    )
    validation_warnings: List[str] = Field(
        default_factory=list,
        description="Validation warnings if any"
    )
    mapping_suggestions: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Field mapping suggestions if applicable"
    )
    planner: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Planner metadata (confidence, notes)"
    )
    llm: Optional[Dict[str, Any]] = Field(
        default=None,
        description="LLM metadata (provider, model, latency)"
    )


@dataclass
class OntologyAgentState:
    """Internal state for the ontology agent loop."""
    db_name: str
    branch: str
    goal: str
    principal_id: str

    # Working ontology (in-memory draft)
    working_ontology: Optional[Dict[str, Any]] = None

    # Existing schema context
    existing_classes: Optional[List[Dict[str, Any]]] = None

    # Inference results
    schema_inference: Optional[Dict[str, Any]] = None  # FunnelClient result
    mapping_suggestions: Optional[Dict[str, Any]] = None  # MappingSuggestionService result

    # Target class for updates
    target_class_id: Optional[str] = None
    target_class_data: Optional[Dict[str, Any]] = None

    # Prompt log (append-only JSONL for prefix caching)
    prompt_items: List[str] = field(default_factory=list)
    last_observation: Optional[Dict[str, Any]] = None
