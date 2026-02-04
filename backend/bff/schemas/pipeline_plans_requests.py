from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field

from shared.models.pipeline_plan import PipelinePlanDataScope


class PipelinePlanCompileRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=2000)
    data_scope: PipelinePlanDataScope | None = Field(default=None)
    answers: dict | None = Field(default=None)
    planner_hints: dict | None = Field(default=None)
    task_spec: dict | None = Field(default=None)


class PipelinePlanPreviewRequest(BaseModel):
    node_id: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=200, ge=1, le=200)
    include_run_tables: bool = Field(default=False)
    run_table_limit: int = Field(default=200, ge=1, le=1000)


class PipelinePlanInspectPreviewRequest(BaseModel):
    preview: Dict[str, Any] | None = Field(default=None)
    node_id: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=200, ge=1, le=200)


class PipelinePlanEvaluateJoinsRequest(BaseModel):
    node_id: str | None = Field(default=None, max_length=200)
    run_tables: Dict[str, Dict[str, Any]] | None = Field(default=None)
    definition_digest: str | None = Field(default=None, max_length=200)

