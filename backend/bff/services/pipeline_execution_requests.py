from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class _DeployRequestPayload:
    definition_json: Optional[Dict[str, Any]]
    promote_build: bool
    build_job_id: Optional[str]
    artifact_id: Optional[str]
    replay_on_deploy: bool
    dependencies_raw: Any
    node_id: Optional[str]
    db_name: str
    outputs: Optional[list[dict[str, Any]]]
    expectations: Optional[list[Any]]
    schema_contract: Optional[list[Any]]
    schedule_interval_seconds: Optional[int]
    schedule_cron: Optional[str]
    branch: Optional[str]
    proposal_status: Optional[str]
    proposal_title: Optional[str]
    proposal_description: Optional[str]


@dataclass(frozen=True)
class _DeployPipelineContext:
    pipeline: Any
    resolved_branch: str
    proposal_required: bool
    proposal_bundle: Dict[str, Any]
    latest: Any
    dependencies: Optional[list[dict[str, str]]]


@dataclass(frozen=True)
class _PromoteBuildSourceContext:
    artifact_record: Any
    build_job_id: str
    output_json: Dict[str, Any]
    build_ontology: Dict[str, Any]
    build_ontology_commit: str


@dataclass(frozen=True)
class _PromoteOutputSelection:
    execution_semantics: str
    is_streaming_promotion: bool
    selected_outputs: list[dict[str, Any]]
    staged_bucket: str
    build_ref: str


@dataclass(frozen=True)
class _PreparedPromoteOutputs:
    normalized_outputs: list[dict[str, Any]]
    staged_bucket: str
    build_ref: str


@dataclass(frozen=True)
class _PromotedOutputMaterializationContext:
    merge_commit_id: str
    staged_bucket: str
    db_name: str
    resolved_branch: str
    pipeline_id: str
    build_job_id: str
    definition_hash: str
    build_ontology_commit: str
    principal_id: str
    replay_on_deploy: bool
    artifact_id: Optional[str]
    definition_json: Dict[str, Any]


@dataclass(frozen=True)
class _PipelineRunRequestPayload:
    limit: int
    definition_json: Optional[Dict[str, Any]]
    node_id: Optional[str]
    db_name: str
    expectations: Optional[list[Any]]
    schema_contract: Optional[list[Any]]
    branch: Optional[str]
    sampling_strategy: Optional[dict[str, Any]]


@dataclass(frozen=True)
class _PipelineRunContext:
    pipeline: Any
    latest: Any
    definition_json: Dict[str, Any]
    db_name: str
    node_id: Optional[str]
    requested_branch: Optional[str]


@dataclass(frozen=True)
class _PreparedPreviewExecution:
    request_payload: _PipelineRunRequestPayload
    run_context: _PipelineRunContext
    preflight: Dict[str, Any]
    definition_hash: str
    definition_commit_id: str
    job_id: str


@dataclass(frozen=True)
class _PreparedBuildExecution:
    request_payload: _PipelineRunRequestPayload
    run_context: _PipelineRunContext
    preflight: Dict[str, Any]
    definition_hash: str
    definition_commit_id: str
    job_id: str
    ontology_branch: str
    ontology_head_commit_id: str


@dataclass(frozen=True)
class _PreparedDeployExecution:
    request_input: _DeployRequestPayload
    deploy_context: _DeployPipelineContext
    definition_json: Dict[str, Any]
    db_name: str
    definition_hash: str
    definition_commit_id: str


@dataclass(frozen=True)
class _DeployPromotionPayload:
    artifact_ref: Optional[str]
    build_job_id: str
    build_ontology: Dict[str, Any]
    build_ontology_commit: str
    normalized_outputs: list[dict[str, Any]]
    staged_bucket: str
    build_ref: str


@dataclass(frozen=True)
class _DeployExecutionResult:
    promote_build: bool
    build_job_id: str
    artifact_ref: Optional[str]
    node_id: Optional[str]
    resolved_branch: str
    replay_on_deploy: bool
    promote_job_id: str
    deployed_commit_id: str
    build_outputs: list[dict[str, Any]]
