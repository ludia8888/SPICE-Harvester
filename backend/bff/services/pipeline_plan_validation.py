"""Pipeline plan validation.

This module intentionally does NOT mutate pipeline plans.

- No auto-wiring associations into joins
- No join input rewiring/reordering
- No auto-injected casts/outputs/canonical contracts

Any repair must be performed explicitly via plan builder tools.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.models.agent_plan_report import PlanCompilationReport, PlanDiagnostic, PlanDiagnosticSeverity
from shared.models.pipeline_plan import PipelinePlan
from shared.models.pipeline_task_spec import PipelineTaskSpec
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.pipeline.pipeline_graph_utils import normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline.output_plugins import OUTPUT_KIND_ONTOLOGY, normalize_output_kind, validate_output_payload
from shared.services.pipeline.pipeline_task_spec_policy import clamp_task_spec, validate_plan_against_task_spec
from shared.utils.canonical_json import sha256_canonical_json_prefixed

from bff.routers import pipeline_ops_preflight as pipeline_router
from shared.observability.tracing import trace_db_operation


@dataclass(frozen=True)
class PipelinePlanValidationResult:
    plan: PipelinePlan
    errors: List[str]
    warnings: List[str]
    preflight: Dict[str, Any]
    compilation_report: PlanCompilationReport


def _is_acyclic(nodes: Dict[str, Dict[str, Any]], edges: List[Dict[str, Any]]) -> bool:
    order = topological_sort(nodes, edges, include_unordered=False)
    return len(order) == len(nodes)


@trace_db_operation("bff.pipeline_plan_validation.validate_pipeline_plan")
async def validate_pipeline_plan(
    *,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
    require_output: bool = True,
    task_spec: Optional[PipelineTaskSpec] = None,
) -> PipelinePlanValidationResult:
    errors: List[str] = []
    warnings: List[str] = []

    # Enforce TaskSpec policy (overreach guardrails) as early as possible.
    effective_spec = task_spec or getattr(plan, "task_spec", None)
    if effective_spec is not None:
        dataset_count = len(list(plan.data_scope.dataset_ids or [])) if plan.data_scope else 0
        effective_spec = clamp_task_spec(spec=effective_spec, dataset_count=dataset_count)
        plan = plan.model_copy(update={"task_spec": effective_spec})
        policy_errors, policy_warnings = validate_plan_against_task_spec(plan=plan, task_spec=effective_spec)
        errors.extend(policy_errors)
        warnings.extend(policy_warnings)

    # Validate output metadata.
    for output in plan.outputs or []:
        kind = normalize_output_kind(getattr(output.output_kind, "value", output.output_kind))
        ontology_payload = {
            "target_class_id": output.target_class_id,
            "source_class_id": output.source_class_id,
            "link_type_id": output.link_type_id,
            "predicate": output.predicate,
            "cardinality": output.cardinality,
            "source_key_column": output.source_key_column,
            "target_key_column": output.target_key_column,
            "relationship_spec_type": output.relationship_spec_type,
        }
        payload = dict(output.output_metadata or {})
        payload.update({key: value for key, value in ontology_payload.items() if value is not None})
        for detail in validate_output_payload(kind=kind, payload=payload):
            errors.append(f"output {output.output_name}: {detail}")

        if kind != OUTPUT_KIND_ONTOLOGY and any(
            str(value).strip() for value in ontology_payload.values() if value is not None
        ):
            warnings.append(
                f"output {output.output_name}: output_kind={kind} but ontology metadata is set"
            )

    definition_json = dict(plan.definition_json or {})

    # Graph sanity + structural validation.
    nodes = normalize_nodes(definition_json.get("nodes"))
    edges = normalize_edges(definition_json.get("edges"))
    if nodes and edges and not _is_acyclic(nodes, edges):
        errors.append("pipeline graph has cycles; remove circular joins/edges")

    errors.extend(
        pipeline_router._validate_pipeline_definition(
            definition_json=definition_json,
            require_output=require_output,
        )
    )

    preflight = await pipeline_router._run_pipeline_preflight(
        definition_json=definition_json,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    if preflight.get("has_blocking_errors"):
        errors.append("pipeline preflight has blocking errors")

    updated_plan = plan.model_copy(update={"definition_json": definition_json})
    digest = sha256_canonical_json_prefixed(updated_plan.model_dump(mode="json"))

    diagnostics: List[PlanDiagnostic] = []
    for message in errors:
        diagnostics.append(
            PlanDiagnostic(
                code="PIPELINE_PLAN_INVALID",
                severity=PlanDiagnosticSeverity.error,
                message=str(message),
            )
        )
    for message in warnings:
        diagnostics.append(
            PlanDiagnostic(
                code="PIPELINE_PLAN_WARNING",
                severity=PlanDiagnosticSeverity.warning,
                message=str(message),
            )
        )

    status = "success" if not errors else "error"
    report = PlanCompilationReport(
        plan_id=str(updated_plan.plan_id or ""),
        status=status,
        plan_digest=digest,
        diagnostics=diagnostics,
    )

    return PipelinePlanValidationResult(
        plan=updated_plan,
        errors=errors,
        warnings=warnings,
        preflight=preflight,
        compilation_report=report,
    )
