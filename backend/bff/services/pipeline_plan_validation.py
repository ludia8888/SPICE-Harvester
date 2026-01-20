"""
Pipeline plan validation against pipeline definition rules + preflight.

Uses existing pipeline definition validation/preflight utilities and returns
machine-readable diagnostics for planners/repair loops.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.models.agent_plan_report import PlanCompilationReport, PlanDiagnostic, PlanDiagnosticSeverity
from shared.models.pipeline_plan import PipelinePlan
from shared.services.dataset_registry import DatasetRegistry
from shared.utils.canonical_json import sha256_canonical_json_prefixed

from bff.routers import pipeline as pipeline_router


@dataclass(frozen=True)
class PipelinePlanValidationResult:
    plan: PipelinePlan
    errors: List[str]
    warnings: List[str]
    preflight: Dict[str, Any]
    compilation_report: PlanCompilationReport


async def validate_pipeline_plan(
    *,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
    require_output: bool = True,
) -> PipelinePlanValidationResult:
    errors: List[str] = []
    warnings: List[str] = []

    for output in plan.outputs or []:
        kind = str(getattr(output.output_kind, "value", output.output_kind) or "unknown").strip().lower()
        if kind == "object":
            if not output.target_class_id:
                errors.append(f"output {output.output_name}: target_class_id is required for object outputs")
        elif kind == "link":
            missing: List[str] = []
            if not output.link_type_id:
                missing.append("link_type_id")
            if not output.source_class_id:
                missing.append("source_class_id")
            if not output.target_class_id:
                missing.append("target_class_id")
            if not output.predicate:
                missing.append("predicate")
            if not output.cardinality:
                missing.append("cardinality")
            if not output.source_key_column:
                missing.append("source_key_column")
            if not output.target_key_column:
                missing.append("target_key_column")
            if not output.relationship_spec_type:
                missing.append("relationship_spec_type")
            if missing:
                errors.append(
                    f"output {output.output_name}: missing required link metadata ({', '.join(missing)})"
                )
        else:
            warnings.append(f"output {output.output_name}: output_kind is unknown")

    definition_json = dict(plan.definition_json or {})
    canonical_output_names = {
        output.output_name
        for output in (plan.outputs or [])
        if str(getattr(output.output_kind, "value", output.output_kind) or "").strip().lower() in {"object", "link"}
        and str(output.output_name or "").strip()
    }
    normalized = await pipeline_router._augment_definition_with_casts(
        definition_json=definition_json,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    normalized = pipeline_router._augment_definition_with_canonical_contract(
        definition_json=normalized,
        branch=branch,
        canonical_output_names=canonical_output_names or None,
    )
    if normalized != definition_json:
        warnings.append("definition_json auto-augmented with casts/canonical contract")

    errors.extend(
        pipeline_router._validate_pipeline_definition(
            definition_json=normalized,
            require_output=require_output,
        )
    )

    preflight = await pipeline_router._run_pipeline_preflight(
        definition_json=normalized,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    if preflight.get("has_blocking_errors"):
        errors.append("pipeline preflight has blocking errors")

    updated_plan = plan.model_copy(update={"definition_json": normalized})
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
