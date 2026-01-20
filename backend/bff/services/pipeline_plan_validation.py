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

    definition_json = dict(plan.definition_json or {})
    normalized = await pipeline_router._augment_definition_with_casts(
        definition_json=definition_json,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    normalized = pipeline_router._augment_definition_with_canonical_contract(
        definition_json=normalized,
        branch=branch,
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
