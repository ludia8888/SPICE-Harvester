"""
TaskSpec policy helpers.

Centralizes TaskSpec normalization/clamping so the compiler/transform/validator
can enforce the same "no overreach" contract.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shared.models.pipeline_plan import PipelinePlan
from shared.models.pipeline_task_spec import PipelineTaskIntent, PipelineTaskScope, PipelineTaskSpec
from shared.services.pipeline.pipeline_transform_spec import normalize_operation


_ADVANCED_OPS_LOWER = {
    "groupby",
    "aggregate",
    "pivot",
    "window",
    "explode",
    "sort",
    "union",
}
_MULTI_INPUT_OPS_LOWER = {"join", "union"}


def clamp_task_spec(*, spec: PipelineTaskSpec, dataset_count: int) -> PipelineTaskSpec:
    """
    Clamp a TaskSpec to safe defaults.

    - allow_write is always false (control-plane only).
    - report_only disables all mutating/plan-building capabilities.
    - dataset_count<=1 disables joins.
    """
    update: Dict[str, Any] = {"allow_write": False}
    if spec.scope == PipelineTaskScope.report_only:
        update.update(
            {
                "allow_join": False,
                "allow_cleansing": False,
                "allow_advanced_transforms": False,
                "allow_specs": False,
                "intent": PipelineTaskIntent.null_check if spec.intent == PipelineTaskIntent.unknown else spec.intent,
            }
        )
    if int(dataset_count or 0) <= 1:
        update["allow_join"] = False
    return spec.model_copy(update=update)


def normalize_task_spec(raw: Optional[Dict[str, Any]], *, dataset_count: int) -> Optional[PipelineTaskSpec]:
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        spec = PipelineTaskSpec.model_validate(raw)
    except Exception:
        return None
    return clamp_task_spec(spec=spec, dataset_count=int(dataset_count or 0))


def _iter_transform_ops(definition_json: Dict[str, Any]) -> List[str]:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return []
    ops: List[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_type = str(node.get("type") or "").strip().lower()
        if node_type in {"input", "output", "read_dataset"}:
            continue
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        op = normalize_operation(metadata.get("operation") or (node.get("type") if node_type != "transform" else ""))
        op = str(op or "").strip()
        if op:
            ops.append(op)
    return ops


def validate_plan_against_task_spec(
    *,
    plan: PipelinePlan,
    task_spec: PipelineTaskSpec,
) -> Tuple[List[str], List[str]]:
    """
    Validate a plan against TaskSpec policy (overreach guardrails).

    Returns (errors, warnings). This does not mutate the plan.
    """
    errors: List[str] = []
    warnings: List[str] = []

    spec = task_spec
    if spec.scope == PipelineTaskScope.report_only:
        errors.append("task_spec.scope=report_only forbids pipeline planning/execution")
        return errors, warnings

    allow_join = bool(spec.allow_join)
    allow_advanced = bool(spec.allow_advanced_transforms)

    if not allow_join and list(plan.associations or []):
        errors.append("task_spec.allow_join=false but plan.associations is non-empty")

    ops = _iter_transform_ops(dict(plan.definition_json or {}))
    ops_lower = [str(op).strip().lower() for op in ops if str(op).strip()]

    if not allow_join:
        forbidden = sorted({op for op in ops_lower if op in _MULTI_INPUT_OPS_LOWER})
        if forbidden:
            errors.append(f"task_spec.allow_join=false but plan contains multi-input ops: {forbidden}")

    if not allow_advanced:
        forbidden = sorted({op for op in ops_lower if op in _ADVANCED_OPS_LOWER})
        if forbidden:
            errors.append(f"task_spec.allow_advanced_transforms=false but plan contains advanced ops: {forbidden}")

    return errors, warnings

