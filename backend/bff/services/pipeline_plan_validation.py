"""
Pipeline plan validation against pipeline definition rules + preflight.

Uses existing pipeline definition validation/preflight utilities and returns
machine-readable diagnostics for planners/repair loops.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.models.agent_plan_report import PlanCompilationReport, PlanDiagnostic, PlanDiagnosticSeverity
from shared.models.pipeline_plan import PipelinePlan
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_graph_utils import normalize_edges, normalize_nodes
from shared.utils.canonical_json import sha256_canonical_json_prefixed

from bff.routers import pipeline as pipeline_router


@dataclass(frozen=True)
class PipelinePlanValidationResult:
    plan: PipelinePlan
    errors: List[str]
    warnings: List[str]
    preflight: Dict[str, Any]
    compilation_report: PlanCompilationReport


def _ensure_output_nodes(definition_json: Dict[str, Any], outputs: List[Any]) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    edges = normalize_edges(definition_json.get("edges"))
    node_by_id = normalize_nodes(nodes)
    if not node_by_id:
        return definition_json
    existing_ids = set(node_by_id.keys())
    output_nodes = [node for node in nodes if node.get("type") == "output"]
    outgoing = {edge["from"] for edge in edges if edge.get("from")}
    sink_ids = [
        node_id
        for node_id, node in node_by_id.items()
        if node.get("type") != "output" and node_id not in outgoing
    ]
    if not sink_ids:
        sink_ids = [list(node_by_id.keys())[-1]]
    if not sink_ids:
        return definition_json

    node_order = [node.get("id") for node in nodes if node.get("id")]
    ordered_sinks = [node_id for node_id in node_order if node_id in set(sink_ids)] or sink_ids
    updated = False

    def attach_output(output_id: str, *, index: int) -> None:
        nonlocal updated
        if not output_id:
            return
        if any(edge.get("to") == output_id for edge in edges):
            return
        attach_to = ordered_sinks[index] if index < len(ordered_sinks) else ordered_sinks[-1]
        if attach_to and attach_to != output_id:
            edges.append({"from": attach_to, "to": output_id})
            updated = True

    if output_nodes:
        for idx, output_node in enumerate(output_nodes):
            attach_output(str(output_node.get("id") or "").strip(), index=idx)

        existing_names: set[str] = set()
        for node in output_nodes:
            node_id = str(node.get("id") or "").strip()
            metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            name = str(metadata.get("outputName") or node.get("title") or node_id or "").strip()
            if name:
                existing_names.add(name)

        for idx, output in enumerate(outputs):
            output_name = str(getattr(output, "output_name", "") or "").strip()
            if not output_name or output_name in existing_names:
                continue
            base_id = re.sub(r"[^A-Za-z0-9_]+", "_", f"output_{output_name}").strip("_") or "output"
            output_id = pipeline_router._unique_node_id(base_id, existing_ids)
            existing_ids.add(output_id)
            nodes.append({"id": output_id, "type": "output", "metadata": {"outputName": output_name}})
            attach_output(output_id, index=idx)
        if not updated:
            return definition_json
    else:
        if not outputs:
            return definition_json
        for idx, output in enumerate(outputs):
            output_name = str(getattr(output, "output_name", "") or "").strip()
            if not output_name:
                continue
            base_id = re.sub(r"[^A-Za-z0-9_]+", "_", f"output_{output_name}").strip("_") or "output"
            output_id = pipeline_router._unique_node_id(base_id, existing_ids)
            existing_ids.add(output_id)
            nodes.append({"id": output_id, "type": "output", "metadata": {"outputName": output_name}})
            attach_output(output_id, index=idx)
        if not updated:
            return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    updated_definition["edges"] = edges
    return updated_definition


def _normalize_join_nodes(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    updated = False
    for node in nodes:
        if str(node.get("type") or "").strip().lower() != "join":
            continue
        node["type"] = "transform"
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        if str(metadata.get("operation") or "").strip().lower() != "join":
            metadata = dict(metadata)
            metadata["operation"] = "join"
            node["metadata"] = metadata
        updated = True
    if not updated:
        return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    return updated_definition


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
    normalized = _normalize_join_nodes(definition_json)
    normalized = await pipeline_router._augment_definition_with_casts(
        definition_json=normalized,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    normalized = _ensure_output_nodes(normalized, plan.outputs or [])
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
