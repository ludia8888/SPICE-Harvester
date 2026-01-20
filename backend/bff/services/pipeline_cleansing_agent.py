"""
Pipeline cleansing agent.

Uses preview inspection signals to propose safe cleansing transforms.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from bff.services.pipeline_plan_validation import validate_pipeline_plan
from shared.models.pipeline_plan import PipelinePlan
from shared.services.audit_log_store import AuditLogStore
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.llm_quota import enforce_llm_quota
from shared.services.redis_service import RedisService
from shared.services.pipeline_graph_utils import normalize_edges, normalize_nodes


class PipelineCleansingAction(BaseModel):
    operation: str = Field(..., description="normalize|cast")
    columns: Optional[List[str]] = Field(default=None)
    casts: Optional[List[Dict[str, str]]] = Field(default=None)
    trim: bool = Field(default=True)
    empty_to_null: bool = Field(default=True)
    whitespace_to_null: bool = Field(default=True)
    lowercase: bool = Field(default=False)
    uppercase: bool = Field(default=False)


class PipelineCleansingEnvelope(BaseModel):
    actions: List[PipelineCleansingAction] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineCleansingResult:
    plan: PipelinePlan
    actions_applied: List[Dict[str, Any]]
    validation_errors: List[str]
    validation_warnings: List[str]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None
    confidence: Optional[float] = None


def _build_cleansing_system_prompt() -> str:
    return (
        "You are a STRICT cleansing planner for SPICE-Harvester.\n"
        "Return ONLY JSON. No markdown.\n"
        "Choose cleansing actions ONLY from the inspector suggestions.\n"
        "Supported operations: normalize, cast.\n"
        "Prefer normalize (trim/empty->null) before cast.\n"
        "Do NOT invent columns.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"actions\": [PipelineCleansingAction],\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_cleansing_user_prompt(
    *,
    goal: str,
    inspector: Dict[str, Any],
    max_actions: int,
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Max actions:\n{max_actions}\n\n"
        f"Inspector summary:\n{json.dumps(inspector or {}, ensure_ascii=False)}\n"
    )


def _unique_node_id(base: str, existing: set[str]) -> str:
    candidate = base
    counter = 1
    while candidate in existing:
        counter += 1
        candidate = f"{base}_{counter}"
    return candidate


def _merge_actions(actions: List[PipelineCleansingAction]) -> List[Dict[str, Any]]:
    normalize_columns: List[str] = []
    normalize_flags = {"trim": False, "emptyToNull": False, "whitespaceToNull": False, "lowercase": False, "uppercase": False}
    cast_map: Dict[str, str] = {}
    for action in actions:
        op = str(action.operation or "").strip().lower()
        if op == "normalize":
            for col in action.columns or []:
                col_name = str(col).strip()
                if col_name and col_name not in normalize_columns:
                    normalize_columns.append(col_name)
            normalize_flags["trim"] = normalize_flags["trim"] or bool(action.trim)
            normalize_flags["emptyToNull"] = normalize_flags["emptyToNull"] or bool(action.empty_to_null)
            normalize_flags["whitespaceToNull"] = normalize_flags["whitespaceToNull"] or bool(action.whitespace_to_null)
            normalize_flags["lowercase"] = normalize_flags["lowercase"] or bool(action.lowercase)
            normalize_flags["uppercase"] = normalize_flags["uppercase"] or bool(action.uppercase)
        elif op == "cast":
            for item in action.casts or []:
                col = str(item.get("column") or "").strip()
                target = str(item.get("type") or "").strip()
                if not col or not target:
                    continue
                if col not in cast_map:
                    cast_map[col] = target

    transforms: List[Dict[str, Any]] = []
    if normalize_columns:
        payload = {"operation": "normalize", "columns": normalize_columns}
        payload.update({k: v for k, v in normalize_flags.items() if v})
        transforms.append(payload)
    if cast_map:
        casts = [{"column": col, "type": cast_map[col]} for col in sorted(cast_map.keys())]
        transforms.append({"operation": "cast", "casts": casts})
    return transforms


def _apply_transforms(definition_json: Dict[str, Any], transforms: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    if not transforms:
        return definition_json, []

    nodes_raw = definition_json.get("nodes")
    edges_raw = definition_json.get("edges")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json, ["definition_json has no nodes"]

    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    edges = normalize_edges(edges_raw)
    node_by_id = normalize_nodes(nodes_raw)
    existing_ids = set(node_by_id.keys())

    output_nodes = [node_id for node_id, node in node_by_id.items() if node.get("type") == "output"]
    warnings: List[str] = []
    updated_edges = list(edges)
    updated_nodes = list(nodes)

    for output_id in output_nodes:
        incoming = [edge for edge in updated_edges if edge.get("to") == output_id]
        if len(incoming) != 1:
            warnings.append(f"output {output_id} has {len(incoming)} incoming edges; skipping cleansing inserts")
            continue
        source_id = str(incoming[0].get("from") or "").strip()
        if not source_id:
            continue
        updated_edges = [edge for edge in updated_edges if not (edge.get("from") == source_id and edge.get("to") == output_id)]

        prev_id = source_id
        for transform in transforms:
            base_id = f"cleanse_{transform.get('operation')}_{output_id}"
            node_id = _unique_node_id(base_id, existing_ids)
            existing_ids.add(node_id)
            updated_nodes.append(
                {
                    "id": node_id,
                    "type": "transform",
                    "metadata": transform,
                }
            )
            updated_edges.append({"from": prev_id, "to": node_id})
            prev_id = node_id
        updated_edges.append({"from": prev_id, "to": output_id})

    updated_definition = dict(definition_json)
    updated_definition["nodes"] = updated_nodes
    updated_definition["edges"] = updated_edges
    return updated_definition, warnings


async def apply_cleansing_plan(
    *,
    plan: PipelinePlan,
    inspector: Dict[str, Any],
    max_actions: int,
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    db_name: str,
    branch: Optional[str],
    dataset_registry: Any,
) -> PipelineCleansingResult:
    system_prompt = _build_cleansing_system_prompt()
    user_prompt = _build_cleansing_user_prompt(
        goal=str(plan.goal or ""),
        inspector=inspector,
        max_actions=int(max_actions),
    )
    llm_meta: Optional[LLMCallMeta] = None

    if data_policies and redis_service:
        model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
        if model_for_quota:
            await enforce_llm_quota(
                redis_service=redis_service,
                tenant_id=tenant_id,
                user_id=user_id,
                model_id=model_for_quota,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                data_policies=data_policies,
            )

    try:
        draft, llm_meta = await llm_gateway.complete_json(
            task="PIPELINE_CLEANSING_PLAN_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineCleansingEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id or uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or uuid4()),
            audit_metadata={"kind": "pipeline_cleansing_plan", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        actions = list(draft.actions or [])[: max(0, int(max_actions))]
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    transforms = _merge_actions(actions)
    updated_definition, warnings = _apply_transforms(plan.definition_json, transforms)
    updated_plan = plan.model_copy(update={"definition_json": updated_definition})

    validation = await validate_pipeline_plan(
        plan=updated_plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
        require_output=True,
    )

    applied_actions = [action.model_dump(mode="json") for action in actions]
    return PipelineCleansingResult(
        plan=validation.plan,
        actions_applied=applied_actions,
        validation_errors=list(validation.errors or []),
        validation_warnings=list(validation.warnings or []),
        notes=list(draft.notes or []),
        warnings=list(draft.warnings or []) + warnings,
        llm_meta=llm_meta,
        confidence=float(draft.confidence) if draft is not None else None,
    )
