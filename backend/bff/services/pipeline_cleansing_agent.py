"""
Pipeline cleansing agent.

Uses preview inspection signals to propose safe cleansing transforms.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from bff.services.pipeline_plan_validation import validate_pipeline_plan
from bff.services.pipeline_cleansing_utils import apply_cleansing_transforms
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
from shared.services.pipeline_preflight_utils import compute_schema_by_node

logger = logging.getLogger(__name__)


class PipelineRegexRule(BaseModel):
    column: str = Field(..., min_length=1, max_length=200)
    pattern: str = Field(..., min_length=1, max_length=500)
    replacement: str = Field(default="")
    flags: Optional[str] = Field(default=None, max_length=10)


class PipelineCleansingAction(BaseModel):
    operation: str = Field(..., description="normalize|cast|dedupe|regexReplace")
    column: Optional[str] = Field(default=None)
    columns: Optional[List[str]] = Field(default=None)
    casts: Optional[List[Dict[str, str]]] = Field(default=None)
    target_type: Optional[str] = Field(default=None)
    rules: Optional[List[PipelineRegexRule]] = Field(default=None)
    pattern: Optional[str] = Field(default=None)
    replacement: Optional[str] = Field(default=None)
    flags: Optional[str] = Field(default=None)
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
        "Supported operations: normalize, cast, dedupe, regexReplace.\n"
        "Prefer normalize (trim/empty->null) before cast.\n"
        "Do NOT invent columns.\n"
        "If planner_hints.null_strategy is \"keep_as_null\", keep nulls and use empty_to_null/whitespace_to_null.\n"
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
    planner_hints: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Max actions:\n{max_actions}\n\n"
        f"Inspector summary:\n{json.dumps(inspector or {}, ensure_ascii=False)}\n"
        f"Planner hints:\n{json.dumps(planner_hints or {}, ensure_ascii=False)}\n"
    )


def _resolve_null_strategy(planner_hints: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(planner_hints, dict):
        return None
    value = str(planner_hints.get("null_strategy") or "").strip().lower()
    return value or None


def _normalize_columns_from_inspector(inspector: Dict[str, Any]) -> List[str]:
    suggestions = inspector.get("suggestions") if isinstance(inspector, dict) else None
    if not isinstance(suggestions, list):
        return []
    columns: List[str] = []
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        if str(item.get("operation") or "").strip().lower() != "normalize":
            continue
        col = str(item.get("column") or "").strip()
        if col and col not in columns:
            columns.append(col)
    return columns


def _merge_actions(
    actions: List[PipelineCleansingAction],
    *,
    null_strategy: Optional[str],
) -> List[Dict[str, Any]]:
    normalize_columns: List[str] = []
    normalize_flags = {"trim": False, "emptyToNull": False, "whitespaceToNull": False, "lowercase": False, "uppercase": False}
    cast_map: Dict[str, str] = {}
    dedupe_columns: Optional[List[str]] = None
    regex_rules: List[Dict[str, Any]] = []
    for action in actions:
        op = str(action.operation or "").strip().lower()
        if op == "normalize":
            normalize_targets = list(action.columns or [])
            if action.column:
                normalize_targets.append(action.column)
            for col in normalize_targets:
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
            if action.column and action.target_type:
                col_name = str(action.column).strip()
                target = str(action.target_type).strip()
                if col_name and target and col_name not in cast_map:
                    cast_map[col_name] = target
        elif op == "dedupe":
            cols = [str(col).strip() for col in (action.columns or []) if str(col).strip()]
            if dedupe_columns is None:
                dedupe_columns = cols
            elif not dedupe_columns:
                continue
            else:
                for col in cols:
                    if col not in dedupe_columns:
                        dedupe_columns.append(col)
        elif op == "regexreplace":
            rules = action.rules or []
            if rules:
                for rule in rules:
                    regex_rules.append(rule.model_dump(mode="json"))
                continue
            pattern = str(action.pattern or "").strip()
            if not pattern:
                continue
            columns = [str(col).strip() for col in (action.columns or []) if str(col).strip()]
            if action.column:
                columns.append(str(action.column).strip())
            for col in columns:
                regex_rules.append(
                    {
                        "column": col,
                        "pattern": pattern,
                        "replacement": str(action.replacement or ""),
                        "flags": action.flags,
                    }
                )

    if null_strategy == "keep_as_null":
        normalize_flags["emptyToNull"] = True
        normalize_flags["whitespaceToNull"] = True

    transforms: List[Dict[str, Any]] = []
    if normalize_columns:
        payload = {"operation": "normalize", "columns": normalize_columns}
        payload.update({k: v for k, v in normalize_flags.items() if v})
        transforms.append(payload)
    if regex_rules:
        transforms.append({"operation": "regexReplace", "rules": regex_rules})
    if cast_map:
        casts = [{"column": col, "type": cast_map[col]} for col in sorted(cast_map.keys())]
        transforms.append({"operation": "cast", "casts": casts})
    if dedupe_columns:
        transforms.append({"operation": "dedupe", "columns": dedupe_columns})
    return transforms


async def apply_cleansing_plan(
    *,
    plan: PipelinePlan,
    inspector: Dict[str, Any],
    max_actions: int,
    planner_hints: Optional[Dict[str, Any]],
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
        planner_hints=planner_hints,
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

    enforcement_warnings: List[str] = []
    null_strategy = _resolve_null_strategy(planner_hints)
    if null_strategy == "keep_as_null":
        has_normalize = any(str(action.operation or "").strip().lower() == "normalize" for action in actions)
        if not has_normalize:
            normalize_columns = _normalize_columns_from_inspector(inspector)
            if normalize_columns:
                injected = PipelineCleansingAction(
                    operation="normalize",
                    columns=normalize_columns,
                    trim=True,
                    empty_to_null=True,
                    whitespace_to_null=True,
                )
                actions = [injected] + actions
                if max_actions > 0 and len(actions) > max_actions:
                    actions = actions[: max_actions]
                    enforcement_warnings.append("null_strategy enforced; extra cleansing actions were dropped")

    transforms = _merge_actions(actions, null_strategy=null_strategy)
    schema_by_node = None
    try:
        schema_by_node = await compute_schema_by_node(
            definition=plan.definition_json,
            db_name=db_name,
            dataset_registry=dataset_registry,
            branch=branch,
        )
    except Exception as exc:
        logger.warning("Cleansing schema analysis failed: %s", exc)
    updated_definition, warnings = apply_cleansing_transforms(
        plan.definition_json,
        transforms,
        schema_by_node=schema_by_node,
    )
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
        warnings=list(draft.warnings or []) + warnings + enforcement_warnings,
        llm_meta=llm_meta,
        confidence=float(draft.confidence) if draft is not None else None,
    )
