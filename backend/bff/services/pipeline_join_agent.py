"""
Pipeline join key agent.

Selects join keys from context pack candidates using LLM guidance.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError, model_validator

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


class PipelineJoinSelection(BaseModel):
    left_dataset_id: str = Field(..., min_length=1, max_length=200)
    right_dataset_id: str = Field(..., min_length=1, max_length=200)
    left_column: str = Field(..., min_length=1, max_length=200)
    right_column: str = Field(..., min_length=1, max_length=200)
    left_columns: Optional[List[str]] = None
    right_columns: Optional[List[str]] = None
    join_type: str = Field(default="inner", max_length=40)
    cardinality: Optional[str] = Field(default=None, max_length=40)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reason: Optional[str] = Field(default=None, max_length=500)

    @model_validator(mode="before")
    @classmethod
    def normalize_join_keys(cls, data: Any):  # noqa: ANN001
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        def _split_composite(value: Any) -> Optional[List[str]]:
            if not isinstance(value, str):
                return None
            if "+" not in value:
                return None
            parts = [item.strip() for item in value.split("+") if item.strip()]
            return parts if len(parts) > 1 else None

        if not str(normalized.get("left_column") or "").strip():
            left_on = normalized.get("left_on")
            if isinstance(left_on, list) and left_on:
                normalized["left_column"] = str(left_on[0])
            elif isinstance(left_on, str) and left_on.strip():
                normalized["left_column"] = left_on.strip()
        left_cols = normalized.get("left_columns")
        if not isinstance(left_cols, list) or not left_cols:
            left_cols = normalized.get("left_on")
        if not isinstance(left_cols, list) or not left_cols:
            left_cols = _split_composite(normalized.get("left_column"))
        if isinstance(left_cols, list) and left_cols:
            normalized["left_columns"] = [str(item).strip() for item in left_cols if str(item).strip()]
            if not str(normalized.get("left_column") or "").strip():
                normalized["left_column"] = str(normalized["left_columns"][0])
        if not str(normalized.get("right_column") or "").strip():
            right_on = normalized.get("right_on")
            if isinstance(right_on, list) and right_on:
                normalized["right_column"] = str(right_on[0])
            elif isinstance(right_on, str) and right_on.strip():
                normalized["right_column"] = right_on.strip()
        right_cols = normalized.get("right_columns")
        if not isinstance(right_cols, list) or not right_cols:
            right_cols = normalized.get("right_on")
        if not isinstance(right_cols, list) or not right_cols:
            right_cols = _split_composite(normalized.get("right_column"))
        if isinstance(right_cols, list) and right_cols:
            normalized["right_columns"] = [str(item).strip() for item in right_cols if str(item).strip()]
            if not str(normalized.get("right_column") or "").strip():
                normalized["right_column"] = str(normalized["right_columns"][0])
        return normalized


class PipelineJoinEnvelope(BaseModel):
    joins: List[PipelineJoinSelection] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineJoinResult:
    joins: List[PipelineJoinSelection]
    confidence: Optional[float]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _normalize_key_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    output: List[str] = []
    for item in items:
        if item is None:
            continue
        raw = str(item).strip()
        if not raw:
            continue
        parts = [part.strip() for part in raw.replace("+", ",").split(",") if part.strip()]
        output.extend(parts if parts else [raw])
    return [item for item in output if item]


def _candidate_keys(candidate: Dict[str, Any], side: str) -> List[str]:
    cols = candidate.get(f"{side}_columns")
    if isinstance(cols, list) and cols:
        return [str(item).strip().lower() for item in cols if str(item).strip()]
    col = candidate.get(f"{side}_column")
    return [item.lower() for item in _normalize_key_list(col)]


def _match_join_candidate(
    candidates: List[Dict[str, Any]],
    *,
    left_dataset_id: str,
    right_dataset_id: str,
    left_keys: List[str],
    right_keys: List[str],
) -> tuple[Optional[Dict[str, Any]], bool]:
    left_keys_norm = [key.lower() for key in left_keys]
    right_keys_norm = [key.lower() for key in right_keys]
    for candidate in candidates:
        cand_left_id = str(candidate.get("left_dataset_id") or "").strip()
        cand_right_id = str(candidate.get("right_dataset_id") or "").strip()
        if not cand_left_id or not cand_right_id:
            continue
        cand_left_keys = _candidate_keys(candidate, "left")
        cand_right_keys = _candidate_keys(candidate, "right")
        if cand_left_id == left_dataset_id and cand_right_id == right_dataset_id:
            if cand_left_keys == left_keys_norm and cand_right_keys == right_keys_norm:
                return candidate, False
            if set(cand_left_keys) == set(left_keys_norm) and set(cand_right_keys) == set(right_keys_norm):
                return candidate, False
        if cand_left_id == right_dataset_id and cand_right_id == left_dataset_id:
            if cand_left_keys == right_keys_norm and cand_right_keys == left_keys_norm:
                return candidate, True
            if set(cand_left_keys) == set(right_keys_norm) and set(cand_right_keys) == set(left_keys_norm):
                return candidate, True
    return None, False


def _filter_join_selections(
    selections: List[PipelineJoinSelection],
    candidates: List[Dict[str, Any]],
) -> tuple[List[PipelineJoinSelection], List[str]]:
    if not selections:
        return [], []
    if not candidates:
        return selections, []
    filtered: List[PipelineJoinSelection] = []
    warnings: List[str] = []
    for join in selections:
        left_keys = list(join.left_columns or []) or [join.left_column]
        right_keys = list(join.right_columns or []) or [join.right_column]
        match, swapped = _match_join_candidate(
            candidates,
            left_dataset_id=join.left_dataset_id,
            right_dataset_id=join.right_dataset_id,
            left_keys=left_keys,
            right_keys=right_keys,
        )
        if not match:
            warnings.append(
                f"dropped join {join.left_dataset_id}:{join.left_column} -> "
                f"{join.right_dataset_id}:{join.right_column} (not in candidates)"
            )
            continue
        update: Dict[str, Any] = {}
        if swapped:
            update = {
                "left_dataset_id": join.right_dataset_id,
                "right_dataset_id": join.left_dataset_id,
                "left_column": join.right_column,
                "right_column": join.left_column,
                "left_columns": join.right_columns,
                "right_columns": join.left_columns,
            }
        if not join.cardinality and match.get("cardinality_hint"):
            update["cardinality"] = match.get("cardinality_hint")
        filtered.append(join.model_copy(update=update) if update else join)
    return filtered, warnings


def _fallback_join_candidates(
    candidates: List[Dict[str, Any]],
    *,
    max_joins: int,
) -> List[PipelineJoinSelection]:
    if not candidates:
        return []
    ordered = sorted(candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True)
    selections: List[PipelineJoinSelection] = []
    for candidate in ordered[: max(0, int(max_joins))]:
        left_cols = candidate.get("left_columns") or []
        right_cols = candidate.get("right_columns") or []
        left_col = candidate.get("left_column") or (left_cols[0] if left_cols else None)
        right_col = candidate.get("right_column") or (right_cols[0] if right_cols else None)
        if not left_col or not right_col:
            continue
        try:
            selections.append(
                PipelineJoinSelection(
                    left_dataset_id=str(candidate.get("left_dataset_id") or "").strip(),
                    right_dataset_id=str(candidate.get("right_dataset_id") or "").strip(),
                    left_column=str(left_col),
                    right_column=str(right_col),
                    left_columns=left_cols or None,
                    right_columns=right_cols or None,
                    join_type="inner",
                    cardinality=str(candidate.get("cardinality_hint") or "") or None,
                    confidence=float(candidate.get("score") or 0.0),
                    reason="fallback from join candidates",
                )
            )
        except Exception:
            continue
    return selections


def _build_join_system_prompt() -> str:
    return (
        "You are a STRICT join-key selector for SPICE-Harvester.\n"
        "Return ONLY JSON. No markdown, no commentary.\n"
        "Choose join keys ONLY from the provided join candidates.\n"
        "Do NOT invent dataset ids or columns.\n"
        "If join evaluation feedback is provided, use it to revise low-coverage or exploding joins.\n"
        "Use cardinality_hint when provided and set cardinality on selections.\n"
        "If cardinality_confidence is low or cardinality_note warns, prefer left join unless goal demands inner.\n"
        "Keep the join chain minimal to satisfy the goal.\n"
        "Use left_column/right_column for single-key joins; use left_columns/right_columns arrays for composite keys.\n"
        "If join candidates include composite_group_id, choose that group as a composite join.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"joins\": [PipelineJoinSelection],\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_join_user_prompt(
    *,
    goal: str,
    context_pack: Dict[str, Any],
    max_joins: int,
    feedback: Optional[Dict[str, Any]] = None,
) -> str:
    suggestions: Dict[str, Any] = {}
    datasets: List[Dict[str, Any]] = []
    if isinstance(context_pack, dict):
        suggestions = context_pack.get("integration_suggestions") or {}
        selected = context_pack.get("selected_datasets")
        if isinstance(selected, list):
            for item in selected:
                if not isinstance(item, dict):
                    continue
                cols = [
                    str(col.get("name") or "").strip()
                    for col in (item.get("columns") or [])
                    if isinstance(col, dict) and str(col.get("name") or "").strip()
                ]
                datasets.append(
                    {
                        "dataset_id": item.get("dataset_id"),
                        "name": item.get("name"),
                        "row_count": item.get("row_count"),
                        "columns": cols[:12],
                    }
                )
    payload = {
        "datasets": datasets,
        "join_key_candidates": suggestions.get("join_key_candidates") if isinstance(suggestions, dict) else None,
        "foreign_key_candidates": suggestions.get("foreign_key_candidates") if isinstance(suggestions, dict) else None,
        "feedback": feedback or {},
    }
    return (
        f"Goal:\n{goal}\n\n"
        f"Max joins:\n{max_joins}\n\n"
        f"Join candidates (include format/cardinality/containment signals):\n{json.dumps(payload, ensure_ascii=False)}\n"
    )


async def select_join_keys(
    *,
    goal: str,
    context_pack: Dict[str, Any],
    max_joins: int,
    feedback: Optional[Dict[str, Any]] = None,
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
) -> PipelineJoinResult:
    suggestions = context_pack.get("integration_suggestions") if isinstance(context_pack, dict) else {}
    candidates = []
    if isinstance(suggestions, dict) and isinstance(suggestions.get("join_key_candidates"), list):
        candidates = [item for item in suggestions.get("join_key_candidates") if isinstance(item, dict)]

    system_prompt = _build_join_system_prompt()
    user_prompt = _build_join_user_prompt(goal=goal, context_pack=context_pack, max_joins=max_joins, feedback=feedback)
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
            task="PIPELINE_JOIN_KEYS_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineJoinEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_join:{uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(uuid4()),
            audit_metadata={"kind": "pipeline_join_keys", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        joins = list(draft.joins or [])[: max(0, int(max_joins))]
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    filtered, filter_warnings = _filter_join_selections(joins, candidates)
    warnings = list(draft.warnings or [])
    warnings.extend(filter_warnings)
    if not filtered and candidates:
        filtered = _fallback_join_candidates(candidates, max_joins=max_joins)
        if filtered:
            warnings.append("join selections invalid; using top join candidates as fallback")

    return PipelineJoinResult(
        joins=filtered,
        confidence=float(draft.confidence) if draft is not None else None,
        notes=list(draft.notes or []),
        warnings=warnings,
        llm_meta=llm_meta,
    )
