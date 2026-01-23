"""
Pipeline intent verifier.

Uses LLM to verify that a pipeline plan matches the user's intent.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

from bff.services.pipeline_plan_compiler import PipelineClarificationQuestion
from shared.models.pipeline_plan import PipelinePlan
from shared.models.pipeline_task_spec import PipelineTaskSpec
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


class PipelineIntentCheckEnvelope(BaseModel):
    status: str = Field(default="pass", max_length=40)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    missing_requirements: List[str] = Field(default_factory=list)
    suggested_actions: List[str] = Field(default_factory=list)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineIntentCheckResult:
    status: str  # pass|needs_revision|clarification_required
    confidence: Optional[float]
    missing_requirements: List[str]
    suggested_actions: List[str]
    questions: List[PipelineClarificationQuestion]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


def _build_intent_system_prompt() -> str:
    return (
        "You are a STRICT (but goal-literal) pipeline intent verifier for SPICE-Harvester.\n"
        "Return ONLY JSON. No markdown, no commentary.\n"
        "Evaluate whether the plan definition satisfies the goal.\n"
        "\n"
        "Rules:\n"
        "- The goal is the ONLY source of hard requirements. Do NOT invent new requirements.\n"
        "- Do NOT require extra columns (e.g., join keys) unless the goal explicitly asks for them.\n"
        "- A join key mentioned in the goal (e.g., \"JOIN on customer_id\") is NOT an output-column requirement.\n"
        "- If the goal explicitly lists output columns, treat that list as the required output (do not add to it).\n"
        "- If you think an improvement is helpful but not required, put it in warnings (not missing_requirements).\n"
        "- Use status=pass only if all goal requirements are satisfied.\n"
        "- Use status=needs_revision if requirements are missing but can be inferred directly from the goal.\n"
        "- Use status=clarification_required only if the goal is ambiguous.\n"
        "- If you return needs_revision, list missing_requirements and suggested_actions.\n"
        "- If you return clarification_required, provide questions.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"status\": \"pass|needs_revision|clarification_required\",\n"
        "  \"confidence\": number (0..1),\n"
        "  \"missing_requirements\": string[],\n"
        "  \"suggested_actions\": string[],\n"
        "  \"questions\": [PipelineClarificationQuestion],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_intent_user_prompt(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    definition_json: Dict[str, Any],
    outputs: List[Dict[str, Any]],
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(data_scope or {}, ensure_ascii=False)}\n\n"
        f"Definition JSON:\n{json.dumps(definition_json or {}, ensure_ascii=False)}\n\n"
        f"Outputs:\n{json.dumps(outputs or [], ensure_ascii=False)}\n\n"
        f"Join plan:\n{json.dumps(join_plan or [], ensure_ascii=False)}\n\n"
        f"Cleansing hints:\n{json.dumps(cleansing_hints or [], ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


def _extract_join_columns(join_plan: Optional[List[Dict[str, Any]]]) -> List[str]:
    cols: List[str] = []
    if not isinstance(join_plan, list):
        return cols
    for item in join_plan:
        if not isinstance(item, dict):
            continue
        for key in ("left_column", "right_column"):
            value = str(item.get(key) or "").strip()
            if value:
                cols.append(value)
        for key in ("left_keys", "right_keys"):
            values = item.get(key)
            if isinstance(values, list):
                for v in values:
                    name = str(v or "").strip()
                    if name:
                        cols.append(name)
    # preserve order, dedupe (case-insensitive)
    seen: set[str] = set()
    out: List[str] = []
    for col in cols:
        lowered = col.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(col)
    return out


def _looks_like_join_key_output_requirement(message: str, join_columns: List[str]) -> bool:
    text = str(message or "").strip()
    if not text:
        return False
    lowered = text.lower()
    includeish = any(token in lowered for token in ("must include", "needs to include", "should include", "include", "add"))
    if includeish and any(token in lowered for token in ("join key", "join-key", "join operation", "part of the join")):
        return True
    if (
        includeish
        and join_columns
        and any(col.lower() in lowered for col in join_columns)
        and any(token in lowered for token in ("output", "select", "column", "columns"))
    ):
        return True
    return False


def _allow_join_key_optional_for_goal(plan: PipelinePlan) -> bool:
    # If the user asked for ontology mapping/specs or a writeback, join keys often become required.
    task_spec: PipelineTaskSpec | None = getattr(plan, "task_spec", None)
    if task_spec is not None:
        if bool(getattr(task_spec, "allow_specs", False)):
            return False
        if bool(getattr(task_spec, "allow_write", False)):
            return False
    output_kinds = {
        str(getattr(output.output_kind, "value", output.output_kind) or "").strip().lower()
        for output in (plan.outputs or [])
    }
    if output_kinds.intersection({"object", "link"}):
        return False
    return True


def _extract_goal_output_columns(goal: str) -> List[str]:
    """
    Best-effort extraction of an explicit output column list from the goal.
    We only use this to avoid suppressing join-key requirements when the user explicitly asked for them.
    """
    text = str(goal or "")
    # Prefer the last mention of "... 컬럼" / "... columns" as it tends to describe output shape.
    matches = list(
        re.finditer(
            r"(?P<cols>(?:[A-Za-z_][A-Za-z0-9_]*\s*,\s*)*[A-Za-z_][A-Za-z0-9_]*)\s*(?:컬럼|columns?)",
            text,
            flags=re.IGNORECASE,
        )
    )
    if not matches:
        return []
    segment = matches[-1].group("cols") or ""
    cols = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", segment)
    # preserve order, dedupe (case-insensitive)
    seen: set[str] = set()
    out: List[str] = []
    for c in cols:
        cl = c.lower()
        if cl in seen:
            continue
        seen.add(cl)
        out.append(c)
    return out


def _extract_selected_columns(definition_json: Dict[str, Any]) -> List[str]:
    cols: List[str] = []
    if not isinstance(definition_json, dict):
        return cols
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return cols
    for node in nodes:
        if not isinstance(node, dict):
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            continue
        op = str(metadata.get("operation") or "").strip().lower()
        if op not in {"select", "project"}:
            continue
        values = metadata.get("columns")
        if not isinstance(values, list):
            continue
        for v in values:
            name = str(v or "").strip()
            if name:
                cols.append(name)
    # preserve order, dedupe (case-insensitive)
    seen: set[str] = set()
    out: List[str] = []
    for col in cols:
        lowered = col.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(col)
    return out


def _extract_mentioned_columns(message: str) -> List[str]:
    """
    Best-effort extraction of column identifiers from LLM messages.
    Prefer quoted identifiers ('col', "col", `col`).
    """
    text = str(message or "")
    cols: List[str] = []

    for match in re.finditer(r"(?:'([^']+)'|\"([^\"]+)\"|`([^`]+)`)", text):
        value = next((g for g in match.groups() if g), "")
        value = str(value or "").strip()
        if value:
            cols.append(value)

    if cols:
        # preserve order, dedupe (case-insensitive)
        seen: set[str] = set()
        out: List[str] = []
        for col in cols:
            lowered = col.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            out.append(col)
        return out

    # Fallback: parse simple comma-separated identifiers after "include" / "columns"
    lowered = text.lower()
    if "column" not in lowered and "columns" not in lowered:
        return []
    if "include" not in lowered and "contain" not in lowered:
        return []
    candidates = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text)
    stop = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "to",
        "of",
        "in",
        "on",
        "output",
        "outputs",
        "column",
        "columns",
        "include",
        "includes",
        "including",
        "contain",
        "contains",
        "must",
        "should",
        "need",
        "needs",
    }
    out: List[str] = []
    seen: set[str] = set()
    for c in candidates:
        cl = c.lower()
        if cl in stop:
            continue
        if cl in seen:
            continue
        seen.add(cl)
        out.append(c)
    return out


def _looks_like_output_column_requirement(message: str) -> bool:
    text = str(message or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if "column" not in lowered and "columns" not in lowered:
        return False
    includeish = any(token in lowered for token in ("must include", "needs to include", "should include", "include", "includes", "including"))
    containish = any(token in lowered for token in ("contain", "contains"))
    missingish = any(token in lowered for token in ("missing", "not present", "absent", "not in the output", "not in the output definition"))
    return includeish or containish or missingish


def _columns_satisfied(required: List[str], selected: List[str]) -> bool:
    if not required:
        return False
    selected_l = {c.lower() for c in (selected or [])}
    return all(str(c or "").strip().lower() in selected_l for c in required)


async def verify_pipeline_intent(
    *,
    plan: PipelinePlan,
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
) -> PipelineIntentCheckResult:
    system_prompt = _build_intent_system_prompt()
    user_prompt = _build_intent_user_prompt(
        goal=str(plan.goal or ""),
        data_scope=plan.data_scope.model_dump(mode="json"),
        definition_json=plan.definition_json,
        outputs=[output.model_dump(mode="json") for output in (plan.outputs or [])],
        join_plan=join_plan,
        cleansing_hints=cleansing_hints,
        context_pack=context_pack,
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
            task="PIPELINE_INTENT_CHECK_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineIntentCheckEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or ""),
            audit_metadata={"kind": "pipeline_intent_check", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        status = str(draft.status or "pass").strip().lower()
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    if status not in {"pass", "needs_revision", "clarification_required"}:
        status = "needs_revision"

    missing_requirements = list(draft.missing_requirements or [])
    suggested_actions = list(draft.suggested_actions or [])
    warnings = list(draft.warnings or [])

    if status == "needs_revision" and missing_requirements:
        join_columns = _extract_join_columns(join_plan)
        goal_output_cols = _extract_goal_output_columns(str(plan.goal or ""))
        goal_output_cols_l = {c.lower() for c in goal_output_cols}
        goal_explicitly_requires_join_key = any(col.lower() in goal_output_cols_l for col in join_columns)
        selected_cols = _extract_selected_columns(plan.definition_json or {})

        new_missing: List[str] = []
        suppressed_missing: List[str] = []

        for msg in missing_requirements:
            # Guardrail: for simple dataset outputs (no specs/write/mapping), do not force join keys into the output
            if (
                _allow_join_key_optional_for_goal(plan)
                and not goal_explicitly_requires_join_key
                and _looks_like_join_key_output_requirement(msg, join_columns)
            ):
                suppressed_missing.append(f"Optional (not required by goal): {msg}")
                continue

            # Guardrail: if the plan already selects the columns that the verifier claims are missing, suppress.
            if selected_cols and _looks_like_output_column_requirement(msg):
                required_cols = _extract_mentioned_columns(msg)
                if required_cols and _columns_satisfied(required_cols, selected_cols):
                    suppressed_missing.append(f"Verifier false-positive suppressed: {msg}")
                    continue

            new_missing.append(msg)

        if suppressed_missing:
            warnings.extend(suppressed_missing)
            missing_requirements = new_missing

        if suggested_actions:
            new_actions: List[str] = []
            suppressed_actions: List[str] = []
            for msg in suggested_actions:
                if (
                    _allow_join_key_optional_for_goal(plan)
                    and not goal_explicitly_requires_join_key
                    and _looks_like_join_key_output_requirement(msg, join_columns)
                ):
                    suppressed_actions.append(f"Optional (not required by goal): {msg}")
                    continue
                if selected_cols and _looks_like_output_column_requirement(msg):
                    required_cols = _extract_mentioned_columns(msg)
                    if required_cols and _columns_satisfied(required_cols, selected_cols):
                        suppressed_actions.append(f"Verifier false-positive suppressed: {msg}")
                        continue
                new_actions.append(msg)
            if suppressed_actions:
                warnings.extend(suppressed_actions)
                suggested_actions = new_actions

        if not missing_requirements:
            status = "pass"

    return PipelineIntentCheckResult(
        status=status,
        confidence=float(draft.confidence) if draft is not None else None,
        missing_requirements=missing_requirements,
        suggested_actions=suggested_actions,
        questions=list(draft.questions or []),
        warnings=warnings,
        llm_meta=llm_meta,
    )
