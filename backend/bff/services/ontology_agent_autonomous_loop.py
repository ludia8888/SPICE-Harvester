"""
Autonomous Ontology Agent loop (single agent + tools).

This is a *single* LLM loop that iteratively:
  BuildPrompt -> Inference -> ToolRequested -> ExecuteTool -> AppendObservation -> ...

Enables natural language ontology schema creation/mapping:
- "이 데이터셋으로 Customer 클래스 만들어줘"
- "email, name, phone 필드를 기존 Person 클래스에 매핑해줘"
- "Customer와 Order 사이에 hasOrders 관계 만들어줘"
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from bff.services.ontology_agent_models import (
    AutonomousOntologyAgentDecision,
    AutonomousOntologyAgentToolCall,
    OntologyClarificationQuestion,
    OntologyAgentState,
)
from shared.services.agent.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.agent.llm_quota import enforce_llm_quota
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.storage.redis_service import RedisService
from shared.utils.llm_safety import mask_pii, stable_json_dumps

logger = logging.getLogger(__name__)


_ONTOLOGY_AGENT_ALLOWED_TOOLS: tuple[str, ...] = (
    # Initialization
    "ontology_new",
    "ontology_load",
    "ontology_reset",
    # Class metadata
    "ontology_set_class_meta",
    "ontology_set_abstract",
    # Property management
    "ontology_add_property",
    "ontology_update_property",
    "ontology_remove_property",
    "ontology_set_primary_key",
    # Relationship management
    "ontology_add_relationship",
    "ontology_update_relationship",
    "ontology_remove_relationship",
    # Schema inference
    "ontology_infer_schema_from_data",
    "ontology_suggest_mappings",
    # Validation
    "ontology_validate",
    "ontology_check_relationships",
    "ontology_check_circular_refs",
    # Query
    "ontology_list_classes",
    "ontology_get_class",
    "ontology_search_classes",
    # Save
    "ontology_create",
    "ontology_update",
    "ontology_preview",
)


def _tool_alias(tool_name: str) -> str:
    """Convert a tool name into a short alias for intra-batch reference resolution."""
    name = str(tool_name or "").strip()
    if name.startswith("ontology_"):
        return name[len("ontology_") :]
    return name


def _drop_none_values(value: Any) -> Any:
    """Remove None values from dicts/lists to avoid MCP JSON schema errors."""
    if isinstance(value, dict):
        return {k: _drop_none_values(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_none_values(v) for v in value if v is not None]
    return value


def _resolve_batch_placeholders(
    value: Any,
    *,
    last: Optional[Dict[str, Any]],
    last_by_alias: Dict[str, Dict[str, Any]],
) -> Any:
    """
    Resolve lightweight placeholders inside a *single batch* of tool calls.

    Supported forms (strings only, must start with '$'):
      - $last.<field>
      - $last.<tool_alias>.<field>
    """
    if isinstance(value, dict):
        return {k: _resolve_batch_placeholders(v, last=last, last_by_alias=last_by_alias) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_batch_placeholders(v, last=last, last_by_alias=last_by_alias) for v in value]
    if not isinstance(value, str):
        return value
    if not value.startswith("$"):
        return value

    raw = value.strip()
    if not raw.startswith("$last."):
        return value

    parts = raw.split(".")
    if len(parts) < 2:
        return value

    base: Optional[Dict[str, Any]] = None
    path: List[str] = []
    if len(parts) == 2:
        base = last
        path = [parts[1]]
    else:
        alias = parts[1]
        base = last_by_alias.get(alias)
        path = parts[2:]
    if base is None:
        raise KeyError(raw)

    cur = base
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            raise KeyError(raw)
    return cur


def _mask_tool_observation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Mask PII in tool observations."""
    return mask_pii(payload or {}, max_string_chars=200)


def _summarize_ontology(ontology: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a summary of the working ontology for prompts."""
    if not isinstance(ontology, dict):
        return {"initialized": False}
    return {
        "initialized": True,
        "id": ontology.get("id"),
        "label": ontology.get("label"),
        "parent_class": ontology.get("parent_class"),
        "abstract": ontology.get("abstract", False),
        "property_count": len(ontology.get("properties") or []),
        "relationship_count": len(ontology.get("relationships") or []),
        "properties": [
            {"name": p.get("name"), "type": p.get("type"), "primary_key": p.get("primary_key", False)}
            for p in (ontology.get("properties") or [])[:20]
        ],
        "relationships": [
            {"predicate": r.get("predicate"), "target": r.get("target")}
            for r in (ontology.get("relationships") or [])[:10]
        ],
    }


def _build_system_prompt(*, allowed_tools: List[str]) -> str:
    """Build the system prompt for the ontology agent."""
    tool_lines = "\n".join([f"- {name}" for name in allowed_tools])
    return (
        "You are an autonomous ontology-building agent for SPICE-Harvester.\n"
        "You can iteratively call tools to create/modify ontology schemas based on natural language instructions.\n"
        "The user prompt is an append-only JSONL log (one JSON object per line). The newest lines are the most recent state/observations.\n"
        "\n"
        "Core rules:\n"
        "- Return ONLY JSON matching the schema. No markdown, no extra text.\n"
        "- Do NOT invent data; prefer tool observations over guessing.\n"
        "- Each tool call must include a session_id parameter (use the one from the header).\n"
        "- Follow the user's goal to build or modify ontology classes.\n"
        "- Use ontology_infer_schema_from_data when sample data is provided to determine types.\n"
        "- Use ontology_suggest_mappings when mapping fields to an existing class.\n"
        "- Use ontology_list_classes or ontology_search_classes to find existing classes.\n"
        "- Validate the ontology with ontology_validate before saving.\n"
        "- Use ontology_create for new classes, ontology_update for existing ones.\n"
        "- Batching: prefer multiple tool calls in a single response via `tool_calls` (target 4-8).\n"
        "\n"
        "Common patterns:\n"
        "- Create new class: ontology_new -> ontology_add_property (multiple) -> ontology_validate -> ontology_create\n"
        "- Modify existing class: ontology_load -> ontology_add_property/ontology_update_property -> ontology_validate -> ontology_update\n"
        "- Map data to class: ontology_infer_schema_from_data -> ontology_suggest_mappings -> apply mappings\n"
        "- Add relationship: ontology_add_relationship(predicate, target, label, cardinality)\n"
        "\n"
        f"Available tools:\n{tool_lines}\n"
        "\n"
        "Schema:\n"
        "{\n"
        '  "action": "call_tool|finish|clarify",\n'
        '  "tool_calls": [{"tool": "string", "args": {"...": "..."}}],\n'
        '  "tool": "string (legacy single tool; optional if tool_calls is set)",\n'
        '  "args": {"...": "..."},\n'
        '  "questions": [OntologyClarificationQuestion],\n'
        '  "notes": string[],\n'
        '  "warnings": string[],\n'
        '  "confidence": number (0..1)\n'
        "}\n"
    )


def _prompt_text(items: List[str]) -> str:
    """Convert prompt items to text."""
    if not items:
        return ""
    return "\n".join([str(item) for item in items]) + "\n"


def _build_prompt_header(
    *,
    state: OntologyAgentState,
    session_id: str,
    answers: Optional[Dict[str, Any]],
    dataset_sample: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build the initial prompt header."""
    return {
        "type": "header",
        "goal": state.goal,
        "session_id": session_id,
        "db_name": state.db_name,
        "branch": state.branch,
        "target_class_id": state.target_class_id,
        "answers": answers or None,
        "dataset_sample": dataset_sample,
    }


def _is_internal_budget_clarification(questions: List[OntologyClarificationQuestion]) -> bool:
    """Check if clarification is about internal budget (reject it)."""
    for q in questions or []:
        text = f"{q.id} {q.question}".lower()
        if any(token in text for token in ("step", "steps", "budget", "limit", "quota", "스텝", "단계", "한도")):
            return True
    return False


async def run_ontology_agent_autonomous(
    *,
    goal: str,
    db_name: str,
    branch: str,
    target_class_id: Optional[str],
    dataset_sample: Optional[Dict[str, Any]],
    answers: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
) -> Dict[str, Any]:
    """
    Run the autonomous ontology agent loop.

    Returns a payload for the API response.
    """
    run_id = str(uuid4())
    session_id = f"ontology_{run_id}"

    db_name = str(db_name or "").strip()
    if not db_name:
        return {
            "run_id": run_id,
            "status": "clarification_required",
            "ontology": None,
            "questions": [
                {
                    "id": "db_name",
                    "question": "Which database (db_name) should I use?",
                    "required": True,
                    "type": "string",
                }
            ],
            "validation_errors": ["db_name is required"],
            "validation_warnings": [],
        }

    # Lazy MCP import
    try:
        try:
            from mcp.mcp_client import get_mcp_manager
        except Exception:
            from backend.mcp.mcp_client import get_mcp_manager
    except Exception as exc:
        return {
            "run_id": run_id,
            "status": "clarification_required",
            "ontology": None,
            "questions": [],
            "validation_errors": [f"MCP client unavailable: {exc}"],
            "validation_warnings": [],
        }

    mcp_manager = get_mcp_manager()

    async def _call_ontology_tool(tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call an ontology MCP tool."""
        payload = await mcp_manager.call_tool("ontology", tool, arguments)
        if isinstance(payload, dict):
            return payload
        structured = getattr(payload, "structuredContent", None)
        if isinstance(structured, dict):
            return structured
        structured = getattr(payload, "structured_content", None)
        if isinstance(structured, dict):
            return structured
        data = getattr(payload, "data", None)
        if isinstance(data, dict):
            return data
        is_error = bool(getattr(payload, "isError", False) or getattr(payload, "is_error", False))
        content = getattr(payload, "content", None)
        if isinstance(content, list) and content:
            texts: List[str] = []
            for part in content:
                text = getattr(part, "text", None)
                if text is None and isinstance(part, dict):
                    text = part.get("text")
                if isinstance(text, str) and text.strip():
                    texts.append(text.strip())
            if is_error:
                return {"error": "\n".join(texts).strip() or f"MCP tool error: {tool}"}
            for text in texts:
                try:
                    parsed = json.loads(text)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    return parsed
            if texts:
                return {"result": texts[0]}
        raise RuntimeError(f"Unexpected MCP tool result type for {tool}: {type(payload)}")

    principal_id = str(user_id or actor or "system").strip() or "system"
    state = OntologyAgentState(
        db_name=db_name,
        branch=str(branch or "").strip() or "main",
        goal=str(goal or "").strip(),
        principal_id=principal_id,
        target_class_id=target_class_id,
    )

    max_steps = 30
    llm_meta: Optional[LLMCallMeta] = None
    notes: List[str] = []
    tool_warnings: List[str] = []
    tool_errors: List[str] = []
    consecutive_llm_failures = 0

    allowed_tools = list(_ONTOLOGY_AGENT_ALLOWED_TOOLS)
    system_prompt = _build_system_prompt(allowed_tools=allowed_tools)
    max_tool_calls_per_step = 12
    prompt_char_limit = int(max(1, int(getattr(llm_gateway, "max_prompt_chars", 20000) or 20000) * 0.9))

    async def _execute_tool_call(*, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single MCP tool call."""
        if tool_name not in allowed_tools:
            state.last_observation = {"error": f"tool not allowed: {tool_name}"}
            return state.last_observation

        try:
            args = _drop_none_values(dict(args or {}))

            # Always inject session_id
            if "session_id" not in args:
                args["session_id"] = session_id

            # For tools that need db_name/branch, inject if not provided
            if tool_name in {"ontology_load", "ontology_list_classes", "ontology_get_class",
                           "ontology_search_classes", "ontology_create", "ontology_update",
                           "ontology_check_relationships", "ontology_check_circular_refs",
                           "ontology_suggest_mappings"}:
                if "db_name" not in args or not args["db_name"]:
                    args["db_name"] = state.db_name
                if "branch" not in args or not args["branch"]:
                    args["branch"] = state.branch

            payload = await _call_ontology_tool(tool_name, args)

            # Track working ontology from preview
            if tool_name == "ontology_preview" and isinstance(payload, dict):
                ont = payload.get("ontology")
                if isinstance(ont, dict):
                    state.working_ontology = ont

            # Track schema inference results
            if tool_name == "ontology_infer_schema_from_data" and isinstance(payload, dict):
                state.schema_inference = payload

            # Track mapping suggestions
            if tool_name == "ontology_suggest_mappings" and isinstance(payload, dict):
                state.mapping_suggestions = payload

            state.last_observation = _mask_tool_observation(
                payload if isinstance(payload, dict) else {"result": payload}
            )
            return state.last_observation

        except Exception as exc:
            logger.warning("ontology agent autonomous tool failed tool=%s err=%s", tool_name, exc)
            state.last_observation = {"error": str(exc)}
            tool_errors.append(f"{tool_name}: {exc}")
            return state.last_observation

    # Initialize prompt log
    state.prompt_items = [
        stable_json_dumps(_build_prompt_header(
            state=state,
            session_id=session_id,
            answers=answers,
            dataset_sample=dataset_sample,
        ))
    ]
    if state.last_observation:
        state.prompt_items.append(
            stable_json_dumps({"type": "bootstrap_observation", "observation": _mask_tool_observation(state.last_observation)})
        )

    for step_idx in range(max_steps):
        step_state: Dict[str, Any] = {
            "type": "state",
            "step": step_idx + 1,
            "ontology_summary": _summarize_ontology(state.working_ontology),
            "has_schema_inference": isinstance(state.schema_inference, dict),
            "has_mapping_suggestions": isinstance(state.mapping_suggestions, dict),
            "last_observation": state.last_observation,
        }
        state.prompt_items.append(stable_json_dumps(step_state))

        # Compact if needed
        current_prompt = _prompt_text(state.prompt_items)
        if len(current_prompt) > prompt_char_limit:
            # Simple compaction: keep header and recent items
            state.prompt_items = [
                state.prompt_items[0],  # header
                stable_json_dumps({
                    "type": "compaction",
                    "reason": "prompt_too_large",
                    "ontology_summary": _summarize_ontology(state.working_ontology),
                    "last_observation": state.last_observation,
                }),
            ]

        user_prompt = _prompt_text(state.prompt_items)

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
            decision, llm_meta = await llm_gateway.complete_json(
                task="ONTOLOGY_AGENT_AUTONOMOUS_STEP_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=AutonomousOntologyAgentDecision,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"ontology_agent:{run_id}",
                audit_actor=actor,
                audit_resource_id=run_id,
                audit_metadata={
                    "kind": "ontology_agent_autonomous",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "step": step_idx + 1,
                },
            )
            consecutive_llm_failures = 0
        except LLMOutputValidationError as exc:
            consecutive_llm_failures += 1
            tool_errors.append(str(exc))
            state.last_observation = {
                "error": "llm_output_invalid_json",
                "detail": str(exc),
                "hint": "Return ONLY valid JSON. Keep tool_calls <= 12 and avoid huge payloads.",
            }
            state.prompt_items.append(
                stable_json_dumps({
                    "type": "llm_error",
                    "step": step_idx + 1,
                    "error_kind": "output_validation",
                    "detail": str(exc)[:500],
                })
            )
            if consecutive_llm_failures >= 3:
                break
            continue
        except LLMRequestError as exc:
            consecutive_llm_failures += 1
            tool_errors.append(str(exc))
            state.last_observation = {"error": "llm_request_failed", "detail": str(exc)}
            state.prompt_items.append(
                stable_json_dumps({
                    "type": "llm_error",
                    "step": step_idx + 1,
                    "error_kind": "request_error",
                    "detail": str(exc)[:500],
                })
            )
            if consecutive_llm_failures >= 3:
                break
            continue
        except LLMUnavailableError as exc:
            tool_errors.append(str(exc))
            break

        # Append decision to prompt log
        state.prompt_items.append(
            stable_json_dumps({
                "type": "decision",
                "step": step_idx + 1,
                "decision": decision.model_dump(mode="json"),
            })
        )

        notes.extend([str(n) for n in (decision.notes or []) if str(n or "").strip()])
        tool_warnings.extend([str(w) for w in (decision.warnings or []) if str(w or "").strip()])

        if decision.action == "clarify":
            if _is_internal_budget_clarification(list(decision.questions or [])):
                state.last_observation = {"error": "internal step budget cannot be changed; proceed using available tools"}
                continue
            return {
                "run_id": run_id,
                "status": "clarification_required",
                "ontology": state.working_ontology,
                "questions": [q.model_dump(mode="json") for q in (decision.questions or [])],
                "validation_errors": tool_errors or ["agent requested clarification"],
                "validation_warnings": tool_warnings,
                "mapping_suggestions": state.mapping_suggestions,
                "planner": {"confidence": float(decision.confidence), "notes": notes},
                "llm": (
                    {
                        "provider": llm_meta.provider,
                        "model": llm_meta.model,
                        "cache_hit": llm_meta.cache_hit,
                        "latency_ms": llm_meta.latency_ms,
                    }
                    if llm_meta
                    else None
                ),
            }

        if decision.action == "finish":
            # Get final ontology preview
            final_preview = await _execute_tool_call(tool_name="ontology_preview", args={"session_id": session_id})
            final_ontology = None
            if isinstance(final_preview, dict) and final_preview.get("status") == "success":
                final_ontology = final_preview.get("ontology")

            return {
                "run_id": run_id,
                "status": "success",
                "ontology": final_ontology,
                "questions": [],
                "validation_errors": [],
                "validation_warnings": tool_warnings,
                "mapping_suggestions": state.mapping_suggestions,
                "planner": {"confidence": float(decision.confidence), "notes": notes},
                "llm": (
                    {
                        "provider": llm_meta.provider,
                        "model": llm_meta.model,
                        "cache_hit": llm_meta.cache_hit,
                        "latency_ms": llm_meta.latency_ms,
                    }
                    if llm_meta
                    else None
                ),
            }

        # call_tool
        tool_calls = list(decision.tool_calls or [])
        if not tool_calls:
            tool_calls = [AutonomousOntologyAgentToolCall(tool=str(decision.tool or ""), args=dict(decision.args or {}))]

        executed_tools: List[str] = []
        stop_reason: Optional[str] = None
        batch_last: Optional[Dict[str, Any]] = None
        batch_last_by_alias: Dict[str, Dict[str, Any]] = {}
        truncated_tool_calls = max(0, len(tool_calls) - max_tool_calls_per_step)

        for call in tool_calls[:max_tool_calls_per_step]:
            tool_name = str(call.tool or "").strip()
            try:
                args = _resolve_batch_placeholders(
                    dict(call.args or {}),
                    last=batch_last,
                    last_by_alias=batch_last_by_alias,
                )
            except Exception as exc:
                state.last_observation = {"error": f"failed to resolve batch tool refs for {tool_name}: {exc}"}
                stop_reason = "bad_batch_ref"
                break
            if not tool_name:
                state.last_observation = {"error": "tool name missing"}
                stop_reason = "missing_tool"
                break

            executed_tools.append(tool_name)
            state.prompt_items.append(stable_json_dumps({"type": "tool_call", "step": step_idx + 1, "tool": tool_name, "args": args}))
            observation = await _execute_tool_call(tool_name=tool_name, args=args)
            state.prompt_items.append(
                stable_json_dumps({"type": "tool_output", "step": step_idx + 1, "tool": tool_name, "output": observation})
            )

            if not isinstance(observation, dict):
                stop_reason = "invalid_observation"
                break
            batch_last = observation
            batch_last_by_alias[_tool_alias(tool_name)] = observation

            if observation.get("error"):
                logger.info(
                    "ontology agent batch stopping: tool_error tool=%s step=%s observation=%s",
                    tool_name,
                    step_idx + 1,
                    _mask_tool_observation(dict(observation)),
                )
                stop_reason = "tool_error"
                break
            if observation.get("status") == "invalid":
                logger.info(
                    "ontology agent batch stopping: invalid_status tool=%s step=%s errors=%s",
                    tool_name,
                    step_idx + 1,
                    observation.get("errors"),
                )
                stop_reason = "invalid_status"
                break

        if executed_tools:
            augmented = dict(state.last_observation or {})
            augmented["executed_tools"] = executed_tools
            augmented["executed_tool_count"] = len(executed_tools)
            if truncated_tool_calls:
                augmented["tool_calls_truncated"] = truncated_tool_calls
            if stop_reason:
                augmented["batch_stopped"] = stop_reason
            state.last_observation = augmented
            state.prompt_items.append(
                stable_json_dumps({
                    "type": "batch_summary",
                    "step": step_idx + 1,
                    "executed_tools": executed_tools,
                    "stop_reason": stop_reason,
                })
            )
        continue

    # Loop exhausted / LLM error. Return best-effort result.
    final_preview = await _execute_tool_call(tool_name="ontology_preview", args={"session_id": session_id})
    final_ontology = None
    if isinstance(final_preview, dict) and final_preview.get("status") == "success":
        final_ontology = final_preview.get("ontology")

    final_status = "partial" if final_ontology else "failed"
    return {
        "run_id": run_id,
        "status": final_status,
        "ontology": final_ontology,
        "questions": [],
        "validation_errors": tool_errors or ["ontology agent did not complete"],
        "validation_warnings": tool_warnings,
        "mapping_suggestions": state.mapping_suggestions,
        "planner": {"confidence": None, "notes": notes or None},
        "llm": (
            {
                "provider": llm_meta.provider,
                "model": llm_meta.model,
                "cache_hit": llm_meta.cache_hit,
                "latency_ms": llm_meta.latency_ms,
            }
            if llm_meta
            else None
        ),
    }
