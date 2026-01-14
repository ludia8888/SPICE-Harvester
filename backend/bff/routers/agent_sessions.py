"""
Agent session APIs (BFF).

These endpoints provide the enterprise-grade session boundary required by AGENT_PRD:
- session CRUD
- message persistence
- job lifecycle + status queries
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import struct
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, Field

from bff.services.agent_plan_compiler import compile_agent_plan
from bff.services.agent_plan_validation import validate_agent_plan
from bff.services.pipeline_context_pack import build_pipeline_context_pack
from shared.config.service_config import ServiceConfig
from shared.config.settings import get_settings
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, RedisServiceDep, StorageServiceDep
from shared.models.agent_plan import AgentPlan, AgentPlanDataScope
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.security.data_encryption import encryptor_from_keys, is_encrypted_text
from shared.services.agent_model_registry import AgentModelRegistry
from shared.services.agent_plan_registry import AgentPlanRegistry
from shared.services.agent_policy_registry import AgentPolicyRegistry
from shared.services.agent_registry import AgentRegistry
from shared.services.agent_session_registry import AgentSessionRegistry
from shared.services.agent_tool_registry import AgentToolRegistry
from shared.services.dataset_registry import DatasetRegistry
from shared.services.llm_gateway import LLMUnavailableError
from shared.services.llm_quota import LLMQuotaExceededError, enforce_llm_quota
from shared.utils.llm_safety import digest_for_audit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent-sessions", tags=["Agent Sessions"])

_APPROVED_DECISIONS = {"APPROVED", "APPROVE", "ALLOW", "YES"}
_REJECTED_DECISIONS = {"REJECTED", "REJECT", "DENY", "NO"}


class AgentSessionCreateRequest(BaseModel):
    selected_model: str | None = Field(default=None, max_length=200)
    enabled_tools: list[str] | None = Field(default=None, description="Optional tool_id allowlist for this session")
    metadata: dict | None = Field(default=None)


class AgentSessionMessageRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=10_000)
    data_scope: AgentPlanDataScope | None = Field(default=None)
    execute: bool = Field(default=True, description="If true, compile a plan and start a job immediately")
    answers: dict | None = Field(default=None, description="Clarification answers when re-compiling")


class AgentSessionJobCreateRequest(BaseModel):
    plan_id: str = Field(..., description="Agent plan id (UUID)")


class AgentSessionSummarizeRequest(BaseModel):
    max_messages: int = Field(default=200, ge=1, le=2000)


class AgentSessionRemoveMessagesRequest(BaseModel):
    message_ids: list[str] | None = Field(default=None, description="Explicit message UUIDs to remove")
    start_message_id: str | None = Field(default=None, description="Remove inclusive range (by created_at order)")
    end_message_id: str | None = Field(default=None, description="Remove inclusive range (by created_at order)")
    reason: str | None = Field(default=None, max_length=500)


class AgentSessionContextItemCreateRequest(BaseModel):
    item_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="dataset|ontology|function|branch|proposal|pr|document_bundle|file_upload|custom",
    )
    include_mode: str = Field(default="summary", description="full|summary|search")
    ref: dict = Field(default_factory=dict, description="Type-specific reference payload (JSON)")
    token_count: int | None = Field(default=None, ge=0)
    metadata: dict | None = Field(default=None)


class AgentSessionCIResultCreateRequest(BaseModel):
    status: str = Field(..., min_length=1, max_length=60, description="queued|in_progress|success|failure|cancelled|skipped|unknown")
    provider: str | None = Field(default=None, max_length=200)
    details_url: str | None = Field(default=None, max_length=2000)
    summary: str | None = Field(default=None, max_length=5000)
    checks: list[dict] | None = Field(default=None, description="Standardized list of check runs")
    raw: dict | None = Field(default=None, description="Raw provider payload (optional)")
    job_id: str | None = Field(default=None, description="Optional agent job id (UUID)")
    plan_id: str | None = Field(default=None, description="Optional agent plan id (UUID)")
    run_id: str | None = Field(default=None, description="Optional agent run id (UUID)")


class AgentSessionApprovalDecisionRequest(BaseModel):
    decision: str = Field(..., min_length=1, max_length=40)
    comment: str | None = Field(default=None, max_length=2000)
    metadata: dict | None = Field(default=None)


class AgentSessionUpdateModelRequest(BaseModel):
    selected_model: str | None = Field(default=None, max_length=200)


class AgentSessionUpdateToolsRequest(BaseModel):
    enabled_tools: list[str] = Field(default_factory=list, description="Tool ids to enable for this session")


class AgentSessionVariablesUpdateRequest(BaseModel):
    variables: dict = Field(default_factory=dict, description="Session variables to set/merge")
    unset_keys: list[str] | None = Field(default=None, description="Variable keys to remove")
    replace: bool = Field(default=False, description="If true, replace the entire variables object")


class AgentSessionClarificationQuestion(BaseModel):
    id: str = Field(..., min_length=1, max_length=100)
    question: str = Field(..., min_length=1, max_length=2000)
    required: bool = Field(default=True)
    type: str = Field(default="string", description="string|enum|boolean|number|object")
    options: list[str] | None = None
    default: Any = None


class AgentSessionClarificationCreateRequest(BaseModel):
    questions: list[AgentSessionClarificationQuestion] = Field(default_factory=list)
    reason: str | None = Field(default=None, max_length=500)


def _approx_token_count(payload: Any) -> int:
    try:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        text = str(payload)
    # Very rough heuristic: ~4 chars/token for English-ish payloads.
    return max(1, int((len(text) + 3) / 4))


_EICAR_SIGNATURE = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_CLAMAV_CHUNK_BYTES = 8192


def _safe_filename(name: str) -> str:
    raw = str(name or "").strip() or "upload"
    # Keep extensions; sanitize the rest.
    parts = raw.rsplit(".", 1)
    if len(parts) == 2 and parts[0]:
        stem, ext = parts
        stem = _SAFE_FILENAME_RE.sub("_", stem).strip("_") or "upload"
        ext = _SAFE_FILENAME_RE.sub("", parts[1]).strip("_") or "bin"
        return f"{stem}.{ext}"[:200]
    stem = _SAFE_FILENAME_RE.sub("_", raw).strip("_") or "upload"
    return stem[:200]


async def _clamav_instream_scan(
    *,
    blob: bytes,
    host: str,
    port: int,
    timeout_seconds: float,
) -> Optional[str]:
    """
    Best-effort ClamAV `clamd` INSTREAM scan.

    Returns:
      - None if clean
      - malware signature name if FOUND
    """
    host_value = str(host or "").strip()
    if not host_value:
        return None

    async def _open():
        return await asyncio.open_connection(host_value, int(port))

    reader, writer = await asyncio.wait_for(_open(), timeout=float(timeout_seconds))
    try:
        writer.write(b"zINSTREAM\x00")
        for idx in range(0, len(blob), _CLAMAV_CHUNK_BYTES):
            chunk = blob[idx : idx + _CLAMAV_CHUNK_BYTES]
            writer.write(struct.pack("!I", len(chunk)))
            writer.write(chunk)
        writer.write(struct.pack("!I", 0))
        await asyncio.wait_for(writer.drain(), timeout=float(timeout_seconds))

        line = await asyncio.wait_for(reader.readline(), timeout=float(timeout_seconds))
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

    text = (line or b"").decode("utf-8", errors="replace").replace("\x00", "").strip()
    # Typical responses:
    #  - "stream: OK"
    #  - "stream: Eicar-Test-Signature FOUND"
    if not text:
        raise RuntimeError("ClamAV scan returned empty response")
    upper = text.upper()
    if "FOUND" not in upper:
        return None
    signature = text
    if ":" in signature:
        signature = signature.split(":", 1)[1].strip()
    signature = signature.replace("FOUND", "").strip()
    return signature or "FOUND"


async def _scan_upload_bytes(*, blob: bytes, agent_settings: Any) -> None:
    if _EICAR_SIGNATURE in blob:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Malware signature detected (EICAR)")
    host = str(getattr(agent_settings, "context_upload_clamav_host", "") or "").strip()
    if not host:
        return
    port = int(getattr(agent_settings, "context_upload_clamav_port", 3310) or 3310)
    timeout_s = float(getattr(agent_settings, "context_upload_clamav_timeout_seconds", 2.0) or 2.0)
    required = bool(getattr(agent_settings, "context_upload_clamav_required", False))

    try:
        signature = await _clamav_instream_scan(blob=blob, host=host, port=port, timeout_seconds=timeout_s)
    except HTTPException:
        raise
    except Exception as exc:
        if required:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Virus scanner unavailable"
            ) from exc
        logger.warning("ClamAV scan failed (best-effort): %s", exc)
        return

    if signature:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Malware detected ({signature})",
        )


def _extract_text_from_upload(blob: bytes, *, max_chars: int) -> str:
    if not blob:
        return ""
    try:
        text = blob.decode("utf-8", errors="replace")
    except Exception:
        text = blob.decode("latin-1", errors="replace")
    text = text.replace("\x00", "").strip()
    if len(text) > max_chars:
        return text[:max_chars] + "…"
    return text


def _normalize_classification(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _context_item_classification(metadata: Any) -> Optional[str]:
    if not isinstance(metadata, dict):
        return None
    for key in (
        "classification",
        "data_classification",
        "security_classification",
        "confidentiality",
        "sensitivity",
    ):
        if key in metadata:
            return _normalize_classification(metadata.get(key))
    return None


def _filter_context_items_for_llm(*, items: list[Any], data_policies: Any) -> tuple[list[Any], list[dict[str, Any]]]:
    llm_policy = None
    if isinstance(data_policies, dict):
        llm_policy = data_policies.get("llm")
    llm_policy = llm_policy if isinstance(llm_policy, dict) else {}

    deny_classes = llm_policy.get("deny_classifications") or llm_policy.get("blocked_classifications") or []
    deny_set = {_normalize_classification(c) for c in deny_classes if _normalize_classification(c)}

    allow_classes = llm_policy.get("allow_classifications") or llm_policy.get("allowed_classifications") or []
    allow_set = {_normalize_classification(c) for c in allow_classes if _normalize_classification(c)}

    filtered: list[Any] = []
    blocked: list[dict[str, Any]] = []
    for item in items:
        metadata = getattr(item, "metadata", None)
        classification = _context_item_classification(metadata)
        if classification and classification in deny_set:
            blocked.append({"item_id": str(getattr(item, "item_id", "")), "classification": classification, "reason": "denied_by_policy"})
            continue
        if allow_set and (classification is None or classification not in allow_set):
            blocked.append({"item_id": str(getattr(item, "item_id", "")), "classification": classification, "reason": "not_in_allowlist"})
            continue
        filtered.append(item)
    return filtered, blocked


def _token_budget_candidates(
    *,
    messages: list[Any],
    context_items: list[Any],
    target_tokens: int,
    message_tokens_total: int,
    context_tokens_total: int,
) -> list[dict[str, Any]]:
    over_by = max(0, (int(message_tokens_total) + int(context_tokens_total)) - int(target_tokens))
    if over_by <= 0:
        return []

    candidates: list[dict[str, Any]] = []
    for item in context_items:
        tokens = int(getattr(item, "token_count", 0) or 0)
        include_mode = str(getattr(item, "include_mode", "summary") or "summary").strip().lower() or "summary"
        if tokens <= 0:
            continue
        if include_mode == "full":
            candidates.append(
                {
                    "kind": "context_item",
                    "id": str(getattr(item, "item_id", "")),
                    "item_type": str(getattr(item, "item_type", "")),
                    "include_mode": include_mode,
                    "token_count": tokens,
                    "suggested_action": "switch_to_summary",
                    "estimated_token_savings": int(tokens * 0.6),
                    "reason": "Full context is expensive; prefer summary unless strictly required.",
                }
            )
        elif include_mode == "summary":
            candidates.append(
                {
                    "kind": "context_item",
                    "id": str(getattr(item, "item_id", "")),
                    "item_type": str(getattr(item, "item_type", "")),
                    "include_mode": include_mode,
                    "token_count": tokens,
                    "suggested_action": "switch_to_search",
                    "estimated_token_savings": int(tokens * 0.3),
                    "reason": "Consider search-based inclusion (RAG) to reduce prompt size.",
                }
            )

    for msg in messages[:-10]:
        tokens = int(getattr(msg, "token_count", 0) or 0)
        if tokens <= 0:
            continue
        candidates.append(
            {
                "kind": "message",
                "id": str(getattr(msg, "message_id", "")),
                "role": str(getattr(msg, "role", "")),
                "token_count": tokens,
                "suggested_action": "summarize_or_remove",
                "estimated_token_savings": tokens,
                "reason": "Older messages can be summarized/removed once captured in a session summary.",
            }
        )

    candidates.sort(key=lambda c: int(c.get("token_count") or 0), reverse=True)
    picked: list[dict[str, Any]] = []
    remaining = over_by
    for candidate in candidates:
        if remaining <= 0:
            break
        savings = int(candidate.get("estimated_token_savings") or 0)
        if savings <= 0:
            continue
        picked.append(candidate)
        remaining -= savings
    return picked


def _plan_risk_level(plan: AgentPlan) -> str:
    risk = getattr(plan.risk_level, "value", plan.risk_level)
    return str(risk or "read").strip().lower() or "read"


def _truncate_preview(value: Any, *, max_chars: int = 1200) -> Optional[str]:
    if value is None:
        return None
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        text = str(value)
    text = text.strip()
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)] + "…"


def _build_approval_request_payload(*, plan: AgentPlan, validation: Any) -> Dict[str, Any]:
    compilation_report = getattr(validation, "compilation_report", None)
    required_controls = getattr(compilation_report, "required_controls", None) if compilation_report else None
    diagnostics = getattr(compilation_report, "diagnostics", None) if compilation_report else None
    scope = plan.data_scope.model_dump(mode="json")

    required_controls_values = [str(getattr(c, "value", c)) for c in (required_controls or [])]
    preview_controls = {"simulate_first", "pipeline_simulate_first"}
    preview_suggested = bool(set(required_controls_values) & preview_controls)
    preview_tools = {"actions.simulate", "pipelines.preview", "pipelines.simulate_definition"}
    preview_present = any(step.tool_id in preview_tools or step.tool_id.endswith(".simulate") for step in plan.steps)

    steps_summary: list[dict[str, Any]] = []
    for step in plan.steps:
        steps_summary.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "method": step.method,
                "path_params": step.path_params,
                "query": step.query,
                "body_preview": _truncate_preview(step.body),
                "body_digest": digest_for_audit(step.body) if step.body is not None else None,
                "requires_approval": bool(step.requires_approval),
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    scope_hints: dict[str, Any] = {
        "db_name": scope.get("db_name"),
        "branch": scope.get("branch"),
        "dataset_id": scope.get("dataset_id"),
        "pipeline_id": scope.get("pipeline_id"),
    }
    for step in plan.steps:
        params = step.path_params or {}
        for key in ("db_name", "branch", "dataset_id", "dataset_version_id", "pipeline_id", "action_type_id"):
            value = params.get(key)
            if value not in (None, ""):
                scope_hints.setdefault(key, value)

    payload: Dict[str, Any] = {
        "plan_id": plan.plan_id,
        "goal": plan.goal,
        "risk_level": _plan_risk_level(plan),
        "requires_approval": bool(plan.requires_approval),
        "data_scope": scope,
        "change_scope": scope_hints,
        "rollback": {
            "possible": _plan_risk_level(plan) != "destructive",
            "suggestion": "Prefer branch/proposal isolation and keep idempotency keys for safe replays.",
        },
        "preview": {
            "suggested": preview_suggested,
            "present_in_plan": preview_present,
            "hint": "For risky writes, run simulate/preview first when available.",
        },
        "steps": steps_summary,
        "validation_warnings": list(getattr(validation, "warnings", []) or []),
        "required_controls": required_controls_values,
        "diagnostics": [d.model_dump(mode="json") for d in (diagnostics or [])] if diagnostics else [],
    }
    if compilation_report is not None:
        payload["compilation_report"] = compilation_report.model_dump(mode="json")
    return payload


def _should_auto_approve(*, plan: AgentPlan, policy: Any) -> bool:
    if policy is None:
        return False
    rules = getattr(policy, "auto_approve_rules", None) or {}
    if not isinstance(rules, dict):
        return False
    if not bool(rules.get("enabled")):
        return False

    allow_risks = {str(r).strip().lower() for r in (rules.get("allow_risk_levels") or []) if str(r).strip()}
    if not allow_risks:
        allow_risks = {"write"}

    risk = _plan_risk_level(plan)
    if risk not in allow_risks:
        return False
    if risk in {"admin", "destructive"}:
        return False

    allow_tool_ids = {str(t).strip() for t in (rules.get("allow_tool_ids") or []) if str(t).strip()}
    deny_tool_ids = {str(t).strip() for t in (rules.get("deny_tool_ids") or []) if str(t).strip()}
    if not allow_tool_ids:
        return False

    allow_branches = {str(b).strip() for b in (rules.get("allow_branches") or []) if str(b).strip()}
    if allow_branches:
        branch = str(plan.data_scope.branch or "").strip() or "main"
        if branch not in allow_branches:
            return False

    for step in plan.steps:
        if step.tool_id in deny_tool_ids:
            return False
        if step.tool_id not in allow_tool_ids:
            return False
    return True


def _resolve_verified_principal(request: Request) -> tuple[str, str, str]:
    user = getattr(request.state, "user", None)
    if user is None or not getattr(user, "verified", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User JWT required")
    tenant_id = getattr(user, "tenant_id", None) or getattr(user, "org_id", None) or "default"
    user_id = str(getattr(user, "id", "") or "").strip() or "unknown"
    actor = f"{getattr(user, 'type', 'user')}:{user_id}"
    return str(tenant_id), user_id, actor


def _ensure_session_owner(*, record: Any, user_id: str) -> None:
    if record is None:
        return
    created_by = str(getattr(record, "created_by", "") or "").strip()
    if created_by and created_by != str(user_id or "").strip():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")


def _forward_headers_to_agent(request: Request) -> Dict[str, str]:
    allowlist = {
        "accept",
        "accept-language",
        "authorization",
        "content-type",
        "user-agent",
        "x-admin-token",
        "x-delegated-authorization",
        "x-request-id",
        "x-org-id",
        "x-tenant-id",
        "x-user",
        "x-user-id",
        "x-user-type",
        "x-principal-id",
        "x-principal-type",
        "x-actor",
        "x-actor-type",
    }
    output: Dict[str, str] = {}
    for key, value in request.headers.items():
        if not value:
            continue
        if key.lower() in allowlist:
            output[key] = value
    output.setdefault("X-Spice-Caller", "bff")
    return output


async def _call_agent_create_run(*, request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
    agent_url = ServiceConfig.get_agent_url().rstrip("/")
    url = f"{agent_url}/api/v1/agent/runs"
    timeout_seconds = get_settings().clients.agent_proxy_timeout_seconds
    ssl_config = ServiceConfig.get_client_ssl_config()

    async with httpx.AsyncClient(timeout=timeout_seconds, verify=ssl_config.get("verify", True)) as client:
        resp = await client.post(url, headers=_forward_headers_to_agent(request), json=payload)
    try:
        body = resp.json()
    except Exception:
        body = {"status": "error", "message": (resp.text or "").strip() or "Agent service error"}

    if resp.status_code >= 400:
        raise HTTPException(status_code=int(resp.status_code), detail=body.get("detail") if isinstance(body, dict) else body)
    return body if isinstance(body, dict) else {"status": "success", "data": body}


async def _start_agent_job_run(
    *,
    request: Request,
    sessions: AgentSessionRegistry,
    tool_registry: AgentToolRegistry,
    tenant_id: str,
    session_id: str,
    job_id: str,
    plan: AgentPlan,
    allowed_tool_ids: Optional[list[str]],
) -> Dict[str, Any]:
    allowed_set = set(allowed_tool_ids or []) if allowed_tool_ids is not None else None
    steps_payload: list[dict[str, Any]] = []
    for step in plan.steps:
        if allowed_set is not None and step.tool_id not in allowed_set:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"tool_id not enabled: {step.tool_id}")
        policy = await tool_registry.get_tool_policy(tool_id=step.tool_id)
        if not policy:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not in allowlist: {step.tool_id}")
        rendered_path = str(policy.path or "")
        path_params = dict(step.path_params or {})
        path_params.setdefault("session_id", session_id)
        path_params.setdefault("job_id", job_id)
        for key, value in path_params.items():
            token = "{" + str(key) + "}"
            if token in rendered_path and value not in (None, ""):
                rendered_path = rendered_path.replace(token, str(value))
        headers: Dict[str, str] = {
            "X-Agent-Session-ID": session_id,
            "X-Agent-Job-ID": job_id,
        }
        if plan.plan_id:
            headers["X-Agent-Plan-ID"] = str(plan.plan_id)
        if step.idempotency_key:
            headers["Idempotency-Key"] = str(step.idempotency_key)
        steps_payload.append(
            {
                "step_id": step.step_id,
                "tool_id": step.tool_id,
                "service": "bff",
                "method": step.method or policy.method,
                "path": rendered_path,
                "query": step.query or {},
                "body": step.body,
                "headers": headers,
                "data_scope": {**(plan.data_scope.model_dump(mode="json") or {}), **(step.data_scope or {})},
                "description": step.description,
                "produces": list(step.produces or []),
                "consumes": list(step.consumes or []),
            }
        )

    run_payload = {
        "goal": plan.goal,
        "steps": steps_payload,
        "context": {
            "plan_id": plan.plan_id,
            "risk_level": str(plan.risk_level.value if hasattr(plan.risk_level, "value") else plan.risk_level),
            "plan_snapshot": plan.model_dump(mode="json"),
            "session_id": session_id,
            "job_id": job_id,
        },
        "dry_run": False,
        "request_id": str(uuid4()),
    }
    agent_resp = await _call_agent_create_run(request=request, payload=run_payload)
    run_data = agent_resp.get("data") if isinstance(agent_resp, dict) else None
    run_id = (run_data or {}).get("run_id") if isinstance(run_data, dict) else None

    await sessions.update_job(job_id=job_id, tenant_id=tenant_id, status="RUNNING", run_id=run_id, error=None)
    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="RUNNING_TOOL")

    return {"run_id": run_id, "agent": run_data, "payload": run_payload}


async def _best_effort_reconcile_job(
    *,
    job: Any,
    tenant_id: str,
    sessions: AgentSessionRegistry,
    agent_registry: AgentRegistry,
) -> Any:
    run_id = getattr(job, "run_id", None)
    status_value = str(getattr(job, "status", "") or "").strip().upper()
    if not run_id or status_value not in {"RUNNING", "PENDING"}:
        return job
    try:
        run = await agent_registry.get_run(run_id=str(run_id), tenant_id=tenant_id)
    except Exception:
        return job
    if not run:
        return job
    run_status = str(run.status or "").strip().upper()
    if run_status not in {"COMPLETED", "FAILED"}:
        return job
    finished_at = run.finished_at or datetime.now(timezone.utc)
    updated = await sessions.update_job(
        job_id=str(getattr(job, "job_id")),
        tenant_id=tenant_id,
        status="COMPLETED" if run_status == "COMPLETED" else "FAILED",
        finished_at=finished_at,
    )
    return updated or job


async def get_agent_session_registry() -> AgentSessionRegistry:
    from bff.main import get_agent_session_registry as _get_agent_session_registry

    return await _get_agent_session_registry()


async def get_agent_tool_registry() -> AgentToolRegistry:
    from bff.main import get_agent_tool_registry as _get_agent_tool_registry

    return await _get_agent_tool_registry()


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


async def get_agent_model_registry() -> AgentModelRegistry:
    from bff.main import get_agent_model_registry as _get_agent_model_registry

    return await _get_agent_model_registry()


async def get_agent_plan_registry() -> AgentPlanRegistry:
    from bff.main import get_agent_plan_registry as _get_agent_plan_registry

    return await _get_agent_plan_registry()


async def get_agent_registry() -> AgentRegistry:
    from bff.main import get_agent_registry as _get_agent_registry

    return await _get_agent_registry()


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


@router.post("/{session_id}/context/items", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def attach_context_item(
    session_id: str,
    body: AgentSessionContextItemCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    item_type = str(payload.get("item_type") or "").strip()
    include_mode = str(payload.get("include_mode") or "summary").strip().lower() or "summary"
    ref = payload.get("ref") if isinstance(payload.get("ref"), dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    token_count = payload.get("token_count")
    if token_count is None:
        token_count = _approx_token_count(ref)

    record = await sessions.add_context_item(
        item_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        item_type=item_type,
        include_mode=include_mode,
        ref=ref,
        token_count=token_count,
        metadata=metadata,
        created_at=datetime.now(timezone.utc),
    )
    return ApiResponse.created(
        message="Context item attached",
        data={
            "context_item": {
                "item_id": record.item_id,
                "session_id": record.session_id,
                "item_type": record.item_type,
                "include_mode": record.include_mode,
                "ref": record.ref,
                "token_count": record.token_count,
                "metadata": record.metadata,
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
            }
        },
    )


@router.post("/{session_id}/context/file-upload", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def upload_context_file(
    session_id: str,
    request: Request,
    storage: StorageServiceDep,
    file: UploadFile = File(...),
    include_mode: str = Query("summary", description="full|summary|search"),
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    include_mode_value = str(include_mode or "summary").strip().lower() or "summary"
    if include_mode_value not in {"full", "summary", "search"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="include_mode must be full|summary|search")

    filename = _safe_filename(file.filename or "upload")
    content_type = (file.content_type or "application/octet-stream").strip() or "application/octet-stream"
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

    agent_settings = get_settings().agent
    max_upload_bytes = int(getattr(agent_settings, "context_upload_max_bytes", 10 * 1024 * 1024) or 10 * 1024 * 1024)
    if len(raw) > max_upload_bytes:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File too large")

    await _scan_upload_bytes(blob=raw, agent_settings=agent_settings)

    max_text_chars = int(getattr(agent_settings, "context_upload_max_text_chars", 20000) or 20000)
    extracted_text = _extract_text_from_upload(raw, max_chars=max_text_chars)
    preview = extracted_text[:2000] + ("…" if len(extracted_text) > 2000 else "")

    token_count = 0
    if extracted_text:
        token_count = _approx_token_count(extracted_text)

    encryptor = encryptor_from_keys(get_settings().security.data_encryption_keys)
    aad = f"session:{session_id}".encode("utf-8")
    blob_to_store = encryptor.encrypt_bytes(raw, aad=aad) if encryptor is not None else raw

    bucket = get_settings().storage.instance_bucket
    key = f"agent_uploads/{tenant_id}/{session_id}/{uuid4().hex}/{filename}"
    checksum = await storage.save_bytes(
        bucket=bucket,
        key=key,
        data=blob_to_store,
        content_type=content_type,
        metadata={
            "tenant_id": tenant_id,
            "session_id": session_id,
            "filename": filename,
            "content_type": content_type,
            "encrypted": "true" if encryptor is not None else "false",
        },
    )

    extracted_value: Any = extracted_text
    if encryptor is not None and extracted_text:
        extracted_value = encryptor.encrypt_text(extracted_text, aad=aad)

    record = await sessions.add_context_item(
        item_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        item_type="file_upload",
        include_mode=include_mode_value,
        ref={
            "bucket": bucket,
            "key": key,
            "filename": filename,
            "content_type": content_type,
            "checksum": checksum,
            "encrypted": bool(encryptor is not None),
        },
        token_count=token_count,
        metadata={
            "size_bytes": len(raw),
            "extracted_text": extracted_value,
            "extracted_text_preview": preview,
        },
        created_at=datetime.now(timezone.utc),
    )

    return ApiResponse.created(
        message="Context file uploaded",
        data={
            "context_item": {
                "item_id": record.item_id,
                "session_id": record.session_id,
                "item_type": record.item_type,
                "include_mode": record.include_mode,
                "ref": record.ref,
                "token_count": record.token_count,
                "metadata": {k: v for k, v in (record.metadata or {}).items() if k != "extracted_text"},
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
            }
        },
    )


@router.post("/{session_id}/ci-results", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_ci_result(
    session_id: str,
    body: AgentSessionCIResultCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    payload = sanitize_input(body.model_dump(exclude_none=True))
    status_value = str(payload.get("status") or "").strip().lower() or "unknown"
    if status_value not in {"queued", "in_progress", "success", "failure", "cancelled", "skipped", "unknown"}:
        status_value = "unknown"

    def _safe_uuid(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        try:
            return str(UUID(str(value)))
        except Exception:
            return None

    ci_record = await sessions.record_ci_result(
        ci_result_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        job_id=_safe_uuid(payload.get("job_id")),
        plan_id=_safe_uuid(payload.get("plan_id")),
        run_id=_safe_uuid(payload.get("run_id")),
        provider=str(payload.get("provider") or "").strip() or None,
        status=status_value,
        details_url=str(payload.get("details_url") or "").strip() or None,
        summary=str(payload.get("summary") or "").strip() or None,
        checks=payload.get("checks") if isinstance(payload.get("checks"), list) else None,
        raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else None,
        created_at=datetime.now(timezone.utc),
    )
    with contextlib.suppress(Exception):
        await sessions.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="CI_RESULT",
            occurred_at=datetime.now(timezone.utc),
            data={
                "ci_result_id": ci_record.ci_result_id,
                "status": ci_record.status,
                "provider": ci_record.provider,
                "details_url": ci_record.details_url,
                "summary": ci_record.summary,
                "job_id": ci_record.job_id,
                "plan_id": ci_record.plan_id,
                "run_id": ci_record.run_id,
            },
        )

    return ApiResponse.created(
        message="CI result recorded",
        data={
            "ci_result": {
                "ci_result_id": ci_record.ci_result_id,
                "session_id": ci_record.session_id,
                "tenant_id": ci_record.tenant_id,
                "job_id": ci_record.job_id,
                "plan_id": ci_record.plan_id,
                "run_id": ci_record.run_id,
                "provider": ci_record.provider,
                "status": ci_record.status,
                "details_url": ci_record.details_url,
                "summary": ci_record.summary,
                "checks": ci_record.checks,
                "raw": ci_record.raw,
                "created_at": ci_record.created_at.isoformat(),
            }
        },
    )


@router.get("/{session_id}/ci-results", response_model=ApiResponse)
async def list_ci_results(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    results = await sessions.list_ci_results(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="CI results fetched",
        data={
            "session_id": session_id,
            "count": len(results),
            "ci_results": [
                {
                    "ci_result_id": r.ci_result_id,
                    "session_id": r.session_id,
                    "tenant_id": r.tenant_id,
                    "job_id": r.job_id,
                    "plan_id": r.plan_id,
                    "run_id": r.run_id,
                    "provider": r.provider,
                    "status": r.status,
                    "details_url": r.details_url,
                    "summary": r.summary,
                    "checks": r.checks,
                    "raw": r.raw,
                    "created_at": r.created_at.isoformat(),
                }
                for r in results
            ],
        },
    )


@router.get("/{session_id}/context/items", response_model=ApiResponse)
async def list_context_items(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="Context items fetched",
        data={
            "session_id": session_id,
            "count": len(items),
            "context_items": [
                {
                    "item_id": item.item_id,
                    "session_id": item.session_id,
                    "item_type": item.item_type,
                    "include_mode": item.include_mode,
                    "ref": item.ref,
                    "token_count": item.token_count,
                    "metadata": item.metadata,
                    "created_at": item.created_at.isoformat(),
                    "updated_at": item.updated_at.isoformat(),
                }
                for item in items
            ],
        },
    )


@router.get("/{session_id}/token-budget", response_model=ApiResponse)
async def get_session_token_budget(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    model_registry: AgentModelRegistry = Depends(get_agent_model_registry),
    soft_limit_ratio: float = Query(0.8, ge=0.1, le=1.0),
    hard_limit_ratio: float = Query(1.0, ge=0.1, le=2.0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)

    messages = await sessions.list_messages(session_id=session_id, tenant_id=tenant_id, limit=2000, offset=0, include_removed=False)
    context_items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=500, offset=0)

    message_tokens_total = sum(int(getattr(m, "token_count", 0) or 0) for m in messages)
    context_tokens_total = sum(int(getattr(i, "token_count", 0) or 0) for i in context_items)

    selected_model = session_record.selected_model
    model_max_context_tokens = None
    if selected_model:
        with contextlib.suppress(Exception):
            model = await model_registry.get_model(model_id=selected_model)
            if model and getattr(model, "max_context_tokens", None):
                model_max_context_tokens = int(getattr(model, "max_context_tokens") or 0) or None
    if model_max_context_tokens is None:
        model_max_context_tokens = 8192

    soft_limit_tokens = int(model_max_context_tokens * float(soft_limit_ratio))
    hard_limit_tokens = int(model_max_context_tokens * float(hard_limit_ratio))
    estimated_prompt_tokens = int(message_tokens_total + context_tokens_total)

    candidates = _token_budget_candidates(
        messages=messages,
        context_items=context_items,
        target_tokens=soft_limit_tokens,
        message_tokens_total=message_tokens_total,
        context_tokens_total=context_tokens_total,
    )

    return ApiResponse.success(
        message="Token budget fetched",
        data={
            "session_id": session_id,
            "selected_model": selected_model,
            "model_max_context_tokens": model_max_context_tokens,
            "soft_limit_ratio": float(soft_limit_ratio),
            "hard_limit_ratio": float(hard_limit_ratio),
            "totals": {
                "message_tokens_total": message_tokens_total,
                "context_tokens_total": context_tokens_total,
                "estimated_prompt_tokens": estimated_prompt_tokens,
            },
            "limits": {
                "soft_limit_tokens": soft_limit_tokens,
                "hard_limit_tokens": hard_limit_tokens,
            },
            "flags": {
                "over_soft_limit": estimated_prompt_tokens > soft_limit_tokens,
                "over_hard_limit": estimated_prompt_tokens > hard_limit_tokens,
            },
            "candidates": candidates,
        },
    )


@router.delete("/{session_id}/context/items/{item_id}", response_model=ApiResponse)
async def remove_context_item(
    session_id: str,
    item_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
        item_uuid = str(UUID(item_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id/item_id must be UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    removed = await sessions.remove_context_item(session_id=session_id, tenant_id=tenant_id, item_id=item_uuid)
    if removed <= 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Context item not found")
    return ApiResponse.success(message="Context item removed", data={"session_id": session_id, "item_id": item_uuid})


@router.post("", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
async def create_session(
    body: AgentSessionCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    payload = sanitize_input(body.model_dump(exclude_none=True))
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)

    allowed_models: set[str] = set()
    if policy:
        allowed_models = {str(m).strip() for m in (policy.allowed_models or []) if str(m).strip()}
        if not allowed_models and policy.default_model:
            allowed_models = {str(policy.default_model).strip()}
    allowed_tools: Optional[set[str]] = None
    if policy:
        allowed_tools_set = {str(t).strip() for t in (policy.allowed_tools or []) if str(t).strip()}
        allowed_tools = allowed_tools_set or None

    requested_selected_model = payload.get("selected_model") if "selected_model" in payload else None
    selected_model = (
        str(requested_selected_model).strip()
        if requested_selected_model is not None
        else (policy.default_model if policy else None)
    )
    if selected_model == "":
        selected_model = None
    if selected_model is None and policy and policy.allowed_models:
        selected_model = policy.allowed_models[0]

    model_restricted = bool(policy and (policy.default_model or policy.allowed_models))
    if model_restricted and selected_model and allowed_models and selected_model not in allowed_models:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Model not allowed for tenant")

    enabled_tools = payload.get("enabled_tools") if "enabled_tools" in payload else None
    if enabled_tools is None:
        if policy is not None and allowed_tools is not None:
            enabled_tools = list(allowed_tools)
        else:
            policies = await tool_registry.list_tool_policies(status="ACTIVE", limit=500)
            enabled_tools = [p.tool_id for p in policies if str(p.tool_id).strip()]
    tools_restricted = True

    if enabled_tools is not None:
        enabled_tools = [str(tool_id).strip() for tool_id in enabled_tools if str(tool_id).strip()]
        if allowed_tools is not None and any(tool_id not in allowed_tools for tool_id in enabled_tools):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tool not allowed for tenant")
        for tool_id in enabled_tools:
            tool_policy = await tool_registry.get_tool_policy(tool_id=tool_id)
            if not tool_policy or str(tool_policy.status or "").strip().upper() != "ACTIVE":
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not ACTIVE: {tool_id}")

    metadata = dict(payload.get("metadata") or {})
    metadata.setdefault("policy_source", "tenant" if policy else "none")
    metadata["tools_restricted"] = tools_restricted
    metadata["model_restricted"] = model_restricted

    record = await sessions.create_session(
        session_id=str(uuid4()),
        tenant_id=tenant_id,
        created_by=user_id,
        status="ACTIVE",
        selected_model=selected_model,
        enabled_tools=enabled_tools,
        metadata=metadata,
        started_at=datetime.now(timezone.utc),
    )
    return ApiResponse.created(
        message="Agent session created",
        data={
            "session": {
                "session_id": record.session_id,
                "tenant_id": record.tenant_id,
                "created_by": record.created_by,
                "status": record.status,
                "selected_model": record.selected_model,
                "enabled_tools": record.enabled_tools,
                "started_at": record.started_at.isoformat(),
                "terminated_at": record.terminated_at.isoformat() if record.terminated_at else None,
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
                "metadata": record.metadata,
            }
        },
    )


@router.get("", response_model=ApiResponse)
async def list_sessions(
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    records = await sessions.list_sessions(tenant_id=tenant_id, created_by=user_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="Agent sessions fetched",
        data={
            "count": len(records),
            "sessions": [
                {
                    "session_id": r.session_id,
                    "status": r.status,
                    "selected_model": r.selected_model,
                    "enabled_tools": r.enabled_tools,
                    "started_at": r.started_at.isoformat(),
                    "terminated_at": r.terminated_at.isoformat() if r.terminated_at else None,
                    "created_at": r.created_at.isoformat(),
                    "updated_at": r.updated_at.isoformat(),
                }
                for r in records
            ],
        },
    )


@router.get("/metrics/llm-usage", response_model=ApiResponse)
async def get_llm_usage_metrics(
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    scope: str = Query("tenant", description="tenant|user"),
    group_by: str = Query("model", description="tenant|model|user|model_user"),
    start_time: datetime | None = Query(None),
    end_time: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    scope_value = str(scope or "").strip().lower() or "tenant"
    if scope_value not in {"tenant", "user"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="scope must be tenant|user")
    created_by = user_id if scope_value == "user" else None

    try:
        rows = await sessions.aggregate_llm_usage(
            tenant_id=tenant_id,
            group_by=group_by,
            created_by=created_by,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    totals = {
        "calls": sum(int(r.calls) for r in rows),
        "prompt_tokens": sum(int(r.prompt_tokens) for r in rows),
        "completion_tokens": sum(int(r.completion_tokens) for r in rows),
        "total_tokens": sum(int(r.total_tokens) for r in rows),
        "cost_estimate": float(sum(float(r.cost_estimate) for r in rows)),
    }

    return ApiResponse.success(
        message="LLM usage metrics fetched",
        data={
            "tenant_id": tenant_id,
            "scope": scope_value,
            "group_by": str(group_by or "").strip() or "model",
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None,
            "count": len(rows),
            "totals": totals,
            "rows": [
                {
                    "tenant_id": r.tenant_id,
                    "user_id": r.user_id,
                    "model_id": r.model_id,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.total_tokens,
                    "cost_estimate": r.cost_estimate,
                    "first_at": r.first_at.isoformat(),
                    "last_at": r.last_at.isoformat(),
                }
                for r in rows
            ],
        },
    )


@router.get("/{session_id}", response_model=ApiResponse)
async def get_session(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    include_messages: bool = Query(False),
    messages_limit: int = Query(200, ge=1, le=500),
    include_context_items: bool = Query(False),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    messages_payload = None
    if include_messages:
        messages = await sessions.list_messages(session_id=session_id, tenant_id=tenant_id, limit=messages_limit)
        messages_payload = [
            {
                "message_id": m.message_id,
                "role": m.role,
                "content": m.content,
                "content_digest": m.content_digest,
                "token_count": m.token_count,
                "cost_estimate": m.cost_estimate,
                "latency_ms": m.latency_ms,
                "metadata": m.metadata,
                "created_at": m.created_at.isoformat(),
            }
            for m in messages
        ]

    context_payload = None
    if include_context_items:
        items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
        context_payload = [
            {
                "item_id": item.item_id,
                "item_type": item.item_type,
                "include_mode": item.include_mode,
                "ref": item.ref,
                "token_count": item.token_count,
                "metadata": item.metadata,
                "created_at": item.created_at.isoformat(),
                "updated_at": item.updated_at.isoformat(),
            }
            for item in items
        ]

    return ApiResponse.success(
        message="Agent session fetched",
        data={
            "session": {
                "session_id": record.session_id,
                "tenant_id": record.tenant_id,
                "created_by": record.created_by,
                "status": record.status,
                "selected_model": record.selected_model,
                "enabled_tools": record.enabled_tools,
                "summary": record.summary,
                "metadata": record.metadata,
                "started_at": record.started_at.isoformat(),
                "terminated_at": record.terminated_at.isoformat() if record.terminated_at else None,
                "created_at": record.created_at.isoformat(),
                "updated_at": record.updated_at.isoformat(),
            },
            "messages": messages_payload,
            "context_items": context_payload,
        },
    )


@router.delete("/{session_id}", response_model=ApiResponse)
async def terminate_session(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    existing = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=existing, user_id=user_id)

    record = await sessions.update_session(
        session_id=session_id,
        tenant_id=tenant_id,
        status="TERMINATED",
        terminated_at=datetime.now(timezone.utc),
    )
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    return ApiResponse.success(message="Agent session terminated", data={"session_id": record.session_id, "status": record.status})


@router.get("/{session_id}/models", response_model=ApiResponse)
async def list_session_models(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    model_registry: AgentModelRegistry = Depends(get_agent_model_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)

    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    allowed_models = [str(m).strip() for m in (policy.allowed_models if policy else []) if str(m).strip()]
    if not allowed_models and policy and policy.default_model:
        allowed_models = [str(policy.default_model).strip()]

    models_payload: list[dict[str, Any]] = []
    for model_id in allowed_models:
        record = None
        with contextlib.suppress(Exception):
            record = await model_registry.get_model(model_id=model_id)
        models_payload.append(
            {
                "model_id": model_id,
                "provider": record.provider if record else None,
                "display_name": record.display_name if record else None,
                "status": record.status if record else None,
                "supports_json_mode": record.supports_json_mode if record else None,
                "supports_native_tool_calling": record.supports_native_tool_calling if record else None,
                "max_context_tokens": record.max_context_tokens if record else None,
                "max_output_tokens": record.max_output_tokens if record else None,
                "selected": model_id == session_record.selected_model,
                "metadata": record.metadata if record else {},
            }
        )

    return ApiResponse.success(
        message="Session models fetched",
        data={"session_id": session_id, "count": len(models_payload), "models": models_payload},
    )


@router.put("/{session_id}/model", response_model=ApiResponse)
async def update_session_model(
    session_id: str,
    body: AgentSessionUpdateModelRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    model_registry: AgentModelRegistry = Depends(get_agent_model_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    payload = sanitize_input(body.model_dump(exclude_none=False))
    selected_model = payload.get("selected_model")
    selected_model = str(selected_model).strip() if selected_model not in (None, "") else None

    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    allowed_models = {str(m).strip() for m in (policy.allowed_models if policy else []) if str(m).strip()}
    if policy and policy.default_model:
        allowed_models.add(str(policy.default_model).strip())
    model_restricted = bool(policy and (policy.default_model or policy.allowed_models))
    if model_restricted and selected_model and allowed_models and selected_model not in allowed_models:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Model not allowed for tenant")

    if selected_model:
        record = await model_registry.get_model(model_id=selected_model)
        if record and str(record.status or "").strip().upper() != "ACTIVE":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Model is not ACTIVE")

    metadata = dict(session_record.metadata or {})
    metadata["model_restricted"] = model_restricted
    updated = await sessions.update_session(
        session_id=session_id,
        tenant_id=tenant_id,
        selected_model=selected_model,
        metadata=metadata,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    await sessions.add_message(
        message_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        role="system",
        content=f"Selected model updated: {selected_model or 'default'}",
        token_count=_approx_token_count(selected_model or "default"),
        metadata={"kind": "session_model_updated", "selected_model": selected_model},
        created_at=datetime.now(timezone.utc),
    )

    return ApiResponse.success(
        message="Session model updated",
        data={"session_id": session_id, "selected_model": updated.selected_model, "metadata": updated.metadata},
    )


@router.get("/{session_id}/tools", response_model=ApiResponse)
async def list_session_tools(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)

    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    allowed_tools = {str(t).strip() for t in (policy.allowed_tools if policy else []) if str(t).strip()}
    allowed_tools = allowed_tools or None

    policies = await tool_registry.list_tool_policies(status="ACTIVE", limit=200)
    if allowed_tools is not None:
        policies = [p for p in policies if p.tool_id in allowed_tools]

    enabled = set(session_record.enabled_tools or [])
    tools_payload = [
        {
            "tool_id": p.tool_id,
            "method": p.method,
            "path": p.path,
            "risk_level": p.risk_level,
            "requires_approval": bool(p.requires_approval),
            "requires_idempotency_key": bool(p.requires_idempotency_key),
            "roles": list(p.roles or []),
            "max_payload_bytes": p.max_payload_bytes,
            "status": p.status,
            "enabled": p.tool_id in enabled,
        }
        for p in policies
    ]
    return ApiResponse.success(
        message="Session tools fetched",
        data={"session_id": session_id, "count": len(tools_payload), "tools": tools_payload},
    )


@router.put("/{session_id}/tools", response_model=ApiResponse)
async def update_session_tools(
    session_id: str,
    body: AgentSessionUpdateToolsRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    enabled_tools_raw = payload.get("enabled_tools") or []
    enabled_tools = [str(t).strip() for t in enabled_tools_raw if str(t).strip()]
    if not enabled_tools:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="enabled_tools must be non-empty")

    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    allowed_tools = {str(t).strip() for t in (policy.allowed_tools if policy else []) if str(t).strip()}
    allowed_tools = allowed_tools or None
    if allowed_tools is not None and any(tool_id not in allowed_tools for tool_id in enabled_tools):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tool not allowed for tenant")

    for tool_id in enabled_tools:
        tool_policy = await tool_registry.get_tool_policy(tool_id=tool_id)
        if not tool_policy or str(tool_policy.status or "").strip().upper() != "ACTIVE":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"tool_id not ACTIVE: {tool_id}")

    metadata = dict(session_record.metadata or {})
    metadata["tools_restricted"] = True
    updated = await sessions.update_session(
        session_id=session_id,
        tenant_id=tenant_id,
        enabled_tools=enabled_tools,
        metadata=metadata,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    await sessions.add_message(
        message_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        role="system",
        content=f"Enabled tools updated: {', '.join(enabled_tools[:15])}",
        token_count=_approx_token_count(enabled_tools),
        metadata={"kind": "session_tools_updated", "enabled_tools": enabled_tools},
        created_at=datetime.now(timezone.utc),
    )

    return ApiResponse.success(
        message="Session tools updated",
        data={"session_id": session_id, "enabled_tools": updated.enabled_tools, "metadata": updated.metadata},
    )


@router.put("/{session_id}/variables", response_model=ApiResponse)
async def update_session_variables(
    session_id: str,
    body: AgentSessionVariablesUpdateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    variables = payload.get("variables") if isinstance(payload.get("variables"), dict) else {}
    unset_keys = payload.get("unset_keys") if isinstance(payload.get("unset_keys"), list) else []
    unset_keys = [str(k).strip() for k in unset_keys if str(k).strip()]
    replace = bool(payload.get("replace") or False)

    meta = dict(session_record.metadata or {})
    existing = meta.get("variables")
    existing_vars = existing if isinstance(existing, dict) else {}
    next_vars: dict[str, Any] = dict(variables) if replace else {**existing_vars, **variables}
    for key in unset_keys:
        next_vars.pop(key, None)
    meta["variables"] = next_vars

    updated = await sessions.update_session(session_id=session_id, tenant_id=tenant_id, metadata=meta)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")

    with contextlib.suppress(Exception):
        await sessions.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="SESSION_VARIABLES_UPDATED",
            occurred_at=datetime.now(timezone.utc),
            data={
                "keys_set": sorted(list(variables.keys())),
                "keys_unset": unset_keys,
                "replace": replace,
            },
        )
    return ApiResponse.success(message="Session variables updated", data={"session_id": session_id, "variables": next_vars})


@router.post("/{session_id}/clarifications", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_clarification_request(
    session_id: str,
    body: AgentSessionClarificationCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    questions = payload.get("questions") if isinstance(payload.get("questions"), list) else []
    if not questions:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="questions are required")

    clarification_id = str(uuid4())
    reason = str(payload.get("reason") or "").strip() or None
    await sessions.add_message(
        message_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        role="system",
        content="Clarification required",
        token_count=_approx_token_count(questions),
        metadata={
            "kind": "clarification_request",
            "clarification_id": clarification_id,
            "reason": reason,
            "questions": questions,
        },
        created_at=datetime.now(timezone.utc),
    )
    with contextlib.suppress(Exception):
        await sessions.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="CLARIFICATION_REQUESTED",
            occurred_at=datetime.now(timezone.utc),
            data={
                "clarification_id": clarification_id,
                "reason": reason,
                "questions": questions,
            },
        )

    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="WAITING_APPROVAL")
    return ApiResponse.accepted(
        message="Clarification requested",
        data={"session_id": session_id, "clarification_id": clarification_id, "questions": questions},
    )


@router.post("/{session_id}/summarize", response_model=ApiResponse)
async def summarize_session(
    session_id: str,
    body: AgentSessionSummarizeRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    allowed_models = [str(m).strip() for m in (policy.allowed_models if policy else []) if str(m).strip()]
    if policy and policy.default_model:
        allowed_models.append(str(policy.default_model).strip())
    allowed_models = [m for m in allowed_models if m]
    selected_model = record.selected_model
    if allowed_models and selected_model and selected_model not in set(allowed_models):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Model not allowed for tenant")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    max_messages = int(payload.get("max_messages") or 200)
    messages = await sessions.list_messages(
        session_id=session_id,
        tenant_id=tenant_id,
        limit=max_messages,
        offset=0,
        include_removed=False,
    )
    if not messages:
        return ApiResponse.success(message="No messages to summarize", data={"session_id": session_id, "summary": record.summary})

    transcript = "\n".join([f"{m.role}: {m.content}" for m in messages if m.content])

    class _SummaryEnvelope(BaseModel):
        summary: str = Field(..., min_length=1, max_length=10_000)
        key_points: list[str] = Field(default_factory=list)

    system_prompt = (
        "You are a STRICT session summarizer.\n"
        "Return a single JSON object only.\n"
        "Do not include any tool calls.\n"
        "Summarize the conversation for enterprise audit and future context.\n"
        "Focus on: user goal, constraints, decisions, and current progress.\n"
    )
    user_prompt = f"Session transcript:\n{transcript}\n"

    try:
        await enforce_llm_quota(
            redis_service=redis_service,
            tenant_id=tenant_id,
            user_id=user_id,
            model_id=str(selected_model or getattr(llm, "model", "") or "").strip(),
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            data_policies=(policy.data_policies if policy else None),
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc

    started = datetime.now(timezone.utc)
    try:
        summary_obj, meta = await llm.complete_json(
            task="SESSION_SUMMARY",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=_SummaryEnvelope,
            model=selected_model,
            allowed_models=allowed_models or None,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"agent_session:{session_id}",
            audit_actor=actor,
            audit_resource_id=session_id,
            audit_metadata={"tenant_id": tenant_id, "session_id": session_id},
        )
        latency_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        with contextlib.suppress(Exception):
            await sessions.record_llm_call(
                llm_call_id=str(uuid4()),
                session_id=session_id,
                tenant_id=tenant_id,
                job_id=None,
                plan_id=None,
                call_type="session_summary",
                provider=meta.provider,
                model_id=meta.model,
                cache_hit=bool(meta.cache_hit),
                latency_ms=int(getattr(meta, "latency_ms", latency_ms) or latency_ms),
                prompt_tokens=int(getattr(meta, "prompt_tokens", 0) or 0),
                completion_tokens=int(getattr(meta, "completion_tokens", 0) or 0),
                total_tokens=int(getattr(meta, "total_tokens", 0) or 0),
                cost_estimate=float(getattr(meta, "cost_estimate", 0.0) or 0.0) if getattr(meta, "cost_estimate", None) is not None else None,
                input_digest=digest_for_audit({"task": "SESSION_SUMMARY", "session_id": session_id}),
                output_digest=digest_for_audit({"summary": str(summary_obj.summary or "").strip()}),
            )
            await sessions.append_event(
                event_id=str(uuid4()),
                session_id=session_id,
                tenant_id=tenant_id,
                event_type="LLM_CALL",
                occurred_at=datetime.now(timezone.utc),
                data={
                    "call_type": "session_summary",
                    "provider": meta.provider,
                    "model": meta.model,
                    "cache_hit": bool(meta.cache_hit),
                    "latency_ms": int(getattr(meta, "latency_ms", latency_ms) or latency_ms),
                    "prompt_tokens": int(getattr(meta, "prompt_tokens", 0) or 0),
                    "completion_tokens": int(getattr(meta, "completion_tokens", 0) or 0),
                    "total_tokens": int(getattr(meta, "total_tokens", 0) or 0),
                    "cost_estimate": float(getattr(meta, "cost_estimate", 0.0) or 0.0),
                },
            )
        summary_text = str(summary_obj.summary or "").strip()
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, summary=summary_text)
        await sessions.add_message(
            message_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            role="system",
            content=summary_text,
            content_digest=digest_for_audit({"summary": summary_text}),
            token_count=_approx_token_count(summary_text),
            cost_estimate=float(getattr(meta, "cost_estimate", 0.0) or 0.0) if getattr(meta, "cost_estimate", None) is not None else None,
            latency_ms=latency_ms,
            metadata={
                "kind": "session_summary",
                "provider": meta.provider,
                "model": meta.model,
                "cache_hit": meta.cache_hit,
                "prompt_tokens": int(getattr(meta, "prompt_tokens", 0) or 0),
                "completion_tokens": int(getattr(meta, "completion_tokens", 0) or 0),
                "total_tokens": int(getattr(meta, "total_tokens", 0) or 0),
            },
            created_at=datetime.now(timezone.utc),
        )
        return ApiResponse.success(
            message="Session summarized",
            data={
                "session_id": session_id,
                "summary": summary_text,
                "key_points": list(summary_obj.key_points or []),
                "llm": meta.__dict__,
            },
        )
    except LLMUnavailableError as exc:
        summary_text = (transcript[:4000] + "…") if len(transcript) > 4000 else transcript
        summary_text = summary_text.strip() or "No content to summarize"
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, summary=summary_text)
        await sessions.add_message(
            message_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            role="system",
            content=summary_text,
            content_digest=digest_for_audit({"summary": summary_text}),
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
            metadata={"kind": "session_summary", "provider": "fallback", "error": str(exc)},
            created_at=datetime.now(timezone.utc),
        )
        return ApiResponse.warning(
            message="LLM unavailable; stored heuristic summary",
            data={"session_id": session_id, "summary": summary_text, "llm_error": str(exc)},
        )
    except Exception as exc:
        try:
            await audit_store.log(
                partition_key=f"agent_session:{session_id}",
                actor=actor,
                action="SESSION_SUMMARY",
                status="failure",
                resource_type="agent_session",
                resource_id=session_id,
                metadata={"tenant_id": tenant_id, "session_id": session_id, "error": str(exc)},
                error=str(exc),
            )
        except Exception:
            pass
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to summarize session") from exc


@router.post("/{session_id}/messages/remove", response_model=ApiResponse)
async def remove_messages(
    session_id: str,
    body: AgentSessionRemoveMessagesRequest,
    request: Request,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    payload = sanitize_input(body.model_dump(exclude_none=True))
    message_ids = payload.get("message_ids")
    start_id = payload.get("start_message_id")
    end_id = payload.get("end_message_id")
    reason = str(payload.get("reason") or "").strip() or None

    selected_ids: list[str] = []
    if message_ids:
        selected_ids = [str(mid) for mid in message_ids]
    elif start_id and end_id:
        try:
            start_uuid = str(UUID(str(start_id)))
            end_uuid = str(UUID(str(end_id)))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start/end_message_id must be UUID") from exc
        all_messages = await sessions.list_messages(
            session_id=session_id,
            tenant_id=tenant_id,
            limit=2000,
            offset=0,
            include_removed=True,
        )
        ids_in_order = [m.message_id for m in all_messages]
        if start_uuid not in ids_in_order or end_uuid not in ids_in_order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="start/end_message_id not found")
        start_idx = ids_in_order.index(start_uuid)
        end_idx = ids_in_order.index(end_uuid)
        lo, hi = sorted((start_idx, end_idx))
        selected_ids = ids_in_order[lo : hi + 1]
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="message_ids or start/end_message_id required")

    normalized_ids: list[str] = []
    for mid in selected_ids:
        try:
            normalized_ids.append(str(UUID(str(mid))))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="message_ids must be UUIDs") from exc

    to_audit = await sessions.get_messages_by_ids(
        session_id=session_id,
        tenant_id=tenant_id,
        message_ids=normalized_ids,
        include_removed=True,
    )
    audit_payload = [{"message_id": m.message_id, "digest": m.content_digest, "role": m.role} for m in to_audit]
    removed_count = await sessions.mark_messages_removed(
        session_id=session_id,
        tenant_id=tenant_id,
        message_ids=normalized_ids,
        removed_by=user_id,
        removed_reason=reason,
        removed_at=datetime.now(timezone.utc),
    )

    with contextlib.suppress(Exception):
        await audit_store.log(
            partition_key=f"agent_session:{session_id}",
            actor=actor,
            action="SESSION_MESSAGES_REMOVE",
            status="success",
            resource_type="agent_session",
            resource_id=session_id,
            metadata={
                "tenant_id": tenant_id,
                "session_id": session_id,
                "removed_by": user_id,
                "reason": reason,
                "removed_count": removed_count,
                "messages": audit_payload,
            },
        )

    return ApiResponse.success(
        message="Messages removed",
        data={"session_id": session_id, "removed_count": removed_count, "message_ids": normalized_ids},
    )


@router.post("/{session_id}/messages", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def post_message(
    session_id: str,
    body: AgentSessionMessageRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    model_registry: AgentModelRegistry = Depends(get_agent_model_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    tenant_id, user_id, actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    tools_restricted = bool((session_record.metadata or {}).get("tools_restricted"))
    allowed_tool_ids = list(session_record.enabled_tools or []) if tools_restricted else None
    selected_model = session_record.selected_model
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    allowed_models = [str(m).strip() for m in (policy.allowed_models if policy else []) if str(m).strip()]
    if policy and policy.default_model:
        allowed_models.append(str(policy.default_model).strip())
    allowed_models = [m for m in allowed_models if m]
    if allowed_models and selected_model and selected_model not in set(allowed_models):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Model not allowed for tenant")

    use_native_tool_calling = False
    if bool(get_settings().llm.native_tool_calling) and selected_model:
        with contextlib.suppress(Exception):
            model_record = await model_registry.get_model(model_id=str(selected_model))
            if model_record and bool(getattr(model_record, "supports_native_tool_calling", False)):
                use_native_tool_calling = True

    payload = sanitize_input(body.model_dump(exclude_none=True))
    content = str(payload.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="content is required")

    message_id = str(uuid4())
    await sessions.add_message(
        message_id=message_id,
        session_id=session_id,
        tenant_id=tenant_id,
        role="user",
        content=content,
        content_digest=digest_for_audit({"content": content}),
        token_count=_approx_token_count(content),
        metadata={"kind": "user_message"},
        created_at=datetime.now(timezone.utc),
    )
    with contextlib.suppress(Exception):
        await sessions.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="SESSION_MESSAGE",
            occurred_at=datetime.now(timezone.utc),
            data={
                "message_id": message_id,
                "role": "user",
                "content_digest": digest_for_audit({"content": content}),
                "token_count": _approx_token_count(content),
            },
        )

    if not bool(payload.get("execute", True)):
        return ApiResponse.accepted(message="Message recorded", data={"session_id": session_id})

    attached_items = await sessions.list_context_items(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
    filtered_items, blocked_items = _filter_context_items_for_llm(
        items=attached_items,
        data_policies=(policy.data_policies if policy else {}),
    )
    if blocked_items:
        with contextlib.suppress(Exception):
            await sessions.append_event(
                event_id=str(uuid4()),
                session_id=session_id,
                tenant_id=tenant_id,
                event_type="SESSION_CONTEXT_BLOCKED",
                occurred_at=datetime.now(timezone.utc),
                data={"blocked": blocked_items},
            )

    session_vars = {}
    if isinstance(session_record.metadata, dict):
        vars_candidate = session_record.metadata.get("variables")
        if isinstance(vars_candidate, dict):
            session_vars = dict(vars_candidate)

    recent_tool_calls: list[dict[str, Any]] = []
    with contextlib.suppress(Exception):
        calls = await sessions.list_tool_calls(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
        for call in (calls or [])[-50:]:
            recent_tool_calls.append(
                {
                    "tool_run_id": call.tool_run_id,
                    "job_id": call.job_id,
                    "plan_id": call.plan_id,
                    "run_id": call.run_id,
                    "step_id": call.step_id,
                    "tool_id": call.tool_id,
                    "method": call.method,
                    "path": call.path,
                    "status": call.status,
                    "error_code": call.error_code,
                    "error_message": call.error_message,
                    "side_effect_summary": call.side_effect_summary,
                    "started_at": call.started_at.isoformat() if call.started_at else None,
                    "finished_at": call.finished_at.isoformat() if call.finished_at else None,
                    "latency_ms": call.latency_ms,
                    "request_digest": call.request_digest,
                    "response_digest": call.response_digest,
                }
            )

    recent_ci_results: list[dict[str, Any]] = []
    with contextlib.suppress(Exception):
        ci_results = await sessions.list_ci_results(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
        for ci in (ci_results or [])[-20:]:
            recent_ci_results.append(
                {
                    "ci_result_id": ci.ci_result_id,
                    "job_id": ci.job_id,
                    "plan_id": ci.plan_id,
                    "run_id": ci.run_id,
                    "provider": ci.provider,
                    "status": ci.status,
                    "details_url": ci.details_url,
                    "summary": ci.summary,
                    "created_at": ci.created_at.isoformat(),
                }
            )

    encryptor = encryptor_from_keys(get_settings().security.data_encryption_keys)
    aad = f"session:{session_id}".encode("utf-8")

    dataset_ids: list[str] = []
    dataset_db_name = None
    dataset_branch = None
    dataset_mode = "summary"
    for item in filtered_items:
        if str(getattr(item, "item_type", "") or "").strip().lower() != "dataset":
            continue
        mode = str(getattr(item, "include_mode", "summary") or "summary").strip().lower() or "summary"
        if mode == "full":
            dataset_mode = "full"
        elif mode == "search" and dataset_mode != "full":
            dataset_mode = "search"
        ref_obj = getattr(item, "ref", None)
        if isinstance(ref_obj, dict):
            dataset_db_name = dataset_db_name or str(ref_obj.get("db_name") or "").strip() or None
            dataset_branch = dataset_branch or str(ref_obj.get("branch") or ref_obj.get("dataset_branch") or "").strip() or None
            for key in ("dataset_id", "datasetId"):
                value = str(ref_obj.get(key) or "").strip()
                if value:
                    dataset_ids.append(value)
            ids_value = ref_obj.get("dataset_ids") or ref_obj.get("datasetIds")
            if isinstance(ids_value, list):
                dataset_ids.extend([str(v).strip() for v in ids_value if str(v).strip()])

    data_scope_obj = payload.get("data_scope")
    if isinstance(data_scope_obj, dict):
        dataset_db_name = dataset_db_name or str(data_scope_obj.get("db_name") or "").strip() or None
        dataset_branch = dataset_branch or str(data_scope_obj.get("branch") or "").strip() or None
        ds = str(data_scope_obj.get("dataset_id") or data_scope_obj.get("datasetId") or "").strip()
        if ds:
            dataset_ids.append(ds)

    dataset_db_name = dataset_db_name or str(session_vars.get("db_name") or "").strip() or None
    dataset_branch = dataset_branch or str(session_vars.get("branch") or "").strip() or None

    dataset_ids = [d for d in (str(v).strip() for v in dataset_ids) if d]
    dataset_ids = sorted({d for d in dataset_ids})

    pipeline_context = None
    if dataset_db_name:
        max_sample_rows = 20 if dataset_mode == "full" else (10 if dataset_mode == "summary" else 5)
        with contextlib.suppress(Exception):
            pipeline_context = await build_pipeline_context_pack(
                db_name=dataset_db_name,
                branch=dataset_branch,
                dataset_ids=dataset_ids or None,
                dataset_registry=dataset_registry,
                max_sample_rows=max_sample_rows,
            )

    document_search: list[dict[str, Any]] = []
    for item in filtered_items:
        if str(getattr(item, "item_type", "") or "").strip().lower() != "document_bundle":
            continue
        if str(getattr(item, "include_mode", "summary") or "summary").strip().lower() != "search":
            continue
        ref_obj = getattr(item, "ref", None)
        if not isinstance(ref_obj, dict):
            continue
        bundle_id = str(ref_obj.get("bundle_id") or ref_obj.get("document_bundle_id") or "").strip()
        if not bundle_id:
            continue
        filters = ref_obj.get("filters") if isinstance(ref_obj.get("filters"), dict) else {}
        try:
            from bff.routers.context7 import get_context7_client

            client = await get_context7_client()
            results = await client.search(query=content, limit=5, filters={**filters, "bundle_id": bundle_id})
        except Exception as exc:
            document_search.append({"bundle_id": bundle_id, "error": str(exc)})
            continue

        citations: list[dict[str, Any]] = []
        chunks: list[dict[str, Any]] = []
        for idx, res in enumerate(results or []):
            obj = dict(res) if isinstance(res, dict) else {"value": res}
            doc_id = str(obj.get("id") or obj.get("entity_id") or obj.get("doc_id") or f"result_{idx}").strip()
            snippet = obj.get("snippet") or obj.get("content") or obj.get("text") or ""
            if not isinstance(snippet, str):
                snippet = str(snippet)
            snippet = snippet.strip()
            if len(snippet) > 1200:
                snippet = snippet[:1200] + "…"
            citation_id = f"context7:{bundle_id}:{doc_id}"
            citations.append({"citation_id": citation_id, "doc_id": doc_id, "title": obj.get("title") or obj.get("name")})
            chunks.append({"citation_id": citation_id, "snippet": snippet, "score": obj.get("score")})

        document_search.append(
            {
                "bundle_id": bundle_id,
                "query": content,
                "results": chunks,
                "citations": citations,
            }
        )

    rendered_uploads: list[dict[str, Any]] = []
    for item in filtered_items:
        if str(getattr(item, "item_type", "") or "").strip().lower() != "file_upload":
            continue
        ref_obj = getattr(item, "ref", None)
        meta_obj = getattr(item, "metadata", None)
        meta_obj = meta_obj if isinstance(meta_obj, dict) else {}
        extracted = meta_obj.get("extracted_text")
        if encryptor is not None and is_encrypted_text(extracted):
            with contextlib.suppress(Exception):
                extracted = encryptor.decrypt_text(str(extracted), aad=aad)
        preview = meta_obj.get("extracted_text_preview")
        mode = str(getattr(item, "include_mode", "summary") or "summary").strip().lower() or "summary"
        rendered_uploads.append(
            {
                "item_id": item.item_id,
                "filename": (ref_obj.get("filename") if isinstance(ref_obj, dict) else None) or None,
                "include_mode": mode,
                "text": extracted if mode == "full" else (preview or ""),
                "size_bytes": meta_obj.get("size_bytes"),
            }
        )

    context_pack = {
        "session": {
            "session_id": session_id,
            "summary": session_record.summary,
            "selected_model": session_record.selected_model,
            "enabled_tools": list(session_record.enabled_tools or []),
            "variables": session_vars,
        },
        "attached_context": [
            {
                "item_id": item.item_id,
                "item_type": item.item_type,
                "include_mode": item.include_mode,
                "ref": item.ref,
                "token_count": item.token_count,
                "metadata": item.metadata,
            }
            for item in filtered_items
        ],
        "recent_tool_calls": recent_tool_calls,
        "ci_results": recent_ci_results,
        "pipeline_context": pipeline_context,
        "document_search": document_search,
        "uploads": rendered_uploads,
    }

    sanitized_data_scope = body.data_scope
    data_scope_obj = payload.get("data_scope")
    if isinstance(data_scope_obj, dict):
        with contextlib.suppress(Exception):
            sanitized_data_scope = AgentPlanDataScope.model_validate(data_scope_obj)

    compile_started = datetime.now(timezone.utc)
    try:
        compile_result = await compile_agent_plan(
            goal=content,
            data_scope=sanitized_data_scope,
            answers=payload.get("answers"),
            context_pack=context_pack,
            actor=actor,
            tenant_id=tenant_id,
            user_id=user_id,
            data_policies=(policy.data_policies if policy else None),
            allowed_tool_ids=allowed_tool_ids,
            selected_model=selected_model,
            allowed_models=allowed_models or None,
            use_native_tool_calling=use_native_tool_calling,
            tool_registry=tool_registry,
            llm_gateway=llm,
            redis_service=redis_service,
            audit_store=audit_store,
        )
    except LLMQuotaExceededError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    compile_latency_ms = int((datetime.now(timezone.utc) - compile_started).total_seconds() * 1000)
    if compile_result.llm_meta is not None:
        with contextlib.suppress(Exception):
            await sessions.record_llm_call(
                llm_call_id=str(uuid4()),
                session_id=session_id,
                tenant_id=tenant_id,
                job_id=None,
                plan_id=compile_result.plan_id,
                call_type="planner",
                provider=compile_result.llm_meta.provider,
                model_id=compile_result.llm_meta.model,
                cache_hit=bool(compile_result.llm_meta.cache_hit),
                latency_ms=int(compile_result.llm_meta.latency_ms),
                prompt_tokens=int(getattr(compile_result.llm_meta, "prompt_tokens", 0) or 0),
                completion_tokens=int(getattr(compile_result.llm_meta, "completion_tokens", 0) or 0),
                total_tokens=int(getattr(compile_result.llm_meta, "total_tokens", 0) or 0),
                cost_estimate=float(getattr(compile_result.llm_meta, "cost_estimate", 0.0) or 0.0),
                input_digest=digest_for_audit({"task": "AGENT_PLAN_COMPILE_V1", "goal": content}),
                output_digest=digest_for_audit({"plan": compile_result.plan.model_dump(mode="json")}) if compile_result.plan else None,
            )
            await sessions.append_event(
                event_id=str(uuid4()),
                session_id=session_id,
                tenant_id=tenant_id,
                event_type="LLM_CALL",
                occurred_at=datetime.now(timezone.utc),
                data={
                    "call_type": "planner",
                    "provider": compile_result.llm_meta.provider,
                    "model": compile_result.llm_meta.model,
                    "cache_hit": bool(compile_result.llm_meta.cache_hit),
                    "latency_ms": int(compile_result.llm_meta.latency_ms),
                    "prompt_tokens": int(getattr(compile_result.llm_meta, "prompt_tokens", 0) or 0),
                    "completion_tokens": int(getattr(compile_result.llm_meta, "completion_tokens", 0) or 0),
                    "total_tokens": int(getattr(compile_result.llm_meta, "total_tokens", 0) or 0),
                    "cost_estimate": float(getattr(compile_result.llm_meta, "cost_estimate", 0.0) or 0.0),
                },
            )

    if compile_result.status == "clarification_required":
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ACTIVE")
        return ApiResponse.warning(
            message="Clarification required",
            data={
                "session_id": session_id,
                "plan_id": compile_result.plan_id,
                "questions": compile_result.questions,
                "validation_errors": compile_result.validation_errors,
                "llm_meta": dict(compile_result.llm_meta.__dict__) if compile_result.llm_meta else None,
            },
        )
    if compile_result.status != "success" or not compile_result.plan:
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ERROR")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to compile agent plan")

    plan = compile_result.plan
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    status_name = "COMPILED" if not validation.errors else "DRAFT"
    await plan_registry.upsert_plan(
        plan_id=plan.plan_id,
        tenant_id=tenant_id,
        status=status_name,
        goal=str(plan.goal or ""),
        risk_level=str(plan.risk_level.value if hasattr(plan.risk_level, "value") else plan.risk_level),
        requires_approval=bool(plan.requires_approval),
        plan=plan.model_dump(mode="json"),
        created_by=user_id,
    )

    job_metadata: dict[str, Any] = {"kind": "agent_plan_job", "compile_latency_ms": compile_latency_ms}
    if compile_result.llm_meta is not None:
        job_metadata["planner_llm"] = dict(compile_result.llm_meta.__dict__)
    if compile_result.planner_confidence is not None:
        job_metadata["planner_confidence"] = float(compile_result.planner_confidence)
    if compile_result.planner_notes:
        job_metadata["planner_notes"] = list(compile_result.planner_notes)

    approval_request_id: str | None = str(uuid4()) if plan.requires_approval else None
    if approval_request_id is not None:
        job_metadata["approval_request_id"] = approval_request_id

    job_id = str(uuid4())
    try:
        await sessions.create_job(
            job_id=job_id,
            session_id=session_id,
            tenant_id=tenant_id,
            plan_id=plan.plan_id,
            status="WAITING_APPROVAL" if plan.requires_approval else "PENDING",
            metadata=job_metadata,
        )
    except ValueError as exc:
        if "active job" in str(exc).lower():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Session already has an active job") from exc
        raise
    with contextlib.suppress(Exception):
        await sessions.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="SESSION_JOB",
            occurred_at=datetime.now(timezone.utc),
            data={
                "job_id": job_id,
                "plan_id": plan.plan_id,
                "status": "WAITING_APPROVAL" if plan.requires_approval else "PENDING",
            },
        )

    if plan.requires_approval:
        approval_payload = _build_approval_request_payload(plan=plan, validation=validation)
        if approval_request_id is None:
            approval_request_id = str(uuid4())
        approval_record = await agent_registry.create_approval_request(
            approval_request_id=approval_request_id,
            plan_id=plan.plan_id,
            tenant_id=tenant_id,
            session_id=session_id,
            job_id=job_id,
            status="PENDING",
            risk_level=_plan_risk_level(plan),
            requested_by=user_id,
            requested_at=datetime.now(timezone.utc),
            request_payload=approval_payload,
            metadata={"kind": "agent_plan_approval", "source": "agent_session"},
        )

        policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
        if _should_auto_approve(plan=plan, policy=policy):
            decided_at = datetime.now(timezone.utc)
            await agent_registry.decide_approval_request(
                approval_request_id=approval_request_id,
                tenant_id=tenant_id,
                decision="APPROVED",
                decided_by="policy:auto",
                decided_at=decided_at,
                status="APPROVED",
                comment="Auto-approved by tenant policy",
                metadata={"auto": True},
            )
            await agent_registry.create_approval(
                approval_id=str(uuid4()),
                plan_id=plan.plan_id,
                tenant_id=tenant_id,
                step_id=None,
                decision="APPROVED",
                approved_by="policy:auto",
                approved_at=decided_at,
                comment="Auto-approved by tenant policy",
                metadata={"approval_request_id": approval_request_id, "auto": True},
            )
            started = await _start_agent_job_run(
                request=request,
                sessions=sessions,
                tool_registry=tool_registry,
                tenant_id=tenant_id,
                session_id=session_id,
                job_id=job_id,
                plan=plan,
                allowed_tool_ids=allowed_tool_ids,
            )
            return ApiResponse.accepted(
                message="Plan auto-approved; job started",
                data={
                    "session_id": session_id,
                    "job_id": job_id,
                    "plan_id": plan.plan_id,
                    "run_id": started.get("run_id"),
                    "approval_request_id": approval_request_id,
                    "auto_approved": True,
                    "compilation_report": validation.compilation_report.model_dump(mode="json"),
                    "agent": started.get("agent"),
                },
            )

        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="WAITING_APPROVAL")
        return ApiResponse.accepted(
            message="Plan compiled; approval required",
            data={
                "session_id": session_id,
                "job_id": job_id,
                "plan_id": plan.plan_id,
                "status": "WAITING_APPROVAL",
                "approval_request_id": approval_request_id,
                "approval_request": {
                    "approval_request_id": approval_record.approval_request_id,
                    "plan_id": approval_record.plan_id,
                    "status": approval_record.status,
                    "risk_level": approval_record.risk_level,
                    "requested_by": approval_record.requested_by,
                    "requested_at": approval_record.requested_at.isoformat(),
                    "request_payload": approval_record.request_payload,
                    "metadata": approval_record.metadata,
                },
                "validation_warnings": validation.warnings,
                "compilation_report": validation.compilation_report.model_dump(mode="json"),
            },
        )

    # Auto-execute when approval is not required.
    started = await _start_agent_job_run(
        request=request,
        sessions=sessions,
        tool_registry=tool_registry,
        tenant_id=tenant_id,
        session_id=session_id,
        job_id=job_id,
        plan=plan,
        allowed_tool_ids=allowed_tool_ids,
    )
    return ApiResponse.accepted(
        message="Agent job started",
        data={
            "session_id": session_id,
            "job_id": job_id,
            "plan_id": plan.plan_id,
            "run_id": started.get("run_id"),
            "agent": started.get("agent"),
        },
    )


@router.post("/{session_id}/jobs", response_model=ApiResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job_from_plan(
    session_id: str,
    body: AgentSessionJobCreateRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    tools_restricted = bool((session_record.metadata or {}).get("tools_restricted"))
    allowed_tool_ids = list(session_record.enabled_tools or []) if tools_restricted else None

    payload = sanitize_input(body.model_dump(exclude_none=True))
    try:
        plan_id = str(UUID(str(payload.get("plan_id"))))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="plan_id must be a UUID") from exc

    record = await plan_registry.get_plan(plan_id=plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    if validation.plan.requires_approval:
        approvals = await agent_registry.list_approvals(plan_id=plan_id, tenant_id=tenant_id)
        if not approvals:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")
        latest = max(approvals, key=lambda a: a.approved_at)
        decision = str(latest.decision or "").strip().upper()
        if decision in _REJECTED_DECISIONS or decision not in _APPROVED_DECISIONS:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Plan approval required")

    job_id = str(uuid4())
    try:
        await sessions.create_job(
            job_id=job_id,
            session_id=session_id,
            tenant_id=tenant_id,
            plan_id=plan_id,
            status="PENDING",
            metadata={"kind": "agent_plan_job"},
        )
    except ValueError as exc:
        if "active job" in str(exc).lower():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Session already has an active job") from exc
        raise
    started = await _start_agent_job_run(
        request=request,
        sessions=sessions,
        tool_registry=tool_registry,
        tenant_id=tenant_id,
        session_id=session_id,
        job_id=job_id,
        plan=validation.plan,
        allowed_tool_ids=allowed_tool_ids,
    )
    return ApiResponse.accepted(
        message="Agent session job started",
        data={
            "session_id": session_id,
            "job_id": job_id,
            "plan_id": plan_id,
            "run_id": started.get("run_id"),
            "agent": started.get("agent"),
        },
    )


@router.get("/{session_id}/jobs", response_model=ApiResponse)
async def list_jobs(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc
    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)
    jobs = await sessions.list_jobs(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    reconciled = []
    for job in jobs:
        reconciled.append(
            await _best_effort_reconcile_job(job=job, tenant_id=tenant_id, sessions=sessions, agent_registry=agent_registry)
        )
    jobs = reconciled
    return ApiResponse.success(
        message="Agent jobs fetched",
        data={
            "session_id": session_id,
            "count": len(jobs),
            "jobs": [
                {
                    "job_id": j.job_id,
                    "plan_id": j.plan_id,
                    "run_id": j.run_id,
                    "status": j.status,
                    "error": j.error,
                    "created_at": j.created_at.isoformat(),
                    "updated_at": j.updated_at.isoformat(),
                    "finished_at": j.finished_at.isoformat() if j.finished_at else None,
                    "metadata": j.metadata,
                }
                for j in jobs
            ],
        },
    )


@router.get("/{session_id}/jobs/{job_id}", response_model=ApiResponse)
async def get_job(
    session_id: str,
    job_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
        job_uuid = str(UUID(job_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id/job_id must be UUID") from exc
    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)

    job = await sessions.get_job(job_id=job_uuid, tenant_id=tenant_id)
    if not job or job.session_id != session_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    job = await _best_effort_reconcile_job(job=job, tenant_id=tenant_id, sessions=sessions, agent_registry=agent_registry)
    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if session_record and job and getattr(job, "finished_at", None):
        current_status = str(session_record.status or "").strip().upper()
        if current_status != "TERMINATED":
            normalized = str(getattr(job, "status", "") or "").strip().upper()
            try:
                if normalized == "COMPLETED":
                    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="COMPLETED")
                elif normalized in {"FAILED"}:
                    await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ERROR")
            except Exception:
                pass

    return ApiResponse.success(
        message="Agent job fetched",
        data={
            "job": {
                "job_id": job.job_id,
                "session_id": job.session_id,
                "plan_id": job.plan_id,
                "run_id": job.run_id,
                "status": job.status,
                "error": job.error,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                "metadata": job.metadata,
            }
        },
    )


@router.get("/{session_id}/events", response_model=ApiResponse)
async def list_events(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    include_messages: bool = Query(True),
    include_jobs: bool = Query(True),
    include_approvals: bool = Query(True),
    include_agent_steps: bool = Query(True),
    include_tool_calls: bool = Query(True),
    include_llm_calls: bool = Query(True),
    include_ci_results: bool = Query(True),
    limit: int = Query(500, ge=1, le=2000),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=session_record, user_id=user_id)

    events: list[dict[str, Any]] = []
    message_tokens_total = 0
    llm_tokens_total = 0
    tool_request_tokens_total = 0
    tool_response_tokens_total = 0
    cost_total = 0.0

    jobs = []
    if include_jobs or include_agent_steps:
        jobs = await sessions.list_jobs(session_id=session_id, tenant_id=tenant_id, limit=200, offset=0)
        reconciled = []
        for job in jobs:
            reconciled.append(
                await _best_effort_reconcile_job(job=job, tenant_id=tenant_id, sessions=sessions, agent_registry=agent_registry)
            )
        jobs = reconciled

    if include_messages:
        messages = await sessions.list_messages(session_id=session_id, tenant_id=tenant_id, limit=500, offset=0, include_removed=True)
        for msg in messages:
            if msg.token_count:
                message_tokens_total += int(msg.token_count)
            if msg.cost_estimate:
                cost_total += float(msg.cost_estimate)
            events.append(
                {
                    "event_id": msg.message_id,
                    "event_type": "SESSION_MESSAGE",
                    "occurred_at": msg.created_at.isoformat(),
                    "data": {
                        "role": msg.role,
                        "content": msg.content,
                        "content_digest": msg.content_digest,
                        "is_removed": msg.is_removed,
                        "token_count": msg.token_count,
                        "cost_estimate": msg.cost_estimate,
                        "latency_ms": msg.latency_ms,
                        "metadata": msg.metadata,
                    },
                }
            )

    if include_llm_calls and hasattr(sessions, "list_llm_calls"):
        with contextlib.suppress(Exception):
            llm_calls = await sessions.list_llm_calls(session_id=session_id, tenant_id=tenant_id, limit=500, offset=0)
            for call in llm_calls:
                llm_tokens_total += int(call.total_tokens or 0)
                if call.cost_estimate:
                    cost_total += float(call.cost_estimate)
                events.append(
                    {
                        "event_id": call.llm_call_id,
                        "event_type": "LLM_CALL",
                        "occurred_at": call.created_at.isoformat(),
                        "data": {
                            "call_type": call.call_type,
                            "provider": call.provider,
                            "model_id": call.model_id,
                            "cache_hit": call.cache_hit,
                            "latency_ms": call.latency_ms,
                            "prompt_tokens": call.prompt_tokens,
                            "completion_tokens": call.completion_tokens,
                            "total_tokens": call.total_tokens,
                            "cost_estimate": call.cost_estimate,
                            "job_id": call.job_id,
                            "plan_id": call.plan_id,
                        },
                    }
                )

    if include_ci_results and hasattr(sessions, "list_ci_results"):
        with contextlib.suppress(Exception):
            ci_results = await sessions.list_ci_results(session_id=session_id, tenant_id=tenant_id, limit=500, offset=0)
            for ci in ci_results:
                events.append(
                    {
                        "event_id": ci.ci_result_id,
                        "event_type": "CI_RESULT",
                        "occurred_at": ci.created_at.isoformat(),
                        "data": {
                            "provider": ci.provider,
                            "status": ci.status,
                            "details_url": ci.details_url,
                            "summary": ci.summary,
                            "job_id": ci.job_id,
                            "plan_id": ci.plan_id,
                            "run_id": ci.run_id,
                        },
                    }
                )

    if include_tool_calls and hasattr(sessions, "list_tool_calls"):
        with contextlib.suppress(Exception):
            tool_calls = await sessions.list_tool_calls(session_id=session_id, tenant_id=tenant_id, limit=500, offset=0)
            for call in tool_calls:
                started_at = call.started_at
                if call.request_token_count:
                    tool_request_tokens_total += int(call.request_token_count)
                events.append(
                    {
                        "event_id": f"{call.tool_run_id}:start",
                        "event_type": "TOOL_CALL_STARTED",
                        "occurred_at": started_at.isoformat(),
                        "data": {
                            "tool_run_id": call.tool_run_id,
                            "tool_id": call.tool_id,
                            "method": call.method,
                            "path": call.path,
                            "request_digest": call.request_digest,
                            "request_token_count": call.request_token_count,
                            "job_id": call.job_id,
                            "plan_id": call.plan_id,
                            "idempotency_key": call.idempotency_key,
                        },
                    }
                )
                if call.finished_at:
                    if call.response_token_count:
                        tool_response_tokens_total += int(call.response_token_count)
                    events.append(
                        {
                            "event_id": f"{call.tool_run_id}:finish",
                            "event_type": "TOOL_CALL_FINISHED",
                            "occurred_at": call.finished_at.isoformat(),
                            "data": {
                                "tool_run_id": call.tool_run_id,
                                "tool_id": call.tool_id,
                                "status": call.status,
                                "http_status": call.response_status,
                                "response_digest": call.response_digest,
                                "response_token_count": call.response_token_count,
                                "latency_ms": call.latency_ms,
                                "error_code": call.error_code,
                                "error_message": call.error_message,
                                "side_effect_summary": call.side_effect_summary,
                            },
                        }
                    )

    if include_jobs:
        for job in jobs:
            events.append(
                {
                    "event_id": job.job_id,
                    "event_type": "SESSION_JOB",
                    "occurred_at": job.created_at.isoformat(),
                    "data": {
                        "job_id": job.job_id,
                        "plan_id": job.plan_id,
                        "run_id": job.run_id,
                        "status": job.status,
                        "error": job.error,
                        "metadata": job.metadata,
                        "created_at": job.created_at.isoformat(),
                        "updated_at": job.updated_at.isoformat(),
                        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                    },
                }
            )

    if include_approvals:
        approvals = await agent_registry.list_approval_requests(
            tenant_id=tenant_id, session_id=session_id, limit=200, offset=0
        )
        for approval in approvals:
            events.append(
                {
                    "event_id": approval.approval_request_id,
                    "event_type": "APPROVAL_REQUESTED",
                    "occurred_at": approval.requested_at.isoformat(),
                    "data": {
                        "approval_request_id": approval.approval_request_id,
                        "plan_id": approval.plan_id,
                        "job_id": approval.job_id,
                        "status": approval.status,
                        "risk_level": approval.risk_level,
                        "requested_by": approval.requested_by,
                        "request_payload": approval.request_payload,
                        "metadata": approval.metadata,
                    },
                }
            )
            if approval.decided_at:
                events.append(
                    {
                        "event_id": f"{approval.approval_request_id}:decision",
                        "event_type": "APPROVAL_DECIDED",
                        "occurred_at": approval.decided_at.isoformat(),
                        "data": {
                            "approval_request_id": approval.approval_request_id,
                            "decision": approval.decision,
                            "decided_by": approval.decided_by,
                            "comment": approval.comment,
                        },
                    }
                )

    if include_agent_steps:
        for job in jobs:
            if not job.run_id:
                continue
            run = None
            with contextlib.suppress(Exception):
                run = await agent_registry.get_run(run_id=job.run_id, tenant_id=tenant_id)
            if run:
                events.append(
                    {
                        "event_id": run.run_id,
                        "event_type": "AGENT_RUN",
                        "occurred_at": run.started_at.isoformat(),
                        "data": {
                            "run_id": run.run_id,
                            "plan_id": run.plan_id,
                            "status": run.status,
                            "risk_level": run.risk_level,
                            "started_at": run.started_at.isoformat(),
                            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                        },
                    }
                )
            with contextlib.suppress(Exception):
                steps = await agent_registry.list_steps(run_id=job.run_id, tenant_id=tenant_id)
                for step in steps:
                    started_at = step.started_at or step.created_at
                    if started_at:
                        events.append(
                            {
                                "event_id": f"{step.run_id}:{step.step_id}:start",
                                "event_type": "AGENT_STEP_STARTED",
                                "occurred_at": started_at.isoformat(),
                                "data": {
                                    "run_id": step.run_id,
                                    "step_id": step.step_id,
                                    "tool_id": step.tool_id,
                                    "status": step.status,
                                    "metadata": step.metadata,
                                },
                            }
                        )
                    if step.finished_at:
                        duration_ms = None
                        if step.started_at:
                            duration_ms = int((step.finished_at - step.started_at).total_seconds() * 1000)
                        events.append(
                            {
                                "event_id": f"{step.run_id}:{step.step_id}:finish",
                                "event_type": "AGENT_STEP_FINISHED",
                                "occurred_at": step.finished_at.isoformat(),
                                "data": {
                                    "run_id": step.run_id,
                                    "step_id": step.step_id,
                                    "tool_id": step.tool_id,
                                    "status": step.status,
                                    "duration_ms": duration_ms,
                                    "output_digest": step.output_digest,
                                    "error": step.error,
                                },
                            }
                        )

    def _sort_key(item: dict[str, Any]) -> tuple[str, str]:
        return (str(item.get("occurred_at") or ""), str(item.get("event_id") or ""))

    events_sorted = sorted(events, key=_sort_key)
    events_sorted = events_sorted[: int(limit)]

    return ApiResponse.success(
        message="Session events fetched",
        data={
            "session_id": session_id,
            "count": len(events_sorted),
            "events": events_sorted,
            "metrics": {
                "message_tokens_total": message_tokens_total,
                "llm_tokens_total": llm_tokens_total,
                "tool_request_tokens_total": tool_request_tokens_total,
                "tool_response_tokens_total": tool_response_tokens_total,
                "token_total_estimate": message_tokens_total + llm_tokens_total + tool_request_tokens_total + tool_response_tokens_total,
                "message_cost_total": cost_total,
                "jobs_total": len(jobs),
            },
        },
    )


@router.get("/{session_id}/tool-calls", response_model=ApiResponse)
async def list_tool_calls(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    tool_calls = await sessions.list_tool_calls(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="Tool calls fetched",
        data={
            "session_id": session_id,
            "count": len(tool_calls),
            "tool_calls": [
                {
                    "tool_run_id": c.tool_run_id,
                    "job_id": c.job_id,
                    "plan_id": c.plan_id,
                    "run_id": c.run_id,
                    "step_id": c.step_id,
                    "tool_id": c.tool_id,
                    "method": c.method,
                    "path": c.path,
                    "query": c.query,
                    "request_body": c.request_body,
                    "request_digest": c.request_digest,
                    "request_token_count": c.request_token_count,
                    "idempotency_key": c.idempotency_key,
                    "status": c.status,
                    "response_status": c.response_status,
                    "response_body": c.response_body,
                    "response_digest": c.response_digest,
                    "response_token_count": c.response_token_count,
                    "error_code": c.error_code,
                    "error_message": c.error_message,
                    "side_effect_summary": c.side_effect_summary,
                    "latency_ms": c.latency_ms,
                    "started_at": c.started_at.isoformat(),
                    "finished_at": c.finished_at.isoformat() if c.finished_at else None,
                }
                for c in tool_calls
            ],
        },
    )


@router.get("/{session_id}/llm-calls", response_model=ApiResponse)
async def list_llm_calls(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    llm_calls = await sessions.list_llm_calls(session_id=session_id, tenant_id=tenant_id, limit=limit, offset=offset)
    return ApiResponse.success(
        message="LLM calls fetched",
        data={
            "session_id": session_id,
            "count": len(llm_calls),
            "llm_calls": [
                {
                    "llm_call_id": c.llm_call_id,
                    "job_id": c.job_id,
                    "plan_id": c.plan_id,
                    "call_type": c.call_type,
                    "provider": c.provider,
                    "model_id": c.model_id,
                    "cache_hit": c.cache_hit,
                    "latency_ms": c.latency_ms,
                    "prompt_tokens": c.prompt_tokens,
                    "completion_tokens": c.completion_tokens,
                    "total_tokens": c.total_tokens,
                    "cost_estimate": c.cost_estimate,
                    "input_digest": c.input_digest,
                    "output_digest": c.output_digest,
                    "created_at": c.created_at.isoformat(),
                }
                for c in llm_calls
            ],
        },
    )


@router.get("/{session_id}/approvals", response_model=ApiResponse)
async def list_approvals(
    session_id: str,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id must be a UUID") from exc

    record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    _ensure_session_owner(record=record, user_id=user_id)

    approvals = await agent_registry.list_approval_requests(
        tenant_id=tenant_id,
        session_id=session_id,
        limit=limit,
        offset=offset,
    )
    return ApiResponse.success(
        message="Approval requests fetched",
        data={
            "session_id": session_id,
            "count": len(approvals),
            "approvals": [
                {
                    "approval_request_id": a.approval_request_id,
                    "plan_id": a.plan_id,
                    "job_id": a.job_id,
                    "status": a.status,
                    "risk_level": a.risk_level,
                    "requested_by": a.requested_by,
                    "requested_at": a.requested_at.isoformat(),
                    "decision": a.decision,
                    "decided_by": a.decided_by,
                    "decided_at": a.decided_at.isoformat() if a.decided_at else None,
                    "comment": a.comment,
                    "request_payload": a.request_payload,
                    "metadata": a.metadata,
                }
                for a in approvals
            ],
        },
    )


@router.post("/{session_id}/approvals/{approval_request_id}", response_model=ApiResponse)
async def decide_approval(
    session_id: str,
    approval_request_id: str,
    body: AgentSessionApprovalDecisionRequest,
    request: Request,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    tool_registry: AgentToolRegistry = Depends(get_agent_tool_registry),
    plan_registry: AgentPlanRegistry = Depends(get_agent_plan_registry),
    agent_registry: AgentRegistry = Depends(get_agent_registry),
) -> ApiResponse:
    tenant_id, user_id, _actor = _resolve_verified_principal(request)
    try:
        session_id = str(UUID(session_id))
        approval_request_id = str(UUID(approval_request_id))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="session_id/approval_request_id must be UUID") from exc

    session_record = await sessions.get_session(session_id=session_id, tenant_id=tenant_id)
    if not session_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found")
    if str(session_record.status or "").strip().upper() == "TERMINATED":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Agent session terminated")

    approval = await agent_registry.get_approval_request(approval_request_id=approval_request_id, tenant_id=tenant_id)
    if not approval or approval.session_id != session_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found")

    payload = sanitize_input(body.model_dump(exclude_none=True))
    decision = str(payload.get("decision") or "").strip().upper()
    comment = str(payload.get("comment") or "").strip() or None
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    if decision in _APPROVED_DECISIONS:
        status_value = "APPROVED"
    elif decision in _REJECTED_DECISIONS:
        status_value = "REJECTED"
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="decision must be APPROVE or REJECT")

    decided_at = datetime.now(timezone.utc)
    decided = await agent_registry.decide_approval_request(
        approval_request_id=approval_request_id,
        tenant_id=tenant_id,
        decision=status_value,
        decided_by=user_id,
        decided_at=decided_at,
        status=status_value,
        comment=comment,
        metadata=metadata,
    )
    if not decided:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found")

    await agent_registry.create_approval(
        approval_id=str(uuid4()),
        plan_id=approval.plan_id,
        tenant_id=tenant_id,
        step_id=None,
        decision=status_value,
        approved_by=user_id,
        approved_at=decided_at,
        comment=comment,
        metadata={"approval_request_id": approval_request_id, **metadata},
    )

    job_id = approval.job_id
    if status_value == "REJECTED":
        if job_id:
            await sessions.update_job(job_id=job_id, tenant_id=tenant_id, status="REJECTED", error="Rejected by user")
        await sessions.update_session(session_id=session_id, tenant_id=tenant_id, status="ACTIVE")
        return ApiResponse.success(
            message="Approval rejected",
            data={"session_id": session_id, "approval_request_id": approval_request_id, "status": status_value},
        )

    if not job_id:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Approval request missing job_id")

    job = await sessions.get_job(job_id=job_id, tenant_id=tenant_id)
    if not job or job.session_id != session_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found for approval")
    if job.run_id:
        return ApiResponse.success(
            message="Job already running",
            data={"session_id": session_id, "job_id": job.job_id, "run_id": job.run_id},
        )

    record = await plan_registry.get_plan(plan_id=approval.plan_id, tenant_id=tenant_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent plan not found")

    plan = AgentPlan.model_validate(record.plan)
    tools_restricted = bool((session_record.metadata or {}).get("tools_restricted"))
    allowed_tool_ids = list(session_record.enabled_tools or []) if tools_restricted else None
    validation = await validate_agent_plan(plan=plan, tool_registry=tool_registry, allowed_tool_ids=allowed_tool_ids)
    if validation.errors:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"errors": validation.errors, "warnings": validation.warnings})

    started = await _start_agent_job_run(
        request=request,
        sessions=sessions,
        tool_registry=tool_registry,
        tenant_id=tenant_id,
        session_id=session_id,
        job_id=job_id,
        plan=validation.plan,
        allowed_tool_ids=allowed_tool_ids,
    )
    return ApiResponse.accepted(
        message="Approval recorded; job resumed",
        data={
            "session_id": session_id,
            "job_id": job_id,
            "plan_id": approval.plan_id,
            "run_id": started.get("run_id"),
            "agent": started.get("agent"),
            "approval_request_id": approval_request_id,
            "status": status_value,
        },
    )
