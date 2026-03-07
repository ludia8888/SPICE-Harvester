from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote
from uuid import UUID, uuid4, uuid5

import aiohttp
import httpx

from agent.models import AgentToolCall
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode
from shared.foundry.payload_ids import (
    extract_build_rid as extract_build_rid_from_payload,
    extract_pipeline_id as extract_pipeline_id_from_payload,
    extract_pipeline_job_id as extract_pipeline_job_id_from_payload,
)
from shared.foundry.rids import build_rid as build_foundry_rid, extract_build_job_id, parse_rid
from shared.models.event_envelope import EventEnvelope
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.storage.event_store import EventStore
from shared.utils.llm_safety import digest_for_audit, mask_pii, truncate_text

logger = logging.getLogger(__name__)

_COMMAND_PENDING_STATUSES = {"PENDING", "PROCESSING", "RETRYING"}
_COMMAND_TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}
_COMMAND_STATUSES = _COMMAND_PENDING_STATUSES | _COMMAND_TERMINAL_STATUSES

_PIPELINE_PENDING_STATUSES = {"QUEUED", "RUNNING", "PENDING", "PROCESSING", "WAITING", "IN_PROGRESS"}
_PIPELINE_TERMINAL_STATUSES = {"SUCCESS", "SUCCEEDED", "FAILED", "CANCELLED", "CANCELED", "DEPLOYED", "DID_NOT_RUN"}
_PIPELINE_STATUSES = _PIPELINE_PENDING_STATUSES | _PIPELINE_TERMINAL_STATUSES

_TEMPLATE_TOKEN_RE = re.compile(r"\$\{([^{}]+)\}")
_TEMPLATE_KEY_ALLOWED = re.compile(r"^[A-Za-z0-9._-]{1,200}$")

_STEP_OUTPUT_KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "pipeline_id": ("pipeline_id", "pipelineId"),
    "pipeline_rid": ("pipeline_rid", "pipelineRid"),
    "dataset_id": ("dataset_id", "datasetId"),
    "dataset_rid": ("dataset_rid", "datasetRid"),
    "dataset_version_id": ("dataset_version_id", "datasetVersionId", "version_id", "versionId"),
    "job_id": ("job_id", "jobId"),
    "build_rid": ("build_rid", "buildRid"),
    "artifact_id": ("artifact_id", "artifactId"),
    "ingest_request_id": ("ingest_request_id", "ingestRequestId"),
    "transaction_rid": ("transaction_rid", "transactionRid"),
    "mapping_spec_id": ("mapping_spec_id", "mappingSpecId"),
    "action_log_id": ("action_log_id", "actionLogId"),
    "command_id": ("command_id", "commandId"),
    "deployed_commit_id": ("deployed_commit_id", "deployedCommitId", "merge_commit_id", "mergeCommitId"),
}

_ARTIFACT_MAX_DEPTH = 6
_ARTIFACT_MAX_LIST_ITEMS = 80
_ARTIFACT_MAX_DICT_ITEMS = 120
_ARTIFACT_MAX_STRING_CHARS = 4000

_DELEGATED_AUTH_HEADER = "X-Delegated-Authorization"


def _clean_url(value: str) -> str:
    return value.rstrip("/")


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, separators=(",", ":"), default=str)


_ERROR_CODE_ALLOWED = re.compile(r"^[A-Z0-9_]{1,120}$")
_REQUEST_VALIDATION_FAILED_CODE = ErrorCode.REQUEST_VALIDATION_FAILED.value
_CONFLICT_CODE = ErrorCode.CONFLICT.value


def _normalize_error_code(value: Optional[str]) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", raw).strip("_").upper()
    if not normalized:
        return None
    if _ERROR_CODE_ALLOWED.match(normalized):
        return normalized
    return normalized[:120]


def _extract_error_message(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    candidates: list[Any] = [payload]
    detail = payload.get("detail")
    if isinstance(detail, dict):
        candidates.append(detail)
    context = payload.get("context")
    if isinstance(context, dict):
        candidates.append(context)
        context_detail = context.get("detail")
        if isinstance(context_detail, dict):
            candidates.append(context_detail)
    data = payload.get("data")
    if isinstance(data, dict):
        candidates.append(data)
    result = payload.get("result")
    if isinstance(result, dict):
        candidates.append(result)

    for item in candidates:
        if isinstance(item, str) and item.strip():
            return item.strip()
        if not isinstance(item, dict):
            continue
        for key in ("message", "detail", "error"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _compute_tool_run_id(*, run_id: str, step_id: Optional[str], step_index: int, attempt: int) -> str:
    """
    Deterministic-ish tool run id for observability.

    We keep this stable across retries by including attempt, so each attempt is distinguishable.
    """
    base = f"{(step_id or f'step_{step_index}').strip()}:{int(attempt)}"
    try:
        return str(uuid5(UUID(str(run_id)), base))
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at agent/services/agent_runtime.py:128", exc_info=True)
        return str(uuid4())


def _extract_retry_after_ms(headers: Dict[str, str]) -> Optional[int]:
    raw = (headers.get("retry-after") or headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        seconds = int(raw)
    except ValueError:
        seconds = None

    if seconds is not None:
        if seconds < 0:
            return None
        return seconds * 1000

    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta_ms = int((dt - datetime.now(timezone.utc)).total_seconds() * 1000)
        if delta_ms <= 0:
            return None
        return delta_ms
    except (TypeError, ValueError, OverflowError):
        return None

def _is_agent_proxy_path(path: str) -> bool:
    normalized = path if path.startswith("/") else f"/{path}"
    return normalized.startswith("/api/v1/agent")


def _resolve_template_token(token: str, context: Dict[str, Any]) -> Any:
    token = str(token or "").strip()
    if not token:
        raise ValueError("empty template token")

    if token.startswith("steps."):
        rest = token[len("steps."):]
        parts = rest.split(".")
        if len(parts) < 2:
            raise ValueError(f"invalid steps token: {token}")
        step_id = ".".join(parts[:-1])
        key = parts[-1]
        if not _TEMPLATE_KEY_ALLOWED.match(step_id):
            raise ValueError(f"invalid step_id in token: {token}")
        if not _TEMPLATE_KEY_ALLOWED.match(key):
            raise ValueError(f"invalid key in token: {token}")
        step_outputs = context.get("step_outputs")
        if not isinstance(step_outputs, dict):
            raise ValueError(f"missing step_outputs for token: {token}")
        step_payload = step_outputs.get(step_id)
        if not isinstance(step_payload, dict) or key not in step_payload:
            raise ValueError(f"missing value for token: {token}")
        return step_payload.get(key)

    if token.startswith("context."):
        key = token[len("context."):]
        if not _TEMPLATE_KEY_ALLOWED.match(key):
            raise ValueError(f"invalid context key token: {token}")
        if key not in context:
            raise ValueError(f"missing context value for token: {token}")
        return context.get(key)

    if token.startswith("artifacts."):
        key = token[len("artifacts."):]
        if not _TEMPLATE_KEY_ALLOWED.match(key):
            raise ValueError(f"invalid artifact token: {token}")
        artifacts = context.get("artifacts")
        if not isinstance(artifacts, dict) or key not in artifacts:
            raise ValueError(f"missing artifact for token: {token}")
        return artifacts.get(key)

    raise ValueError(f"unsupported template token: {token}")


def _resolve_template_string(value: str, context: Dict[str, Any]) -> Any:
    raw = str(value or "")
    if "${" not in raw:
        return value

    matches = list(_TEMPLATE_TOKEN_RE.finditer(raw))
    if not matches:
        return value

    if len(matches) == 1 and matches[0].start() == 0 and matches[0].end() == len(raw):
        token = matches[0].group(1)
        return _resolve_template_token(token, context)

    out_parts: list[str] = []
    cursor = 0
    for match in matches:
        out_parts.append(raw[cursor:match.start()])
        token = match.group(1)
        resolved = _resolve_template_token(token, context)
        out_parts.append("" if resolved is None else str(resolved))
        cursor = match.end()
    out_parts.append(raw[cursor:])
    return "".join(out_parts)


def _resolve_templates(obj: Any, context: Dict[str, Any]) -> Any:
    if obj is None:
        return None
    if isinstance(obj, str):
        return _resolve_template_string(obj, context)
    if isinstance(obj, dict):
        return {key: _resolve_templates(value, context) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_resolve_templates(value, context) for value in obj]
    return obj


def _artifact_value_from_response(payload: Any) -> Any:
    """
    Choose the most useful artifact value from a tool response payload.

    Convention: If the payload looks like ApiResponse/CommandResult and has a top-level `data` field,
    store `data` as the artifact so `${artifacts.*}` is directly usable as a request body.
    """
    if isinstance(payload, dict) and "data" in payload:
        data = payload.get("data")
        if data not in (None, ""):
            return data
    return payload


def _compact_tool_payload(payload: Any) -> Any:
    """
    Best-effort compaction for large tool payloads.

    This keeps the response envelope but drops known heavy fields so the agent
    graph can still read essential outputs (e.g., preview/preflight) under the
    `AGENT_TOOL_MAX_PAYLOAD_BYTES` cap.
    """
    if not isinstance(payload, dict):
        return payload

    # Common ApiResponse shape: {"status": "...", "message": "...", "data": {...}}
    data = payload.get("data")
    if isinstance(data, dict):
        trimmed_data = dict(data)
        # Preview endpoints can return large "run_tables" payloads.
        trimmed_data.pop("run_tables", None)
        trimmed_data.pop("runTables", None)
        # Some tools may embed raw tables under other common names.
        trimmed_data.pop("tables", None)
        trimmed_data.pop("raw_tables", None)
        trimmed = dict(payload)
        trimmed["data"] = trimmed_data
        return trimmed

    return payload


def _compact_artifact_value(obj: Any, *, depth: int = 0) -> Any:
    if depth > _ARTIFACT_MAX_DEPTH:
        return "<truncated>"
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        if len(obj) <= _ARTIFACT_MAX_STRING_CHARS:
            return obj
        return obj[: max(0, _ARTIFACT_MAX_STRING_CHARS - 1)] + "…"
    if isinstance(obj, list):
        trimmed = [_compact_artifact_value(v, depth=depth + 1) for v in obj[:_ARTIFACT_MAX_LIST_ITEMS]]
        if len(obj) > _ARTIFACT_MAX_LIST_ITEMS:
            trimmed.append(f"<truncated:{len(obj) - _ARTIFACT_MAX_LIST_ITEMS}>")
        return trimmed
    if isinstance(obj, dict):
        items = list(obj.items())
        out: dict[str, Any] = {}
        for key, value in items[:_ARTIFACT_MAX_DICT_ITEMS]:
            out[str(key)] = _compact_artifact_value(value, depth=depth + 1)
        if len(items) > _ARTIFACT_MAX_DICT_ITEMS:
            out["_truncated_keys"] = len(items) - _ARTIFACT_MAX_DICT_ITEMS
        return out
    return str(obj)


def _walk_json_for_key(obj: Any, *, wanted_keys: set[str], max_depth: int = 6, max_nodes: int = 500) -> dict[str, Any]:
    found: dict[str, Any] = {}
    if not wanted_keys:
        return found

    queue: list[tuple[Any, int]] = [(obj, 0)]
    visited = 0
    while queue and visited < max_nodes and len(found) < len(wanted_keys):
        current, depth = queue.pop(0)
        visited += 1
        if depth > max_depth:
            continue
        if isinstance(current, dict):
            for key in wanted_keys:
                if key in found:
                    continue
                if key in current and current[key] not in (None, ""):
                    found[key] = current[key]
            for value in current.values():
                if isinstance(value, (dict, list)):
                    queue.append((value, depth + 1))
        elif isinstance(current, list):
            for value in current[:50]:
                if isinstance(value, (dict, list)):
                    queue.append((value, depth + 1))
    return found


def _coerce_scalar(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)):
        return value
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return None
    return str(value)


def _extract_foundry_rid_derived_outputs(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, (dict, list)):
        return {}

    found = _walk_json_for_key(
        payload,
        wanted_keys={"rid", "buildRid", "datasetRid", "pipelineRid", "targetRid", "transactionRid"},
    )

    outputs: dict[str, Any] = {}
    for value in found.values():
        raw = str(value or "").strip()
        if not raw:
            continue
        try:
            kind, rid_id = parse_rid(raw)
        except ValueError:
            continue

        if kind == "build":
            outputs.setdefault("build_rid", build_foundry_rid("build", rid_id))
            outputs.setdefault("job_id", rid_id)
        elif kind == "dataset":
            outputs.setdefault("dataset_rid", build_foundry_rid("dataset", rid_id))
            outputs.setdefault("dataset_id", rid_id)
        elif kind == "pipeline":
            outputs.setdefault("pipeline_rid", build_foundry_rid("pipeline", rid_id))
            outputs.setdefault("pipeline_id", rid_id)
        elif kind == "transaction":
            outputs.setdefault("transaction_rid", build_foundry_rid("transaction", rid_id))

    return outputs


def _extract_step_outputs(payload: Any) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    if not isinstance(payload, (dict, list)):
        return outputs
    wanted = {alias for aliases in _STEP_OUTPUT_KEY_ALIASES.values() for alias in aliases}
    found = _walk_json_for_key(payload, wanted_keys=wanted)
    for canonical, aliases in _STEP_OUTPUT_KEY_ALIASES.items():
        value = None
        for alias in aliases:
            if alias in found:
                value = found.get(alias)
                break
        scalar = _coerce_scalar(value)
        if scalar not in (None, ""):
            outputs[canonical] = scalar
    outputs.update(_extract_foundry_rid_derived_outputs(payload))
    return outputs


def _extract_scope(context: Dict[str, Any]) -> Dict[str, Any]:
    allowed_keys = {
        "db_name",
        "branch",
        "dataset_id",
        "dataset_version",
        "pipeline_id",
        "object_type",
        "class_id",
        "link_type",
        "index_target",
        "mapping_spec_version",
    }
    scope: Dict[str, Any] = {}
    for key in allowed_keys:
        if key in context and context[key] not in (None, ""):
            scope[key] = context[key]
    return scope


def _normalize_status(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip().upper()
    return raw or None


def _extract_command_id_from_url(url: str) -> Optional[str]:
    match = re.search(r"/commands/([^/]+)/status", url or "")
    if match:
        return match.group(1)
    return None


def _extract_command_id(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("command_id", "commandId"):
        value = payload.get(key)
        if value:
            return str(value)
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("command_id", "commandId"):
            value = data.get(key)
            if value:
                return str(value)
        commands = data.get("commands")
        if isinstance(commands, list) and len(commands) == 1 and isinstance(commands[0], dict):
            cmd_id = commands[0].get("command_id") or commands[0].get("commandId")
            if cmd_id:
                return str(cmd_id)
    status_url = payload.get("status_url") or payload.get("statusUrl")
    if not status_url and isinstance(data, dict):
        status_url = data.get("status_url") or data.get("statusUrl")
    if isinstance(status_url, str):
        return _extract_command_id_from_url(status_url)
    return None


def _extract_command_status(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    status = _normalize_status(payload.get("status"))
    if status:
        return status
    data = payload.get("data")
    if isinstance(data, dict):
        status = _normalize_status(data.get("status"))
        if status:
            return status
    result = payload.get("result")
    if isinstance(result, dict):
        status = _normalize_status(result.get("status"))
        if status:
            return status
    return None


def _extract_pipeline_job_id(payload: Any) -> Optional[str]:
    return extract_pipeline_job_id_from_payload(payload)


def _extract_pipeline_id(payload: Any) -> Optional[str]:
    return extract_pipeline_id_from_payload(payload)


def _extract_build_job_id_from_rid(build_rid: str) -> Optional[str]:
    return extract_build_job_id(build_rid)


def _extract_build_rid(payload: Any) -> Optional[str]:
    return extract_build_rid_from_payload(payload, keys=("buildRid", "rid"))


def _extract_status_url(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    status_url = payload.get("status_url") or payload.get("statusUrl")
    if status_url:
        return str(status_url)
    data = payload.get("data")
    if isinstance(data, dict):
        status_url = data.get("status_url") or data.get("statusUrl")
        if status_url:
            return str(status_url)
    return None


def _extract_progress(payload: Any) -> Tuple[Optional[float], Optional[str], Dict[str, Any]]:
    progress_payload: Dict[str, Any] = {}
    message: Optional[str] = None
    progress_value: Optional[float] = None

    if isinstance(payload, dict):
        data = payload.get("data")
        result = payload.get("result")
        if isinstance(result, dict) and not isinstance(data, dict):
            data = result
        if isinstance(data, dict):
            progress = data.get("progress")
            if isinstance(progress, dict):
                progress_payload = dict(progress)
                if "percentage" in progress:
                    progress_value = progress.get("percentage")
                elif "percent" in progress:
                    progress_value = progress.get("percent")
                elif "current" in progress and "total" in progress:
                    total = progress.get("total") or 0
                    current = progress.get("current") or 0
                    progress_value = (float(current) / float(total) * 100.0) if total else None
                message = progress.get("message") or progress.get("note")
            elif isinstance(progress, (int, float)):
                progress_value = float(progress)
            if not message:
                message = data.get("progress_message") or data.get("message")
        if not message:
            message = payload.get("message") if isinstance(payload.get("message"), str) else None

    if progress_value is not None:
        try:
            progress_value = max(0.0, min(100.0, float(progress_value)))
        except (TypeError, ValueError):
            progress_value = None

    if progress_value is not None:
        progress_payload.setdefault("percentage", progress_value)
    if message:
        progress_payload.setdefault("message", message)

    return progress_value, message, progress_payload


def _method_is_write(method: str) -> bool:
    return str(method or "").strip().upper() in {"POST", "PUT", "PATCH", "DELETE"}


def _tool_call_expects_pipeline_job(*, tool_id: Optional[str], path: str) -> bool:
    resolved_tool = str(tool_id or "").strip()
    if resolved_tool == "orchestration.builds.create":
        return True
    normalized_path = str(path or "").rstrip("/")
    return normalized_path == "/api/v2/orchestration/builds/create"


def _extract_overlay_status(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    candidates = [payload]
    detail = payload.get("detail")
    if isinstance(detail, dict):
        candidates.append(detail)
    context = payload.get("context")
    if isinstance(context, dict):
        candidates.append(context)
        context_detail = context.get("detail")
        if isinstance(context_detail, dict):
            candidates.append(context_detail)
    data = payload.get("data")
    if isinstance(data, dict):
        candidates.append(data)

    for item in candidates:
        value = item.get("overlay_status")
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return None


def _extract_enterprise_external_code(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    enterprise = payload.get("enterprise")
    if not isinstance(enterprise, dict):
        return None
    external_code = enterprise.get("external_code")
    if isinstance(external_code, str) and external_code.strip():
        return external_code.strip()
    return None


def _iter_error_candidates(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    candidates: list[dict[str, Any]] = [payload]
    for key in ("detail", "context", "result", "data"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)
    return candidates


def _extract_error_key(payload: Any) -> Optional[str]:
    enterprise = _extract_enterprise(payload)
    if enterprise:
        external_code = enterprise.get("external_code")
        if isinstance(external_code, str) and external_code.strip():
            return external_code.strip()
    for candidate in _iter_error_candidates(payload):
        value = candidate.get("error")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_enterprise(payload: Any) -> Optional[dict[str, Any]]:
    for candidate in _iter_error_candidates(payload):
        enterprise = candidate.get("enterprise")
        if isinstance(enterprise, dict) and enterprise:
            return dict(enterprise)
    return None


def _extract_api_code(payload: Any) -> Optional[str]:
    for candidate in _iter_error_candidates(payload):
        value = candidate.get("code")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_api_category(payload: Any) -> Optional[str]:
    for candidate in _iter_error_candidates(payload):
        value = candidate.get("category")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_retryable(payload: Any) -> Optional[bool]:
    for candidate in _iter_error_candidates(payload):
        value = candidate.get("retryable")
        if isinstance(value, bool):
            return value
    enterprise = _extract_enterprise(payload)
    if enterprise:
        retryable = enterprise.get("retryable")
        if isinstance(retryable, bool):
            return retryable
    return None


def _extract_action_log_signals(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if not isinstance(data, dict):
        return {}
    class_id = str(data.get("class_id") or "").strip()
    if class_id.lower() != "actionlog" and "action_log_id" not in data:
        return {}
    result = data.get("result") if isinstance(data.get("result"), dict) else {}
    signals: dict[str, Any] = {
        "action_log_id": str(data.get("action_log_id") or data.get("instance_id") or "").strip() or None,
        "action_log_status": str(data.get("status") or "").strip().upper() or None,
        "action_log_error": str(result.get("error") or "").strip() or None,
    }
    for key in ("reason", "reasons", "criteria_identifiers", "actor_role"):
        if key in result:
            signals[f"action_log_{key}"] = result.get(key)
    return {k: v for k, v in signals.items() if v not in (None, "", [], {})}


def _extract_action_simulation_signals(payload: Any) -> dict[str, Any]:
    """
    Extract minimal, policy-relevant signals from ActionSimulation responses.

    Note: OMS/BFF simulate endpoints often return HTTP 200 even when the *effective* scenario is REJECTED,
    so we must inspect the body to drive safe automation decisions.
    """

    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if not isinstance(data, dict):
        return {}
    results = data.get("results")
    if not isinstance(results, list):
        return {}
    if "preview_action_log_id" not in data:
        return {}

    scenario_summaries: list[dict[str, Any]] = []
    effective: Optional[dict[str, Any]] = None
    for item in results:
        if not isinstance(item, dict):
            continue
        scenario_id = str(item.get("scenario_id") or "default").strip() or "default"
        status = str(item.get("status") or "").strip().upper() or None
        conflict_override = item.get("conflict_policy_override")
        scenario_summaries.append(
            {
                "scenario_id": scenario_id,
                "status": status,
                "conflict_policy_override": conflict_override,
            }
        )
        if effective is None and (conflict_override is None) and scenario_id in {"default", "effective"}:
            effective = item

    if effective is None and scenario_summaries:
        effective = next((i for i in results if isinstance(i, dict)), None)

    effective_status = str((effective or {}).get("status") or "").strip().upper() or None
    signals: dict[str, Any] = {
        "action_simulation_preview_action_log_id": str(data.get("preview_action_log_id") or "").strip() or None,
        "action_simulation_effective_status": effective_status,
        "action_simulation_scenarios": scenario_summaries[:20] if scenario_summaries else None,
    }

    error_payload = (effective or {}).get("error")
    if effective_status == "REJECTED" and isinstance(error_payload, dict):
        reason = error_payload.get("reason")
        if isinstance(reason, str) and reason.strip():
            signals["action_log_reason"] = reason.strip()
        reasons = error_payload.get("reasons")
        if reasons not in (None, "", [], {}):
            signals["action_log_reasons"] = reasons

    return {k: v for k, v in signals.items() if v not in (None, "", [], {})}


def _extract_action_simulation_rejection(payload: Any) -> tuple[Optional[str], Optional[dict[str, Any]], Optional[str]]:
    """
    If the payload is an ActionSimulation response and the effective scenario is REJECTED,
    return (error_key, enterprise, message).
    """

    if not isinstance(payload, dict):
        return None, None, None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None, None, None
    results = data.get("results")
    if not isinstance(results, list) or not results:
        return None, None, None

    effective: Optional[dict[str, Any]] = None
    for item in results:
        if not isinstance(item, dict):
            continue
        if (item.get("conflict_policy_override") is None) and str(item.get("scenario_id") or "").strip() in {
            "",
            "default",
            "effective",
        }:
            effective = item
            break
    if effective is None:
        first = results[0]
        effective = first if isinstance(first, dict) else None

    if not effective:
        return None, None, None
    status = str(effective.get("status") or "").strip().upper()
    if status != "REJECTED":
        return None, None, None

    err = effective.get("error")
    if not isinstance(err, dict):
        return "action_simulation_rejected", None, "simulation rejected"

    error_key = str(err.get("error") or "action_simulation_rejected").strip() or "action_simulation_rejected"
    enterprise = err.get("enterprise") if isinstance(err.get("enterprise"), dict) else None
    message = str(err.get("message") or "simulation rejected").strip() or "simulation rejected"
    return error_key, (dict(enterprise) if enterprise else None), message


@dataclass(frozen=True)
class AgentRuntimeConfig:
    bff_url: str
    allowed_services: Tuple[str, ...]
    max_preview_chars: int
    max_payload_bytes: int
    timeout_s: float
    service_name: str
    bff_token: Optional[str]
    command_timeout_s: float
    command_poll_interval_s: float
    command_ws_idle_s: float
    command_ws_enabled: bool
    pipeline_wait_enabled: bool
    block_writes_on_overlay_degraded: bool
    allow_degraded_writes: bool
    auto_retry_enabled: bool
    auto_retry_max_attempts: int
    auto_retry_base_delay_s: float
    auto_retry_max_delay_s: float
    auto_retry_allow_writes: bool


class AgentRuntime:
    def __init__(
        self,
        *,
        event_store: EventStore,
        audit_store: Optional[AuditLogStore],
        config: AgentRuntimeConfig,
    ):
        self.event_store = event_store
        self.audit_store = audit_store
        self.config = config

    @classmethod
    def from_env(
        cls,
        *,
        event_store: EventStore,
        audit_store: Optional[AuditLogStore],
    ) -> "AgentRuntime":
        settings = get_settings()
        agent_settings = settings.agent

        bff_url_raw = (agent_settings.bff_base_url or "").strip() or settings.services.bff_base_url
        bff_url = _clean_url(bff_url_raw)
        bff_token = (agent_settings.bff_token or "").strip() or None
        allowed_services = ("bff",)

        tool_timeout = max(float(agent_settings.tool_timeout_seconds), float(settings.llm.timeout_seconds))
        config = AgentRuntimeConfig(
            bff_url=bff_url,
            allowed_services=allowed_services,
            max_preview_chars=agent_settings.audit_max_preview_chars,
            max_payload_bytes=agent_settings.tool_max_payload_bytes,
            timeout_s=tool_timeout,
            service_name=agent_settings.service_name,
            bff_token=bff_token,
            command_timeout_s=agent_settings.command_timeout_seconds,
            command_poll_interval_s=agent_settings.command_poll_interval_seconds,
            command_ws_idle_s=agent_settings.command_ws_idle_seconds,
            command_ws_enabled=agent_settings.command_ws_enabled,
            pipeline_wait_enabled=agent_settings.pipeline_wait_enabled,
            block_writes_on_overlay_degraded=agent_settings.block_writes_on_overlay_degraded,
            allow_degraded_writes=agent_settings.allow_degraded_writes,
            auto_retry_enabled=agent_settings.auto_retry_enabled,
            auto_retry_max_attempts=agent_settings.auto_retry_max_attempts,
            auto_retry_base_delay_s=agent_settings.auto_retry_base_delay_seconds,
            auto_retry_max_delay_s=agent_settings.auto_retry_max_delay_seconds,
            auto_retry_allow_writes=agent_settings.auto_retry_allow_writes,
        )
        return cls(event_store=event_store, audit_store=audit_store, config=config)

    def _resolve_base_url(self, tool_call: AgentToolCall) -> str:
        service = tool_call.service.lower()
        if service not in self.config.allowed_services:
            raise ValueError(f"service not allowed: {service}")
        if service == "bff":
            return self.config.bff_url
        raise ValueError(f"unsupported service: {service}")

    def _preview_payload(self, obj: Any) -> str:
        masked = mask_pii(obj, max_string_chars=200)
        raw = _safe_json(masked)
        return truncate_text(raw, max_chars=self.config.max_preview_chars)

    def _payload_size(self, obj: Any) -> int:
        try:
            return len(_safe_json(obj).encode("utf-8"))
        except Exception:
            logging.getLogger(__name__).warning("Exception fallback at agent/services/agent_runtime.py:872", exc_info=True)
            return 0

    def _forward_headers(self, headers: Dict[str, str], actor: str) -> Dict[str, str]:
        def _get_header(name: str) -> Optional[str]:
            for variant in (name, name.lower(), name.upper()):
                value = headers.get(variant)
                if value:
                    return value
            return None

        allowlist = {
            "x-user-id",
            "x-user",
            "x-user-type",
            "x-actor",
            "x-actor-type",
            "x-principal-id",
            "x-principal-type",
            "x-db-name",
            "x-project",
            "x-project-id",
            "x-admin-token",
            "authorization",
            "x-request-id",
        }
        output = {
            key: value for key, value in headers.items() if key.lower() in allowlist and value
        }
        output["X-Spice-Caller"] = "agent"
        delegated = _get_header(_DELEGATED_AUTH_HEADER) or _get_header("Authorization")
        delegated_token = delegated.strip() if isinstance(delegated, str) else ""
        if delegated_token.lower().startswith("bearer "):
            delegated_token = delegated_token.split(" ", 1)[1].strip()
        if delegated_token:
            output[_DELEGATED_AUTH_HEADER] = f"Bearer {delegated_token}"
            for key in list(output.keys()):
                if key.lower() == _DELEGATED_AUTH_HEADER.lower() and key != _DELEGATED_AUTH_HEADER:
                    output.pop(key, None)

        if self.config.bff_token:
            output["X-Admin-Token"] = self.config.bff_token
            for key in list(output.keys()):
                if key.lower() == "x-admin-token" and key != "X-Admin-Token":
                    output.pop(key, None)
        output.setdefault("X-Actor", actor)
        return output

    def _resolve_status_url(self, command_id: str, payload: Any) -> str:
        status_url = _extract_status_url(payload)
        if status_url:
            if status_url.startswith("http://") or status_url.startswith("https://"):
                return status_url
            if status_url.startswith("/"):
                return f"{self.config.bff_url}{status_url}"
            return f"{self.config.bff_url}/{status_url}"
        return f"{self.config.bff_url}/api/v1/commands/{command_id}/status"

    def _resolve_ws_token(self, request_headers: Dict[str, str]) -> Optional[str]:
        if self.config.bff_token:
            return self.config.bff_token
        for key in ("authorization", "Authorization"):
            value = request_headers.get(key)
            if value:
                raw = value.strip()
                if raw.lower().startswith("bearer "):
                    return raw.split(" ", 1)[1].strip()
                return raw
        for key in ("x-admin-token", "X-Admin-Token"):
            value = request_headers.get(key)
            if value:
                return value.strip()
        return None

    def _resolve_ws_url(self, command_id: str, token: Optional[str]) -> str:
        base = self.config.bff_url
        if base.startswith("https://"):
            ws_base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            ws_base = "ws://" + base[len("http://"):]
        else:
            ws_base = f"ws://{base}"
        url = f"{ws_base}/api/v1/ws/commands/{command_id}"
        if token:
            url = f"{url}?token={quote(token)}"
        return url

    def _update_progress_context(
        self,
        *,
        context: Dict[str, Any],
        command_id: str,
        status: Optional[str],
        progress_payload: Dict[str, Any],
    ) -> None:
        if context is None:
            return
        entry = {"command_id": command_id}
        if status:
            entry["status"] = status
        entry.update(progress_payload)
        command_progress = context.setdefault("command_progress", {})
        if isinstance(command_progress, dict):
            command_progress[command_id] = entry
        context["latest_progress"] = entry

    async def _fetch_command_status(
        self,
        *,
        status_url: str,
        actor: str,
        request_headers: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    status_url,
                    headers=self._forward_headers(request_headers, actor),
                )
            if response.status_code >= 400:
                return None
            if "application/json" in response.headers.get("content-type", ""):
                return response.json()
            return {"status": response.text}
        except Exception as exc:
            logger.debug("Command status poll failed: %s", exc)
            return None

    async def _wait_for_command_completion(
        self,
        *,
        run_id: str,
        actor: str,
        step_index: int,
        attempt: int,
        command_id: str,
        initial_payload: Any,
        request_id: Optional[str],
        request_headers: Dict[str, str],
        context: Dict[str, Any],
    ) -> Tuple[Any, Optional[str]]:
        status_url = self._resolve_status_url(command_id, initial_payload)
        deadline = time.monotonic() + max(1.0, float(self.config.command_timeout_s))
        poll_interval = max(0.5, float(self.config.command_poll_interval_s))
        ws_idle = max(0.5, float(self.config.command_ws_idle_s))

        last_status: Optional[str] = None
        last_progress: Optional[float] = None
        last_payload: Any = initial_payload

        async def handle_update(payload: Dict[str, Any]) -> Optional[str]:
            nonlocal last_status, last_progress, last_payload
            last_payload = payload
            status = _extract_command_status(payload) or last_status
            progress_value, message, progress_payload = _extract_progress(payload)
            if progress_value is not None or message:
                progress_payload = dict(progress_payload)
                if message:
                    progress_payload["message"] = message
                if progress_value is not None:
                    progress_payload["percentage"] = progress_value
            if status or progress_payload:
                if status != last_status or progress_value != last_progress or progress_payload:
                    await self.record_event(
                        event_type="AGENT_TOOL_PROGRESS",
                        run_id=run_id,
                        actor=actor,
                        status="success",
                        data={
                            "step_index": step_index,
                            "attempt": attempt,
                            "command_id": command_id,
                            "status": status,
                            "progress": progress_payload,
                        },
                        request_id=request_id,
                        step_index=step_index,
                        resource_type="agent_tool",
                    )
                self._update_progress_context(
                    context=context,
                    command_id=command_id,
                    status=status,
                    progress_payload=progress_payload,
                )
            if status:
                last_status = status
            if progress_value is not None:
                last_progress = progress_value
            return status

        token = self._resolve_ws_token(request_headers)
        if self.config.command_ws_enabled:
            ws_url = self._resolve_ws_url(command_id, token)
            try:
                async with aiohttp.ClientSession() as session:
                    ws_headers = self._forward_headers(request_headers, actor)
                    async with session.ws_connect(ws_url, heartbeat=30, timeout=10, headers=ws_headers) as ws:
                        while time.monotonic() < deadline:
                            timeout = min(ws_idle, max(0.1, deadline - time.monotonic()))
                            try:
                                msg = await ws.receive(timeout=timeout)
                            except asyncio.TimeoutError:
                                payload = await self._fetch_command_status(
                                    status_url=status_url,
                                    actor=actor,
                                    request_headers=request_headers,
                                )
                                if payload:
                                    status = await handle_update(payload)
                                    if status in _COMMAND_TERMINAL_STATUSES:
                                        return last_payload, status
                                continue
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    parsed = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    continue
                                if parsed.get("type") == "command_update":
                                    payload = parsed.get("data") or {}
                                    status = await handle_update(payload)
                                    if status in _COMMAND_TERMINAL_STATUSES:
                                        final_payload = await self._fetch_command_status(
                                            status_url=status_url,
                                            actor=actor,
                                            request_headers=request_headers,
                                        )
                                        if final_payload:
                                            await handle_update(final_payload)
                                            return final_payload, _extract_command_status(final_payload) or status
                                        return last_payload, status
                            elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                                break
            except Exception as exc:
                logger.debug("WebSocket progress stream failed: %s", exc)

        while time.monotonic() < deadline:
            payload = await self._fetch_command_status(
                status_url=status_url,
                actor=actor,
                request_headers=request_headers,
            )
            if payload:
                status = await handle_update(payload)
                if status in _COMMAND_TERMINAL_STATUSES:
                    return last_payload, status
            await asyncio.sleep(poll_interval)

        timeout_status = last_status or "TIMEOUT"
        await self.record_event(
            event_type="AGENT_TOOL_PROGRESS",
            run_id=run_id,
            actor=actor,
            status="failure",
            data={
                "step_index": step_index,
                "attempt": attempt,
                "command_id": command_id,
                "status": "TIMEOUT",
                "progress": {"message": f"Command timed out after {int(self.config.command_timeout_s)}s"},
            },
            request_id=request_id,
            step_index=step_index,
            resource_type="agent_tool",
        )
        return last_payload, timeout_status

    def _update_pipeline_progress_context(
        self,
        *,
        context: Dict[str, Any],
        pipeline_id: Optional[str],
        job_id: str,
        build_rid: Optional[str],
        status: Optional[str],
        build_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not isinstance(context, dict):
            return
        entry: Dict[str, Any] = {"job_id": job_id}
        if pipeline_id:
            entry["pipeline_id"] = pipeline_id
        if build_rid:
            entry["build_rid"] = build_rid
        if status:
            entry["status"] = status
        if isinstance(build_payload, dict):
            if build_payload.get("createdTime"):
                entry["created_time"] = build_payload.get("createdTime")
            if build_payload.get("createdBy"):
                entry["created_by"] = build_payload.get("createdBy")
        pipeline_progress = context.setdefault("pipeline_progress", {})
        if isinstance(pipeline_progress, dict):
            pipeline_progress[job_id] = entry
        context["latest_pipeline_progress"] = entry

    async def _fetch_orchestration_build(
        self,
        *,
        build_rid: str,
        actor: str,
        request_headers: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        if not build_rid:
            return None
        encoded_build_rid = quote(str(build_rid), safe="")
        url = f"{self.config.bff_url}/api/v2/orchestration/builds/{encoded_build_rid}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    url,
                    headers=self._forward_headers(request_headers, actor),
                )
            if response.status_code >= 400:
                return None
            if "application/json" in response.headers.get("content-type", ""):
                return response.json()
            return None
        except Exception as exc:
            logger.debug("Orchestration build poll failed: %s", exc)
            return None

    async def _wait_for_pipeline_run_completion(
        self,
        *,
        run_id: str,
        actor: str,
        step_index: int,
        attempt: int,
        pipeline_id: Optional[str],
        job_id: str,
        build_rid: str,
        initial_payload: Any,
        request_id: Optional[str],
        request_headers: Dict[str, str],
        context: Dict[str, Any],
    ) -> Tuple[Any, Optional[str]]:
        deadline = time.monotonic() + max(1.0, float(self.config.command_timeout_s))
        poll_interval = max(0.5, float(self.config.command_poll_interval_s))

        last_status: Optional[str] = None
        last_payload: Any = initial_payload
        pipeline_id_value = str(pipeline_id or "").strip() or None
        job_id = str(job_id)
        build_rid = str(build_rid)

        while time.monotonic() < deadline:
            payload = await self._fetch_orchestration_build(
                build_rid=build_rid,
                actor=actor,
                request_headers=request_headers,
            )
            if isinstance(payload, dict):
                last_payload = payload
                status_value = _normalize_status(payload.get("status"))
                if status_value:
                    if status_value != last_status:
                        await self.record_event(
                            event_type="AGENT_TOOL_PROGRESS",
                            run_id=run_id,
                            actor=actor,
                            status="success",
                            data={
                                "step_index": step_index,
                                "attempt": attempt,
                                "pipeline_id": pipeline_id_value,
                                "build_rid": build_rid,
                                "job_id": job_id,
                                "status": status_value,
                            },
                            request_id=request_id,
                            step_index=step_index,
                            resource_type="agent_tool",
                        )
                    self._update_pipeline_progress_context(
                        context=context,
                        pipeline_id=pipeline_id_value,
                        job_id=job_id,
                        build_rid=build_rid,
                        status=status_value,
                        build_payload=dict(payload),
                    )
                    last_status = status_value
                    if status_value in _PIPELINE_TERMINAL_STATUSES:
                        enriched = initial_payload
                        if isinstance(enriched, dict):
                            enriched = dict(enriched)
                            data_out = enriched.get("data") if isinstance(enriched.get("data"), dict) else {}
                            enriched["data"] = {
                                **data_out,
                                "build": dict(payload),
                                "build_rid": build_rid,
                                "pipeline_run_status": status_value,
                            }
                        else:
                            enriched = {
                                "status": "success",
                                "message": "Pipeline run completed",
                                "data": {
                                    "pipeline_id": pipeline_id_value,
                                    "build_rid": build_rid,
                                    "job_id": job_id,
                                    "build": dict(payload),
                                    "pipeline_run_status": status_value,
                                },
                            }
                        return enriched, status_value

            await asyncio.sleep(poll_interval)

        timeout_status = last_status or "TIMEOUT"
        await self.record_event(
            event_type="AGENT_TOOL_PROGRESS",
            run_id=run_id,
            actor=actor,
            status="failure",
            data={
                "step_index": step_index,
                "attempt": attempt,
                "pipeline_id": pipeline_id_value,
                "build_rid": build_rid,
                "job_id": job_id,
                "status": "TIMEOUT",
                "progress": {"message": f"Pipeline job timed out after {int(self.config.command_timeout_s)}s"},
            },
            request_id=request_id,
            step_index=step_index,
            resource_type="agent_tool",
        )
        return last_payload, timeout_status

    async def record_event(
        self,
        *,
        event_type: str,
        run_id: str,
        actor: str,
        status: str,
        data: Dict[str, Any],
        request_id: Optional[str],
        step_index: Optional[int] = None,
        resource_type: str = "agent_run",
        resource_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> str:
        metadata = {
            "kind": "agent",
            "service": self.config.service_name,
            "request_id": request_id,
            "step_index": step_index,
            "status": status,
        }
        envelope = EventEnvelope(
            event_type=event_type,
            aggregate_type="AgentRun",
            aggregate_id=run_id,
            actor=actor,
            data=data,
            metadata=metadata,
            occurred_at=datetime.now(timezone.utc),
        )
        await self.event_store.append_event(envelope)
        if self.audit_store:
            await self.audit_store.log(
                partition_key=f"agent_run:{run_id}",
                actor=actor,
                action=event_type,
                status="success" if status == "success" else "failure",
                resource_type=resource_type,
                resource_id=resource_id or run_id,
                event_id=str(envelope.event_id),
                metadata={
                    "step_index": step_index,
                    "request_id": request_id,
                    "event_type": event_type,
                },
                error=error,
                occurred_at=envelope.occurred_at,
            )
        return str(envelope.event_id)

    async def execute_tool_call(
        self,
        *,
        run_id: str,
        actor: str,
        step_index: int,
        attempt: int = 0,
        tool_call: AgentToolCall,
        context: Dict[str, Any],
        dry_run: bool,
        request_headers: Dict[str, str],
        request_id: Optional[str],
    ) -> Dict[str, Any]:
        context = context if isinstance(context, dict) else {}
        tool_run_id = _compute_tool_run_id(
            run_id=run_id,
            step_id=tool_call.step_id,
            step_index=step_index,
            attempt=attempt,
        )
        try:
            resolved_path = _resolve_templates(tool_call.path, context)
            if not isinstance(resolved_path, str):
                raise ValueError("resolved path must be a string")
            resolved_query = _resolve_templates(tool_call.query, context)
            if resolved_query is None:
                resolved_query = {}
            if not isinstance(resolved_query, dict):
                raise ValueError("resolved query must be an object")
            resolved_body = _resolve_templates(tool_call.body, context) if tool_call.body is not None else None
            if resolved_body is not None and not isinstance(resolved_body, dict):
                raise ValueError("resolved body must be an object")
            resolved_headers = _resolve_templates(tool_call.headers, context)
            if resolved_headers is None:
                resolved_headers = {}
            if not isinstance(resolved_headers, dict):
                raise ValueError("resolved headers must be an object")
            normalized_headers: Dict[str, str] = {}
            for key, value in resolved_headers.items():
                if value in (None, ""):
                    continue
                normalized_headers[str(key)] = str(value)
            resolved_data_scope = _resolve_templates(tool_call.data_scope, context)
            if resolved_data_scope is None:
                resolved_data_scope = {}
            if not isinstance(resolved_data_scope, dict):
                raise ValueError("resolved data_scope must be an object")
            tool_call = tool_call.model_copy(
                update={
                    "path": resolved_path,
                    "query": resolved_query,
                    "body": resolved_body,
                    "headers": normalized_headers,
                    "data_scope": resolved_data_scope,
                }
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception fallback at agent/services/agent_runtime.py:1406", exc_info=True)
            error = f"template resolution failed: {exc}"
            await self.record_event(
                event_type="AGENT_TOOL_RESULT",
                run_id=run_id,
                actor=actor,
                status="failure",
                data={
                    "step_index": step_index,
                    "step_id": tool_call.step_id,
                    "tool_run_id": tool_run_id,
                    "attempt": attempt,
                    "tool": tool_call.service,
                    "tool_id": tool_call.tool_id,
                    "method": tool_call.method,
                    "path": tool_call.path,
                    "data_scope": mask_pii(tool_call.data_scope, max_string_chars=200),
                    "output_digest": None,
                    "output_preview": None,
                    "output_size_bytes": None,
                    "http_status": 400,
                    "duration_ms": 0,
                    "error": error,
                    "error_key": "template_unresolved",
                },
                request_id=request_id,
                step_index=step_index,
                resource_type="agent_tool",
                error=error,
            )
            return {
                "status": "failed",
                "error_code": _REQUEST_VALIDATION_FAILED_CODE,
                "error_message": error,
                "payload": None,
                "side_effect_summary": {},
                "http_status": 400,
                "tool_run_id": tool_run_id,
                "output_digest": None,
                "duration_ms": 0,
                "data_scope": mask_pii(tool_call.data_scope, max_string_chars=200),
                "error": error,
                "error_key": "template_unresolved",
                "api_code": _REQUEST_VALIDATION_FAILED_CODE,
            }

        missing_artifacts: list[str] = []
        consumes = [str(key).strip() for key in (tool_call.consumes or []) if str(key).strip()]
        if consumes:
            artifacts = context.get("artifacts")
            artifacts = artifacts if isinstance(artifacts, dict) else {}
            for key in consumes:
                if key not in artifacts:
                    missing_artifacts.append(key)

        if missing_artifacts:
            error = f"missing artifacts: {missing_artifacts}"
            await self.record_event(
                event_type="AGENT_TOOL_RESULT",
                run_id=run_id,
                actor=actor,
                status="failure",
                data={
                    "step_index": step_index,
                    "step_id": tool_call.step_id,
                    "tool_run_id": tool_run_id,
                    "attempt": attempt,
                    "tool": tool_call.service,
                    "tool_id": tool_call.tool_id,
                    "method": tool_call.method,
                    "path": tool_call.path,
                    "data_scope": mask_pii(tool_call.data_scope, max_string_chars=200),
                    "output_digest": None,
                    "output_preview": None,
                    "output_size_bytes": None,
                    "http_status": 400,
                    "duration_ms": 0,
                    "error": error,
                    "error_key": "artifact_missing",
                    "missing_artifacts": missing_artifacts,
                },
                request_id=request_id,
                step_index=step_index,
                resource_type="agent_tool",
                error=error,
            )
            return {
                "status": "failed",
                "error_code": _REQUEST_VALIDATION_FAILED_CODE,
                "error_message": error,
                "payload": None,
                "side_effect_summary": {},
                "http_status": 400,
                "tool_run_id": tool_run_id,
                "output_digest": None,
                "duration_ms": 0,
                "data_scope": mask_pii(tool_call.data_scope, max_string_chars=200),
                "error": error,
                "error_key": "artifact_missing",
                "missing_artifacts": missing_artifacts,
                "api_code": _REQUEST_VALIDATION_FAILED_CODE,
            }

        base_url = self._resolve_base_url(tool_call)
        path = tool_call.path if tool_call.path.startswith("/") else f"/{tool_call.path}"
        url = f"{base_url}{path}"
        data_scope = {**_extract_scope(context or {}), **(tool_call.data_scope or {})}
        data_scope = mask_pii(data_scope, max_string_chars=200)
        input_payload = {"query": tool_call.query, "body": tool_call.body}
        input_digest = digest_for_audit(input_payload)
        input_size = self._payload_size(input_payload)
        input_preview = (
            self._preview_payload(input_payload)
            if input_size <= self.config.max_payload_bytes
            else f"<omitted: {input_size} bytes>"
        )

        await self.record_event(
            event_type="AGENT_TOOL_STARTED",
            run_id=run_id,
            actor=actor,
            status="success",
            data={
                "step_index": step_index,
                "step_id": tool_call.step_id,
                "tool_run_id": tool_run_id,
                "attempt": attempt,
                "tool": tool_call.service,
                "tool_id": tool_call.tool_id,
                "method": tool_call.method,
                "path": path,
                "data_scope": data_scope,
                "consumes": list(tool_call.consumes or []),
                "produces": list(tool_call.produces or []),
                "input_digest": input_digest,
                "input_preview": input_preview,
                "input_size_bytes": input_size,
            },
            request_id=request_id,
            step_index=step_index,
            resource_type="agent_tool",
        )

        if _is_agent_proxy_path(path):
            error = "blocked: agent_proxy_loop"
            await self.record_event(
                event_type="AGENT_TOOL_RESULT",
                run_id=run_id,
                actor=actor,
                status="failure",
                data={
                    "step_index": step_index,
                    "step_id": tool_call.step_id,
                    "tool_run_id": tool_run_id,
                    "attempt": attempt,
                    "tool": tool_call.service,
                    "tool_id": tool_call.tool_id,
                    "method": tool_call.method,
                    "path": path,
                    "data_scope": data_scope,
                    "output_digest": None,
                    "output_preview": None,
                    "output_size_bytes": None,
                    "http_status": 400,
                    "duration_ms": 0,
                    "error": error,
                },
                request_id=request_id,
                step_index=step_index,
                resource_type="agent_tool",
                error=error,
            )
            return {
                "status": "failed",
                "error_code": _REQUEST_VALIDATION_FAILED_CODE,
                "error_message": error,
                "payload": None,
                "side_effect_summary": {},
                "http_status": 400,
                "tool_run_id": tool_run_id,
                "output_digest": None,
                "duration_ms": 0,
                "data_scope": data_scope,
                "error": error,
                "error_key": "agent_proxy_loop",
                "api_code": _REQUEST_VALIDATION_FAILED_CODE,
            }

        if dry_run:
            return {
                "status": "success",
                "error_code": None,
                "error_message": None,
                "payload": {"skipped": True, "dry_run": True},
                "side_effect_summary": {"dry_run": True},
                "http_status": None,
                "tool_run_id": tool_run_id,
                "output_digest": None,
                "duration_ms": 0,
                "data_scope": data_scope,
            }

        if self.config.block_writes_on_overlay_degraded and _method_is_write(tool_call.method):
            overlay_status = str((context or {}).get("overlay_status") or "").strip().upper()
            allow_override = bool((context or {}).get("allow_degraded_writes")) or self.config.allow_degraded_writes
            if overlay_status == "DEGRADED" and not allow_override:
                error = "blocked: overlay_degraded"
                await self.record_event(
                    event_type="AGENT_TOOL_RESULT",
                    run_id=run_id,
                    actor=actor,
                    status="failure",
                    data={
                        "step_index": step_index,
                        "step_id": tool_call.step_id,
                        "tool_run_id": tool_run_id,
                        "attempt": attempt,
                        "tool": tool_call.service,
                        "tool_id": tool_call.tool_id,
                        "method": tool_call.method,
                        "path": path,
                        "data_scope": data_scope,
                        "output_digest": None,
                        "output_preview": None,
                        "output_size_bytes": None,
                        "http_status": 409,
                        "duration_ms": 0,
                        "error": error,
                    },
                    request_id=request_id,
                    step_index=step_index,
                    resource_type="agent_tool",
                    error=error,
                )
                return {
                    "status": "failed",
                    "error_code": _CONFLICT_CODE,
                    "error_message": error,
                    "payload": None,
                    "side_effect_summary": {},
                    "http_status": 409,
                    "tool_run_id": tool_run_id,
                    "output_digest": None,
                    "duration_ms": 0,
                    "data_scope": data_scope,
                    "error": error,
                    "error_key": "overlay_degraded",
                    "api_code": _CONFLICT_CODE,
                }

        start_time = time.monotonic()
        error: Optional[str] = None
        output_preview = None
        output_digest = None
        output_size = None
        http_status = None
        response_payload: Any = None
        command_id: Optional[str] = None
        command_status: Optional[str] = None
        pipeline_job_id: Optional[str] = None
        pipeline_build_rid: Optional[str] = None
        pipeline_run_status: Optional[str] = None
        error_key: Optional[str] = None
        api_code: Optional[str] = None
        api_category: Optional[str] = None
        enterprise: Optional[dict[str, Any]] = None
        retryable: Optional[bool] = None
        retry_after_ms: Optional[int] = None
        signals: dict[str, Any] = {}
        try:
            async with httpx.AsyncClient(timeout=self.config.timeout_s) as client:
                request_headers_payload = {**self._forward_headers(request_headers, actor), **tool_call.headers}
                request_headers_payload["X-Agent-Tool-ID"] = str(tool_call.tool_id or "").strip()
                request_headers_payload["X-Agent-Tool-Run-ID"] = tool_run_id
                db_name_candidate = None
                if isinstance(tool_call.data_scope, dict):
                    db_name_candidate = (
                        tool_call.data_scope.get("db_name")
                        or tool_call.data_scope.get("project")
                        or tool_call.data_scope.get("db")
                    )
                db_name_value = str(db_name_candidate).strip() if db_name_candidate not in (None, "") else ""
                if db_name_value:
                    if not any(key.lower() == "x-db-name" for key in request_headers_payload):
                        request_headers_payload["X-DB-Name"] = db_name_value
                    if not any(key.lower() == "x-project" for key in request_headers_payload):
                        request_headers_payload["X-Project"] = db_name_value
                response = await client.request(
                    tool_call.method,
                    url,
                    params=tool_call.query or None,
                    json=tool_call.body,
                    headers=request_headers_payload,
                )
            http_status = response.status_code
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                response_payload = response.json()
            else:
                response_payload = response.text
            command_id = _extract_command_id(response_payload)
            command_status = _extract_command_status(response_payload)
            pipeline_build_rid = _extract_build_rid(response_payload)
            pipeline_job_id = _extract_pipeline_job_id(response_payload)
            detected_overlay_status = _extract_overlay_status(response_payload)
            if detected_overlay_status and isinstance(context, dict):
                context["overlay_status"] = detected_overlay_status
                if detected_overlay_status == "DEGRADED":
                    context["overlay_degraded"] = True

            if response.status_code >= 400:
                error_key = _extract_error_key(response_payload)
                api_code = _extract_api_code(response_payload)
                api_category = _extract_api_category(response_payload)
                enterprise = _extract_enterprise(response_payload)
                retryable = _extract_retryable(response_payload)
                retry_after_ms = _extract_retry_after_ms(dict(response.headers))

            signals = _extract_action_log_signals(response_payload)
            simulation_signals = _extract_action_simulation_signals(response_payload)
            if simulation_signals:
                signals = {**signals, **simulation_signals}

            simulation_error_key, simulation_enterprise, simulation_message = _extract_action_simulation_rejection(
                response_payload
            )
            if simulation_error_key and error is None:
                error_key = simulation_error_key
                if simulation_enterprise:
                    enterprise = simulation_enterprise
                    retryable = _extract_retryable({"enterprise": enterprise})
                error = simulation_message or "simulation rejected"

            external_code = _extract_enterprise_external_code(response_payload)
            if external_code == "overlay_degraded" and isinstance(context, dict):
                context["overlay_status"] = "DEGRADED"
                context["overlay_degraded"] = True
            if response.status_code >= 400:
                error = f"HTTP {response.status_code}"
            if not error and command_id and (command_status is None or command_status in _COMMAND_PENDING_STATUSES):
                response_payload, command_status = await self._wait_for_command_completion(
                    run_id=run_id,
                    actor=actor,
                    step_index=step_index,
                    attempt=attempt,
                    command_id=command_id,
                    initial_payload=response_payload,
                    request_id=request_id,
                    request_headers=request_headers,
                    context=context,
                )
                if command_status in {"FAILED", "CANCELLED", "TIMEOUT"}:
                    error_key = _extract_error_key(response_payload) or error_key
                    api_code = _extract_api_code(response_payload) or api_code
                    api_category = _extract_api_category(response_payload) or api_category
                    enterprise = _extract_enterprise(response_payload) or enterprise
                    retryable = _extract_retryable(response_payload)
                    retry_after_ms = _extract_retry_after_ms(dict(response.headers)) or retry_after_ms
                if command_status in {"FAILED", "CANCELLED", "TIMEOUT"}:
                    error = f"command {command_id} {command_status}"

            if (
                not error
                and pipeline_build_rid
                and self.config.pipeline_wait_enabled
                and _tool_call_expects_pipeline_job(tool_id=tool_call.tool_id, path=path)
            ):
                if pipeline_job_id is None:
                    pipeline_job_id = _extract_build_job_id_from_rid(pipeline_build_rid)
                response_payload, pipeline_run_status = await self._wait_for_pipeline_run_completion(
                    run_id=run_id,
                    actor=actor,
                    step_index=step_index,
                    attempt=attempt,
                    pipeline_id=_extract_pipeline_id(response_payload),
                    job_id=str(pipeline_job_id or "unknown"),
                    build_rid=pipeline_build_rid,
                    initial_payload=response_payload,
                    request_id=request_id,
                    request_headers=request_headers,
                    context=context,
                )
                if pipeline_run_status in {"FAILED", "CANCELLED", "CANCELED", "TIMEOUT"}:
                    error = f"pipeline job {pipeline_job_id or 'unknown'} {pipeline_run_status}"
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception fallback at agent/services/agent_runtime.py:1787", exc_info=True)
            error = str(exc)
            if not error:
                error = exc.__class__.__name__
        duration_ms = int((time.monotonic() - start_time) * 1000)

        output_size = self._payload_size(response_payload)
        output_digest = digest_for_audit(response_payload)
        output_preview = (
            self._preview_payload(response_payload)
            if output_size <= self.config.max_payload_bytes
            else f"<omitted: {output_size} bytes>"
        )
        side_effect_summary: dict[str, Any] = {}
        if command_id:
            side_effect_summary["command_id"] = command_id
        if pipeline_job_id:
            side_effect_summary["pipeline_job_id"] = pipeline_job_id
        if pipeline_build_rid:
            side_effect_summary["pipeline_build_rid"] = pipeline_build_rid
        if pipeline_run_status:
            side_effect_summary["pipeline_run_status"] = pipeline_run_status
        if isinstance(signals, dict) and signals:
            side_effect_summary["signals"] = dict(signals)

        tool_status = "failed" if error else "success"
        event_status = "failure" if error else "success"
        error_message = _extract_error_message(response_payload) or error
        error_code = _normalize_error_code(api_code or error_key or (f"HTTP_{http_status}" if http_status else None))
        payload_value: Any = response_payload
        if output_size is not None and output_size > self.config.max_payload_bytes:
            candidate = _compact_tool_payload(response_payload)
            candidate_size = self._payload_size(candidate)
            if candidate_size is not None and candidate_size <= self.config.max_payload_bytes:
                payload_value = candidate
            else:
                payload_value = {"_omitted": True, "digest": output_digest, "size_bytes": output_size}
        await self.record_event(
            event_type="AGENT_TOOL_RESULT",
            run_id=run_id,
            actor=actor,
            status=event_status,
            data={
                "step_index": step_index,
                "step_id": tool_call.step_id,
                "tool_run_id": tool_run_id,
                "attempt": attempt,
                "tool": tool_call.service,
                "tool_id": tool_call.tool_id,
                "method": tool_call.method,
                "path": path,
                "data_scope": data_scope,
                "output_digest": output_digest,
                "output_preview": output_preview,
                "output_size_bytes": output_size,
                "http_status": http_status,
                "command_id": command_id,
                "command_status": command_status,
                "pipeline_job_id": pipeline_job_id,
                "pipeline_build_rid": pipeline_build_rid,
                "pipeline_run_status": pipeline_run_status,
                "duration_ms": duration_ms,
                "error": error,
                "error_key": error_key,
                "api_code": api_code,
                "api_category": api_category,
                "enterprise": enterprise,
                "retryable": retryable,
                "retry_after_ms": retry_after_ms,
                "side_effect_summary": side_effect_summary,
            },
            request_id=request_id,
            step_index=step_index,
            resource_type="agent_tool",
            error=error,
        )

        if not error and isinstance(context, dict):
            step_id = str(tool_call.step_id or "").strip() or f"step_{step_index}"
            if _TEMPLATE_KEY_ALLOWED.match(step_id):
                extracted = _extract_step_outputs(response_payload)
                if command_id:
                    extracted.setdefault("command_id", command_id)
                if pipeline_job_id:
                    extracted.setdefault("job_id", pipeline_job_id)
                if pipeline_build_rid:
                    extracted.setdefault("build_rid", pipeline_build_rid)
                if pipeline_run_status:
                    extracted.setdefault("pipeline_run_status", pipeline_run_status)
                step_outputs = context.setdefault("step_outputs", {})
                if isinstance(step_outputs, dict):
                    existing = step_outputs.get(step_id) if isinstance(step_outputs.get(step_id), dict) else {}
                    step_outputs[step_id] = {**existing, **extracted}
                    context["latest_step_output"] = {"step_id": step_id, **extracted}

            produces = [str(key).strip() for key in (tool_call.produces or []) if str(key).strip()]
            if produces:
                artifacts = context.setdefault("artifacts", {})
                if isinstance(artifacts, dict):
                    artifact_value = _artifact_value_from_response(response_payload)
                    candidate = artifact_value
                    if self._payload_size(candidate) > self.config.max_payload_bytes:
                        candidate = _compact_artifact_value(candidate)
                    if self._payload_size(candidate) > self.config.max_payload_bytes:
                        candidate = {"_artifact_omitted": True, "digest": digest_for_audit(artifact_value)}
                    for key in produces:
                        if _TEMPLATE_KEY_ALLOWED.match(key):
                            artifacts[key] = candidate
                    if len(produces) == 1 and _TEMPLATE_KEY_ALLOWED.match(produces[0]):
                        context["latest_artifact"] = {"key": produces[0], "digest": digest_for_audit(candidate)}

        return {
            "status": tool_status,
            "error_code": error_code,
            "error_message": error_message if error else None,
            "payload": payload_value,
            "side_effect_summary": side_effect_summary,
            "http_status": http_status,
            "tool_run_id": tool_run_id,
            "output_digest": output_digest,
            "payload_preview": output_preview,
            "duration_ms": duration_ms,
            "data_scope": data_scope,
            "command_id": command_id,
            "command_status": command_status,
            "pipeline_job_id": pipeline_job_id,
            "pipeline_build_rid": pipeline_build_rid,
            "pipeline_run_status": pipeline_run_status,
            "error": error,
            "error_key": error_key,
            "api_code": api_code,
            "api_category": api_category,
            "enterprise": enterprise,
            "retryable": retryable,
            "retry_after_ms": retry_after_ms,
            "signals": signals,
        }
