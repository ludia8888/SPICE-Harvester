from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import aiohttp
import httpx

from agent.models import AgentToolCall
from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.audit_log_store import AuditLogStore
from shared.services.event_store import EventStore
from shared.utils.llm_safety import digest_for_audit, mask_pii, truncate_text

logger = logging.getLogger(__name__)

_COMMAND_PENDING_STATUSES = {"PENDING", "PROCESSING", "RETRYING"}
_COMMAND_TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}
_COMMAND_STATUSES = _COMMAND_PENDING_STATUSES | _COMMAND_TERMINAL_STATUSES


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _clean_url(value: str) -> str:
    return value.rstrip("/")


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, separators=(",", ":"), default=str)


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

    with contextlib.suppress(Exception):
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta_ms = int((dt - datetime.now(timezone.utc)).total_seconds() * 1000)
        if delta_ms <= 0:
            return None
        return delta_ms

    return None

def _is_agent_proxy_path(path: str) -> bool:
    normalized = path if path.startswith("/") else f"/{path}"
    return normalized.startswith("/api/v1/agent")


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


def _extract_enterprise_legacy_code(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    enterprise = payload.get("enterprise")
    if not isinstance(enterprise, dict):
        return None
    legacy_code = enterprise.get("legacy_code")
    if isinstance(legacy_code, str) and legacy_code.strip():
        return legacy_code.strip()
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
        legacy_code = enterprise.get("legacy_code")
        if isinstance(legacy_code, str) and legacy_code.strip():
            return legacy_code.strip()
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
    if "simulation_id" not in data and "preview_action_log_id" not in data:
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
        "action_simulation_id": str(data.get("simulation_id") or "").strip() or None,
        "action_simulation_version": data.get("version"),
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
        bff_url = _clean_url(os.getenv("AGENT_BFF_BASE_URL") or ServiceConfig.get_bff_url())
        bff_token = (os.getenv("AGENT_BFF_TOKEN") or os.getenv("BFF_AGENT_TOKEN") or "").strip() or None
        allowed_services = ("bff",)
        command_timeout_raw = (
            os.getenv("AGENT_COMMAND_TIMEOUT_SECONDS")
            or os.getenv("PIPELINE_RUN_TIMEOUT_SECONDS")
            or os.getenv("PIPELINE_RUN_TIMEOUT")
            or "600"
        )
        try:
            command_timeout = float(command_timeout_raw)
        except ValueError:
            command_timeout = 600.0

        block_writes_on_overlay_degraded = _env_bool("AGENT_BLOCK_WRITES_ON_OVERLAY_DEGRADED", True)
        allow_degraded_writes = _env_bool("AGENT_ALLOW_DEGRADED_WRITES", False)
        auto_retry_enabled = _env_bool("AGENT_AUTO_RETRY_ENABLED", True)
        auto_retry_allow_writes = _env_bool("AGENT_AUTO_RETRY_ALLOW_WRITES", False)

        config = AgentRuntimeConfig(
            bff_url=bff_url,
            allowed_services=allowed_services,
            max_preview_chars=_env_int("AGENT_AUDIT_MAX_PREVIEW_CHARS", 2000),
            max_payload_bytes=_env_int("AGENT_TOOL_MAX_PAYLOAD_BYTES", 200000),
            timeout_s=float(os.getenv("AGENT_TOOL_TIMEOUT_SECONDS", "30")),
            service_name=os.getenv("AGENT_SERVICE_NAME", "agent"),
            bff_token=bff_token,
            command_timeout_s=command_timeout,
            command_poll_interval_s=float(os.getenv("AGENT_COMMAND_POLL_INTERVAL_SECONDS", "2")),
            command_ws_idle_s=float(os.getenv("AGENT_COMMAND_WS_IDLE_SECONDS", "5")),
            command_ws_enabled=_env_bool("AGENT_COMMAND_WS_ENABLED", True),
            block_writes_on_overlay_degraded=block_writes_on_overlay_degraded,
            allow_degraded_writes=allow_degraded_writes,
            auto_retry_enabled=auto_retry_enabled,
            auto_retry_max_attempts=_env_int("AGENT_AUTO_RETRY_MAX_ATTEMPTS", 3),
            auto_retry_base_delay_s=_env_float("AGENT_AUTO_RETRY_BASE_DELAY_SECONDS", 0.5),
            auto_retry_max_delay_s=_env_float("AGENT_AUTO_RETRY_MAX_DELAY_SECONDS", 8.0),
            auto_retry_allow_writes=auto_retry_allow_writes,
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
            return 0

    def _forward_headers(self, headers: Dict[str, str], actor: str) -> Dict[str, str]:
        allowlist = {
            "x-user-id",
            "x-user",
            "x-user-type",
            "x-actor",
            "x-actor-type",
            "x-principal-id",
            "x-principal-type",
            "x-admin-token",
            "authorization",
            "x-request-id",
        }
        output = {
            key: value for key, value in headers.items() if key.lower() in allowlist and value
        }
        output["X-Spice-Caller"] = "agent"
        if self.config.bff_token:
            output["Authorization"] = f"Bearer {self.config.bff_token}"
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
                    async with session.ws_connect(ws_url, heartbeat=30, timeout=10) as ws:
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
                "attempt": attempt,
                "tool": tool_call.service,
                "method": tool_call.method,
                "path": path,
                "data_scope": data_scope,
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
                    "attempt": attempt,
                    "tool": tool_call.service,
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
                "status": "failure",
                "http_status": 400,
                "output_digest": None,
                "duration_ms": 0,
                "data_scope": data_scope,
                "error": error,
                "error_key": "agent_proxy_loop",
            }

        if dry_run:
            return {
                "status": "skipped",
                "http_status": None,
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
                        "attempt": attempt,
                        "tool": tool_call.service,
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
                    "status": "failure",
                    "http_status": 409,
                    "output_digest": None,
                    "duration_ms": 0,
                    "data_scope": data_scope,
                    "error": error,
                    "error_key": "overlay_degraded",
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
        error_key: Optional[str] = None
        api_code: Optional[str] = None
        api_category: Optional[str] = None
        enterprise: Optional[dict[str, Any]] = None
        retryable: Optional[bool] = None
        retry_after_ms: Optional[int] = None
        signals: dict[str, Any] = {}
        try:
            async with httpx.AsyncClient(timeout=self.config.timeout_s) as client:
                response = await client.request(
                    tool_call.method,
                    url,
                    params=tool_call.query or None,
                    json=tool_call.body,
                    headers={**self._forward_headers(request_headers, actor), **tool_call.headers},
                )
            http_status = response.status_code
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                response_payload = response.json()
            else:
                response_payload = response.text
            command_id = _extract_command_id(response_payload)
            command_status = _extract_command_status(response_payload)
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

            legacy_code = _extract_enterprise_legacy_code(response_payload)
            if legacy_code == "overlay_degraded" and isinstance(context, dict):
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
        except Exception as exc:
            error = str(exc)
        duration_ms = int((time.monotonic() - start_time) * 1000)

        output_size = self._payload_size(response_payload)
        output_digest = digest_for_audit(response_payload)
        output_preview = (
            self._preview_payload(response_payload)
            if output_size <= self.config.max_payload_bytes
            else f"<omitted: {output_size} bytes>"
        )

        status = "failure" if error else "success"
        await self.record_event(
            event_type="AGENT_TOOL_RESULT",
            run_id=run_id,
            actor=actor,
            status=status,
            data={
                "step_index": step_index,
                "attempt": attempt,
                "tool": tool_call.service,
                "method": tool_call.method,
                "path": path,
                "data_scope": data_scope,
                "output_digest": output_digest,
                "output_preview": output_preview,
                "output_size_bytes": output_size,
                "http_status": http_status,
                "command_id": command_id,
                "command_status": command_status,
                "duration_ms": duration_ms,
                "error": error,
                "error_key": error_key,
                "api_code": api_code,
                "api_category": api_category,
                "enterprise": enterprise,
                "retryable": retryable,
                "retry_after_ms": retry_after_ms,
            },
            request_id=request_id,
            step_index=step_index,
            resource_type="agent_tool",
            error=error,
        )

        return {
            "status": status,
            "http_status": http_status,
            "output_digest": output_digest,
            "duration_ms": duration_ms,
            "data_scope": data_scope,
            "command_id": command_id,
            "command_status": command_status,
            "error": error,
            "error_key": error_key,
            "api_code": api_code,
            "api_category": api_category,
            "enterprise": enterprise,
            "retryable": retryable,
            "retry_after_ms": retry_after_ms,
            "signals": signals,
        }
