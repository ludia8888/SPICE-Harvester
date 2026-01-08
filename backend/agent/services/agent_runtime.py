from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

from agent.models import AgentToolCall
from shared.config.service_config import ServiceConfig
from shared.models.event_envelope import EventEnvelope
from shared.services.audit_log_store import AuditLogStore
from shared.services.event_store import EventStore
from shared.utils.llm_safety import digest_for_audit, mask_pii, truncate_text


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


def _clean_url(value: str) -> str:
    return value.rstrip("/")


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, separators=(",", ":"), default=str)


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


@dataclass(frozen=True)
class AgentRuntimeConfig:
    bff_url: str
    allowed_services: Tuple[str, ...]
    max_preview_chars: int
    max_payload_bytes: int
    timeout_s: float
    service_name: str
    bff_token: Optional[str]


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
        config = AgentRuntimeConfig(
            bff_url=bff_url,
            allowed_services=allowed_services,
            max_preview_chars=_env_int("AGENT_AUDIT_MAX_PREVIEW_CHARS", 2000),
            max_payload_bytes=_env_int("AGENT_TOOL_MAX_PAYLOAD_BYTES", 200000),
            timeout_s=float(os.getenv("AGENT_TOOL_TIMEOUT_SECONDS", "30")),
            service_name=os.getenv("AGENT_SERVICE_NAME", "agent"),
            bff_token=bff_token,
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
            }

        if dry_run:
            return {
                "status": "skipped",
                "http_status": None,
                "output_digest": None,
                "duration_ms": 0,
                "data_scope": data_scope,
            }

        start_time = time.monotonic()
        error: Optional[str] = None
        output_preview = None
        output_digest = None
        output_size = None
        http_status = None
        response_payload: Any = None
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
            output_size = self._payload_size(response_payload)
            output_digest = digest_for_audit(response_payload)
            output_preview = (
                self._preview_payload(response_payload)
                if output_size <= self.config.max_payload_bytes
                else f"<omitted: {output_size} bytes>"
            )
            if response.status_code >= 400:
                error = f"HTTP {response.status_code}"
        except Exception as exc:
            error = str(exc)
        duration_ms = int((time.monotonic() - start_time) * 1000)

        status = "failure" if error else "success"
        await self.record_event(
            event_type="AGENT_TOOL_RESULT",
            run_id=run_id,
            actor=actor,
            status=status,
            data={
                "step_index": step_index,
                "tool": tool_call.service,
                "method": tool_call.method,
                "path": path,
                "data_scope": data_scope,
                "output_digest": output_digest,
                "output_preview": output_preview,
                "output_size_bytes": output_size,
                "http_status": http_status,
                "duration_ms": duration_ms,
                "error": error,
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
            "error": error,
        }
