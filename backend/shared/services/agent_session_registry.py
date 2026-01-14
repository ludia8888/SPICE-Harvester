"""
Agent session registry (Postgres).

Persists long-lived, user-scoped agent conversations so the system can:
- maintain a clean session boundary (no implicit context leakage),
- track messages, tool calls, approvals, and jobs,
- provide an outline/event stream for UI clients,
- enforce tenant/user isolation at the storage layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.config.settings import get_settings
from shared.security.data_encryption import encryptor_from_keys, is_encrypted_json, is_encrypted_text
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


def _wrap_json_object(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        return {"raw": value}
    if isinstance(value, (list, tuple)):
        return {"value": list(value)}
    if isinstance(value, (bool, int, float)):
        return {"value": value}
    return {"raw": str(value)}


SESSION_STATUS_ACTIVE = "ACTIVE"
SESSION_STATUS_WAITING_APPROVAL = "WAITING_APPROVAL"
SESSION_STATUS_RUNNING_TOOL = "RUNNING_TOOL"
SESSION_STATUS_ERROR = "ERROR"
SESSION_STATUS_COMPLETED = "COMPLETED"
SESSION_STATUS_TERMINATED = "TERMINATED"

_SESSION_STATUSES = {
    SESSION_STATUS_ACTIVE,
    SESSION_STATUS_WAITING_APPROVAL,
    SESSION_STATUS_RUNNING_TOOL,
    SESSION_STATUS_ERROR,
    SESSION_STATUS_COMPLETED,
    SESSION_STATUS_TERMINATED,
}

_SESSION_STATUS_TRANSITIONS = {
    SESSION_STATUS_ACTIVE: {
        SESSION_STATUS_ACTIVE,
        SESSION_STATUS_WAITING_APPROVAL,
        SESSION_STATUS_RUNNING_TOOL,
        SESSION_STATUS_ERROR,
        SESSION_STATUS_COMPLETED,
        SESSION_STATUS_TERMINATED,
    },
    SESSION_STATUS_WAITING_APPROVAL: {
        SESSION_STATUS_WAITING_APPROVAL,
        SESSION_STATUS_RUNNING_TOOL,
        SESSION_STATUS_ACTIVE,
        SESSION_STATUS_ERROR,
        SESSION_STATUS_TERMINATED,
    },
    SESSION_STATUS_RUNNING_TOOL: {
        SESSION_STATUS_RUNNING_TOOL,
        SESSION_STATUS_COMPLETED,
        SESSION_STATUS_ERROR,
        SESSION_STATUS_TERMINATED,
        SESSION_STATUS_ACTIVE,
        SESSION_STATUS_WAITING_APPROVAL,
    },
    SESSION_STATUS_ERROR: {
        SESSION_STATUS_ERROR,
        SESSION_STATUS_ACTIVE,
        SESSION_STATUS_TERMINATED,
    },
    SESSION_STATUS_COMPLETED: {
        SESSION_STATUS_COMPLETED,
        SESSION_STATUS_ACTIVE,
        SESSION_STATUS_TERMINATED,
    },
    SESSION_STATUS_TERMINATED: {SESSION_STATUS_TERMINATED},
}


_ENCRYPTOR_CACHE: tuple[str, object] | None = None


def _get_encryptor():
    global _ENCRYPTOR_CACHE
    raw = str(getattr(get_settings().security, "data_encryption_keys", "") or "").strip()
    cache = _ENCRYPTOR_CACHE
    if cache and cache[0] == raw:
        return cache[1] if cache[1] is not None else None
    encryptor = encryptor_from_keys(raw)
    _ENCRYPTOR_CACHE = (raw, encryptor)
    return encryptor


def _aad_for_session(*, session_id: str) -> bytes:
    return f"session:{session_id}".encode("utf-8")


def validate_session_status_transition(*, current_status: str, next_status: str) -> None:
    current = str(current_status or "").strip().upper()
    nxt = str(next_status or "").strip().upper()
    if not current or current not in _SESSION_STATUSES:
        raise ValueError(f"unknown current session status: {current_status}")
    if not nxt or nxt not in _SESSION_STATUSES:
        raise ValueError(f"unknown next session status: {next_status}")
    allowed = _SESSION_STATUS_TRANSITIONS.get(current, set())
    if nxt not in allowed:
        raise ValueError(f"invalid session status transition: {current} -> {nxt}")


@dataclass(frozen=True)
class AgentSessionRecord:
    session_id: str
    tenant_id: str
    created_by: str
    status: str
    selected_model: Optional[str]
    enabled_tools: List[str]
    summary: Optional[str]
    metadata: Dict[str, Any]
    started_at: datetime
    terminated_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentSessionMessageRecord:
    message_id: str
    session_id: str
    role: str
    content: str
    content_digest: Optional[str]
    is_removed: bool
    removed_at: Optional[datetime]
    removed_by: Optional[str]
    removed_reason: Optional[str]
    token_count: Optional[int]
    cost_estimate: Optional[float]
    latency_ms: Optional[int]
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AgentSessionJobRecord:
    job_id: str
    session_id: str
    plan_id: Optional[str]
    run_id: Optional[str]
    status: str
    error: Optional[str]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    finished_at: Optional[datetime]


@dataclass(frozen=True)
class AgentSessionContextItemRecord:
    item_id: str
    session_id: str
    item_type: str
    include_mode: str
    ref: Dict[str, Any]
    token_count: Optional[int]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentSessionEventRecord:
    event_id: str
    session_id: str
    tenant_id: str
    event_type: str
    occurred_at: datetime
    trace_id: Optional[str]
    correlation_id: Optional[str]
    data: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AgentSessionCIResultRecord:
    ci_result_id: str
    session_id: str
    tenant_id: str
    job_id: Optional[str]
    plan_id: Optional[str]
    run_id: Optional[str]
    provider: Optional[str]
    status: str
    details_url: Optional[str]
    summary: Optional[str]
    checks: List[Dict[str, Any]]
    raw: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AgentSessionToolCallRecord:
    tool_run_id: str
    session_id: str
    tenant_id: str
    job_id: Optional[str]
    plan_id: Optional[str]
    run_id: Optional[str]
    step_id: Optional[str]
    tool_id: str
    method: str
    path: str
    query: Dict[str, Any]
    request_body: Any
    request_digest: Optional[str]
    request_token_count: Optional[int]
    idempotency_key: Optional[str]
    status: str
    response_status: Optional[int]
    response_body: Any
    response_digest: Optional[str]
    response_token_count: Optional[int]
    error_code: Optional[str]
    error_message: Optional[str]
    side_effect_summary: Dict[str, Any]
    latency_ms: Optional[int]
    started_at: datetime
    finished_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentSessionLLMCallRecord:
    llm_call_id: str
    session_id: str
    tenant_id: str
    job_id: Optional[str]
    plan_id: Optional[str]
    call_type: str
    provider: str
    model_id: str
    cache_hit: bool
    latency_ms: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cost_estimate: Optional[float]
    input_digest: Optional[str]
    output_digest: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class AgentSessionLLMUsageAggregateRecord:
    tenant_id: str
    user_id: Optional[str]
    model_id: Optional[str]
    calls: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cost_estimate: float
    first_at: datetime
    last_at: datetime


class AgentSessionRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_agent",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(pool_min or 1)
        self._pool_max = int(pool_max or 5)

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=30,
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def shutdown(self) -> None:
        await self.close()

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_sessions (
                    session_id UUID PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    selected_model TEXT,
                    enabled_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
                    summary TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    terminated_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_tenant ON {self._schema}.agent_sessions(tenant_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_creator ON {self._schema}.agent_sessions(created_by)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_status ON {self._schema}.agent_sessions(status)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_messages (
                    message_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    content_digest TEXT,
                    is_removed BOOLEAN NOT NULL DEFAULT false,
                    removed_at TIMESTAMPTZ,
                    removed_by TEXT,
                    removed_reason TEXT,
                    token_count INTEGER,
                    cost_estimate DOUBLE PRECISION,
                    latency_ms INTEGER,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            # Forward/backward compatible schema upgrades.
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_session_messages ADD COLUMN IF NOT EXISTS is_removed BOOLEAN NOT NULL DEFAULT false"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_session_messages ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_session_messages ADD COLUMN IF NOT EXISTS removed_by TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_session_messages ADD COLUMN IF NOT EXISTS removed_reason TEXT"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_messages_session ON {self._schema}.agent_session_messages(session_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_messages_removed ON {self._schema}.agent_session_messages(session_id, is_removed)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_jobs (
                    job_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    plan_id UUID,
                    run_id UUID,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    error TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_session ON {self._schema}.agent_session_jobs(session_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_status ON {self._schema}.agent_session_jobs(status)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_context_items (
                    item_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    item_type TEXT NOT NULL,
                    include_mode TEXT NOT NULL DEFAULT 'summary',
                    ref JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    token_count INTEGER,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_session ON {self._schema}.agent_session_context_items(session_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_type ON {self._schema}.agent_session_context_items(item_type)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_updated ON {self._schema}.agent_session_context_items(updated_at)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_events (
                    event_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    trace_id TEXT,
                    correlation_id TEXT,
                    data JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_events_session_time ON {self._schema}.agent_session_events(session_id, occurred_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_events_tenant_time ON {self._schema}.agent_session_events(tenant_id, occurred_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_events_type ON {self._schema}.agent_session_events(event_type)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_tool_calls (
                    tool_run_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL,
                    job_id UUID,
                    plan_id UUID,
                    run_id UUID,
                    step_id TEXT,
                    tool_id TEXT NOT NULL,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    query JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    request_body JSONB,
                    request_digest TEXT,
                    request_token_count INTEGER,
                    idempotency_key TEXT,
                    status TEXT NOT NULL DEFAULT 'STARTED',
                    response_status INTEGER,
                    response_body JSONB,
                    response_digest TEXT,
                    response_token_count INTEGER,
                    error_code TEXT,
                    error_message TEXT,
                    side_effect_summary JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    latency_ms INTEGER,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_session_tool_calls ADD COLUMN IF NOT EXISTS request_token_count INTEGER"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_session_tool_calls ADD COLUMN IF NOT EXISTS response_token_count INTEGER"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_session_time ON {self._schema}.agent_session_tool_calls(session_id, started_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_tenant_time ON {self._schema}.agent_session_tool_calls(tenant_id, started_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_tool_id ON {self._schema}.agent_session_tool_calls(tool_id)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_llm_calls (
                    llm_call_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL,
                    job_id UUID,
                    plan_id UUID,
                    call_type TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model_id TEXT NOT NULL,
                    cache_hit BOOLEAN NOT NULL DEFAULT false,
                    latency_ms INTEGER NOT NULL,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    cost_estimate DOUBLE PRECISION,
                    input_digest TEXT,
                    output_digest TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_session_time ON {self._schema}.agent_session_llm_calls(session_id, created_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_tenant_time ON {self._schema}.agent_session_llm_calls(tenant_id, created_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_model ON {self._schema}.agent_session_llm_calls(model_id)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_ci_results (
                    ci_result_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL,
                    job_id UUID,
                    plan_id UUID,
                    run_id UUID,
                    provider TEXT,
                    status TEXT NOT NULL,
                    details_url TEXT,
                    summary TEXT,
                    checks JSONB NOT NULL DEFAULT '[]'::jsonb,
                    raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_ci_results_session_time ON {self._schema}.agent_session_ci_results(session_id, created_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_ci_results_tenant_time ON {self._schema}.agent_session_ci_results(tenant_id, created_at)"
            )

    def _row_to_session(self, row: asyncpg.Record) -> AgentSessionRecord:
        return AgentSessionRecord(
            session_id=str(row["session_id"]),
            tenant_id=str(row["tenant_id"]),
            created_by=str(row["created_by"]),
            status=str(row["status"]),
            selected_model=row["selected_model"],
            enabled_tools=[str(t) for t in (coerce_json_dataset(row["enabled_tools"]) or []) if t],
            summary=row["summary"],
            metadata=coerce_json_dataset(row["metadata"]),
            started_at=row["started_at"],
            terminated_at=row["terminated_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_message(self, row: asyncpg.Record) -> AgentSessionMessageRecord:
        content_value = str(row["content"])
        encryptor = _get_encryptor()
        if encryptor is not None and is_encrypted_text(content_value):
            try:
                content_value = encryptor.decrypt_text(
                    content_value,
                    aad=_aad_for_session(session_id=str(row["session_id"])),
                )
            except Exception:
                content_value = "<encrypted>"
        return AgentSessionMessageRecord(
            message_id=str(row["message_id"]),
            session_id=str(row["session_id"]),
            role=str(row["role"]),
            content=content_value,
            content_digest=row["content_digest"],
            is_removed=bool(row.get("is_removed") or False),
            removed_at=row.get("removed_at"),
            removed_by=row.get("removed_by"),
            removed_reason=row.get("removed_reason"),
            token_count=row["token_count"],
            cost_estimate=row["cost_estimate"],
            latency_ms=row["latency_ms"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
        )

    def _row_to_job(self, row: asyncpg.Record) -> AgentSessionJobRecord:
        return AgentSessionJobRecord(
            job_id=str(row["job_id"]),
            session_id=str(row["session_id"]),
            plan_id=str(row["plan_id"]) if row["plan_id"] else None,
            run_id=str(row["run_id"]) if row["run_id"] else None,
            status=str(row["status"]),
            error=row["error"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            finished_at=row["finished_at"],
        )

    def _row_to_context_item(self, row: asyncpg.Record) -> AgentSessionContextItemRecord:
        return AgentSessionContextItemRecord(
            item_id=str(row["item_id"]),
            session_id=str(row["session_id"]),
            item_type=str(row["item_type"]),
            include_mode=str(row["include_mode"] or "summary"),
            ref=coerce_json_dataset(row["ref"]),
            token_count=row["token_count"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_event(self, row: asyncpg.Record) -> AgentSessionEventRecord:
        return AgentSessionEventRecord(
            event_id=str(row["event_id"]),
            session_id=str(row["session_id"]),
            tenant_id=str(row["tenant_id"]),
            event_type=str(row["event_type"]),
            occurred_at=row["occurred_at"],
            trace_id=row.get("trace_id"),
            correlation_id=row.get("correlation_id"),
            data=coerce_json_dataset(row.get("data")),
            created_at=row["created_at"],
        )

    def _row_to_ci_result(self, row: asyncpg.Record) -> AgentSessionCIResultRecord:
        return AgentSessionCIResultRecord(
            ci_result_id=str(row["ci_result_id"]),
            session_id=str(row["session_id"]),
            tenant_id=str(row["tenant_id"]),
            job_id=str(row["job_id"]) if row.get("job_id") else None,
            plan_id=str(row["plan_id"]) if row.get("plan_id") else None,
            run_id=str(row["run_id"]) if row.get("run_id") else None,
            provider=str(row["provider"]) if row.get("provider") else None,
            status=str(row["status"] or ""),
            details_url=str(row["details_url"]) if row.get("details_url") else None,
            summary=str(row["summary"]) if row.get("summary") else None,
            checks=[dict(item) for item in (coerce_json_dataset(row.get("checks")) or []) if isinstance(item, dict)],
            raw=coerce_json_dataset(row.get("raw")),
            created_at=row["created_at"],
        )

    def _row_to_tool_call(self, row: asyncpg.Record) -> AgentSessionToolCallRecord:
        encryptor = _get_encryptor()
        session_value = str(row.get("session_id") or "")
        tenant_value = str(row.get("tenant_id") or "default")
        aad = _aad_for_session(session_id=session_value) if session_value else None

        request_body = coerce_json_dataset(row.get("request_body"))
        response_body = coerce_json_dataset(row.get("response_body"))
        if encryptor is not None and aad is not None:
            if is_encrypted_json(request_body):
                try:
                    request_body = encryptor.decrypt_json(request_body, aad=aad)
                except Exception:
                    request_body = None
            if is_encrypted_json(response_body):
                try:
                    response_body = encryptor.decrypt_json(response_body, aad=aad)
                except Exception:
                    response_body = None
        return AgentSessionToolCallRecord(
            tool_run_id=str(row["tool_run_id"]),
            session_id=session_value,
            tenant_id=tenant_value,
            job_id=str(row["job_id"]) if row.get("job_id") else None,
            plan_id=str(row["plan_id"]) if row.get("plan_id") else None,
            run_id=str(row["run_id"]) if row.get("run_id") else None,
            step_id=row.get("step_id"),
            tool_id=str(row["tool_id"]),
            method=str(row["method"]),
            path=str(row["path"]),
            query=coerce_json_dataset(row.get("query")),
            request_body=request_body,
            request_digest=row.get("request_digest"),
            request_token_count=int(row["request_token_count"]) if row.get("request_token_count") is not None else None,
            idempotency_key=row.get("idempotency_key"),
            status=str(row["status"]),
            response_status=int(row["response_status"]) if row.get("response_status") is not None else None,
            response_body=response_body,
            response_digest=row.get("response_digest"),
            response_token_count=int(row["response_token_count"]) if row.get("response_token_count") is not None else None,
            error_code=row.get("error_code"),
            error_message=row.get("error_message"),
            side_effect_summary=coerce_json_dataset(row.get("side_effect_summary")),
            latency_ms=int(row["latency_ms"]) if row.get("latency_ms") is not None else None,
            started_at=row["started_at"],
            finished_at=row.get("finished_at"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_llm_call(self, row: asyncpg.Record) -> AgentSessionLLMCallRecord:
        return AgentSessionLLMCallRecord(
            llm_call_id=str(row["llm_call_id"]),
            session_id=str(row["session_id"]),
            tenant_id=str(row["tenant_id"]),
            job_id=str(row["job_id"]) if row.get("job_id") else None,
            plan_id=str(row["plan_id"]) if row.get("plan_id") else None,
            call_type=str(row["call_type"]),
            provider=str(row["provider"]),
            model_id=str(row["model_id"]),
            cache_hit=bool(row.get("cache_hit") or False),
            latency_ms=int(row.get("latency_ms") or 0),
            prompt_tokens=int(row.get("prompt_tokens") or 0),
            completion_tokens=int(row.get("completion_tokens") or 0),
            total_tokens=int(row.get("total_tokens") or 0),
            cost_estimate=float(row["cost_estimate"]) if row.get("cost_estimate") is not None else None,
            input_digest=row.get("input_digest"),
            output_digest=row.get("output_digest"),
            created_at=row["created_at"],
        )

    async def create_session(
        self,
        *,
        session_id: str,
        tenant_id: str,
        created_by: str,
        status: str = "ACTIVE",
        selected_model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
    ) -> AgentSessionRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        enabled_tools_payload = normalize_json_payload([t for t in (enabled_tools or []) if str(t).strip()])
        metadata_payload = normalize_json_payload(metadata or {})

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_sessions (
                    session_id, tenant_id, created_by, status, selected_model,
                    enabled_tools, metadata, started_at
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7::jsonb, COALESCE($8, NOW()))
                RETURNING session_id, tenant_id, created_by, status, selected_model,
                          enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                """,
                session_id,
                tenant_id,
                created_by,
                status,
                selected_model,
                enabled_tools_payload,
                metadata_payload,
                started_at,
            )
        return self._row_to_session(row)

    async def get_session(self, *, session_id: str, tenant_id: str) -> Optional[AgentSessionRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT session_id, tenant_id, created_by, status, selected_model,
                       enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                FROM {self._schema}.agent_sessions
                WHERE session_id = $1::uuid AND tenant_id = $2
                """,
                session_id,
                tenant_id,
            )
        return self._row_to_session(row) if row else None

    async def list_sessions(
        self,
        *,
        tenant_id: str,
        created_by: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AgentSessionRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        clauses: list[str] = ["tenant_id = $1"]
        values: list[Any] = [tenant_id]
        if created_by:
            values.append(created_by)
            clauses.append(f"created_by = ${len(values)}")
        if status:
            values.append(status)
            clauses.append(f"status = ${len(values)}")
        where = " AND ".join(clauses)
        values.extend([int(limit), int(offset)])
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT session_id, tenant_id, created_by, status, selected_model,
                       enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                FROM {self._schema}.agent_sessions
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT ${len(values) - 1}
                OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_session(row) for row in rows]

    async def update_session(
        self,
        *,
        session_id: str,
        tenant_id: str,
        status: Optional[str] = None,
        selected_model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        summary: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        terminated_at: Optional[datetime] = None,
    ) -> Optional[AgentSessionRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        current = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not current:
            return None

        if status is not None:
            validate_session_status_transition(current_status=current.status, next_status=status)

        enabled_tools_payload = (
            normalize_json_payload([t for t in (enabled_tools or []) if str(t).strip()])
            if enabled_tools is not None
            else None
        )
        metadata_payload = normalize_json_payload(metadata) if metadata is not None else None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_sessions
                SET status = COALESCE($3, status),
                    selected_model = COALESCE($4, selected_model),
                    enabled_tools = COALESCE($5::jsonb, enabled_tools),
                    summary = COALESCE($6, summary),
                    metadata = COALESCE($7::jsonb, metadata),
                    terminated_at = COALESCE($8, terminated_at),
                    updated_at = NOW()
                WHERE session_id = $1::uuid AND tenant_id = $2
                RETURNING session_id, tenant_id, created_by, status, selected_model,
                          enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                """,
                session_id,
                tenant_id,
                status,
                selected_model,
                enabled_tools_payload,
                summary,
                metadata_payload,
                terminated_at,
            )
        return self._row_to_session(row) if row else None

    async def add_message(
        self,
        *,
        message_id: str,
        session_id: str,
        tenant_id: str,
        role: str,
        content: str,
        content_digest: Optional[str] = None,
        token_count: Optional[int] = None,
        cost_estimate: Optional[float] = None,
        latency_ms: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionMessageRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        # Tenant isolation guard: fail closed if the session isn't in the tenant.
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        encryptor = _get_encryptor()
        aad = _aad_for_session(session_id=session_id)
        content_payload = (
            encryptor.encrypt_text(content, aad=aad) if encryptor is not None and content not in (None, "") else content
        )

        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_messages (
                    message_id, session_id, role, content, content_digest,
                    token_count, cost_estimate, latency_ms, metadata, created_at
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9::jsonb, COALESCE($10, NOW()))
                RETURNING message_id, session_id, role, content, content_digest,
                          token_count, cost_estimate, latency_ms, metadata, created_at
                """,
                message_id,
                session_id,
                role,
                content_payload,
                content_digest,
                token_count,
                cost_estimate,
                latency_ms,
                payload,
                created_at,
            )
        return self._row_to_message(row)

    async def list_messages(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 200,
        offset: int = 0,
        include_removed: bool = False,
    ) -> List[AgentSessionMessageRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        where = "session_id = $1::uuid"
        if not include_removed:
            where += " AND is_removed = false"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT message_id, session_id, role, content, content_digest,
                       is_removed, removed_at, removed_by, removed_reason,
                       token_count, cost_estimate, latency_ms, metadata, created_at
                FROM {self._schema}.agent_session_messages
                WHERE {where}
                ORDER BY created_at ASC
                LIMIT $2
                OFFSET $3
                """,
                session_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_message(row) for row in rows]

    async def get_messages_by_ids(
        self,
        *,
        session_id: str,
        tenant_id: str,
        message_ids: List[str],
        include_removed: bool = True,
    ) -> List[AgentSessionMessageRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        unique_ids = [mid for mid in dict.fromkeys(message_ids or []) if mid]
        if not unique_ids:
            return []

        where = "m.session_id = $1::uuid AND m.message_id = ANY($2::uuid[])"
        if not include_removed:
            where += " AND m.is_removed = false"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT m.message_id, m.session_id, m.role, m.content, m.content_digest,
                       m.is_removed, m.removed_at, m.removed_by, m.removed_reason,
                       m.token_count, m.cost_estimate, m.latency_ms, m.metadata, m.created_at
                FROM {self._schema}.agent_session_messages m
                JOIN {self._schema}.agent_sessions s
                  ON m.session_id = s.session_id
                WHERE {where}
                  AND s.tenant_id = $3
                ORDER BY m.created_at ASC
                """,
                session_id,
                unique_ids,
                tenant_id,
            )
        return [self._row_to_message(row) for row in rows]

    async def mark_messages_removed(
        self,
        *,
        session_id: str,
        tenant_id: str,
        message_ids: List[str],
        removed_by: str,
        removed_reason: Optional[str] = None,
        removed_at: Optional[datetime] = None,
        placeholder: str = "<removed>",
    ) -> int:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        unique_ids = [mid for mid in dict.fromkeys(message_ids or []) if mid]
        if not unique_ids:
            return 0

        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"""
                UPDATE {self._schema}.agent_session_messages m
                SET content = $4,
                    is_removed = true,
                    removed_at = COALESCE($5, NOW()),
                    removed_by = $6,
                    removed_reason = $7
                FROM {self._schema}.agent_sessions s
                WHERE m.session_id = s.session_id
                  AND m.session_id = $1::uuid
                  AND m.message_id = ANY($2::uuid[])
                  AND s.tenant_id = $3
                """,
                session_id,
                unique_ids,
                tenant_id,
                str(placeholder or "<removed>"),
                removed_at,
                str(removed_by or "").strip() or None,
                str(removed_reason or "").strip() or None,
            )
        try:
            return int(str(result).split()[-1])
        except Exception:  # pragma: no cover
            return 0

    async def create_job(
        self,
        *,
        job_id: str,
        session_id: str,
        tenant_id: str,
        plan_id: Optional[str] = None,
        run_id: Optional[str] = None,
        status: str = "PENDING",
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionJobRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        payload = normalize_json_payload(metadata or {})
        active_statuses = ("PENDING", "RUNNING", "WAITING_APPROVAL")
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                session_row = await conn.fetchrow(
                    f"""
                    SELECT session_id
                    FROM {self._schema}.agent_sessions
                    WHERE session_id = $1::uuid AND tenant_id = $2
                    FOR UPDATE
                    """,
                    session_id,
                    tenant_id,
                )
                if not session_row:
                    raise ValueError("session not found")

                active = await conn.fetchrow(
                    f"""
                    SELECT job_id, status
                    FROM {self._schema}.agent_session_jobs
                    WHERE session_id = $1::uuid
                      AND status = ANY($2::text[])
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    session_id,
                    list(active_statuses),
                )
                if active:
                    raise ValueError("session already has an active job")

                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self._schema}.agent_session_jobs (
                        job_id, session_id, plan_id, run_id, status, error,
                        metadata, created_at
                    )
                    VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6, $7::jsonb, COALESCE($8, NOW()))
                    RETURNING job_id, session_id, plan_id, run_id, status, error, metadata,
                              created_at, updated_at, finished_at
                    """,
                    job_id,
                    session_id,
                    plan_id,
                    run_id,
                    status,
                    error,
                    payload,
                    created_at,
                )
        return self._row_to_job(row)

    async def update_job(
        self,
        *,
        job_id: str,
        tenant_id: str,
        status: Optional[str] = None,
        run_id: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentSessionJobRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        metadata_payload = normalize_json_payload(metadata) if metadata is not None else None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_session_jobs j
                SET status = COALESCE($2, j.status),
                    run_id = COALESCE($3::uuid, j.run_id),
                    error = COALESCE($4, j.error),
                    metadata = COALESCE($5::jsonb, j.metadata),
                    finished_at = COALESCE($6, j.finished_at),
                    updated_at = NOW()
                FROM {self._schema}.agent_sessions s
                WHERE j.job_id = $1::uuid
                  AND j.session_id = s.session_id
                  AND s.tenant_id = $7
                RETURNING j.job_id, j.session_id, j.plan_id, j.run_id, j.status, j.error,
                          j.metadata, j.created_at, j.updated_at, j.finished_at
                """,
                job_id,
                status,
                run_id,
                error,
                metadata_payload,
                finished_at,
                tenant_id,
            )
        return self._row_to_job(row) if row else None

    async def get_job(self, *, job_id: str, tenant_id: str) -> Optional[AgentSessionJobRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT j.job_id, j.session_id, j.plan_id, j.run_id, j.status, j.error,
                       j.metadata, j.created_at, j.updated_at, j.finished_at
                FROM {self._schema}.agent_session_jobs j
                JOIN {self._schema}.agent_sessions s
                  ON j.session_id = s.session_id
                WHERE j.job_id = $1::uuid
                  AND s.tenant_id = $2
                """,
                job_id,
                tenant_id,
            )
        return self._row_to_job(row) if row else None

    async def list_jobs(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AgentSessionJobRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT job_id, session_id, plan_id, run_id, status, error,
                       metadata, created_at, updated_at, finished_at
                FROM {self._schema}.agent_session_jobs
                WHERE session_id = $1::uuid
                ORDER BY created_at DESC
                LIMIT $2
                OFFSET $3
                """,
                session_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_job(row) for row in rows]

    async def add_context_item(
        self,
        *,
        item_id: str,
        session_id: str,
        tenant_id: str,
        item_type: str,
        include_mode: str = "summary",
        ref: Optional[Dict[str, Any]] = None,
        token_count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionContextItemRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        ref_payload = normalize_json_payload(ref or {})
        metadata_payload = normalize_json_payload(metadata or {})
        include_mode_value = str(include_mode or "summary").strip().lower() or "summary"
        item_type_value = str(item_type or "").strip()
        if not item_type_value:
            raise ValueError("item_type is required")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_context_items (
                    item_id, session_id, item_type, include_mode, ref, token_count, metadata, created_at
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6, $7::jsonb, COALESCE($8, NOW()))
                RETURNING item_id, session_id, item_type, include_mode, ref, token_count, metadata, created_at, updated_at
                """,
                item_id,
                session_id,
                item_type_value,
                include_mode_value,
                ref_payload,
                token_count,
                metadata_payload,
                created_at,
            )
        return self._row_to_context_item(row)

    async def list_context_items(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentSessionContextItemRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT item_id, session_id, item_type, include_mode, ref, token_count, metadata, created_at, updated_at
                FROM {self._schema}.agent_session_context_items
                WHERE session_id = $1::uuid
                ORDER BY created_at ASC
                LIMIT $2
                OFFSET $3
                """,
                session_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_context_item(row) for row in rows]

    async def remove_context_item(
        self,
        *,
        session_id: str,
        tenant_id: str,
        item_id: str,
    ) -> int:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"""
                DELETE FROM {self._schema}.agent_session_context_items c
                USING {self._schema}.agent_sessions s
                WHERE c.session_id = s.session_id
                  AND c.session_id = $1::uuid
                  AND c.item_id = $2::uuid
                  AND s.tenant_id = $3
                """,
                session_id,
                item_id,
                tenant_id,
            )
        try:
            return int(str(result).split()[-1])
        except Exception:  # pragma: no cover
            return 0

    async def append_event(
        self,
        *,
        event_id: str,
        session_id: str,
        tenant_id: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
        occurred_at: Optional[datetime] = None,
        trace_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionEventRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        payload = normalize_json_payload(data or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_events (
                    event_id, session_id, tenant_id, event_type, occurred_at,
                    trace_id, correlation_id, data, created_at
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, COALESCE($5, NOW()), $6, $7, $8::jsonb, COALESCE($9, NOW()))
                RETURNING event_id, session_id, tenant_id, event_type, occurred_at,
                          trace_id, correlation_id, data, created_at
                """,
                event_id,
                session_id,
                tenant_id,
                str(event_type or "").strip() or "UNKNOWN",
                occurred_at,
                str(trace_id).strip() if trace_id else None,
                str(correlation_id).strip() if correlation_id else None,
                payload,
                created_at,
            )
        return self._row_to_event(row)

    async def list_session_events(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 500,
        after: Optional[datetime] = None,
    ) -> List[AgentSessionEventRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")
        after_value = after
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT event_id, session_id, tenant_id, event_type, occurred_at,
                       trace_id, correlation_id, data, created_at
                FROM {self._schema}.agent_session_events
                WHERE session_id = $1::uuid
                  AND tenant_id = $2
                  AND occurred_at > COALESCE($3, to_timestamp(0))
                ORDER BY occurred_at ASC
                LIMIT $4
                """,
                session_id,
                tenant_id,
                after_value,
                int(limit),
            )
        return [self._row_to_event(row) for row in rows]

    async def start_tool_call(
        self,
        *,
        tool_run_id: str,
        session_id: str,
        tenant_id: str,
        tool_id: str,
        method: str,
        path: str,
        query: Optional[Dict[str, Any]] = None,
        request_body: Any = None,
        request_digest: Optional[str] = None,
        request_token_count: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        job_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        run_id: Optional[str] = None,
        step_id: Optional[str] = None,
        started_at: Optional[datetime] = None,
    ) -> AgentSessionToolCallRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        query_payload = normalize_json_payload(query or {})
        aad = _aad_for_session(session_id=session_id)
        encryptor = _get_encryptor()
        body_payload = normalize_json_payload(_wrap_json_object(request_body)) if request_body is not None else None
        if encryptor is not None and body_payload is not None:
            body_payload = normalize_json_payload(encryptor.encrypt_json(body_payload, aad=aad))
        side_effect_payload = normalize_json_payload({})

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_tool_calls (
                    tool_run_id, session_id, tenant_id, job_id, plan_id, run_id, step_id,
                    tool_id, method, path, query, request_body, request_digest, idempotency_key,
                    request_token_count, status, side_effect_summary, started_at
                )
                VALUES (
                    $1::uuid, $2::uuid, $3, $4::uuid, $5::uuid, $6::uuid, $7,
                    $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14,
                    $15, 'STARTED', $16::jsonb, COALESCE($17, NOW())
                )
                ON CONFLICT (tool_run_id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    session_id = EXCLUDED.session_id,
                    job_id = COALESCE(EXCLUDED.job_id, {self._schema}.agent_session_tool_calls.job_id),
                    plan_id = COALESCE(EXCLUDED.plan_id, {self._schema}.agent_session_tool_calls.plan_id),
                    run_id = COALESCE(EXCLUDED.run_id, {self._schema}.agent_session_tool_calls.run_id),
                    step_id = COALESCE(EXCLUDED.step_id, {self._schema}.agent_session_tool_calls.step_id),
                    tool_id = EXCLUDED.tool_id,
                    method = EXCLUDED.method,
                    path = EXCLUDED.path,
                    query = EXCLUDED.query,
                    request_body = COALESCE(EXCLUDED.request_body, {self._schema}.agent_session_tool_calls.request_body),
                    request_digest = COALESCE(EXCLUDED.request_digest, {self._schema}.agent_session_tool_calls.request_digest),
                    request_token_count = COALESCE(EXCLUDED.request_token_count, {self._schema}.agent_session_tool_calls.request_token_count),
                    idempotency_key = COALESCE(EXCLUDED.idempotency_key, {self._schema}.agent_session_tool_calls.idempotency_key),
                    updated_at = NOW()
                RETURNING tool_run_id, session_id, tenant_id, job_id, plan_id, run_id, step_id, tool_id, method, path,
                          query, request_body, request_digest, request_token_count, idempotency_key, status, response_status,
                          response_body, response_digest, response_token_count, error_code, error_message, side_effect_summary,
                          latency_ms, started_at, finished_at, created_at, updated_at
                """,
                tool_run_id,
                session_id,
                tenant_id,
                job_id,
                plan_id,
                run_id,
                step_id,
                str(tool_id or "").strip(),
                str(method or "").strip().upper() or "GET",
                str(path or "").strip() or "/",
                query_payload,
                body_payload,
                str(request_digest).strip() if request_digest else None,
                str(idempotency_key).strip() if idempotency_key else None,
                int(request_token_count) if request_token_count is not None else None,
                side_effect_payload,
                started_at,
            )
        return self._row_to_tool_call(row)

    async def finish_tool_call(
        self,
        *,
        tool_run_id: str,
        tenant_id: str,
        status: str,
        response_status: Optional[int] = None,
        response_body: Any = None,
        response_digest: Optional[str] = None,
        response_token_count: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        side_effect_summary: Optional[Dict[str, Any]] = None,
        latency_ms: Optional[int] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentSessionToolCallRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        payload_response = normalize_json_payload(_wrap_json_object(response_body)) if response_body is not None else None
        payload_side_effect = normalize_json_payload(side_effect_summary or {}) if side_effect_summary is not None else None

        encryptor = _get_encryptor()
        async with self._pool.acquire() as conn:
            if encryptor is not None and payload_response is not None:
                with_aad = None
                session_row = await conn.fetchrow(
                    f"""
                    SELECT session_id
                    FROM {self._schema}.agent_session_tool_calls
                    WHERE tool_run_id = $1::uuid
                      AND tenant_id = $2
                    """,
                    tool_run_id,
                    tenant_id,
                )
                if session_row and session_row.get("session_id"):
                    with_aad = _aad_for_session(session_id=str(session_row["session_id"]))
                if with_aad is not None:
                    payload_response = normalize_json_payload(encryptor.encrypt_json(payload_response, aad=with_aad))

            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_session_tool_calls
                SET status = $3,
                    response_status = COALESCE($4, response_status),
                    response_body = COALESCE($5::jsonb, response_body),
                    response_digest = COALESCE($6, response_digest),
                    response_token_count = COALESCE($7, response_token_count),
                    error_code = COALESCE($8, error_code),
                    error_message = COALESCE($9, error_message),
                    side_effect_summary = COALESCE($10::jsonb, side_effect_summary),
                    latency_ms = COALESCE($11, latency_ms),
                    finished_at = COALESCE($12, finished_at),
                    updated_at = NOW()
                WHERE tool_run_id = $1::uuid
                  AND tenant_id = $2
                RETURNING tool_run_id, session_id, tenant_id, job_id, plan_id, run_id, step_id, tool_id, method, path,
                          query, request_body, request_digest, request_token_count, idempotency_key, status, response_status,
                          response_body, response_digest, response_token_count, error_code, error_message, side_effect_summary,
                          latency_ms, started_at, finished_at, created_at, updated_at
                """,
                tool_run_id,
                tenant_id,
                str(status or "").strip().upper() or "COMPLETED",
                int(response_status) if response_status is not None else None,
                payload_response,
                str(response_digest).strip() if response_digest else None,
                int(response_token_count) if response_token_count is not None else None,
                str(error_code).strip() if error_code else None,
                str(error_message).strip() if error_message else None,
                payload_side_effect,
                int(latency_ms) if latency_ms is not None else None,
                finished_at,
            )
        return self._row_to_tool_call(row) if row else None

    async def list_tool_calls(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentSessionToolCallRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT tool_run_id, session_id, tenant_id, job_id, plan_id, run_id, step_id, tool_id, method, path,
                       query, request_body, request_digest, request_token_count, idempotency_key, status, response_status,
                       response_body, response_digest, response_token_count, error_code, error_message, side_effect_summary,
                       latency_ms, started_at, finished_at, created_at, updated_at
                FROM {self._schema}.agent_session_tool_calls
                WHERE session_id = $1::uuid
                  AND tenant_id = $2
                ORDER BY started_at ASC
                LIMIT $3
                OFFSET $4
                """,
                session_id,
                tenant_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_tool_call(row) for row in rows]

    async def record_llm_call(
        self,
        *,
        llm_call_id: str,
        session_id: str,
        tenant_id: str,
        call_type: str,
        provider: str,
        model_id: str,
        cache_hit: bool,
        latency_ms: int,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        cost_estimate: Optional[float] = None,
        input_digest: Optional[str] = None,
        output_digest: Optional[str] = None,
        job_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionLLMCallRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_llm_calls (
                    llm_call_id, session_id, tenant_id, job_id, plan_id, call_type,
                    provider, model_id, cache_hit, latency_ms,
                    prompt_tokens, completion_tokens, total_tokens, cost_estimate,
                    input_digest, output_digest, created_at
                )
                VALUES (
                    $1::uuid, $2::uuid, $3, $4::uuid, $5::uuid, $6,
                    $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, $16, COALESCE($17, NOW())
                )
                RETURNING llm_call_id, session_id, tenant_id, job_id, plan_id, call_type, provider, model_id,
                          cache_hit, latency_ms, prompt_tokens, completion_tokens, total_tokens, cost_estimate,
                          input_digest, output_digest, created_at
                """,
                llm_call_id,
                session_id,
                tenant_id,
                job_id,
                plan_id,
                str(call_type or "").strip() or "unknown",
                str(provider or "").strip() or "unknown",
                str(model_id or "").strip() or "unknown",
                bool(cache_hit),
                int(latency_ms),
                int(prompt_tokens),
                int(completion_tokens),
                int(total_tokens),
                float(cost_estimate) if cost_estimate is not None else None,
                str(input_digest).strip() if input_digest else None,
                str(output_digest).strip() if output_digest else None,
                created_at,
            )
        return self._row_to_llm_call(row)

    async def list_llm_calls(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentSessionLLMCallRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT llm_call_id, session_id, tenant_id, job_id, plan_id, call_type, provider, model_id,
                       cache_hit, latency_ms, prompt_tokens, completion_tokens, total_tokens, cost_estimate,
                       input_digest, output_digest, created_at
                FROM {self._schema}.agent_session_llm_calls
                WHERE session_id = $1::uuid
                  AND tenant_id = $2
                ORDER BY created_at ASC
                LIMIT $3
                OFFSET $4
                """,
                session_id,
                tenant_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_llm_call(row) for row in rows]

    async def record_ci_result(
        self,
        *,
        ci_result_id: str,
        session_id: str,
        tenant_id: str,
        job_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        run_id: Optional[str] = None,
        provider: Optional[str] = None,
        status: str,
        details_url: Optional[str] = None,
        summary: Optional[str] = None,
        checks: Optional[List[Dict[str, Any]]] = None,
        raw: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionCIResultRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        payload_checks = normalize_json_payload(checks or [])
        payload_raw = normalize_json_payload(raw or {})

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_ci_results (
                    ci_result_id, session_id, tenant_id,
                    job_id, plan_id, run_id,
                    provider, status, details_url, summary,
                    checks, raw, created_at
                )
                VALUES (
                    $1::uuid, $2::uuid, $3,
                    $4::uuid, $5::uuid, $6::uuid,
                    $7, $8, $9, $10,
                    $11::jsonb, $12::jsonb, COALESCE($13, NOW())
                )
                RETURNING ci_result_id, session_id, tenant_id,
                          job_id, plan_id, run_id,
                          provider, status, details_url, summary,
                          checks, raw, created_at
                """,
                ci_result_id,
                session_id,
                tenant_id,
                job_id,
                plan_id,
                run_id,
                str(provider).strip() if provider else None,
                str(status or "").strip() or "unknown",
                str(details_url).strip() if details_url else None,
                str(summary).strip() if summary else None,
                payload_checks,
                payload_raw,
                created_at,
            )
        return self._row_to_ci_result(row)

    async def list_ci_results(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentSessionCIResultRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT ci_result_id, session_id, tenant_id,
                       job_id, plan_id, run_id,
                       provider, status, details_url, summary,
                       checks, raw, created_at
                FROM {self._schema}.agent_session_ci_results
                WHERE session_id = $1::uuid
                  AND tenant_id = $2
                ORDER BY created_at ASC
                LIMIT $3
                OFFSET $4
                """,
                session_id,
                tenant_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_ci_result(row) for row in rows]

    async def list_expired_file_uploads(
        self,
        *,
        cutoff: datetime,
        tenant_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, str]]:
        """
        Return `file_upload` context items older than `cutoff` that still contain bucket/key refs.

        This supports SEC-005/CTX-006 retention for uploaded files (delete underlying objects).
        """
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        tenant_value = str(tenant_id).strip() if tenant_id is not None else None
        if tenant_value == "":
            tenant_value = None

        values: list[Any] = [cutoff]
        tenant_clause = ""
        if tenant_value is not None:
            values.append(tenant_value)
            tenant_clause = f" AND s.tenant_id = ${len(values)}"

        values.append(int(limit))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT c.item_id,
                       c.session_id,
                       c.ref->>'bucket' AS bucket,
                       c.ref->>'key' AS key,
                       c.ref->>'filename' AS filename
                FROM {self._schema}.agent_session_context_items c
                JOIN {self._schema}.agent_sessions s
                  ON s.session_id = c.session_id
                WHERE c.item_type = 'file_upload'
                  AND c.created_at < $1
                  AND (c.ref ? 'bucket')
                  AND (c.ref ? 'key')
                  {tenant_clause}
                ORDER BY c.created_at ASC
                LIMIT ${len(values)}
                """,
                *values,
            )

        results: list[dict[str, str]] = []
        for row in rows:
            bucket = str(row.get("bucket") or "").strip()
            key = str(row.get("key") or "").strip()
            if not bucket or not key:
                continue
            results.append(
                {
                    "item_id": str(row.get("item_id") or ""),
                    "session_id": str(row.get("session_id") or ""),
                    "bucket": bucket,
                    "key": key,
                    "filename": str(row.get("filename") or "").strip(),
                }
            )
        return results

    async def aggregate_llm_usage(
        self,
        *,
        tenant_id: str,
        group_by: str = "model",
        created_by: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentSessionLLMUsageAggregateRecord]:
        """
        Aggregate LLM usage/cost across sessions for a tenant (OBS-005).

        group_by:
          - tenant
          - model
          - user
          - model_user
        """
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        tenant_value = str(tenant_id or "").strip()
        if not tenant_value:
            raise ValueError("tenant_id is required")

        group = str(group_by or "").strip().lower() or "model"
        select_parts = ["c.tenant_id AS tenant_id"]
        group_parts = ["c.tenant_id"]

        if group in {"tenant", "org"}:
            pass
        elif group in {"model", "model_id"}:
            select_parts.append("c.model_id AS model_id")
            group_parts.append("c.model_id")
        elif group in {"user", "user_id"}:
            select_parts.append("s.created_by AS user_id")
            group_parts.append("s.created_by")
        elif group in {"model_user", "user_model"}:
            select_parts.append("c.model_id AS model_id")
            select_parts.append("s.created_by AS user_id")
            group_parts.extend(["c.model_id", "s.created_by"])
        else:
            raise ValueError("group_by must be one of: tenant, model, user, model_user")

        values: list[Any] = [tenant_value]
        where_clauses = [f"c.tenant_id = ${len(values)}"]

        created_by_value = str(created_by).strip() if created_by is not None else None
        if created_by_value:
            values.append(created_by_value)
            where_clauses.append(f"s.created_by = ${len(values)}")

        if start_time is not None:
            values.append(start_time)
            where_clauses.append(f"c.created_at >= ${len(values)}")
        if end_time is not None:
            values.append(end_time)
            where_clauses.append(f"c.created_at <= ${len(values)}")

        values.extend([int(limit), int(offset)])
        select_sql = ", ".join(select_parts)
        group_sql = ", ".join(group_parts)
        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT
                    {select_sql},
                    COUNT(*) AS calls,
                    COALESCE(SUM(c.prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(c.completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(c.total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(COALESCE(c.cost_estimate, 0)), 0) AS cost_estimate,
                    MIN(c.created_at) AS first_at,
                    MAX(c.created_at) AS last_at
                FROM {self._schema}.agent_session_llm_calls c
                JOIN {self._schema}.agent_sessions s
                  ON s.session_id = c.session_id
                 AND s.tenant_id = c.tenant_id
                WHERE {where_sql}
                GROUP BY {group_sql}
                ORDER BY cost_estimate DESC, total_tokens DESC, calls DESC
                LIMIT ${len(values) - 1}
                OFFSET ${len(values)}
                """,
                *values,
            )

        records: list[AgentSessionLLMUsageAggregateRecord] = []
        for row in rows:
            records.append(
                AgentSessionLLMUsageAggregateRecord(
                    tenant_id=str(row.get("tenant_id") or tenant_value),
                    user_id=str(row["user_id"]) if row.get("user_id") else None,
                    model_id=str(row["model_id"]) if row.get("model_id") else None,
                    calls=int(row.get("calls") or 0),
                    prompt_tokens=int(row.get("prompt_tokens") or 0),
                    completion_tokens=int(row.get("completion_tokens") or 0),
                    total_tokens=int(row.get("total_tokens") or 0),
                    cost_estimate=float(row.get("cost_estimate") or 0.0),
                    first_at=row["first_at"],
                    last_at=row["last_at"],
                )
            )
        return records

    async def apply_retention(
        self,
        *,
        cutoff: datetime,
        tenant_id: Optional[str] = None,
        action: str = "redact",
        message_placeholder: str = "<expired>",
        removed_by: str = "retention",
        removed_reason: str = "retention",
        include_messages: bool = True,
        include_tool_calls: bool = True,
        include_context_items: bool = True,
        include_ci_results: bool = True,
        include_events: bool = True,
        include_llm_calls: bool = True,
        context_item_type: Optional[str] = None,
        exclude_context_item_types: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Apply retention to agent session data (SEC-005).

        This is intentionally conservative and focuses on user-provided content:
        - messages: redact content (or delete rows)
        - tool calls: redact request/response bodies (or delete rows)
        - context items: redact metadata/token counts (or delete rows)
        - CI results / session events / llm calls: delete rows (metadata-only)
        - session events / llm calls: delete rows (metadata-only)
        """
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        action_value = str(action or "").strip().lower() or "redact"
        if action_value not in {"redact", "delete"}:
            raise ValueError("action must be redact|delete")

        tenant_value = str(tenant_id).strip() if tenant_id is not None else None
        if tenant_value == "":
            tenant_value = None

        context_item_type_value = str(context_item_type).strip() if context_item_type is not None else None
        if context_item_type_value == "":
            context_item_type_value = None

        exclude_types: list[str] = []
        if context_item_type_value is None and exclude_context_item_types:
            for item in exclude_context_item_types:
                value = str(item or "").strip()
                if value:
                    exclude_types.append(value)
        exclude_types = sorted(set(exclude_types))

        def _count(result: Any) -> int:
            try:
                return int(str(result).split()[-1])
            except Exception:  # pragma: no cover
                return 0

        stats: dict[str, int] = {
            "messages": 0,
            "tool_calls": 0,
            "context_items": 0,
            "ci_results": 0,
            "events": 0,
            "llm_calls": 0,
        }

        async with self._pool.acquire() as conn:
            if include_messages:
                if action_value == "delete":
                    values: list[Any] = [cutoff]
                    tenant_clause = ""
                    if tenant_value is not None:
                        values.append(tenant_value)
                        tenant_clause = f" AND s.tenant_id = ${len(values)}"
                    res = await conn.execute(
                        f"""
                        DELETE FROM {self._schema}.agent_session_messages m
                        USING {self._schema}.agent_sessions s
                        WHERE m.session_id = s.session_id
                          AND m.created_at < $1
                          {tenant_clause}
                        """,
                        *values,
                    )
                    stats["messages"] = _count(res)
                else:
                    values = [
                        str(message_placeholder or "<expired>"),
                        str(removed_by or "retention"),
                        str(removed_reason or "retention"),
                        cutoff,
                    ]
                    tenant_clause = ""
                    if tenant_value is not None:
                        values.append(tenant_value)
                        tenant_clause = f" AND s.tenant_id = ${len(values)}"
                    res = await conn.execute(
                        f"""
                        UPDATE {self._schema}.agent_session_messages m
                        SET content = $1,
                            is_removed = true,
                            removed_at = NOW(),
                            removed_by = $2,
                            removed_reason = $3
                        FROM {self._schema}.agent_sessions s
                        WHERE m.session_id = s.session_id
                          AND m.is_removed = false
                          AND m.created_at < $4
                          {tenant_clause}
                        """,
                        *values,
                    )
                    stats["messages"] = _count(res)

            if include_tool_calls:
                if action_value == "delete":
                    values = [cutoff]
                    tenant_clause = ""
                    if tenant_value is not None:
                        values.append(tenant_value)
                        tenant_clause = f" AND tenant_id = ${len(values)}"
                    res = await conn.execute(
                        f"""
                        DELETE FROM {self._schema}.agent_session_tool_calls
                        WHERE started_at < $1
                        {tenant_clause}
                        """,
                        *values,
                    )
                    stats["tool_calls"] = _count(res)
                else:
                    values = [cutoff]
                    tenant_clause = ""
                    if tenant_value is not None:
                        values.append(tenant_value)
                        tenant_clause = f" AND tenant_id = ${len(values)}"
                    res = await conn.execute(
                        f"""
                        UPDATE {self._schema}.agent_session_tool_calls
                        SET request_body = NULL,
                            response_body = NULL,
                            updated_at = NOW()
                        WHERE started_at < $1
                        {tenant_clause}
                        """,
                        *values,
                    )
                    stats["tool_calls"] = _count(res)

            if include_context_items:
                if action_value == "delete":
                    values: list[Any] = [cutoff]
                    type_clause = ""
                    if context_item_type_value is not None:
                        values.append(context_item_type_value)
                        type_clause = f" AND c.item_type = ${len(values)}"
                    elif exclude_types:
                        values.append(exclude_types)
                        type_clause = f" AND NOT (c.item_type = ANY(${len(values)}::text[]))"

                    tenant_clause = ""
                    if tenant_value is not None:
                        values.append(tenant_value)
                        tenant_clause = f" AND s.tenant_id = ${len(values)}"

                    res = await conn.execute(
                        f"""
                        DELETE FROM {self._schema}.agent_session_context_items c
                        USING {self._schema}.agent_sessions s
                        WHERE c.session_id = s.session_id
                          AND c.created_at < $1
                          {type_clause}
                          {tenant_clause}
                        """,
                        *values,
                    )
                    stats["context_items"] = _count(res)
                else:
                    if context_item_type_value is not None:
                        if context_item_type_value == "file_upload":
                            values = [cutoff]
                            tenant_clause = ""
                            if tenant_value is not None:
                                values.append(tenant_value)
                                tenant_clause = f" AND s.tenant_id = ${len(values)}"
                            res = await conn.execute(
                                f"""
                                UPDATE {self._schema}.agent_session_context_items c
                                SET ref = (c.ref - 'bucket' - 'key' - 'checksum'),
                                    token_count = 0,
                                    metadata = (c.metadata - 'extracted_text' - 'extracted_text_preview')
                                              || jsonb_build_object('retention_redacted', true, 'retention_redacted_at', NOW()),
                                    updated_at = NOW()
                                FROM {self._schema}.agent_sessions s
                                WHERE c.session_id = s.session_id
                                  AND c.item_type = 'file_upload'
                                  AND c.created_at < $1
                                  {tenant_clause}
                                """,
                                *values,
                            )
                            stats["context_items"] = _count(res)
                        else:
                            values = [cutoff, context_item_type_value]
                            tenant_clause = ""
                            if tenant_value is not None:
                                values.append(tenant_value)
                                tenant_clause = f" AND s.tenant_id = ${len(values)}"
                            res = await conn.execute(
                                f"""
                                UPDATE {self._schema}.agent_session_context_items c
                                SET token_count = 0,
                                    metadata = (c.metadata - 'extracted_text' - 'extracted_text_preview')
                                              || jsonb_build_object('retention_redacted', true, 'retention_redacted_at', NOW()),
                                    updated_at = NOW()
                                FROM {self._schema}.agent_sessions s
                                WHERE c.session_id = s.session_id
                                  AND c.item_type = $2
                                  AND c.created_at < $1
                                  {tenant_clause}
                                """,
                                *values,
                            )
                            stats["context_items"] = _count(res)
                    else:
                        values = [cutoff]
                        tenant_clause = ""
                        if tenant_value is not None:
                            values.append(tenant_value)
                            tenant_clause = f" AND s.tenant_id = ${len(values)}"
                        exclude_clause = ""
                        if exclude_types:
                            values.append(exclude_types)
                            exclude_clause = f" AND NOT (c.item_type = ANY(${len(values)}::text[]))"

                        res_upload = await conn.execute(
                            f"""
                            UPDATE {self._schema}.agent_session_context_items c
                            SET ref = (c.ref - 'bucket' - 'key' - 'checksum'),
                                token_count = 0,
                                metadata = (c.metadata - 'extracted_text' - 'extracted_text_preview')
                                          || jsonb_build_object('retention_redacted', true, 'retention_redacted_at', NOW()),
                                updated_at = NOW()
                            FROM {self._schema}.agent_sessions s
                            WHERE c.session_id = s.session_id
                              AND c.item_type = 'file_upload'
                              AND c.created_at < $1
                              {tenant_clause}
                              {exclude_clause}
                            """,
                            *values,
                        )
                        res_other = await conn.execute(
                            f"""
                            UPDATE {self._schema}.agent_session_context_items c
                            SET token_count = 0,
                                metadata = (c.metadata - 'extracted_text' - 'extracted_text_preview')
                                          || jsonb_build_object('retention_redacted', true, 'retention_redacted_at', NOW()),
                                updated_at = NOW()
                            FROM {self._schema}.agent_sessions s
                            WHERE c.session_id = s.session_id
                              AND c.item_type <> 'file_upload'
                              AND c.created_at < $1
                              {tenant_clause}
                              {exclude_clause}
                            """,
                            *values,
                        )
                        stats["context_items"] = _count(res_upload) + _count(res_other)

            if include_ci_results:
                values = [cutoff]
                tenant_clause = ""
                if tenant_value is not None:
                    values.append(tenant_value)
                    tenant_clause = f" AND tenant_id = ${len(values)}"
                res = await conn.execute(
                    f"""
                    DELETE FROM {self._schema}.agent_session_ci_results
                    WHERE created_at < $1
                    {tenant_clause}
                    """,
                    *values,
                )
                stats["ci_results"] = _count(res)

            if include_events:
                values = [cutoff]
                tenant_clause = ""
                if tenant_value is not None:
                    values.append(tenant_value)
                    tenant_clause = f" AND tenant_id = ${len(values)}"
                res = await conn.execute(
                    f"""
                    DELETE FROM {self._schema}.agent_session_events
                    WHERE occurred_at < $1
                    {tenant_clause}
                    """,
                    *values,
                )
                stats["events"] = _count(res)

            if include_llm_calls:
                values = [cutoff]
                tenant_clause = ""
                if tenant_value is not None:
                    values.append(tenant_value)
                    tenant_clause = f" AND tenant_id = ${len(values)}"
                res = await conn.execute(
                    f"""
                    DELETE FROM {self._schema}.agent_session_llm_calls
                    WHERE created_at < $1
                    {tenant_clause}
                    """,
                    *values,
                )
                stats["llm_calls"] = _count(res)

        return stats
