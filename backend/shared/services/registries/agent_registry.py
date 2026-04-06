"""
Agent registry for runs/steps/approvals (Postgres).

This is a lightweight control-plane store to persist agent execution state,
plan approvals, and step-level audit references.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from shared.services.registries.agent_registry_schema import ensure_agent_registry_schema
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.json_utils import coerce_json_dataset, coerce_json_pipeline, normalize_json_payload


@dataclass(frozen=True)
class AgentRunRecord:
    run_id: str
    tenant_id: str
    plan_id: Optional[str]
    status: str
    risk_level: str
    requester: Optional[str]
    delegated_actor: Optional[str]
    context: Dict[str, Any]
    plan_snapshot: Dict[str, Any]
    started_at: datetime
    finished_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentStepRecord:
    run_id: str
    step_id: str
    tenant_id: str
    tool_id: str
    status: str
    command_id: Optional[str]
    task_id: Optional[str]
    input_digest: Optional[str]
    output_digest: Optional[str]
    error: Optional[str]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentApprovalRecord:
    approval_id: str
    plan_id: str
    tenant_id: str
    step_id: Optional[str]
    decision: str
    approved_by: str
    approved_at: datetime
    comment: Optional[str]
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AgentApprovalRequestRecord:
    approval_request_id: str
    plan_id: str
    tenant_id: str
    session_id: Optional[str]
    job_id: Optional[str]
    status: str
    risk_level: str
    requested_by: str
    requested_at: datetime
    decision: Optional[str]
    decided_by: Optional[str]
    decided_at: Optional[datetime]
    comment: Optional[str]
    request_payload: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentToolIdempotencyRecord:
    tenant_id: str
    idempotency_key: str
    tool_id: str
    request_digest: str
    status: str
    response_status: Optional[int]
    response_body: Any
    error: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class AgentRegistry(PostgresSchemaRegistry):
    _REQUIRED_TABLES = (
        "agent_runs",
        "agent_steps",
        "agent_approvals",
        "agent_approval_requests",
        "agent_tool_idempotency",
    )

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:  # type: ignore[override]
        await ensure_agent_registry_schema(conn, schema=self._schema)

    def _row_to_run(self, row: asyncpg.Record) -> AgentRunRecord:
        return AgentRunRecord(
            run_id=str(row["run_id"]),
            tenant_id=str(row.get("tenant_id") or "default"),
            plan_id=str(row["plan_id"]) if row["plan_id"] else None,
            status=str(row["status"]),
            risk_level=str(row["risk_level"]),
            requester=row["requester"],
            delegated_actor=row["delegated_actor"],
            context=coerce_json_dataset(row["context"]),
            plan_snapshot=coerce_json_dataset(row["plan_snapshot"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_step(self, row: asyncpg.Record) -> AgentStepRecord:
        return AgentStepRecord(
            run_id=str(row["run_id"]),
            step_id=str(row["step_id"]),
            tenant_id=str(row.get("tenant_id") or "default"),
            tool_id=str(row["tool_id"]),
            status=str(row["status"]),
            command_id=row["command_id"],
            task_id=row["task_id"],
            input_digest=row["input_digest"],
            output_digest=row["output_digest"],
            error=row["error"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_approval(self, row: asyncpg.Record) -> AgentApprovalRecord:
        return AgentApprovalRecord(
            approval_id=str(row["approval_id"]),
            plan_id=str(row["plan_id"]),
            tenant_id=str(row.get("tenant_id") or "default"),
            step_id=row["step_id"],
            decision=str(row["decision"]),
            approved_by=str(row["approved_by"]),
            approved_at=row["approved_at"],
            comment=row["comment"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
        )

    def _row_to_approval_request(self, row: asyncpg.Record) -> AgentApprovalRequestRecord:
        return AgentApprovalRequestRecord(
            approval_request_id=str(row["approval_request_id"]),
            plan_id=str(row["plan_id"]),
            tenant_id=str(row.get("tenant_id") or "default"),
            session_id=str(row["session_id"]) if row.get("session_id") else None,
            job_id=str(row["job_id"]) if row.get("job_id") else None,
            status=str(row["status"]),
            risk_level=str(row["risk_level"]),
            requested_by=str(row["requested_by"]),
            requested_at=row["requested_at"],
            decision=row.get("decision"),
            decided_by=row.get("decided_by"),
            decided_at=row.get("decided_at"),
            comment=row.get("comment"),
            request_payload=coerce_json_dataset(row["request_payload"]),
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_tool_idempotency(self, row: asyncpg.Record) -> AgentToolIdempotencyRecord:
        return AgentToolIdempotencyRecord(
            tenant_id=str(row.get("tenant_id") or "default"),
            idempotency_key=str(row["idempotency_key"]),
            tool_id=str(row["tool_id"]),
            request_digest=str(row["request_digest"]),
            status=str(row["status"]),
            response_status=row.get("response_status"),
            response_body=coerce_json_pipeline(row.get("response_body")),
            error=row.get("error"),
            started_at=row["started_at"],
            finished_at=row.get("finished_at"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def create_run(
        self,
        *,
        run_id: str,
        tenant_id: str,
        plan_id: Optional[str],
        status: str,
        risk_level: str,
        requester: Optional[str],
        delegated_actor: Optional[str],
        context: Optional[Dict[str, Any]] = None,
        plan_snapshot: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
    ) -> AgentRunRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        payload_context = normalize_json_payload(context or {})
        payload_snapshot = normalize_json_payload(plan_snapshot or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_runs (
                    run_id, tenant_id, plan_id, status, risk_level, requester, delegated_actor,
                    context, plan_snapshot, started_at
                )
                VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6, $7, $8::jsonb, $9::jsonb, COALESCE($10, NOW()))
                RETURNING run_id, plan_id, status, risk_level, requester, delegated_actor,
                          context, plan_snapshot, started_at, finished_at, created_at, updated_at, tenant_id
                """,
                run_id,
                tenant_value,
                plan_id,
                status,
                risk_level,
                requester,
                delegated_actor,
                payload_context,
                payload_snapshot,
                started_at,
            )
        return self._row_to_run(row)

    async def update_run_status(
        self,
        *,
        run_id: str,
        tenant_id: str,
        status: str,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentRunRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_runs
                SET status = $2,
                    finished_at = COALESCE($3, finished_at),
                    updated_at = NOW()
                WHERE run_id = $1::uuid AND tenant_id = $4
                RETURNING run_id, plan_id, status, risk_level, requester, delegated_actor,
                          context, plan_snapshot, started_at, finished_at, created_at, updated_at, tenant_id
                """,
                run_id,
                status,
                finished_at,
                tenant_value,
            )
        return self._row_to_run(row) if row else None

    async def get_run(self, *, run_id: str, tenant_id: str) -> Optional[AgentRunRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT run_id, plan_id, status, risk_level, requester, delegated_actor,
                       context, plan_snapshot, started_at, finished_at, created_at, updated_at, tenant_id
                FROM {self._schema}.agent_runs
                WHERE run_id = $1::uuid AND tenant_id = $2
                """,
                run_id,
                tenant_value,
            )
        return self._row_to_run(row) if row else None

    async def list_runs(
        self,
        *,
        tenant_id: str,
        plan_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[AgentRunRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        clauses = []
        values: List[Any] = [str(tenant_id or "").strip() or "default"]
        clauses.append("tenant_id = $1")
        if plan_id:
            values.append(plan_id)
            clauses.append(f"plan_id = ${len(values)}::uuid")
        if status:
            values.append(status)
            clauses.append(f"status = ${len(values)}")
        where = f"WHERE {' AND '.join(clauses)}"
        values.append(limit)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, plan_id, status, risk_level, requester, delegated_actor,
                       context, plan_snapshot, started_at, finished_at, created_at, updated_at, tenant_id
                FROM {self._schema}.agent_runs
                {where}
                ORDER BY created_at DESC
                LIMIT ${len(values)}
                """,
                *values,
            )
        return [self._row_to_run(row) for row in rows]

    async def create_step(
        self,
        *,
        run_id: str,
        step_id: str,
        tenant_id: str,
        tool_id: str,
        status: str,
        command_id: Optional[str] = None,
        task_id: Optional[str] = None,
        input_digest: Optional[str] = None,
        output_digest: Optional[str] = None,
        error: Optional[str] = None,
        started_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentStepRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_steps (
                    run_id, step_id, tenant_id, tool_id, status, command_id, task_id,
                    input_digest, output_digest, error, started_at, metadata
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11, NOW()), $12::jsonb)
                RETURNING run_id, step_id, tool_id, status, command_id, task_id, input_digest,
                          output_digest, error, started_at, finished_at, metadata, created_at, updated_at, tenant_id
                """,
                run_id,
                step_id,
                tenant_value,
                tool_id,
                status,
                command_id,
                task_id,
                input_digest,
                output_digest,
                error,
                started_at,
                payload,
            )
        return self._row_to_step(row)

    async def update_step_status(
        self,
        *,
        run_id: str,
        step_id: str,
        tenant_id: str,
        status: str,
        output_digest: Optional[str] = None,
        error: Optional[str] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentStepRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_steps
                SET status = $3,
                    output_digest = COALESCE($4, output_digest),
                    error = COALESCE($5, error),
                    finished_at = COALESCE($6, finished_at),
                    updated_at = NOW()
                WHERE run_id = $1::uuid AND step_id = $2 AND tenant_id = $7
                RETURNING run_id, step_id, tool_id, status, command_id, task_id, input_digest,
                          output_digest, error, started_at, finished_at, metadata, created_at, updated_at, tenant_id
                """,
                run_id,
                step_id,
                status,
                output_digest,
                error,
                finished_at,
                tenant_value,
            )
        return self._row_to_step(row) if row else None

    async def list_steps(self, *, run_id: str, tenant_id: str) -> List[AgentStepRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, step_id, tool_id, status, command_id, task_id, input_digest,
                       output_digest, error, started_at, finished_at, metadata, created_at, updated_at, tenant_id
                FROM {self._schema}.agent_steps
                WHERE run_id = $1::uuid AND tenant_id = $2
                ORDER BY created_at ASC
                """,
                run_id,
                tenant_value,
            )
        return [self._row_to_step(row) for row in rows]

    async def create_approval(
        self,
        *,
        approval_id: str,
        plan_id: str,
        tenant_id: str,
        step_id: Optional[str],
        decision: str,
        approved_by: str,
        approved_at: Optional[datetime] = None,
        comment: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentApprovalRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        payload = normalize_json_payload(metadata or {})
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_approvals (
                    approval_id, plan_id, tenant_id, step_id, decision, approved_by, approved_at, comment, metadata
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, COALESCE($7, NOW()), $8, $9::jsonb)
                RETURNING approval_id, plan_id, step_id, decision, approved_by, approved_at,
                          comment, metadata, created_at, tenant_id
                """,
                approval_id,
                plan_id,
                tenant_value,
                step_id,
                decision,
                approved_by,
                approved_at,
                comment,
                payload,
            )
        return self._row_to_approval(row)

    async def list_approvals(self, *, plan_id: str, tenant_id: str) -> List[AgentApprovalRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT approval_id, plan_id, step_id, decision, approved_by, approved_at,
                       comment, metadata, created_at, tenant_id
                FROM {self._schema}.agent_approvals
                WHERE plan_id = $1::uuid AND tenant_id = $2
                ORDER BY approved_at ASC
                """,
                plan_id,
                tenant_value,
            )
        return [self._row_to_approval(row) for row in rows]

    async def create_approval_request(
        self,
        *,
        approval_request_id: str,
        plan_id: str,
        tenant_id: str,
        session_id: Optional[str],
        job_id: Optional[str],
        status: str = "PENDING",
        risk_level: str = "write",
        requested_by: str,
        requested_at: Optional[datetime] = None,
        request_payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentApprovalRequestRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        payload = normalize_json_payload(request_payload or {})
        meta_payload = normalize_json_payload(metadata or {})
        status_value = str(status or "PENDING").strip().upper() or "PENDING"
        risk_value = str(risk_level or "write").strip().lower() or "write"
        requested_by_value = str(requested_by or "").strip() or "unknown"

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_approval_requests (
                    approval_request_id, plan_id, tenant_id, session_id, job_id,
                    status, risk_level, requested_by, requested_at,
                    request_payload, metadata
                )
                VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::uuid,
                        $6, $7, $8, COALESCE($9, NOW()), $10::jsonb, $11::jsonb)
                RETURNING approval_request_id, plan_id, tenant_id, session_id, job_id,
                          status, risk_level, requested_by, requested_at,
                          decision, decided_by, decided_at, comment,
                          request_payload, metadata, created_at, updated_at
                """,
                approval_request_id,
                plan_id,
                tenant_value,
                session_id,
                job_id,
                status_value,
                risk_value,
                requested_by_value,
                requested_at,
                payload,
                meta_payload,
            )
        return self._row_to_approval_request(row)

    async def get_approval_request(
        self, *, approval_request_id: str, tenant_id: str
    ) -> Optional[AgentApprovalRequestRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT approval_request_id, plan_id, tenant_id, session_id, job_id,
                       status, risk_level, requested_by, requested_at,
                       decision, decided_by, decided_at, comment,
                       request_payload, metadata, created_at, updated_at
                FROM {self._schema}.agent_approval_requests
                WHERE approval_request_id = $1::uuid AND tenant_id = $2
                """,
                approval_request_id,
                tenant_value,
            )
        return self._row_to_approval_request(row) if row else None

    async def list_approval_requests(
        self,
        *,
        tenant_id: str,
        session_id: Optional[str] = None,
        job_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AgentApprovalRequestRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        clauses: list[str] = ["tenant_id = $1"]
        values: list[Any] = [tenant_value]
        if session_id:
            values.append(session_id)
            clauses.append(f"session_id = ${len(values)}::uuid")
        if job_id:
            values.append(job_id)
            clauses.append(f"job_id = ${len(values)}::uuid")
        if plan_id:
            values.append(plan_id)
            clauses.append(f"plan_id = ${len(values)}::uuid")
        if status:
            values.append(str(status).strip().upper())
            clauses.append(f"status = ${len(values)}")
        where = " AND ".join(clauses)
        values.extend([int(limit), int(offset)])
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT approval_request_id, plan_id, tenant_id, session_id, job_id,
                       status, risk_level, requested_by, requested_at,
                       decision, decided_by, decided_at, comment,
                       request_payload, metadata, created_at, updated_at
                FROM {self._schema}.agent_approval_requests
                WHERE {where}
                ORDER BY requested_at DESC
                LIMIT ${len(values) - 1}
                OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_approval_request(row) for row in rows]

    async def decide_approval_request(
        self,
        *,
        approval_request_id: str,
        tenant_id: str,
        decision: str,
        decided_by: str,
        decided_at: Optional[datetime] = None,
        comment: Optional[str] = None,
        status: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[AgentApprovalRequestRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        decision_value = str(decision or "").strip().upper()
        decided_by_value = str(decided_by or "").strip() or "unknown"
        comment_value = str(comment).strip() if comment is not None else None
        status_value = str(status).strip().upper() if status is not None else None
        meta_payload = normalize_json_payload(metadata or {}) if metadata is not None else None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_approval_requests
                SET decision = COALESCE($3, decision),
                    decided_by = COALESCE($4, decided_by),
                    decided_at = COALESCE($5, decided_at),
                    comment = COALESCE($6, comment),
                    status = COALESCE($7, status),
                    metadata = COALESCE($8::jsonb, metadata),
                    updated_at = NOW()
                WHERE approval_request_id = $1::uuid AND tenant_id = $2
                RETURNING approval_request_id, plan_id, tenant_id, session_id, job_id,
                          status, risk_level, requested_by, requested_at,
                          decision, decided_by, decided_at, comment,
                          request_payload, metadata, created_at, updated_at
                """,
                approval_request_id,
                tenant_value,
                decision_value,
                decided_by_value,
                decided_at,
                comment_value,
                status_value,
                meta_payload,
            )
        return self._row_to_approval_request(row) if row else None

    async def get_tool_idempotency(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
    ) -> Optional[AgentToolIdempotencyRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        key_value = str(idempotency_key or "").strip()
        if not key_value:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT tenant_id, idempotency_key, tool_id, request_digest, status,
                       response_status, response_body, error,
                       started_at, finished_at, created_at, updated_at
                FROM {self._schema}.agent_tool_idempotency
                WHERE tenant_id = $1 AND idempotency_key = $2
                """,
                tenant_value,
                key_value,
            )
        return self._row_to_tool_idempotency(row) if row else None

    async def begin_tool_idempotency(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
        tool_id: str,
        request_digest: str,
        started_at: Optional[datetime] = None,
    ) -> Tuple[Optional[AgentToolIdempotencyRecord], bool]:
        """
        Create or fetch a tool idempotency record.

        Returns (record, created).
        """
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        key_value = str(idempotency_key or "").strip()
        tool_value = str(tool_id or "").strip()
        digest_value = str(request_digest or "").strip()
        if not (key_value and tool_value and digest_value):
            return None, False

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_tool_idempotency (
                    tenant_id, idempotency_key, tool_id, request_digest, status, started_at
                )
                VALUES ($1, $2, $3, $4, 'IN_PROGRESS', COALESCE($5, NOW()))
                ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                RETURNING tenant_id, idempotency_key, tool_id, request_digest, status,
                          response_status, response_body, error,
                          started_at, finished_at, created_at, updated_at
                """,
                tenant_value,
                key_value,
                tool_value,
                digest_value,
                started_at,
            )
            if row:
                return self._row_to_tool_idempotency(row), True
            row = await conn.fetchrow(
                f"""
                SELECT tenant_id, idempotency_key, tool_id, request_digest, status,
                       response_status, response_body, error,
                       started_at, finished_at, created_at, updated_at
                FROM {self._schema}.agent_tool_idempotency
                WHERE tenant_id = $1 AND idempotency_key = $2
                """,
                tenant_value,
                key_value,
            )
        return (self._row_to_tool_idempotency(row) if row else None), False

    async def finalize_tool_idempotency(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
        tool_id: str,
        request_digest: str,
        response_status: Optional[int],
        response_body: Any,
        error: Optional[str] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentToolIdempotencyRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        key_value = str(idempotency_key or "").strip()
        tool_value = str(tool_id or "").strip()
        digest_value = str(request_digest or "").strip()
        if not (key_value and tool_value and digest_value):
            return None
        status_code = int(response_status) if response_status is not None else None
        body_payload = normalize_json_payload(response_body if response_body is not None else {})
        error_value = str(error).strip() if error is not None else None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_tool_idempotency
                SET status = 'COMPLETED',
                    response_status = COALESCE($5, response_status),
                    response_body = COALESCE($6::jsonb, response_body),
                    error = COALESCE($7, error),
                    finished_at = COALESCE($8, finished_at, NOW()),
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND idempotency_key = $2
                  AND tool_id = $3
                  AND request_digest = $4
                RETURNING tenant_id, idempotency_key, tool_id, request_digest, status,
                          response_status, response_body, error,
                          started_at, finished_at, created_at, updated_at
                """,
                tenant_value,
                key_value,
                tool_value,
                digest_value,
                status_code,
                body_payload,
                error_value,
                finished_at,
            )
        return self._row_to_tool_idempotency(row) if row else None
