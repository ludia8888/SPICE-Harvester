from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from oms.database.postgres import db as postgres_db
from shared.observability.tracing import trace_db_operation


@dataclass
class OntologyDeployOutboxItem:
    outbox_id: str
    deployment_id: str
    payload: Dict[str, Any]
    status: str
    publish_attempts: int
    error: Optional[str]
    claimed_by: Optional[str]
    claimed_at: Optional[datetime]
    next_attempt_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class OntologyDeployOutboxTableSpec:
    table_name: str
    attempts_column: str
    error_column: str


class OntologyDeployOutboxStore:
    """
    Outbox operations (Strategy via table/column spec).

    Both v1 and v2 deployment registries use the same claim/publish/fail flow,
    but differ in table/column naming. This store centralizes the workflow to
    prevent drift between schemas.
    """

    def __init__(self, *, table: OntologyDeployOutboxTableSpec) -> None:
        self._table = table

    @trace_db_operation("oms.deploy_outbox_store.claim_batch")
    async def claim_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[OntologyDeployOutboxItem]:
        async with postgres_db.transaction() as conn:
            claim_timeout = max(0, int(claim_timeout_seconds))
            clause = """
                WHERE (
                    status IN ('pending', 'failed')
                    AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                )
            """
            values: List[Any] = [limit]
            if claim_timeout > 0:
                clause += f"""
                    OR (
                        status = 'publishing'
                        AND (claimed_at IS NULL OR claimed_at <= NOW() - (${len(values) + 1}::int * INTERVAL '1 second'))
                    )
                """
                values.append(claim_timeout)

            table = self._table.table_name
            attempts_col = self._table.attempts_column
            error_col = self._table.error_column

            rows = await conn.fetch(
                f"""
                SELECT outbox_id, deployment_id, payload, status,
                       {attempts_col} AS publish_attempts,
                       {error_col} AS error,
                       claimed_by, claimed_at, next_attempt_at, created_at, updated_at
                FROM {table}
                {clause}
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                *values,
            )
            if not rows:
                return []
            outbox_ids = [str(row["outbox_id"]) for row in rows]
            await conn.execute(
                f"""
                UPDATE {table}
                SET status = 'publishing',
                    {attempts_col} = {attempts_col} + 1,
                    claimed_by = $2,
                    claimed_at = NOW(),
                    updated_at = NOW()
                WHERE outbox_id = ANY($1::uuid[])
                """,
                outbox_ids,
                claimed_by,
            )
            return [
                OntologyDeployOutboxItem(
                    outbox_id=str(row["outbox_id"]),
                    deployment_id=str(row["deployment_id"]),
                    payload=row["payload"] or {},
                    status=str(row["status"]),
                    publish_attempts=int(row["publish_attempts"]),
                    error=row["error"],
                    claimed_by=str(row["claimed_by"]) if row["claimed_by"] else None,
                    claimed_at=row["claimed_at"],
                    next_attempt_at=row["next_attempt_at"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]

    @trace_db_operation("oms.deploy_outbox_store.mark_published")
    async def mark_published(self, *, outbox_id: str) -> None:
        table = self._table.table_name
        error_col = self._table.error_column
        await postgres_db.execute(
            f"""
            UPDATE {table}
            SET status = 'published',
                updated_at = NOW(),
                next_attempt_at = NULL,
                {error_col} = NULL,
                claimed_by = NULL,
                claimed_at = NULL
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
        )

    @trace_db_operation("oms.deploy_outbox_store.mark_failed")
    async def mark_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        table = self._table.table_name
        error_col = self._table.error_column
        await postgres_db.execute(
            f"""
            UPDATE {table}
            SET status = 'failed',
                {error_col} = $2,
                next_attempt_at = $3,
                updated_at = NOW()
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
            error,
            next_attempt_at,
        )

