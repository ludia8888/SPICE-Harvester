"""
Ontology deployment registry v2 (Postgres SSoT).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4, uuid5, NAMESPACE_URL

from oms.database.postgres import db as postgres_db
from shared.config.app_config import AppConfig

logger = logging.getLogger(__name__)


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


class OntologyDeploymentRegistryV2:
    """Record ontology deployments in Postgres (v2 schema)."""

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _maybe_decode_json(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
        return value

    async def ensure_schema(self) -> None:
        await postgres_db.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_deployments_v2 (
                deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                db_name VARCHAR(255) NOT NULL,
                target_branch VARCHAR(255) NOT NULL,
                ontology_commit_id VARCHAR(255) NOT NULL,
                snapshot_rid VARCHAR(255),
                proposal_id UUID,
                status VARCHAR(50) NOT NULL DEFAULT 'succeeded'
                    CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
                gate_policy JSONB,
                health_summary JSONB,
                deployed_by VARCHAR(255) DEFAULT 'system',
                deployed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                error TEXT,
                metadata JSONB
            );
            """
        )
        await postgres_db.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_deploy_outbox_v2 (
                outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                deployment_id UUID NOT NULL REFERENCES ontology_deployments_v2(deployment_id) ON DELETE CASCADE,
                payload JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                claimed_by TEXT,
                claimed_at TIMESTAMPTZ,
                next_attempt_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_db ON ontology_deployments_v2(db_name);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_target_branch ON ontology_deployments_v2(target_branch);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_proposal ON ontology_deployments_v2(proposal_id);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_created_at ON ontology_deployments_v2(deployed_at DESC);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_v2_status ON ontology_deploy_outbox_v2(status, next_attempt_at, created_at);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_v2_claimed ON ontology_deploy_outbox_v2(status, claimed_at);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_v2_deployment ON ontology_deploy_outbox_v2(deployment_id, status, created_at);"
        )

    @staticmethod
    def build_deploy_event_payload(
        *,
        deployment_id: str,
        db_name: str,
        proposal_id: Optional[str],
        target_branch: str,
        ontology_commit_id: str,
        snapshot_rid: Optional[str],
        deployed_by: str,
        gate_policy: Optional[Dict[str, Any]],
        health_summary: Optional[Dict[str, Any]],
        occurred_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        event_id = str(uuid5(NAMESPACE_URL, f"ontology-deploy:{deployment_id}"))
        occurred_at = occurred_at or datetime.now(timezone.utc)
        metadata = {
            "kind": "domain",
            "kafka_topic": AppConfig.ONTOLOGY_EVENTS_TOPIC,
            "ontology": {
                "ref": f"branch:{target_branch}",
                "commit": ontology_commit_id,
            },
        }
        data = {
            "deployment_id": deployment_id,
            "db_name": db_name,
            "proposal_id": proposal_id,
            "target_branch": target_branch,
            "ontology_commit_id": ontology_commit_id,
            "snapshot_rid": snapshot_rid,
            "gate_policy": gate_policy,
            "health_summary": health_summary,
        }
        return {
            "event_id": event_id,
            "event_type": "ONTOLOGY_DEPLOYED",
            "aggregate_type": "OntologyDeployment",
            "aggregate_id": deployment_id,
            "occurred_at": occurred_at,
            "actor": deployed_by,
            "data": data,
            "metadata": metadata,
        }

    async def record_deployment(
        self,
        *,
        db_name: str,
        target_branch: str,
        ontology_commit_id: str,
        snapshot_rid: Optional[str] = None,
        proposal_id: Optional[str] = None,
        status: str = "succeeded",
        gate_policy: Optional[Dict[str, Any]] = None,
        health_summary: Optional[Dict[str, Any]] = None,
        deployed_by: str = "system",
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        await self.ensure_schema()
        gate_policy_json = json.dumps(gate_policy) if gate_policy is not None else None
        health_summary_json = json.dumps(health_summary) if health_summary is not None else None
        metadata_json = json.dumps(metadata) if metadata is not None else None
        async with postgres_db.transaction() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO ontology_deployments_v2 (
                    db_name,
                    target_branch,
                    ontology_commit_id,
                    snapshot_rid,
                    proposal_id,
                    status,
                    gate_policy,
                    health_summary,
                    deployed_by,
                    error,
                    metadata
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                RETURNING deployment_id, deployed_at;
                """,
                db_name,
                target_branch,
                ontology_commit_id,
                snapshot_rid,
                proposal_id,
                status,
                gate_policy_json,
                health_summary_json,
                deployed_by,
                error,
                metadata_json,
            )
            deployment_id = str(row["deployment_id"])
            payload = self.build_deploy_event_payload(
                deployment_id=deployment_id,
                db_name=db_name,
                proposal_id=proposal_id,
                target_branch=target_branch,
                ontology_commit_id=ontology_commit_id,
                snapshot_rid=snapshot_rid,
                deployed_by=deployed_by,
                gate_policy=gate_policy,
                health_summary=health_summary,
                occurred_at=row["deployed_at"],
            )
            await conn.execute(
                """
                INSERT INTO ontology_deploy_outbox_v2 (
                    outbox_id, deployment_id, payload, status
                ) VALUES ($1::uuid, $2::uuid, $3::jsonb, 'pending')
                """,
                str(uuid4()),
                deployment_id,
                json.dumps(payload, default=self._json_default),
            )

        return {
            "deployment_id": deployment_id,
            "deployed_at": row["deployed_at"].isoformat() if row and row["deployed_at"] else None,
            "db_name": db_name,
            "target_branch": target_branch,
            "ontology_commit_id": ontology_commit_id,
            "snapshot_rid": snapshot_rid,
            "proposal_id": str(proposal_id) if proposal_id else None,
            "status": status,
            "gate_policy": gate_policy,
            "health_summary": health_summary,
        }

    async def claim_outbox_batch(
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

            rows = await conn.fetch(
                f"""
                SELECT outbox_id, deployment_id, payload, status,
                       retry_count AS publish_attempts,
                       last_error AS error,
                       claimed_by, claimed_at, next_attempt_at, created_at, updated_at
                FROM ontology_deploy_outbox_v2
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
                """
                UPDATE ontology_deploy_outbox_v2
                SET status = 'publishing',
                    retry_count = retry_count + 1,
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
                    payload=self._maybe_decode_json(row["payload"]) or {},
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

    async def mark_outbox_published(self, *, outbox_id: str) -> None:
        await postgres_db.execute(
            """
            UPDATE ontology_deploy_outbox_v2
            SET status = 'published',
                updated_at = NOW(),
                next_attempt_at = NULL,
                last_error = NULL,
                claimed_by = NULL,
                claimed_at = NULL
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
        )

    async def mark_outbox_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        await postgres_db.execute(
            """
            UPDATE ontology_deploy_outbox_v2
            SET status = 'failed',
                last_error = $2,
                next_attempt_at = $3,
                updated_at = NOW()
            WHERE outbox_id = $1::uuid
            """,
            outbox_id,
            error,
            next_attempt_at,
        )

    async def purge_outbox(self, *, retention_days: int, limit: int = 10000) -> int:
        if retention_days <= 0:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        row = await postgres_db.fetchrow(
            """
            WITH deleted AS (
                DELETE FROM ontology_deploy_outbox_v2
                WHERE outbox_id IN (
                    SELECT outbox_id
                    FROM ontology_deploy_outbox_v2
                    WHERE status = 'published' AND updated_at < $1
                    ORDER BY updated_at ASC
                    LIMIT $2
                )
                RETURNING outbox_id
            )
            SELECT COUNT(*) AS count FROM deleted;
            """,
            cutoff,
            limit,
        )
        return int(row["count"] or 0) if row else 0
