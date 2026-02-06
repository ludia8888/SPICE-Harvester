"""
Ontology deployment registry (Postgres SSoT).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from oms.database.postgres import db as postgres_db
from oms.services.ontology_deploy_outbox_store import (
    OntologyDeployOutboxItem,
    OntologyDeployOutboxStore,
    OntologyDeployOutboxTableSpec,
)
from shared.config.app_config import AppConfig
from shared.utils.deterministic_ids import deterministic_uuid5_str

logger = logging.getLogger(__name__)

_OUTBOX_STORE = OntologyDeployOutboxStore(
    table=OntologyDeployOutboxTableSpec(
        table_name="ontology_deploy_outbox",
        attempts_column="publish_attempts",
        error_column="error",
    )
)


class OntologyDeploymentRegistry:
    """Record ontology deployments in Postgres."""

    async def ensure_schema(self) -> None:
        await postgres_db.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_deployments (
                deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                db_name VARCHAR(255) NOT NULL,
                proposal_id UUID NOT NULL,
                source_branch VARCHAR(255) NOT NULL,
                target_branch VARCHAR(255) NOT NULL,
                approved_ontology_commit_id VARCHAR(255) NOT NULL,
                merge_commit_id VARCHAR(255) NOT NULL,
                definition_hash VARCHAR(255),
                status VARCHAR(50) NOT NULL DEFAULT 'succeeded'
                    CHECK (status IN ('succeeded', 'failed')),
                deployed_by VARCHAR(255) DEFAULT 'system',
                deployed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                metadata JSONB
            );
            """
        )
        await postgres_db.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_deploy_outbox (
                outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                deployment_id UUID NOT NULL REFERENCES ontology_deployments(deployment_id) ON DELETE CASCADE,
                payload JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                publish_attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                claimed_by TEXT,
                claimed_at TIMESTAMPTZ,
                next_attempt_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_db ON ontology_deployments(db_name);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_target_branch ON ontology_deployments(target_branch);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_proposal ON ontology_deployments(proposal_id);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deployments_created_at ON ontology_deployments(deployed_at DESC);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_status ON ontology_deploy_outbox(status, next_attempt_at, created_at);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_claimed ON ontology_deploy_outbox(status, claimed_at);"
        )
        await postgres_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_deployment ON ontology_deploy_outbox(deployment_id, status, created_at);"
        )

    @staticmethod
    def build_deploy_event_payload(
        *,
        deployment_id: str,
        db_name: str,
        proposal_id: str,
        source_branch: str,
        target_branch: str,
        approved_ontology_commit_id: str,
        merge_commit_id: str,
        deployed_by: str,
        definition_hash: Optional[str],
        occurred_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        event_id = deterministic_uuid5_str(f"ontology-deploy:{deployment_id}")
        occurred_at = occurred_at or datetime.now(timezone.utc)
        metadata = {
            "kind": "domain",
            "kafka_topic": AppConfig.ONTOLOGY_EVENTS_TOPIC,
            "ontology": {
                "ref": f"branch:{target_branch}",
                "commit": approved_ontology_commit_id,
            },
        }
        data = {
            "deployment_id": deployment_id,
            "db_name": db_name,
            "proposal_id": proposal_id,
            "source_branch": source_branch,
            "target_branch": target_branch,
            "approved_ontology_commit_id": approved_ontology_commit_id,
            "merge_commit_id": merge_commit_id,
            "definition_hash": definition_hash,
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
        proposal_id: str,
        source_branch: str,
        target_branch: str,
        approved_ontology_commit_id: str,
        merge_commit_id: str,
        deployed_by: str,
        definition_hash: Optional[str] = None,
        status: str = "succeeded",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        await self.ensure_schema()
        async with postgres_db.transaction() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO ontology_deployments (
                    db_name,
                    proposal_id,
                    source_branch,
                    target_branch,
                    approved_ontology_commit_id,
                    merge_commit_id,
                    definition_hash,
                    status,
                    deployed_by,
                    metadata
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                RETURNING deployment_id, deployed_at;
                """,
                db_name,
                proposal_id,
                source_branch,
                target_branch,
                approved_ontology_commit_id,
                merge_commit_id,
                definition_hash,
                status,
                deployed_by,
                metadata,
            )
            deployment_id = str(row["deployment_id"])
            payload = self.build_deploy_event_payload(
                deployment_id=deployment_id,
                db_name=db_name,
                proposal_id=proposal_id,
                source_branch=source_branch,
                target_branch=target_branch,
                approved_ontology_commit_id=approved_ontology_commit_id,
                merge_commit_id=merge_commit_id,
                deployed_by=deployed_by,
                definition_hash=definition_hash,
                occurred_at=row["deployed_at"],
            )
            await conn.execute(
                """
                INSERT INTO ontology_deploy_outbox (
                    outbox_id, deployment_id, payload, status
                ) VALUES ($1::uuid, $2::uuid, $3::jsonb, 'pending')
                """,
                str(uuid4()),
                deployment_id,
                payload,
            )

        return {
            "deployment_id": deployment_id,
            "deployed_at": row["deployed_at"].isoformat() if row and row["deployed_at"] else None,
        }

    async def claim_outbox_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[OntologyDeployOutboxItem]:
        return await _OUTBOX_STORE.claim_batch(
            limit=limit,
            claimed_by=claimed_by,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    async def mark_outbox_published(self, *, outbox_id: str) -> None:
        await _OUTBOX_STORE.mark_published(outbox_id=outbox_id)

    async def mark_outbox_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        await _OUTBOX_STORE.mark_failed(outbox_id=outbox_id, error=error, next_attempt_at=next_attempt_at)

    async def purge_outbox(self, *, retention_days: int, limit: int = 10000) -> int:
        if retention_days <= 0:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        row = await postgres_db.fetchrow(
            """
            WITH deleted AS (
                DELETE FROM ontology_deploy_outbox
                WHERE outbox_id IN (
                    SELECT outbox_id
                    FROM ontology_deploy_outbox
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
