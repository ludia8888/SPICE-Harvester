"""
Ontology deployment registry (Postgres SSoT).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

from oms.database.postgres import db as postgres_db
from oms.services.ontology_deploy_outbox_store import (
    OntologyDeployOutboxStore,
    OntologyDeployOutboxTableSpec,
)
from oms.services.ontology_deployment_registry_base import BaseOntologyDeploymentRegistry
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)

_OUTBOX_STORE = OntologyDeployOutboxStore(
    table=OntologyDeployOutboxTableSpec(
        table_name="ontology_deploy_outbox",
        attempts_column="publish_attempts",
        error_column="error",
    )
)


class OntologyDeploymentRegistry(BaseOntologyDeploymentRegistry):
    """Record ontology deployments in Postgres."""

    DEPLOYMENT_TABLE = "ontology_deployments"
    OUTBOX_TABLE = "ontology_deploy_outbox"
    OUTBOX_STORE = _OUTBOX_STORE
    DEPLOYMENT_TABLE_DDL = """
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
    OUTBOX_TABLE_DDL = """
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
        return OntologyDeploymentRegistry.build_common_event_payload(
            deployment_id=deployment_id,
            target_branch=target_branch,
            ontology_commit_id=approved_ontology_commit_id,
            deployed_by=deployed_by,
            data=data,
            occurred_at=occurred_at,
        )

    @trace_db_operation("oms.deployment_registry.record_deployment")
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
