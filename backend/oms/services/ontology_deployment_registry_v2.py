"""
Ontology deployment registry v2 (Postgres SSoT).
"""

from __future__ import annotations

import json
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
from shared.utils.json_utils import json_default, maybe_decode_json

logger = logging.getLogger(__name__)

_OUTBOX_STORE = OntologyDeployOutboxStore(
    table=OntologyDeployOutboxTableSpec(
        table_name="ontology_deploy_outbox_v2",
        attempts_column="retry_count",
        error_column="last_error",
    )
)


class OntologyDeploymentRegistryV2(BaseOntologyDeploymentRegistry):
    """Record ontology deployments in Postgres (v2 schema)."""

    DEPLOYMENT_TABLE = "ontology_deployments_v2"
    OUTBOX_TABLE = "ontology_deploy_outbox_v2"
    OUTBOX_STORE = _OUTBOX_STORE
    DEPLOYMENT_TABLE_DDL = """
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
    OUTBOX_TABLE_DDL = """
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
        return OntologyDeploymentRegistryV2.build_common_event_payload(
            deployment_id=deployment_id,
            target_branch=target_branch,
            ontology_commit_id=ontology_commit_id,
            deployed_by=deployed_by,
            data=data,
            occurred_at=occurred_at,
        )

    @trace_db_operation("oms.deployment_registry_v2.record_deployment")
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
                json.dumps(payload, default=json_default),
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

    def _normalize_claimed_payload(self, payload: Any) -> Dict[str, Any]:
        decoded = maybe_decode_json(payload)
        return decoded if isinstance(decoded, dict) else {}

    @trace_db_operation("oms.deployment_registry_v2.get_latest_deployed_commit")
    async def get_latest_deployed_commit(
        self,
        *,
        db_name: str,
        target_branch: str = "main",
    ) -> Optional[Dict[str, Any]]:
        """
        Return the latest succeeded deployment record for a db/branch.

        Actions must execute against a deployed ontology commit (not a floating branch head).
        """
        await self.ensure_schema()
        row = await postgres_db.fetchrow(
            """
            SELECT deployment_id, db_name, target_branch, ontology_commit_id, snapshot_rid,
                   status, deployed_by, deployed_at, gate_policy, health_summary, error, metadata
            FROM ontology_deployments_v2
            WHERE db_name = $1
              AND target_branch = $2
              AND status = 'succeeded'
            ORDER BY deployed_at DESC
            LIMIT 1
            """,
            db_name,
            target_branch,
        )
        if not row:
            return None
        return {
            "deployment_id": str(row["deployment_id"]),
            "db_name": row["db_name"],
            "target_branch": row["target_branch"],
            "ontology_commit_id": row["ontology_commit_id"],
            "snapshot_rid": row["snapshot_rid"],
            "status": row["status"],
            "deployed_by": row["deployed_by"],
            "deployed_at": row["deployed_at"].isoformat() if row["deployed_at"] else None,
            "gate_policy": maybe_decode_json(row["gate_policy"]),
            "health_summary": maybe_decode_json(row["health_summary"]),
            "error": row["error"],
            "metadata": maybe_decode_json(row["metadata"]),
        }
