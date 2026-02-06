"""
Shared Template Method for ontology deployment registries.
"""

from __future__ import annotations

from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from oms.database.postgres import db as postgres_db
from oms.services.ontology_deploy_outbox_store import OntologyDeployOutboxItem, OntologyDeployOutboxStore
from shared.config.app_config import AppConfig
from shared.utils.deterministic_ids import deterministic_uuid5_str

_DEPLOYMENT_INDEX_COLUMNS: Sequence[Tuple[str, str]] = (
    ("db", "db_name"),
    ("target_branch", "target_branch"),
    ("proposal", "proposal_id"),
    ("created_at", "deployed_at DESC"),
)
_OUTBOX_INDEX_COLUMNS: Sequence[Tuple[str, str]] = (
    ("status", "status, next_attempt_at, created_at"),
    ("claimed", "status, claimed_at"),
    ("deployment", "deployment_id, status, created_at"),
)


class BaseOntologyDeploymentRegistry(ABC):
    DEPLOYMENT_TABLE: str = ""
    OUTBOX_TABLE: str = ""
    DEPLOYMENT_TABLE_DDL: str = ""
    OUTBOX_TABLE_DDL: str = ""
    OUTBOX_STORE: Optional[OntologyDeployOutboxStore] = None

    async def ensure_schema(self) -> None:
        self._require_schema_configuration()
        await postgres_db.execute(self.DEPLOYMENT_TABLE_DDL)
        await postgres_db.execute(self.OUTBOX_TABLE_DDL)
        await self._ensure_indexes()

    async def _ensure_indexes(self) -> None:
        for suffix, columns in _DEPLOYMENT_INDEX_COLUMNS:
            await postgres_db.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.DEPLOYMENT_TABLE}_{suffix} "
                f"ON {self.DEPLOYMENT_TABLE}({columns});"
            )
        for suffix, columns in _OUTBOX_INDEX_COLUMNS:
            await postgres_db.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.OUTBOX_TABLE}_{suffix} "
                f"ON {self.OUTBOX_TABLE}({columns});"
            )

    @staticmethod
    def build_common_event_payload(
        *,
        deployment_id: str,
        target_branch: str,
        ontology_commit_id: str,
        deployed_by: str,
        data: Dict[str, Any],
        occurred_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        event_id = deterministic_uuid5_str(f"ontology-deploy:{deployment_id}")
        occurred_at = occurred_at or datetime.now(timezone.utc)
        metadata = {
            "kind": "domain",
            "kafka_topic": AppConfig.ONTOLOGY_EVENTS_TOPIC,
            "ontology": {
                "ref": f"branch:{target_branch}",
                "commit": ontology_commit_id,
            },
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

    async def claim_outbox_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[OntologyDeployOutboxItem]:
        store = self._require_outbox_store()
        batch = await store.claim_batch(
            limit=limit,
            claimed_by=claimed_by,
            claim_timeout_seconds=claim_timeout_seconds,
        )
        for item in batch:
            item.payload = self._normalize_claimed_payload(item.payload)
        return batch

    async def mark_outbox_published(self, *, outbox_id: str) -> None:
        store = self._require_outbox_store()
        await store.mark_published(outbox_id=outbox_id)

    async def mark_outbox_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        store = self._require_outbox_store()
        await store.mark_failed(outbox_id=outbox_id, error=error, next_attempt_at=next_attempt_at)

    async def purge_outbox(self, *, retention_days: int, limit: int = 10000) -> int:
        if retention_days <= 0:
            return 0
        self._require_schema_configuration()
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        row = await postgres_db.fetchrow(
            f"""
            WITH deleted AS (
                DELETE FROM {self.OUTBOX_TABLE}
                WHERE outbox_id IN (
                    SELECT outbox_id
                    FROM {self.OUTBOX_TABLE}
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

    def _normalize_claimed_payload(self, payload: Any) -> Dict[str, Any]:
        return payload if isinstance(payload, dict) else {}

    def _require_schema_configuration(self) -> None:
        if not self.DEPLOYMENT_TABLE or not self.OUTBOX_TABLE:
            raise RuntimeError(f"{self.__class__.__name__} schema table names are not configured")
        if not self.DEPLOYMENT_TABLE_DDL or not self.OUTBOX_TABLE_DDL:
            raise RuntimeError(f"{self.__class__.__name__} schema DDL is not configured")

    def _require_outbox_store(self) -> OntologyDeployOutboxStore:
        if self.OUTBOX_STORE is None:
            raise RuntimeError(f"{self.__class__.__name__} outbox store is not configured")
        return self.OUTBOX_STORE
