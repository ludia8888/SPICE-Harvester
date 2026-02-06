from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

from oms.services.ontology_deploy_outbox_store import OntologyDeployOutboxItem
from oms.services.ontology_deployment_registry import OntologyDeploymentRegistry
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2


class _FakeOutboxStore:
    def __init__(self, *, claim_items: List[OntologyDeployOutboxItem] | None = None) -> None:
        self.claim_items = list(claim_items or [])
        self.claim_calls: List[Dict[str, Any]] = []
        self.published: List[str] = []
        self.failed: List[Dict[str, Any]] = []

    async def claim_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: str | None = None,
        claim_timeout_seconds: int = 300,
    ) -> List[OntologyDeployOutboxItem]:
        self.claim_calls.append(
            {
                "limit": limit,
                "claimed_by": claimed_by,
                "claim_timeout_seconds": claim_timeout_seconds,
            }
        )
        return list(self.claim_items)

    async def mark_published(self, *, outbox_id: str) -> None:
        self.published.append(outbox_id)

    async def mark_failed(self, *, outbox_id: str, error: str, next_attempt_at=None) -> None:
        self.failed.append({"outbox_id": outbox_id, "error": error, "next_attempt_at": next_attempt_at})


@pytest.mark.asyncio
async def test_registry_ensure_schema_builds_indexes_for_v1(monkeypatch) -> None:
    executed: List[str] = []

    async def _fake_execute(sql: str, *args):  # noqa: ANN001
        _ = args
        executed.append(sql)

    monkeypatch.setattr("oms.services.ontology_deployment_registry_base.postgres_db.execute", _fake_execute)
    registry = OntologyDeploymentRegistry()
    await registry.ensure_schema()

    assert any("CREATE TABLE IF NOT EXISTS ontology_deployments" in sql for sql in executed)
    assert any("CREATE TABLE IF NOT EXISTS ontology_deploy_outbox" in sql for sql in executed)
    assert any("idx_ontology_deployments_db" in sql for sql in executed)
    assert any("idx_ontology_deploy_outbox_status" in sql for sql in executed)


@pytest.mark.asyncio
async def test_registry_ensure_schema_builds_indexes_for_v2(monkeypatch) -> None:
    executed: List[str] = []

    async def _fake_execute(sql: str, *args):  # noqa: ANN001
        _ = args
        executed.append(sql)

    monkeypatch.setattr("oms.services.ontology_deployment_registry_base.postgres_db.execute", _fake_execute)
    registry = OntologyDeploymentRegistryV2()
    await registry.ensure_schema()

    assert any("CREATE TABLE IF NOT EXISTS ontology_deployments_v2" in sql for sql in executed)
    assert any("CREATE TABLE IF NOT EXISTS ontology_deploy_outbox_v2" in sql for sql in executed)
    assert any("idx_ontology_deployments_v2_db" in sql for sql in executed)
    assert any("idx_ontology_deploy_outbox_v2_status" in sql for sql in executed)


@pytest.mark.asyncio
async def test_registry_v2_claim_batch_normalizes_json_payload(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    fake_store = _FakeOutboxStore(
        claim_items=[
            OntologyDeployOutboxItem(
                outbox_id="o1",
                deployment_id="d1",
                payload='{"event_id":"e1","data":{"x":1}}',
                status="pending",
                publish_attempts=0,
                error=None,
                claimed_by=None,
                claimed_at=None,
                next_attempt_at=None,
                created_at=now,
                updated_at=now,
            )
        ]
    )
    registry = OntologyDeploymentRegistryV2()
    monkeypatch.setattr(registry, "OUTBOX_STORE", fake_store)

    batch = await registry.claim_outbox_batch(limit=1, claimed_by="worker-v2", claim_timeout_seconds=15)

    assert len(batch) == 1
    assert batch[0].payload["event_id"] == "e1"
    assert fake_store.claim_calls[0]["claimed_by"] == "worker-v2"


@pytest.mark.asyncio
async def test_registry_purge_outbox_uses_registry_table(monkeypatch) -> None:
    captured_sql: List[str] = []
    captured_args: List[Any] = []

    async def _fake_fetchrow(sql: str, *args):  # noqa: ANN001
        captured_sql.append(sql)
        captured_args.extend(args)
        return {"count": 3}

    monkeypatch.setattr("oms.services.ontology_deployment_registry_base.postgres_db.fetchrow", _fake_fetchrow)
    registry = OntologyDeploymentRegistryV2()
    deleted = await registry.purge_outbox(retention_days=7, limit=123)

    assert deleted == 3
    assert captured_args[1] == 123
    assert "DELETE FROM ontology_deploy_outbox_v2" in captured_sql[0]
