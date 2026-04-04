from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from shared.services.registries import dataset_registry_ingest as _ingest_registry
from shared.services.registries import dataset_registry_publish as _publish_registry
from shared.services.registries.dataset_registry_models import (
    DatasetIngestOutboxItem,
    DatasetIngestRequestRecord,
    DatasetIngestTransactionRecord,
    DatasetRecord,
    DatasetVersionRecord,
)


class DatasetRegistryIngestMixin:
    async def get_ingest_request_by_key(
        self,
        *,
        idempotency_key: str,
    ) -> Optional[DatasetIngestRequestRecord]:
        return await _ingest_registry.get_ingest_request_by_key(self, idempotency_key=idempotency_key)

    async def get_ingest_request(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetIngestRequestRecord]:
        return await _ingest_registry.get_ingest_request(self, ingest_request_id=ingest_request_id)

    async def create_ingest_request(
        self,
        *,
        dataset_id: str,
        db_name: str,
        branch: str,
        idempotency_key: str,
        request_fingerprint: Optional[str],
        schema_json: Optional[Dict[str, Any]] = None,
        sample_json: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> tuple[DatasetIngestRequestRecord, bool]:
        return await _ingest_registry.create_ingest_request(
            self,
            dataset_id=dataset_id,
            db_name=db_name,
            branch=branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
        )

    async def get_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.get_ingest_transaction(self, ingest_request_id=ingest_request_id)

    async def get_ingest_transaction_by_id(
        self,
        *,
        transaction_id: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.get_ingest_transaction_by_id(self, transaction_id=transaction_id)

    async def list_ingest_transactions_for_dataset(
        self,
        *,
        dataset_id: str,
        branch: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[DatasetIngestTransactionRecord]:
        return await _ingest_registry.list_ingest_transactions_for_dataset(
            self,
            dataset_id=dataset_id,
            branch=branch,
            limit=limit,
            offset=offset,
        )

    async def create_ingest_transaction(
        self,
        *,
        ingest_request_id: str,
        status: str = "OPEN",
    ) -> DatasetIngestTransactionRecord:
        return await _ingest_registry.create_ingest_transaction(
            self,
            ingest_request_id=ingest_request_id,
            status=status,
        )

    async def mark_ingest_transaction_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.mark_ingest_transaction_committed(
            self,
            ingest_request_id=ingest_request_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )

    async def mark_ingest_transaction_aborted(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> Optional[DatasetIngestTransactionRecord]:
        return await _ingest_registry.mark_ingest_transaction_aborted(
            self,
            ingest_request_id=ingest_request_id,
            error=error,
        )

    async def mark_ingest_committed(
        self,
        *,
        ingest_request_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
    ) -> DatasetIngestRequestRecord:
        return await _ingest_registry.mark_ingest_committed(
            self,
            ingest_request_id=ingest_request_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
        )

    async def mark_ingest_failed(
        self,
        *,
        ingest_request_id: str,
        error: str,
    ) -> None:
        await _ingest_registry.mark_ingest_failed(self, ingest_request_id=ingest_request_id, error=error)

    async def update_ingest_request_payload(
        self,
        *,
        ingest_request_id: str,
        schema_json: Optional[Dict[str, Any]] = None,
        sample_json: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await _ingest_registry.update_ingest_request_payload(
            self,
            ingest_request_id=ingest_request_id,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
        )

    async def approve_ingest_schema(
        self,
        *,
        ingest_request_id: str,
        schema_json: Optional[Dict[str, Any]] = None,
        approved_by: Optional[str] = None,
    ) -> tuple[DatasetRecord, DatasetIngestRequestRecord]:
        return await _ingest_registry.approve_ingest_schema(
            self,
            ingest_request_id=ingest_request_id,
            schema_json=schema_json,
            approved_by=approved_by,
        )

    async def publish_ingest_request(
        self,
        *,
        ingest_request_id: str,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: Optional[str],
        row_count: Optional[int],
        sample_json: Optional[Dict[str, Any]],
        schema_json: Optional[Dict[str, Any]],
        apply_schema: bool = True,
        outbox_entries: Optional[List[Dict[str, Any]]] = None,
    ) -> DatasetVersionRecord:
        return await _publish_registry.publish_ingest_request(
            self,
            ingest_request_id=ingest_request_id,
            dataset_id=dataset_id,
            lakefs_commit_id=lakefs_commit_id,
            artifact_key=artifact_key,
            row_count=row_count,
            sample_json=sample_json,
            schema_json=schema_json,
            apply_schema=apply_schema,
            outbox_entries=outbox_entries,
        )

    async def claim_ingest_outbox_batch(
        self,
        *,
        limit: int = 50,
        claimed_by: Optional[str] = None,
        claim_timeout_seconds: int = 300,
    ) -> List[DatasetIngestOutboxItem]:
        return await _publish_registry.claim_ingest_outbox_batch(
            self,
            limit=limit,
            claimed_by=claimed_by,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    async def mark_ingest_outbox_published(self, *, outbox_id: str) -> None:
        await _publish_registry.mark_ingest_outbox_published(self, outbox_id=outbox_id)

    async def mark_ingest_outbox_failed(
        self,
        *,
        outbox_id: str,
        error: str,
        next_attempt_at: Optional[datetime] = None,
    ) -> None:
        await _publish_registry.mark_ingest_outbox_failed(
            self,
            outbox_id=outbox_id,
            error=error,
            next_attempt_at=next_attempt_at,
        )

    async def mark_ingest_outbox_dead(self, *, outbox_id: str, error: str) -> None:
        await _publish_registry.mark_ingest_outbox_dead(self, outbox_id=outbox_id, error=error)

    async def purge_ingest_outbox(
        self,
        *,
        retention_days: int = 7,
        limit: int = 10_000,
    ) -> int:
        return await _publish_registry.purge_ingest_outbox(
            self,
            retention_days=retention_days,
            limit=limit,
        )

    async def get_ingest_outbox_metrics(self) -> Dict[str, Any]:
        return await _publish_registry.get_ingest_outbox_metrics(self)

    async def reconcile_ingest_state(
        self,
        *,
        stale_after_seconds: int = 3600,
        limit: int = 200,
        use_lock: bool = True,
        lock_key: Optional[int] = None,
    ) -> Dict[str, int]:
        return await _ingest_registry.reconcile_ingest_state(
            self,
            stale_after_seconds=stale_after_seconds,
            limit=limit,
            use_lock=use_lock,
            lock_key=lock_key,
        )
