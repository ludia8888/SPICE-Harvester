"""
Action Outbox Worker (reconciler).

Ensures Action writeback state machine progresses despite partial failures:
PENDING -> COMMIT_WRITTEN -> EVENT_EMITTED -> SUCCEEDED

This worker scans Postgres Action logs for outbox candidates and retries:
- Emit ActionApplied for COMMIT_WRITTEN logs
- Append writeback queue entries and mark SUCCEEDED for EVENT_EMITTED logs

It is intentionally idempotent and safe to run continuously.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Any, Dict, Optional

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.models.events import ActionAppliedEvent
from shared.observability.context_propagation import attach_context_from_carrier, carrier_from_envelope_metadata
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.registries.action_log_registry import ActionLogRecord, ActionLogRegistry, ActionLogStatus
from shared.services.storage.event_store import event_store
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service, LakeFSStorageService
from shared.services.registries.processed_event_registry import (
    ClaimDecision,
    ProcessedEventRegistry,
)
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.utils.action_writeback import action_applied_event_id, is_noop_changes, safe_str
from shared.utils.resource_rid import strip_rid_revision
from shared.utils.worker_runner import run_worker_until_stopped
from shared.utils.writeback_paths import queue_entry_key, ref_key, writeback_patchset_key
from shared.utils.app_logger import configure_logging

_LOG_LEVEL = get_settings().observability.log_level
configure_logging(_LOG_LEVEL)
logger = logging.getLogger(__name__)

def _resolve_overlay_branch(log: ActionLogRecord) -> str:
    # Prefer explicit submission metadata; fall back to writeback branch.
    meta = log.metadata if isinstance(log.metadata, dict) else {}
    submission = meta.get("__writeback_submission") if isinstance(meta.get("__writeback_submission"), dict) else {}
    overlay = str(submission.get("overlay_branch") or "").strip()
    if overlay:
        return overlay
    target = log.writeback_target if isinstance(log.writeback_target, dict) else {}
    branch = str(target.get("branch") or "").strip()
    if branch:
        return branch
    return AppConfig.get_ontology_writeback_branch(log.db_name)


class ActionOutboxWorker:
    def __init__(self) -> None:
        self.running = False
        self.tracing = get_tracing_service("action-outbox-worker")
        self.metrics = get_metrics_collector("action-outbox-worker")
        self.action_logs = ActionLogRegistry()
        self.processed_event_registry: Optional[ProcessedEventRegistry] = None
        self.lakefs_client: Optional[LakeFSClient] = None
        self.lakefs_storage: Optional[LakeFSStorageService] = None

    async def initialize(self) -> None:
        await event_store.connect()
        await self.action_logs.connect()
        self.processed_event_registry = await create_processed_event_registry()

        self.lakefs_client = LakeFSClient()
        settings = get_settings()
        self.lakefs_storage = create_lakefs_storage_service(settings)
        if not self.lakefs_storage:
            raise RuntimeError("LakeFSStorageService unavailable (boto3 missing?)")

    async def shutdown(self) -> None:
        self.running = False
        if self.processed_event_registry:
            await self.processed_event_registry.close()
        await self.action_logs.close()

    async def _get_event_seq(self, *, event_id: str) -> Optional[int]:
        key = await event_store.get_event_object_key(event_id=event_id)
        if not key:
            return None
        env = await event_store.read_event_by_key(key=key)
        return int(env.sequence_number) if env.sequence_number is not None else None

    async def _emit_action_applied(self, *, log: ActionLogRecord) -> tuple[str, int]:
        if not log.writeback_commit_id:
            raise RuntimeError("COMMIT_WRITTEN log missing writeback_commit_id")

        action_log_id = str(log.action_log_id)
        db_name = log.db_name
        patchset_commit_id = str(log.writeback_commit_id)
        writeback_target = log.writeback_target or {
            "repo": AppConfig.ONTOLOGY_WRITEBACK_REPO,
            "branch": AppConfig.get_ontology_writeback_branch(db_name),
        }
        overlay_branch = _resolve_overlay_branch(log)

        meta = log.metadata if isinstance(log.metadata, dict) else {}
        submission = meta.get("__writeback_submission") if isinstance(meta.get("__writeback_submission"), dict) else {}
        base_branch = str(submission.get("base_branch") or "main").strip() or "main"
        ontology_commit_id = str(log.ontology_commit_id or "").strip()

        evt = ActionAppliedEvent(
            event_id=action_applied_event_id(action_log_id),
            db_name=db_name,
            action_log_id=action_log_id,
            patchset_commit_id=patchset_commit_id,
            writeback_target=writeback_target,
            overlay_branch=overlay_branch,
            data={
                "db_name": db_name,
                "action_log_id": action_log_id,
                "patchset_commit_id": patchset_commit_id,
                "writeback_target": writeback_target,
                "overlay_branch": overlay_branch,
            },
            metadata={
                "correlation_id": log.correlation_id,
                "ontology": {"ref": f"branch:{base_branch}", "commit": ontology_commit_id} if ontology_commit_id else None,
            },
            occurred_at=log.submitted_at,
            occurred_by=log.submitted_by,
        )

        action_env = EventEnvelope.from_base_event(
            evt,
            kafka_topic=AppConfig.ACTION_EVENTS_TOPIC,
            metadata={"service": "action_outbox_worker", "mode": "action_writeback_outbox"},
        )
        await event_store.append_event(action_env)

        seq = action_env.sequence_number
        if seq is None:
            seq = await self._get_event_seq(event_id=str(action_env.event_id))
        if seq is None:
            raise RuntimeError("ActionApplied event persisted without sequence_number")
        return str(action_env.event_id), int(seq)

    async def _ensure_branch(self, *, repository: str, branch: str) -> None:
        if not self.lakefs_client:
            raise RuntimeError("lakefs_client not initialized")
        try:
            await self.lakefs_client.create_branch(repository=repository, name=branch, source="main")
        except LakeFSConflictError:
            return

    async def _append_queue_entries(
        self,
        *,
        log: ActionLogRecord,
    ) -> None:
        if not self.lakefs_client or not self.lakefs_storage:
            raise RuntimeError("lakefs services not initialized")
        if not log.writeback_commit_id:
            raise RuntimeError("writeback_commit_id is required for queue append")
        if log.action_applied_seq is None:
            raise RuntimeError("action_applied_seq is required for queue append")

        writeback_target = log.writeback_target or {
            "repo": AppConfig.ONTOLOGY_WRITEBACK_REPO,
            "branch": AppConfig.get_ontology_writeback_branch(log.db_name),
        }
        repo = str(writeback_target.get("repo") or AppConfig.ONTOLOGY_WRITEBACK_REPO).strip()
        branch = str(writeback_target.get("branch") or AppConfig.get_ontology_writeback_branch(log.db_name)).strip()
        if not repo or not branch:
            raise RuntimeError("writeback_target.repo and writeback_target.branch are required")

        await self._ensure_branch(repository=repo, branch=branch)

        action_log_id = str(log.action_log_id)
        patchset_commit_id = str(log.writeback_commit_id)
        action_applied_seq = int(log.action_applied_seq)

        staging_branch = AppConfig.sanitize_lakefs_branch_id(f"{branch}__queue__{action_applied_seq}_{action_log_id}")
        try:
            await self.lakefs_client.create_branch(repository=repo, name=staging_branch, source=branch)
        except LakeFSConflictError:
            pass

        patchset = await self.lakefs_storage.load_json(
            bucket=repo,
            key=ref_key(patchset_commit_id, writeback_patchset_key(action_log_id)),
        )
        targets = patchset.get("targets") if isinstance(patchset, dict) else None
        if not isinstance(targets, list):
            targets = []

        submitted_at = None
        meta = patchset.get("metadata") if isinstance(patchset, dict) else None
        if isinstance(meta, dict):
            submitted_at = meta.get("submitted_at")

        entries: list[tuple[str, Dict[str, Any]]] = []
        for t in targets:
            if not isinstance(t, dict):
                continue
            applied = t.get("applied_changes") if isinstance(t.get("applied_changes"), dict) else None
            changes = applied if isinstance(applied, dict) else (t.get("changes") if isinstance(t.get("changes"), dict) else {})
            if isinstance(applied, dict) and is_noop_changes(changes):
                continue

            resource_rid = safe_str(t.get("resource_rid"))
            instance_id = safe_str(t.get("instance_id"))
            lifecycle_id = safe_str(t.get("lifecycle_id") or "lc-0") or "lc-0"
            if not resource_rid or not instance_id:
                logger.warning(
                    "Skipping writeback target with missing identifiers (action_log_id=%s resource_rid=%s instance_id=%s)",
                    action_log_id,
                    resource_rid or None,
                    instance_id or None,
                )
                continue
            base_token = t.get("base_token") if isinstance(t.get("base_token"), dict) else {}
            object_type = strip_rid_revision(resource_rid) or "object"

            payload = {
                "action_log_id": action_log_id,
                "patchset_commit_id": patchset_commit_id,
                "action_applied_seq": int(action_applied_seq),
                "resource_rid": resource_rid,
                "instance_id": instance_id,
                "lifecycle_id": lifecycle_id,
                "base_token": base_token,
                "submitted_at": submitted_at,
            }
            key = ref_key(
                staging_branch,
                queue_entry_key(
                    object_type=object_type,
                    instance_id=instance_id,
                    lifecycle_id=lifecycle_id,
                    action_applied_seq=int(action_applied_seq),
                    action_log_id=action_log_id,
                ),
            )
            entries.append((key, payload))

        # Nothing to write (e.g., conflict_policy=BASE_WINS skipped all targets).
        # Avoid failing reconciliation on empty commits; cleanup staging branch.
        if not entries:
            with suppress(Exception):
                await self.lakefs_client.delete_branch(repository=repo, name=staging_branch)
            return

        for key, payload in entries:
            await self.lakefs_storage.save_json(bucket=repo, key=key, data=payload)

        try:
            await self.lakefs_client.commit(
                repository=repo,
                branch=staging_branch,
                message=f"Writeback queue entries {action_applied_seq}_{action_log_id}",
                metadata={"kind": "writeback_queue_entries", "action_log_id": action_log_id},
            )
        except LakeFSError as exc:
            if "predicate failed" not in str(exc).lower():
                raise

        await self.lakefs_client.merge(
            repository=repo,
            source_ref=staging_branch,
            destination_branch=branch,
            message=f"Merge writeback queue entries {action_applied_seq}_{action_log_id}",
            metadata={"kind": "writeback_queue_entries_merge", "action_log_id": action_log_id},
            allow_empty=True,
        )
        with suppress(Exception):
            await self.lakefs_client.delete_branch(repository=repo, name=staging_branch)

    async def _reconcile_log(self, log: ActionLogRecord) -> None:
        action_log_id = str(log.action_log_id)

        if log.status == ActionLogStatus.COMMIT_WRITTEN.value:
            if log.action_applied_event_id and log.action_applied_seq is not None:
                event_id = str(log.action_applied_event_id)
                seq = int(log.action_applied_seq)
            else:
                event_id, seq = await self._emit_action_applied(log=log)

            await self.action_logs.mark_event_emitted(
                action_log_id=action_log_id,
                action_applied_event_id=event_id,
                action_applied_seq=seq,
            )
            log = await self.action_logs.get_log(action_log_id=action_log_id) or log

        if log.status == ActionLogStatus.EVENT_EMITTED.value:
            if log.action_applied_seq is None:
                event_id = str(log.action_applied_event_id or action_applied_event_id(action_log_id))
                seq = await self._get_event_seq(event_id=event_id)
                if seq is None:
                    raise RuntimeError("EVENT_EMITTED log missing action_applied_seq and event not found")
                log = await self.action_logs.mark_event_emitted(
                    action_log_id=action_log_id,
                    action_applied_event_id=str(event_id),
                    action_applied_seq=int(seq),
                ) or log

            await self._append_queue_entries(log=log)
            await self.action_logs.mark_succeeded(
                action_log_id=action_log_id,
                result={
                    "writeback_commit_id": log.writeback_commit_id,
                    "action_applied_event_id": log.action_applied_event_id,
                    "action_applied_seq": log.action_applied_seq,
                    "reconciled_by": "action_outbox_worker",
                },
            )

    async def run(self) -> None:
        self.running = True
        cfg = get_settings().workers.action_outbox
        poll_seconds = float(cfg.poll_seconds)
        batch = int(cfg.batch_size)

        while self.running:
            try:
                candidates = await self.action_logs.list_outbox_candidates(limit=batch)
            except Exception as e:
                logger.error("Failed to list outbox candidates: %s", e, exc_info=True)
                await asyncio.sleep(poll_seconds)
                continue

            if not candidates:
                await asyncio.sleep(poll_seconds)
                continue

            for log in candidates:
                if not isinstance(log, ActionLogRecord):
                    continue
                if log.status not in {ActionLogStatus.COMMIT_WRITTEN.value, ActionLogStatus.EVENT_EMITTED.value}:
                    continue

                claim = None
                if self.processed_event_registry:
                    claim = await self.processed_event_registry.claim(
                        handler="action_outbox_worker",
                        event_id=str(log.action_log_id),
                        aggregate_id=f"writeback:{log.db_name}",
                        sequence_number=int(log.action_applied_seq) if log.action_applied_seq is not None else None,
                    )
                    if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                        continue
                    if claim.decision == ClaimDecision.IN_PROGRESS:
                        continue

                try:
                    carrier = carrier_from_envelope_metadata(log.metadata if isinstance(log.metadata, dict) else {})
                    start = time.monotonic()
                    with attach_context_from_carrier(carrier or None, service_name="action-outbox-worker"):
                        with self.tracing.span(
                            "action_outbox.reconcile",
                            attributes={
                                "action_log_id": str(log.action_log_id),
                                "db.name": str(log.db_name),
                                "action.status": str(log.status),
                            },
                        ):
                            await self._reconcile_log(log)
                    try:
                        self.metrics.record_event("ACTION_LOG_RECONCILE", action="processed", duration=time.monotonic() - start)
                    except Exception:
                        pass
                    if self.processed_event_registry:
                        await self.processed_event_registry.mark_done(
                            handler="action_outbox_worker",
                            event_id=str(log.action_log_id),
                            aggregate_id=f"writeback:{log.db_name}",
                            sequence_number=int(log.action_applied_seq) if log.action_applied_seq is not None else None,
                        )
                except Exception as e:
                    logger.warning("Outbox reconcile failed action_log_id=%s: %s", log.action_log_id, e, exc_info=True)
                    if self.processed_event_registry:
                        with suppress(Exception):
                            await self.processed_event_registry.mark_failed(
                                handler="action_outbox_worker",
                                event_id=str(log.action_log_id),
                                error=str(e),
                            )

            await asyncio.sleep(poll_seconds)


async def main() -> None:
    await run_worker_until_stopped(
        ActionOutboxWorker(),
        task_name="action-outbox-worker.run",
    )


if __name__ == "__main__":
    asyncio.run(main())
