"""
Writeback Materializer Worker.

Builds `writeback_merged_snapshot` artifacts in lakeFS to bound queue growth and
provide a reproducible merged view for rebuilds (ACTION_WRITEBACK_DESIGN.md).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import signal
import time
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Tuple
from uuid import uuid4

from shared.config.app_config import AppConfig
from shared.config.settings import settings as app_settings
from shared.observability.logging import install_trace_context_filter
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.lakefs_client import LakeFSClient, LakeFSConflictError
from shared.services.lakefs_storage_service import LakeFSStorageService, create_lakefs_storage_service
from shared.services.storage_service import StorageService, create_storage_service
from shared.services.writeback_merge_service import WritebackMergeService
from shared.utils.canonical_json import CANONICAL_JSON_VERSION, sha256_canonical_json_prefixed
from shared.utils.writeback_paths import (
    queue_compaction_marker_key,
    ref_key,
    snapshot_latest_pointer_key,
    snapshot_manifest_key,
    snapshot_object_key,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - trace_id=%(trace_id)s span_id=%(span_id)s req_id=%(request_id)s corr_id=%(correlation_id)s db=%(db_name)s - %(message)s",
)
install_trace_context_filter()
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_queue_seq(filename: str) -> Optional[int]:
    name = str(filename or "").rsplit("/", 1)[-1]
    if not name.endswith(".json"):
        return None
    stem = name[:-5]
    head = stem.split("_", 1)[0]
    try:
        return int(head)
    except (TypeError, ValueError):
        return None


def _hash_inputs(keys: list[str]) -> str:
    # Stable digest over queue entry keys (best-effort surrogate for patchset commit merkle).
    hasher = hashlib.sha256()
    for key in sorted({str(k) for k in keys if str(k)}):
        hasher.update(key.encode("utf-8"))
        hasher.update(b"\n")
    return f"sha256:{hasher.hexdigest()}"


def _extract_base_dataset_version_id(payload: Dict[str, Any]) -> Optional[str]:
    meta = payload.get("_metadata") if isinstance(payload.get("_metadata"), dict) else {}
    for key in ("dataset_version_id", "backing_datasource_version_id", "base_dataset_version_id"):
        value = meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


class WritebackMaterializerWorker:
    def __init__(self) -> None:
        self.running = False
        self.tracing = get_tracing_service("writeback-materializer-worker")
        self.metrics = get_metrics_collector("writeback-materializer-worker")
        self.lakefs_client: Optional[LakeFSClient] = None
        self.lakefs_storage: Optional[LakeFSStorageService] = None
        self.base_storage: Optional[StorageService] = None

    async def initialize(self) -> None:
        self.lakefs_client = LakeFSClient()
        self.lakefs_storage = create_lakefs_storage_service(app_settings)
        if not self.lakefs_storage:
            raise RuntimeError("LakeFSStorageService unavailable (boto3 missing?)")
        self.base_storage = create_storage_service(app_settings)
        if not self.base_storage:
            raise RuntimeError("StorageService unavailable (boto3 missing?)")

    async def shutdown(self) -> None:
        self.running = False

    async def _ensure_branch(self, *, repository: str, branch: str) -> None:
        if not self.lakefs_client:
            raise RuntimeError("lakefs_client not initialized")
        try:
            await self.lakefs_client.create_branch(repository=repository, name=branch, source="main")
        except LakeFSConflictError:
            return

    async def _scan_queue(
        self,
        *,
        repository: str,
        branch: str,
    ) -> tuple[Set[Tuple[str, str, str]], int, list[str]]:
        if not self.lakefs_storage:
            raise RuntimeError("lakefs_storage not initialized")

        root_prefix = ref_key(branch, "writeback_edits_queue/queue/by_object/")
        objects: Set[Tuple[str, str, str]] = set()
        max_seq = -1
        keys: list[str] = []

        async for obj in self.lakefs_storage.iter_objects(bucket=repository, prefix=root_prefix):
            key = str(obj.get("Key") or "")
            if not key or not key.endswith(".json"):
                continue
            keys.append(key)

            rel = key[len(root_prefix) :] if key.startswith(root_prefix) else ""
            parts = [p for p in rel.split("/") if p]
            if len(parts) < 4:
                continue
            object_type, instance_id, lifecycle_id = parts[0], parts[1], parts[2]
            objects.add((object_type, instance_id, lifecycle_id))

            seq = _parse_queue_seq(parts[3])
            if seq is not None:
                max_seq = max(max_seq, int(seq))

        return objects, max_seq, keys

    async def materialize_db(self, *, db_name: str) -> None:
        if not self.lakefs_client or not self.lakefs_storage or not self.base_storage:
            raise RuntimeError("worker not initialized")

        start = time.monotonic()
        with self.tracing.span("writeback_materializer.materialize_db", attributes={"db.name": db_name}):
            await self._materialize_db_inner(db_name=db_name)
        try:
            self.metrics.record_event(
                "WRITEBACK_SNAPSHOT_MATERIALIZE",
                action="processed",
                duration=time.monotonic() - start,
            )
        except Exception:
            pass

    async def _materialize_db_inner(self, *, db_name: str) -> None:
        repo = AppConfig.ONTOLOGY_WRITEBACK_REPO
        branch = AppConfig.get_ontology_writeback_branch(db_name)
        base_branch = (os.getenv("WRITEBACK_MATERIALIZER_BASE_BRANCH") or "main").strip() or "main"

        await self._ensure_branch(repository=repo, branch=branch)

        objects, max_seq, queue_keys = await self._scan_queue(repository=repo, branch=branch)
        if not objects:
            logger.info("No queue entries found (db=%s); skipping snapshot", db_name)
            return

        snapshot_id = f"{_utcnow().strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
        staging_branch = AppConfig.sanitize_lakefs_branch_id(f"{branch}__snapshots__{snapshot_id}")

        try:
            await self.lakefs_client.create_branch(repository=repo, name=staging_branch, source=branch)
        except LakeFSConflictError:
            pass

        merger = WritebackMergeService(base_storage=self.base_storage, lakefs_storage=self.lakefs_storage)

        written = 0
        base_dataset_version_ids: Set[str] = set()
        ontology_commit_ids: Set[str] = set()
        for object_type, instance_id, lifecycle_id in sorted(objects):
            try:
                merged = await merger.merge_instance(
                    db_name=db_name,
                    base_branch=base_branch,
                    overlay_branch=branch,
                    class_id=object_type,
                    instance_id=instance_id,
                    writeback_repo=repo,
                    writeback_branch=branch,
                )
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning(
                    "Failed to merge object for snapshot (db=%s, type=%s, id=%s): %s",
                    db_name,
                    object_type,
                    instance_id,
                    e,
                )
                continue

            if merged.lifecycle_id != lifecycle_id:
                # Skip stale lifecycle entries; snapshot is for the active lineage.
                continue

            payload = merged.document.get("data")
            if not isinstance(payload, dict):
                continue

            base_version = _extract_base_dataset_version_id(payload)
            if base_version:
                base_dataset_version_ids.add(base_version)
            if merged.last_ontology_commit_id:
                ontology_commit_ids.add(merged.last_ontology_commit_id)

            obj_key = ref_key(
                staging_branch,
                snapshot_object_key(
                    snapshot_id=snapshot_id,
                    object_type=object_type,
                    instance_id=instance_id,
                    lifecycle_id=lifecycle_id,
                ),
            )
            await self.lakefs_storage.save_json(bucket=repo, key=obj_key, data=payload)
            written += 1

        if len(base_dataset_version_ids) == 1:
            manifest_base_dataset_version_id = next(iter(base_dataset_version_ids))
        elif len(base_dataset_version_ids) > 1:
            manifest_base_dataset_version_id = "mixed"
        else:
            manifest_base_dataset_version_id = "unknown"

        if len(ontology_commit_ids) == 1:
            manifest_ontology_commit_id = next(iter(ontology_commit_ids))
        elif len(ontology_commit_ids) > 1:
            manifest_ontology_commit_id = "mixed"
        else:
            manifest_ontology_commit_id = "unknown"

        manifest = {
            "snapshot_id": snapshot_id,
            "snapshot_revision": int(max_seq) if max_seq >= 0 else 0,
            "queue_high_watermark": int(max_seq) if max_seq >= 0 else 0,
            "created_at": _utcnow().isoformat(),
            "db_name": db_name,
            "base_branch": base_branch,
            "base_dataset_version_id": manifest_base_dataset_version_id,
            "ontology_commit_id": manifest_ontology_commit_id,
            "materializer_definition_hash": sha256_canonical_json_prefixed(
                {
                    "kind": "writeback_materializer",
                    "version": "v1",
                    "canonical_json_version": CANONICAL_JSON_VERSION,
                }
            ),
            "inputs_digest": _hash_inputs(queue_keys),
            "object_count": int(written),
        }

        await self.lakefs_storage.save_json(
            bucket=repo,
            key=ref_key(staging_branch, snapshot_manifest_key(snapshot_id)),
            data=manifest,
        )

        await self.lakefs_storage.save_json(
            bucket=repo,
            key=ref_key(staging_branch, snapshot_latest_pointer_key()),
            data={
                "snapshot_id": snapshot_id,
                "created_at": manifest["created_at"],
                "queue_high_watermark": manifest["queue_high_watermark"],
            },
        )

        # Optional compaction marker for faster scans.
        await self.lakefs_storage.save_json(
            bucket=repo,
            key=ref_key(staging_branch, queue_compaction_marker_key()),
            data={
                "snapshot_id": snapshot_id,
                "compacted_until": manifest["queue_high_watermark"],
                "created_at": manifest["created_at"],
            },
        )

        commit_id = await self.lakefs_client.commit(
            repository=repo,
            branch=staging_branch,
            message=f"Writeback merged snapshot {db_name} {snapshot_id}",
            metadata={"kind": "writeback_merged_snapshot", "db_name": db_name, "snapshot_id": snapshot_id},
        )
        await self.lakefs_client.merge(
            repository=repo,
            source_ref=staging_branch,
            destination_branch=branch,
            message=f"Merge writeback snapshot {db_name} {snapshot_id}",
            metadata={"kind": "writeback_merged_snapshot_merge", "db_name": db_name, "snapshot_id": snapshot_id},
            allow_empty=True,
        )
        with suppress(Exception):
            await self.lakefs_client.delete_branch(repository=repo, name=staging_branch)

        logger.info(
            "Materialized writeback snapshot db=%s snapshot_id=%s commit_id=%s objects=%s high_watermark=%s",
            db_name,
            snapshot_id,
            commit_id,
            written,
            manifest["queue_high_watermark"],
        )

    async def run(self) -> None:
        self.running = True
        interval = float(os.getenv("WRITEBACK_MATERIALIZER_INTERVAL_SECONDS", str(6 * 60 * 60)) or str(6 * 60 * 60))
        run_once = os.getenv("WRITEBACK_MATERIALIZER_RUN_ONCE", "").strip().lower() in {"1", "true", "yes", "on"}

        while self.running:
            raw = (os.getenv("WRITEBACK_MATERIALIZER_DB_NAMES") or "").strip()
            dbs = [p.strip() for p in raw.split(",") if p.strip()]
            if not dbs:
                logger.info("WRITEBACK_MATERIALIZER_DB_NAMES is empty; sleeping")
            for db_name in dbs:
                try:
                    await self.materialize_db(db_name=db_name)
                except Exception as e:
                    logger.error("Writeback materialize failed (db=%s): %s", db_name, e, exc_info=True)

            if run_once:
                break
            await asyncio.sleep(interval)


async def main() -> None:
    worker = WritebackMaterializerWorker()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    try:
        await worker.initialize()
        task = asyncio.create_task(worker.run())
        await stop_event.wait()
        worker.running = False
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
