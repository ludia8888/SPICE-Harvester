from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from shared.models.objectify_job import ObjectifyJob
from shared.services.dataset_registry import DatasetRegistry
from shared.services.objectify_registry import ObjectifyJobRecord, ObjectifyRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.utils.env_utils import parse_int_env

logger = logging.getLogger(__name__)


def _match_output_name(output: Dict[str, Any], name: str) -> bool:
    target = (name or "").strip()
    if not target:
        return False
    for key in ("output_name", "dataset_name", "node_id"):
        candidate = str(output.get(key) or "").strip()
        if candidate and candidate == target:
            return True
    return False


async def _build_objectify_payload(
    *,
    job: ObjectifyJobRecord,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    pipeline_registry: PipelineRegistry,
) -> Dict[str, Any]:
    dataset = await dataset_registry.get_dataset(dataset_id=job.dataset_id)
    if not dataset:
        raise RuntimeError(f"dataset_not_found:{job.dataset_id}")

    mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=job.mapping_spec_id)
    if not mapping_spec:
        raise RuntimeError(f"mapping_spec_not_found:{job.mapping_spec_id}")

    artifact_key: Optional[str] = None
    resolved_output_name: Optional[str] = None

    if job.dataset_version_id:
        version = await dataset_registry.get_version(version_id=job.dataset_version_id)
        if not version or version.dataset_id != job.dataset_id:
            raise RuntimeError("dataset_version_mismatch")
        artifact_key = version.artifact_key
    else:
        artifact = await pipeline_registry.get_artifact(artifact_id=job.artifact_id or "")
        if not artifact:
            raise RuntimeError("artifact_not_found")
        outputs = artifact.outputs or []
        if not outputs:
            raise RuntimeError("artifact_outputs_missing")
        if not job.artifact_output_name:
            if len(outputs) == 1:
                output = outputs[0]
                resolved_output_name = (
                    str(output.get("output_name") or output.get("dataset_name") or output.get("node_id") or "").strip()
                    or None
                )
            if not resolved_output_name:
                raise RuntimeError("artifact_output_name_missing")
        matches = [
            out for out in outputs if _match_output_name(out, job.artifact_output_name or resolved_output_name or "")
        ]
        if not matches:
            raise RuntimeError("artifact_output_not_found")
        if len(matches) > 1:
            raise RuntimeError("artifact_output_ambiguous")
        selected = matches[0]
        resolved_output_name = job.artifact_output_name or resolved_output_name
        artifact_key = str(selected.get("artifact_commit_key") or selected.get("artifact_key") or "").strip() or None

    if not artifact_key:
        raise RuntimeError("artifact_key_missing")

    options = dict(mapping_spec.options or {})
    payload = ObjectifyJob(
        job_id=job.job_id,
        db_name=dataset.db_name,
        dataset_id=job.dataset_id,
        dataset_version_id=job.dataset_version_id,
        artifact_id=job.artifact_id,
        artifact_output_name=resolved_output_name or job.artifact_output_name,
        dataset_branch=job.dataset_branch,
        artifact_key=artifact_key,
        mapping_spec_id=job.mapping_spec_id,
        mapping_spec_version=job.mapping_spec_version,
        target_class_id=job.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options=options,
    )
    return payload.model_dump(mode="json")


async def reconcile_objectify_jobs(
    *,
    objectify_registry: ObjectifyRegistry,
    dataset_registry: DatasetRegistry,
    pipeline_registry: PipelineRegistry,
    stale_after_seconds: int = 600,
    enqueued_stale_seconds: Optional[int] = None,
    limit: int = 200,
    use_lock: bool = True,
    lock_key: Optional[int] = None,
) -> Dict[str, int]:
    if not objectify_registry:
        raise RuntimeError("ObjectifyRegistry not available")

    results = {"outbox_created": 0, "republished": 0, "skipped": 0, "errors": 0}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=max(60, int(stale_after_seconds)))
    enqueued_cutoff = None
    if enqueued_stale_seconds is not None:
        enqueued_cutoff = now - timedelta(seconds=max(60, int(enqueued_stale_seconds)))

    resolved_lock_key = lock_key
    if resolved_lock_key is None:
        try:
            resolved_lock_key = int(os.getenv("OBJECTIFY_RECONCILER_LOCK_KEY", "910215"))
        except ValueError:
            resolved_lock_key = 910215

    lock_conn = None
    try:
        if use_lock and resolved_lock_key is not None:
            lock_conn = await objectify_registry._pool.acquire()  # type: ignore[attr-defined]
            locked = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", resolved_lock_key)
            if not locked:
                results["skipped"] = 1
                return results

        jobs = await objectify_registry.list_objectify_jobs(
            statuses=["QUEUED", "ENQUEUE_REQUESTED"],
            older_than=cutoff,
            limit=limit,
        )
        if enqueued_cutoff is not None:
            jobs.extend(
                await objectify_registry.list_objectify_jobs(
                    statuses=["ENQUEUED"],
                    older_than=enqueued_cutoff,
                    limit=limit,
                )
            )

        for job in jobs:
            try:
                if job.status == "ENQUEUED":
                    has_pending = await objectify_registry.has_outbox_for_job(
                        job_id=job.job_id,
                        statuses=["pending", "publishing", "failed"],
                    )
                    if has_pending:
                        continue
                    payload = await _build_objectify_payload(
                        job=job,
                        dataset_registry=dataset_registry,
                        objectify_registry=objectify_registry,
                        pipeline_registry=pipeline_registry,
                    )
                    await objectify_registry.enqueue_outbox_for_job(job_id=job.job_id, payload=payload)
                    await objectify_registry.update_objectify_job_status(
                        job_id=job.job_id,
                        status="ENQUEUE_REQUESTED",
                    )
                    results["republished"] += 1
                    continue

                has_outbox = await objectify_registry.has_outbox_for_job(
                    job_id=job.job_id,
                    statuses=["pending", "publishing", "failed"],
                )
                if has_outbox:
                    continue

                payload = await _build_objectify_payload(
                    job=job,
                    dataset_registry=dataset_registry,
                    objectify_registry=objectify_registry,
                    pipeline_registry=pipeline_registry,
                )
                await objectify_registry.enqueue_outbox_for_job(job_id=job.job_id, payload=payload)
                await objectify_registry.update_objectify_job_status(
                    job_id=job.job_id,
                    status="ENQUEUE_REQUESTED",
                )
                results["outbox_created"] += 1
            except Exception as exc:
                results["errors"] += 1
                logger.warning("Objectify reconciler failed for job %s: %s", job.job_id, exc)
    finally:
        if lock_conn is not None and resolved_lock_key is not None:
            try:
                await lock_conn.execute("SELECT pg_advisory_unlock($1)", resolved_lock_key)
            except Exception:
                logger.warning("Failed to release objectify reconciler lock", exc_info=True)
            await objectify_registry._pool.release(lock_conn)  # type: ignore[attr-defined]

    return results


async def run_objectify_reconciler(
    *,
    objectify_registry: ObjectifyRegistry,
    dataset_registry: DatasetRegistry,
    pipeline_registry: PipelineRegistry,
    poll_interval_seconds: int = 60,
    stale_after_seconds: int = 600,
    enqueued_stale_seconds: Optional[int] = None,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    while not stop_event.is_set():
        try:
            await reconcile_objectify_jobs(
                objectify_registry=objectify_registry,
                dataset_registry=dataset_registry,
                pipeline_registry=pipeline_registry,
                stale_after_seconds=stale_after_seconds,
                enqueued_stale_seconds=enqueued_stale_seconds,
            )
        except Exception as exc:
            logger.warning("Objectify reconciler failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
        except asyncio.TimeoutError:
            continue
