"""
Admin Reindex Instances Service.

Dataset-primary rebuild: re-runs objectify for all active mapping specs
in a database to fully rebuild the ES instances index.

Unlike ontology projection rebuild (which replays S3 events), dataset-primary
instances are rebuilt by re-executing objectify jobs from source datasets.
This follows the Palantir Foundry principle that dataset artifacts are the
source of truth for instances.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from shared.models.objectify_job import ObjectifyJob
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


def _extract_spec_field(spec: Any, field: str) -> Any:
    if isinstance(spec, dict):
        return spec.get(field)
    return getattr(spec, field, None)


@trace_external_call("bff.admin_reindex.reindex_all_instances")
async def reindex_all_instances(
    *,
    db_name: str,
    branch: str = "main",
    objectify_registry: Any,
    dataset_registry: Any,
    job_queue: Any,
    delete_index_first: bool = False,
    elasticsearch_service: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Re-index all instances for a database by re-running objectify jobs.

    Algorithm:
    1. List all active mapping specs for the database
    2. For each mapping spec, find the latest dataset version
    3. Submit full-mode objectify jobs to the job queue
    4. Return a summary of submitted jobs

    Args:
        db_name: Database name
        branch: Branch (default: main)
        objectify_registry: ObjectifyRegistry for querying mapping specs
        dataset_registry: DatasetRegistry for querying dataset versions
        job_queue: ObjectifyJobQueue for submitting jobs
        delete_index_first: If True, delete the ES index before reindexing
        elasticsearch_service: Required if delete_index_first is True
    """
    task_id = str(uuid4())
    started_at = datetime.now(timezone.utc)

    # Optionally delete the ES index first (clean rebuild)
    if delete_index_first and elasticsearch_service:
        from shared.config.search_config import get_instances_index_name
        index_name = get_instances_index_name(db_name, branch=branch)
        try:
            if await elasticsearch_service.index_exists(index_name):
                await elasticsearch_service.delete_index(index_name)
                logger.info("Deleted index %s for clean rebuild", index_name)
        except Exception:
            logger.warning("Failed to delete index %s", index_name, exc_info=True)

    # List all active mapping specs
    try:
        mapping_specs = await objectify_registry.list_mapping_specs(
            include_inactive=False,
        )
    except Exception as exc:
        logger.error("Failed to list mapping specs: %s", exc)
        return {
            "task_id": task_id,
            "status": "failed",
            "reason": f"Failed to list mapping specs: {exc}",
        }

    if not mapping_specs:
        return {
            "task_id": task_id,
            "status": "no_mapping_specs",
            "message": f"No active mapping specs found for {db_name}/{branch}",
        }

    # Filter mapping specs for this database.
    # Legacy records may not carry db_name, so resolve via dataset registry.
    db_specs = []
    errors: List[Dict[str, Any]] = []
    for spec in mapping_specs:
        spec_db = _extract_spec_field(spec, "db_name")
        if spec_db and str(spec_db).strip() == db_name:
            db_specs.append(spec)
            continue
        if spec_db and str(spec_db).strip() != db_name:
            continue

        dataset_id = str(_extract_spec_field(spec, "dataset_id") or "").strip()
        if not dataset_id:
            errors.append(
                {
                    "mapping_spec_id": str(_extract_spec_field(spec, "mapping_spec_id") or ""),
                    "target_class_id": str(_extract_spec_field(spec, "target_class_id") or ""),
                    "error": "Missing dataset_id on mapping spec",
                }
            )
            continue
        try:
            dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        except Exception as exc:
            logger.warning("Failed to resolve dataset %s for mapping spec filter: %s", dataset_id, exc)
            errors.append(
                {
                    "mapping_spec_id": str(_extract_spec_field(spec, "mapping_spec_id") or ""),
                    "target_class_id": str(_extract_spec_field(spec, "target_class_id") or ""),
                    "error": f"Failed to resolve dataset metadata: {exc}",
                }
            )
            continue
        if dataset and str(dataset.db_name).strip() == db_name:
            db_specs.append(spec)

    submitted_jobs: List[Dict[str, Any]] = []

    for spec in db_specs:
        # Extract spec fields (handle both object and dict)
        if isinstance(spec, dict):
            spec_id = spec.get("mapping_spec_id", "")
            spec_version = spec.get("version", 1)
            target_class_id = spec.get("target_class_id", "")
            dataset_id = spec.get("dataset_id", "")
            dataset_branch = spec.get("dataset_branch", branch)
        else:
            spec_id = getattr(spec, "mapping_spec_id", "")
            spec_version = getattr(spec, "version", 1)
            target_class_id = getattr(spec, "target_class_id", "")
            dataset_id = getattr(spec, "dataset_id", "")
            dataset_branch = getattr(spec, "dataset_branch", branch)

        if not spec_id or not target_class_id or not dataset_id:
            continue

        # Get latest dataset version
        dataset_version_id = None
        try:
            versions = await dataset_registry.list_dataset_versions(
                dataset_id=dataset_id,
                branch=dataset_branch,
                limit=1,
            )
            if versions:
                v = versions[0]
                dataset_version_id = (
                    v.get("version_id") if isinstance(v, dict) else getattr(v, "version_id", None)
                )
        except Exception as exc:
            logger.warning("Failed to get dataset version for %s: %s", dataset_id, exc)

        if not dataset_version_id:
            errors.append({
                "mapping_spec_id": spec_id,
                "target_class_id": target_class_id,
                "error": "No dataset version found",
            })
            continue

        # Create and submit objectify job
        job_id = f"reindex-{task_id}-{target_class_id}"
        try:
            job = ObjectifyJob(
                job_id=job_id,
                db_name=db_name,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
                dataset_branch=dataset_branch,
                mapping_spec_id=spec_id,
                mapping_spec_version=spec_version,
                target_class_id=target_class_id,
                execution_mode="full",  # Always full for rebuild
                options={"reindex_task_id": task_id},
            )
            await job_queue.enqueue(job)
            submitted_jobs.append({
                "job_id": job_id,
                "target_class_id": target_class_id,
                "dataset_id": dataset_id,
                "mapping_spec_id": spec_id,
            })
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception fallback at bff/services/admin_reindex_instances_service.py:166", exc_info=True)
            errors.append({
                "mapping_spec_id": spec_id,
                "target_class_id": target_class_id,
                "error": str(exc),
            })

    elapsed_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)

    return {
        "task_id": task_id,
        "status": "submitted",
        "db_name": db_name,
        "branch": branch,
        "submitted_jobs": len(submitted_jobs),
        "errors": len(errors),
        "jobs": submitted_jobs,
        "error_details": errors[:20],
        "elapsed_ms": elapsed_ms,
    }
