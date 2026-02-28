"""Pipeline dataset Objectify helpers.

Small, stable helpers extracted from `bff.routers.pipeline_datasets_ops`.
"""


import logging
from typing import Optional
from uuid import uuid4

from shared.models.objectify_job import ObjectifyJob
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.utils.schema_hash import compute_schema_hash

logger = logging.getLogger(__name__)


async def _maybe_enqueue_objectify_job(
    *,
    dataset,
    version,
    objectify_registry: Optional[ObjectifyRegistry],
    job_queue: Optional[ObjectifyJobQueue],
    dataset_registry: Optional[DatasetRegistry],
    actor_user_id: Optional[str],
) -> Optional[str]:
    if not objectify_registry or not job_queue:
        return None
    if not getattr(version, "artifact_key", None):
        return None
    resolved_output_name = getattr(dataset, "name", None)
    schema_hash = (
        compute_schema_hash(version.sample_json.get("columns"))
        if isinstance(getattr(version, "sample_json", None), dict)
        else None
    )
    if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
        schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])
    mapping_spec = await objectify_registry.get_active_mapping_spec(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        artifact_output_name=resolved_output_name,
        schema_hash=schema_hash,
    )
    if not mapping_spec or not mapping_spec.auto_sync:
        if dataset_registry and objectify_registry and schema_hash:
            try:
                candidates = await objectify_registry.list_mapping_specs(dataset_id=dataset.dataset_id)
                mismatched = [
                    spec.schema_hash
                    for spec in candidates
                    if spec.artifact_output_name == resolved_output_name and spec.schema_hash != schema_hash
                ]
                if mismatched:
                    await dataset_registry.record_gate_result(
                        scope="objectify_schema",
                        subject_type="dataset_version",
                        subject_id=version.version_id,
                        status="FAIL",
                        details={
                            "dataset_id": dataset.dataset_id,
                            "dataset_version_id": version.version_id,
                            "observed_schema_hash": schema_hash,
                            "expected_schema_hashes": sorted(set(mismatched)),
                            "message": "Schema hash mismatch; migration required",
                        },
                    )
            except Exception as exc:
                logger.warning("Failed to record schema gate: %s", exc)
        return None

    # Auto-sync mapping specs are validated against a specific dataset version (impact_scope /
    # backing_datasource_version_id). When a new dataset version is published (same schema_hash),
    # ensure the backing datasource version is advanced so objectify can run on the latest data.
    options = dict(mapping_spec.options or {})
    try:
        if dataset_registry and mapping_spec.backing_datasource_id and version.version_id:
            backing_version = await dataset_registry.get_or_create_backing_datasource_version(
                backing_id=mapping_spec.backing_datasource_id,
                dataset_version_id=version.version_id,
                schema_hash=schema_hash or mapping_spec.schema_hash,
                metadata={"artifact_key": getattr(version, "artifact_key", None)},
            )
            if backing_version and mapping_spec.backing_datasource_version_id != backing_version.version_id:
                impact_scope = options.get("impact_scope") if isinstance(options.get("impact_scope"), dict) else {}
                impact_scope = dict(impact_scope or {})
                impact_scope["dataset_version_id"] = version.version_id
                impact_scope.setdefault("schema_hash", schema_hash or mapping_spec.schema_hash)
                impact_scope.setdefault("artifact_output_name", resolved_output_name)
                options["impact_scope"] = impact_scope
                await objectify_registry.update_mapping_spec(
                    mapping_spec_id=mapping_spec.mapping_spec_id,
                    backing_datasource_version_id=backing_version.version_id,
                    options=options,
                )
    except Exception as exc:
        logger.warning("Failed to advance mapping spec backing version (mapping_spec_id=%s): %s", mapping_spec.mapping_spec_id, exc)

    dedupe_key = objectify_registry.build_dedupe_key(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        mapping_spec_id=mapping_spec.mapping_spec_id,
        mapping_spec_version=mapping_spec.version,
        dataset_version_id=version.version_id,
        artifact_id=None,
        artifact_output_name=resolved_output_name,
    )
    existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
    if existing:
        return existing.job_id
    job_id = str(uuid4())
    job = ObjectifyJob(
        job_id=job_id,
        db_name=dataset.db_name,
        dataset_id=dataset.dataset_id,
        dataset_version_id=version.version_id,
        artifact_output_name=resolved_output_name,
        dedupe_key=dedupe_key,
        dataset_branch=dataset.branch,
        artifact_key=version.artifact_key or "",
        mapping_spec_id=mapping_spec.mapping_spec_id,
        mapping_spec_version=mapping_spec.version,
        target_class_id=mapping_spec.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        execution_mode="full",
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options={
            **options,
            "actor_user_id": actor_user_id,
        },
    )
    await job_queue.publish(job, require_delivery=False)
    return job_id
