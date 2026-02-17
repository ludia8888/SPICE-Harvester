"""Build objectify link-index jobs from relationship specs.

This helper centralizes link-index enqueue payload construction so runtime
workers and E2E test harnesses use the same deterministic rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry


@dataclass(frozen=True)
class LinkIndexObjectifyJobBuildResult:
    existing_job_id: Optional[str]
    job: Optional[ObjectifyJob]
    skip_reason: Optional[str] = None


async def build_link_index_objectify_job(
    *,
    db_name: str,
    link_type_id: str,
    dataset_registry: DatasetRegistry,
    objectify_registry: ObjectifyRegistry,
    job_id: Optional[str] = None,
) -> LinkIndexObjectifyJobBuildResult:
    relationship = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
    if not relationship:
        return LinkIndexObjectifyJobBuildResult(
            existing_job_id=None,
            job=None,
            skip_reason="relationship_spec_missing",
        )

    dataset = await dataset_registry.get_dataset(dataset_id=relationship.dataset_id)
    if not dataset:
        return LinkIndexObjectifyJobBuildResult(
            existing_job_id=None,
            job=None,
            skip_reason="dataset_missing",
        )

    version = None
    if relationship.dataset_version_id:
        version = await dataset_registry.get_version(version_id=relationship.dataset_version_id)
        if version and getattr(version, "dataset_id", None) != relationship.dataset_id:
            version = None
    if not version:
        version = await dataset_registry.get_latest_version(dataset_id=relationship.dataset_id)
    if not version or not version.artifact_key:
        return LinkIndexObjectifyJobBuildResult(
            existing_job_id=None,
            job=None,
            skip_reason="dataset_version_or_artifact_missing",
        )

    mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=relationship.mapping_spec_id)
    if not mapping_spec:
        return LinkIndexObjectifyJobBuildResult(
            existing_job_id=None,
            job=None,
            skip_reason="mapping_spec_missing",
        )

    mapping_spec_version = int(relationship.mapping_spec_version or mapping_spec.version or 0)
    if mapping_spec_version <= 0:
        return LinkIndexObjectifyJobBuildResult(
            existing_job_id=None,
            job=None,
            skip_reason="mapping_spec_version_invalid",
        )

    dedupe_key = objectify_registry.build_dedupe_key(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        mapping_spec_id=relationship.mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        dataset_version_id=version.version_id,
        artifact_id=None,
        artifact_output_name=dataset.name,
    )
    existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
    if existing:
        return LinkIndexObjectifyJobBuildResult(
            existing_job_id=existing.job_id,
            job=None,
            skip_reason="dedupe_hit",
        )

    options = dict(mapping_spec.options or {})
    options.setdefault("mode", "link_index")
    options.setdefault("relationship_spec_id", relationship.relationship_spec_id)
    options.setdefault("link_type_id", link_type_id)

    resolved_job_id = str(job_id or uuid4())
    job = ObjectifyJob(
        job_id=resolved_job_id,
        db_name=db_name,
        dataset_id=dataset.dataset_id,
        dataset_version_id=version.version_id,
        artifact_output_name=dataset.name,
        dedupe_key=dedupe_key,
        dataset_branch=dataset.branch,
        artifact_key=version.artifact_key,
        mapping_spec_id=relationship.mapping_spec_id,
        mapping_spec_version=mapping_spec_version,
        target_class_id=mapping_spec.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options=options,
        execution_mode="full",
    )
    return LinkIndexObjectifyJobBuildResult(
        existing_job_id=None,
        job=job,
        skip_reason=None,
    )
