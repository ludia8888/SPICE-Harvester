"""Objectify job helper functions (BFF).

Centralizes objectify job enqueue logic shared across multiple routers.
This reduces duplication and provides a single source of truth (SSoT) for
dedupe-key semantics and job option shaping.
"""

from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import status

from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry


async def enqueue_objectify_job_for_mapping_spec(
    *,
    objectify_registry: ObjectifyRegistry,
    mapping_spec_id: str,
    mapping_spec_version: Optional[int] = None,
    dataset_registry: Optional[DatasetRegistry] = None,
    dataset_id: Optional[str] = None,
    dataset_version_id: Optional[str] = None,
    dataset: Any = None,
    version: Any = None,
    mapping_spec_record: Any = None,
    options_override: Optional[Dict[str, Any]] = None,
    options_defaults: Optional[Dict[str, Any]] = None,
    strict_dataset_match: bool = False,
) -> Optional[str]:
    if not mapping_spec_id:
        return None
    mapping_spec = mapping_spec_record or await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
    if not mapping_spec:
        return None

    if dataset is None:
        if not dataset_registry or not dataset_id:
            return None
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            return None

    if strict_dataset_match and getattr(mapping_spec, "dataset_id", None) != getattr(dataset, "dataset_id", None):
        raise classified_http_exception(
            status.HTTP_409_CONFLICT,
            "Mapping spec does not match dataset",
            code=ErrorCode.CONFLICT,
            extra={
                "reason": "MAPPING_SPEC_DATASET_MISMATCH",
                "mapping_spec_id": mapping_spec_id,
                "dataset_id": getattr(dataset, "dataset_id", None),
            },
        )

    if version is None:
        if not dataset_registry:
            return None
        if dataset_version_id:
            version = await dataset_registry.get_version(version_id=dataset_version_id)
            if not version or getattr(version, "dataset_id", None) != getattr(dataset, "dataset_id", None):
                return None
        if not version:
            version = await dataset_registry.get_latest_version(dataset_id=getattr(dataset, "dataset_id", ""))
    if not version or not getattr(version, "artifact_key", None):
        return None

    resolved_mapping_version = int(mapping_spec_version or getattr(mapping_spec, "version", 0) or 0)
    if not resolved_mapping_version:
        return None

    dedupe_key = objectify_registry.build_dedupe_key(
        dataset_id=getattr(dataset, "dataset_id", ""),
        dataset_branch=getattr(dataset, "branch", ""),
        mapping_spec_id=getattr(mapping_spec, "mapping_spec_id", mapping_spec_id),
        mapping_spec_version=resolved_mapping_version,
        dataset_version_id=getattr(version, "version_id", None),
        artifact_id=None,
        artifact_output_name=getattr(dataset, "name", None),
    )
    job_id = str(uuid4())
    options = dict(getattr(mapping_spec, "options", None) or {})
    if options_override:
        options.update(options_override)
    if options_defaults:
        for key, value in options_defaults.items():
            options.setdefault(key, value)

    job = ObjectifyJob(
        job_id=job_id,
        db_name=getattr(dataset, "db_name", ""),
        dataset_id=getattr(dataset, "dataset_id", ""),
        dataset_version_id=getattr(version, "version_id", None),
        artifact_output_name=getattr(dataset, "name", None),
        dedupe_key=dedupe_key,
        dataset_branch=getattr(dataset, "branch", ""),
        artifact_key=getattr(version, "artifact_key", "") or "",
        mapping_spec_id=getattr(mapping_spec, "mapping_spec_id", mapping_spec_id),
        mapping_spec_version=resolved_mapping_version,
        target_class_id=getattr(mapping_spec, "target_class_id", None),
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options=options,
    )
    record = await objectify_registry.enqueue_objectify_job(job=job)
    resolved_job_id = str(getattr(record, "job_id", "") or "").strip() or job_id
    return resolved_job_id
