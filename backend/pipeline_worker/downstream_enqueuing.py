from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.backing_source_adapter import is_oms_mapping_spec
from shared.utils.schema_hash import compute_schema_hash

from pipeline_worker.spark_schema_helpers import _schema_diff

logger = logging.getLogger(__name__)


async def maybe_enqueue_objectify_job(worker: Any, *, dataset: Any, version: Any) -> Optional[str]:
    self = worker
    if not self.objectify_registry:
        return None
    if not getattr(version, "artifact_key", None):
        return None
    resolved_output_name = getattr(dataset, "name", None)
    schema_hash = compute_schema_hash(version.sample_json.get("columns")) if isinstance(getattr(version, "sample_json", None), dict) else None
    if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
        schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])

    mapping_spec = None
    if self.mapping_resolver:
        try:
            mapping_spec = await self.mapping_resolver.resolve(
                db_name=dataset.db_name,
                dataset_id=dataset.dataset_id,
                branch=dataset.branch,
                schema_hash=schema_hash,
                artifact_output_name=resolved_output_name,
            )
        except Exception as resolver_exc:
            logger.warning(
                "MappingSpecResolver failed (dataset=%s), falling back to PostgreSQL: %s",
                dataset.dataset_id, resolver_exc,
            )
    if not mapping_spec:
        mapping_spec = await self.objectify_registry.get_active_mapping_spec(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            artifact_output_name=resolved_output_name,
            schema_hash=schema_hash,
        )
    if not mapping_spec or not mapping_spec.auto_sync:
        if self.dataset_registry and schema_hash:
            try:
                candidates = await self.objectify_registry.list_mapping_specs(dataset_id=dataset.dataset_id)
                mismatched = [
                    spec.schema_hash
                    for spec in candidates
                    if spec.artifact_output_name == resolved_output_name and spec.schema_hash != schema_hash
                ]
                if mismatched:
                    current_columns: List[Dict[str, Any]] = []
                    if isinstance(getattr(version, "sample_json", None), dict):
                        current_columns = version.sample_json.get("columns") or []
                    if not current_columns and isinstance(getattr(dataset, "schema_json", None), dict):
                        current_columns = dataset.schema_json.get("columns") or []

                    expected_columns: List[Dict[str, Any]] = []
                    latest_spec = None
                    if candidates:
                        latest_spec = max(candidates, key=lambda spec: spec.created_at or datetime.min)
                    if latest_spec and latest_spec.backing_datasource_version_id:
                        backing_version = await self.dataset_registry.get_backing_datasource_version(
                            version_id=latest_spec.backing_datasource_version_id
                        )
                        if backing_version:
                            expected_version = await self.dataset_registry.get_version(
                                version_id=backing_version.dataset_version_id
                            )
                            if expected_version and isinstance(expected_version.sample_json, dict):
                                expected_columns = expected_version.sample_json.get("columns") or []
                            if not expected_columns and expected_version and isinstance(expected_version.schema_json, dict):
                                expected_columns = expected_version.schema_json.get("columns") or []

                    schema_diff = (
                        _schema_diff(current_columns=current_columns, expected_columns=expected_columns)
                        if current_columns and expected_columns
                        else None
                    )
                    await self.dataset_registry.record_gate_result(
                        scope="objectify_schema",
                        subject_type="dataset_version",
                        subject_id=version.version_id,
                        status="FAIL",
                        details={
                            "dataset_id": dataset.dataset_id,
                            "dataset_version_id": version.version_id,
                            "observed_schema_hash": schema_hash,
                            "expected_schema_hashes": sorted(set(mismatched)),
                            "schema_diff": schema_diff,
                            "message": "Schema hash mismatch; migration required",
                        },
                    )
            except Exception as exc:
                logger.warning("Failed to record schema gate: %s", exc)
        return None
    oms_sourced = is_oms_mapping_spec(mapping_spec.mapping_spec_id) if mapping_spec.mapping_spec_id else False
    dedupe_spec_id = mapping_spec.target_class_id if oms_sourced else mapping_spec.mapping_spec_id
    dedupe_key = self.objectify_registry.build_dedupe_key(
        dataset_id=dataset.dataset_id,
        dataset_branch=dataset.branch,
        mapping_spec_id=dedupe_spec_id,
        mapping_spec_version=mapping_spec.version,
        dataset_version_id=version.version_id,
        artifact_id=None,
        artifact_output_name=resolved_output_name,
    )
    job_id = str(uuid4())
    options = dict(mapping_spec.options or {})
    pg_mapping_spec_id = None if oms_sourced else mapping_spec.mapping_spec_id
    pg_mapping_spec_version = None if oms_sourced else mapping_spec.version
    job = ObjectifyJob(
        job_id=job_id,
        db_name=dataset.db_name,
        dataset_id=dataset.dataset_id,
        dataset_version_id=version.version_id,
        artifact_output_name=resolved_output_name,
        dedupe_key=dedupe_key,
        dataset_branch=dataset.branch,
        artifact_key=version.artifact_key or "",
        mapping_spec_id=pg_mapping_spec_id,
        mapping_spec_version=pg_mapping_spec_version,
        target_class_id=mapping_spec.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options=options,
    )
    try:
        if self.objectify_job_queue:
            enqueue_result = await self.objectify_job_queue.publish(job, require_delivery=False)
            return enqueue_result.record.job_id
    except Exception as exc:
        logger.warning("Failed to enqueue objectify job %s: %s", job_id, exc)
    return None


async def maybe_enqueue_relationship_jobs(worker: Any, *, dataset: Any, version: Any) -> List[str]:
    self = worker
    if not self.objectify_registry or not self.dataset_registry:
        return []
    if not getattr(version, "artifact_key", None):
        return []
    try:
        specs = await self.dataset_registry.list_relationship_specs(
            dataset_id=dataset.dataset_id,
            status="ACTIVE",
        )
    except Exception as exc:
        logger.warning("Failed to load relationship specs: %s", exc)
        return []

    job_ids: List[str] = []
    schema_hash = compute_schema_hash(version.sample_json.get("columns")) if isinstance(getattr(version, "sample_json", None), dict) else None
    if not schema_hash and isinstance(getattr(dataset, "schema_json", None), dict):
        schema_hash = compute_schema_hash(dataset.schema_json.get("columns") or [])

    for spec in specs:
        if not spec.auto_sync:
            continue
        if spec.dataset_version_id and spec.dataset_version_id != version.version_id:
            continue
        mapping_spec = await self.objectify_registry.get_mapping_spec(mapping_spec_id=spec.mapping_spec_id)
        if not mapping_spec or not mapping_spec.auto_sync:
            continue
        if schema_hash and mapping_spec.schema_hash and mapping_spec.schema_hash != schema_hash:
            try:
                current_columns: List[Dict[str, Any]] = []
                if isinstance(getattr(version, "sample_json", None), dict):
                    current_columns = version.sample_json.get("columns") or []
                if not current_columns and isinstance(getattr(dataset, "schema_json", None), dict):
                    current_columns = dataset.schema_json.get("columns") or []

                expected_columns: List[Dict[str, Any]] = []
                if mapping_spec.backing_datasource_version_id:
                    backing_version = await self.dataset_registry.get_backing_datasource_version(
                        version_id=mapping_spec.backing_datasource_version_id
                    )
                    if backing_version:
                        expected_version = await self.dataset_registry.get_version(
                            version_id=backing_version.dataset_version_id
                        )
                        if expected_version and isinstance(expected_version.sample_json, dict):
                            expected_columns = expected_version.sample_json.get("columns") or []
                        if not expected_columns and expected_version and isinstance(expected_version.schema_json, dict):
                            expected_columns = expected_version.schema_json.get("columns") or []

                schema_diff = (
                    _schema_diff(current_columns=current_columns, expected_columns=expected_columns)
                    if current_columns and expected_columns
                    else None
                )
                await self.dataset_registry.record_gate_result(
                    scope="relationship_schema",
                    subject_type="dataset_version",
                    subject_id=version.version_id,
                    status="FAIL",
                    details={
                        "dataset_id": dataset.dataset_id,
                        "dataset_version_id": version.version_id,
                        "relationship_spec_id": spec.relationship_spec_id,
                        "observed_schema_hash": schema_hash,
                        "expected_schema_hash": mapping_spec.schema_hash,
                        "schema_diff": schema_diff,
                        "message": "Relationship mapping schema hash mismatch",
                    },
                )
            except Exception as exc:
                logger.warning("Failed to record relationship schema gate: %s", exc)
            continue
        dedupe_key = self.objectify_registry.build_dedupe_key(
            dataset_id=dataset.dataset_id,
            dataset_branch=dataset.branch,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=spec.mapping_spec_version,
            dataset_version_id=version.version_id,
            artifact_id=None,
            artifact_output_name=dataset.name,
        )
        job_id = str(uuid4())
        options = dict(mapping_spec.options or {})
        job = ObjectifyJob(
            job_id=job_id,
            db_name=dataset.db_name,
            dataset_id=dataset.dataset_id,
            dataset_version_id=version.version_id,
            artifact_output_name=dataset.name,
            dedupe_key=dedupe_key,
            dataset_branch=dataset.branch,
            artifact_key=version.artifact_key or "",
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=spec.mapping_spec_version,
            target_class_id=mapping_spec.target_class_id,
            ontology_branch=options.get("ontology_branch"),
            max_rows=options.get("max_rows"),
            batch_size=options.get("batch_size"),
            allow_partial=bool(options.get("allow_partial")),
            options=options,
        )
        try:
            if self.objectify_job_queue:
                enqueue_result = await self.objectify_job_queue.publish(job, require_delivery=False)
                job_ids.append(enqueue_result.record.job_id)
        except Exception as exc:
            logger.warning("Failed to enqueue relationship job %s: %s", job_id, exc)

    return job_ids
