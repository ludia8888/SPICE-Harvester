from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from objectify_worker.validation_codes import ObjectifyValidationCode as VC

logger = logging.getLogger(__name__)


async def process_lakefs_delta_rows(
    worker: Any,
    *,
    job: Any,
    lakefs_delta_result: Any,
    delta_computer: Any,
    allow_partial: bool,
    errors: List[Dict[str, Any]],
    batch_size: int,
    rel_map: Dict[str, Any],
    ontology_version: Optional[dict[str, Any]],
    target_field_types: Dict[str, Any],
    property_mappings: List[Any],
    relationship_mappings: List[Any],
    mapping_sources: Dict[str, Any],
    sources_by_target: Dict[str, list[str]],
    required_targets: Set[str],
    pk_targets: List[str],
    pk_fields: List[str],
    field_constraints: Dict[str, Any],
    field_raw_types: Dict[str, str],
    seen_row_keys: Set[str],
    stable_seed: Optional[str],
    command_ids: List[str],
    indexed_instance_ids: Set[str],
    instance_ids_sample: List[str],
    fail_job: Any,
    extract_instance_relationships: Any,
) -> Dict[str, Any]:
    delta_rows_all = list(lakefs_delta_result.added_rows or []) + list(lakefs_delta_result.modified_rows or [])
    effective_deleted_keys: List[str] = list(lakefs_delta_result.deleted_keys or [])
    prepared_instances = 0
    total_rows_seen = 0

    if effective_deleted_keys and delta_computer and delta_rows_all:
        try:
            new_row_keys = {
                delta_computer.compute_row_key(row)
                for row in delta_rows_all
                if isinstance(row, dict)
            }
            effective_deleted_keys = [key for key in effective_deleted_keys if key not in new_row_keys]
        except Exception:
            logger.warning(
                "Failed to compute delta row keys for deletion filtering; skipping deletions for safety"
            )
            effective_deleted_keys = []

    if delta_rows_all:
        delta_columns = list(delta_rows_all[0].keys()) if delta_rows_all else []
        delta_rows_as_lists = [[row.get(column) for column in delta_columns] for row in delta_rows_all]
        total_rows_seen = len(delta_rows_as_lists)

        batch = worker._build_instances_with_validation(
            columns=delta_columns,
            rows=delta_rows_as_lists,
            row_offset=0,
            mappings=property_mappings,
            relationship_mappings=relationship_mappings,
            relationship_meta=rel_map,
            target_field_types=target_field_types,
            mapping_sources=mapping_sources,
            sources_by_target=sources_by_target,
            required_targets=required_targets,
            pk_targets=pk_targets,
            pk_fields=pk_fields,
            field_constraints=field_constraints,
            field_raw_types=field_raw_types,
            seen_row_keys=seen_row_keys,
        )
        batch_errors = batch.get("errors") or []
        if batch_errors:
            if worker._has_p0_errors(batch_errors):
                await fail_job("validation_failed", report={"errors": batch_errors[:200]})
            if allow_partial:
                remaining = max(0, 200 - len(errors))
                if remaining:
                    errors.extend(batch_errors[:remaining])
            else:
                await fail_job("validation_failed", report={"errors": batch_errors[:200]})

        instances = batch.get("instances") or []
        row_keys = batch.get("row_keys") or []
        if any(not key for key in row_keys):
            await fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": VC.PRIMARY_KEY_MISSING.value,
                            "message": "Row key cannot be derived from primary key values",
                        }
                    ]
                },
            )

        instance_id_field = None
        if pk_targets:
            for target in pk_targets:
                if target in target_field_types:
                    instance_id_field = target
                    break
        if not instance_id_field:
            for candidate in (f"{job.target_class_id.lower()}_id", "id"):
                if candidate in target_field_types:
                    instance_id_field = candidate
                    break

        instances, instance_ids = worker._ensure_instance_ids(
            instances,
            class_id=job.target_class_id,
            stable_seed=stable_seed or job.job_id,
            mapping_spec_version=job.mapping_spec_version,
            row_keys=row_keys,
            instance_id_field=instance_id_field,
        )

        if instances:
            for idx in range(0, len(instances), batch_size):
                instance_batch = instances[idx : idx + batch_size]
                batch_rels: Dict[str, Dict[str, Any]] = {}
                if rel_map:
                    for inst in instance_batch:
                        if not isinstance(inst, dict):
                            continue
                        instance_id = str(inst.get("instance_id") or "").strip()
                        if instance_id:
                            batch_rels[instance_id] = extract_instance_relationships(inst, rel_map=rel_map)
                write_result = await worker.instance_write_path.write_instances(
                    job=job,
                    instances=instance_batch,
                    ontology_version=ontology_version,
                    objectify_pk_fields=pk_targets,
                    objectify_instance_id_field=instance_id_field,
                    instance_relationships=batch_rels if batch_rels else None,
                    target_field_types=target_field_types,
                )
                if write_result.command_ids:
                    command_ids.extend([str(value) for value in write_result.command_ids if str(value).strip()])
                if write_result.indexed_instance_ids:
                    indexed_instance_ids.update(
                        str(value).strip() for value in write_result.indexed_instance_ids if str(value).strip()
                    )

            prepared_instances += len(instances)
            if len(instance_ids_sample) < 10:
                instance_ids_sample.extend(instance_ids[: max(0, 10 - len(instance_ids_sample))])

    if effective_deleted_keys:
        logger.info(
            "Deferred %d LakeFS delta deletes until authoritative objectify commit",
            len(effective_deleted_keys),
        )

    return {
        "prepared_instances": prepared_instances,
        "total_rows_seen": total_rows_seen,
        "effective_deleted_keys": effective_deleted_keys,
    }
