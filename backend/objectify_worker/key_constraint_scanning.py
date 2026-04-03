from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shared.services.core.sheet_import_service import FieldMapping

from objectify_worker.validation_codes import ObjectifyValidationCode as VC


async def scan_key_constraints(
    worker: Any,
    *,
    job: Any,
    options: Dict[str, Any],
    mappings: List[FieldMapping],
    relationship_meta: Dict[str, Dict[str, Any]],
    target_field_types: Dict[str, str],
    sources_by_target: Dict[str, List[str]],
    required_targets: set[str],
    pk_targets: List[str],
    pk_fields: List[str],
    unique_keys: List[List[str]],
    row_batch_size: int,
    max_rows: Optional[int],
) -> Tuple[int, List[Dict[str, Any]], List[int], Dict[str, Any]]:
    errors: List[Dict[str, Any]] = []
    error_row_indices: List[int] = []
    total_rows = 0
    pk_duplicate_count = 0
    pk_missing_count = 0
    unique_duplicate_count = 0
    pk_duplicate_samples: List[str] = []
    unique_duplicate_samples: List[str] = []
    seen_row_keys: set[str] = set()
    seen_unique: Dict[Tuple[str, ...], set[str]] = {
        tuple(keys): set() for keys in unique_keys if keys
    }

    key_targets = set(pk_targets)
    for keys in unique_keys:
        key_targets.update(keys)
    key_mappings = [m for m in mappings if m.target_field in key_targets]
    key_sources_by_target = {t: sources_by_target.get(t, []) for t in key_targets}
    key_mapping_sources = [m.source_field for m in key_mappings if m.source_field]
    key_field_types = {t: target_field_types.get(t) for t in key_targets if t in target_field_types}

    async for columns, rows, row_offset in worker._iter_dataset_batches(
        job=job,
        options=options,
        row_batch_size=row_batch_size,
        max_rows=max_rows,
    ):
        if not rows:
            continue
        total_rows += len(rows)
        batch = worker._build_instances_with_validation(
            columns=columns,
            rows=rows,
            row_offset=row_offset,
            mappings=key_mappings,
            relationship_mappings=[],
            relationship_meta=relationship_meta,
            target_field_types=key_field_types,
            mapping_sources=key_mapping_sources,
            sources_by_target=key_sources_by_target,
            required_targets=required_targets,
            pk_targets=pk_targets,
            pk_fields=pk_fields,
            field_constraints={},
            field_raw_types={},
            seen_row_keys=seen_row_keys,
        )
        batch_errors = batch.get("errors") or []
        for err in batch_errors:
            code = err.get("code") if isinstance(err, dict) else None
            if code == "PRIMARY_KEY_DUPLICATE":
                pk_duplicate_count += 1
                row_key = err.get("row_key") if isinstance(err, dict) else None
                if row_key and len(pk_duplicate_samples) < 5:
                    pk_duplicate_samples.append(str(row_key))
            if code == "PRIMARY_KEY_MISSING":
                pk_missing_count += 1
            if len(errors) < 200:
                errors.append(err)
        error_row_indices.extend(batch.get("error_row_indices") or [])

        instances = batch.get("instances") or []
        instance_row_indices = batch.get("instance_row_indices") or []
        for inst, row_idx in zip(instances, instance_row_indices):
            absolute_idx = int(row_offset) + int(row_idx)
            if not isinstance(inst, dict):
                continue
            for keys in unique_keys:
                if not keys:
                    continue
                key_value = worker._derive_unique_key(inst, keys)
                if not key_value:
                    continue
                key_tuple = tuple(keys)
                seen_set = seen_unique.setdefault(key_tuple, set())
                if key_value in seen_set:
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": VC.UNIQUE_KEY_DUPLICATE.value,
                            "key_fields": keys,
                            "key_value": key_value,
                            "message": "Duplicate unique key detected",
                        }
                    )
                    unique_duplicate_count += 1
                    if len(unique_duplicate_samples) < 5:
                        unique_duplicate_samples.append(key_value)
                    error_row_indices.append(absolute_idx)
                else:
                    seen_set.add(key_value)

    error_row_indices = sorted(set(error_row_indices))
    stats = {
        "input_rows": total_rows,
        "error_rows": len(error_row_indices),
        "error_count": len(errors),
        "pk_duplicates": pk_duplicate_count,
        "pk_missing": pk_missing_count,
        "unique_key_duplicates": unique_duplicate_count,
        "pk_duplicate_samples": pk_duplicate_samples,
        "unique_duplicate_samples": unique_duplicate_samples,
        "unique_keys_checked": len(unique_keys),
    }
    return total_rows, errors, error_row_indices, stats
