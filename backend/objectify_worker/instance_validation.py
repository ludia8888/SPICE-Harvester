from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shared.services.core.sheet_import_service import FieldMapping, SheetImportService

from objectify_worker.validation_codes import ObjectifyValidationCode as VC

logger = logging.getLogger(__name__)


def build_instances_with_validation(
    worker: Any,
    *,
    columns: List[Any],
    rows: List[List[Any]],
    row_offset: int,
    mappings: List[FieldMapping],
    relationship_mappings: List[FieldMapping],
    relationship_meta: Dict[str, Dict[str, Any]],
    target_field_types: Dict[str, str],
    mapping_sources: List[str],
    sources_by_target: Dict[str, List[str]],
    required_targets: set[str],
    pk_targets: List[str],
    pk_fields: List[str],
    field_constraints: Dict[str, Any],
    field_raw_types: Dict[str, Optional[Any]],
    seen_row_keys: Optional[set[str]] = None,
) -> Dict[str, Any]:
    col_index = SheetImportService.build_column_index(columns)
    missing_sources = sorted({s for s in mapping_sources if s and s not in col_index})
    if missing_sources:
        return {
            "instances": [],
            "instance_row_indices": [],
            "errors": [
                {
                    "code": VC.SOURCE_FIELD_MISSING.value,
                    "missing_sources": missing_sources,
                    "message": "Mapping source fields missing from dataset schema",
                }
            ],
            "error_row_indices": [],
            "row_keys": [],
            "fatal": True,
        }

    build = SheetImportService.build_instances(
        columns=columns,
        rows=rows,
        mappings=mappings,
        target_field_types=target_field_types,
    )
    raw_errors = build.get("errors") or []
    errors: List[Dict[str, Any]] = []
    error_row_indices: set[int] = set()
    for err in raw_errors:
        if isinstance(err, dict):
            adjusted = dict(err)
        else:
            adjusted = {"message": str(err)}
        row_index = adjusted.get("row_index")
        if row_index is not None:
            try:
                row_index = int(row_index) + int(row_offset)
            except (TypeError, ValueError):
                row_index = None
            adjusted["row_index"] = row_index
        errors.append(adjusted)
        if row_index is not None:
            error_row_indices.add(row_index)

    for idx in build.get("error_row_indices") or []:
        try:
            error_row_indices.add(int(row_offset) + int(idx))
        except (TypeError, ValueError) as exc:
            logger.warning("Invalid error_row_index value=%r offset=%r: %s", idx, row_offset, exc, exc_info=True)
            continue

    instances = build.get("instances") or []
    instance_row_indices = build.get("instance_row_indices") or []

    if relationship_mappings:
        for inst, row_idx in zip(instances, instance_row_indices):
            if not isinstance(inst, dict):
                continue
            if row_idx >= len(rows):
                continue
            row = rows[row_idx]
            for mapping in relationship_mappings:
                source_name = mapping.source_field
                target_name = mapping.target_field
                idx = col_index.get(source_name)
                if idx is None or idx >= len(row):
                    continue
                raw = row[idx]
                if worker._is_blank(raw):
                    continue
                rel_meta = relationship_meta.get(target_name) or {}
                target_class = str(rel_meta.get("target") or rel_meta.get("linkTarget") or "").strip()
                if not target_class:
                    errors.append(
                        {
                            "row_index": int(row_offset) + int(row_idx),
                            "source_field": source_name,
                            "target_field": target_name,
                            "code": VC.RELATIONSHIP_TARGET_MISSING.value,
                            "message": "Relationship target class missing from ontology",
                        }
                    )
                    error_row_indices.add(int(row_offset) + int(row_idx))
                    continue
                try:
                    ref = worker._normalize_relationship_ref(raw, target_class=target_class)
                except ValueError as exc:
                    errors.append(
                        {
                            "row_index": int(row_offset) + int(row_idx),
                            "source_field": source_name,
                            "target_field": target_name,
                            "raw_value": raw,
                            "code": VC.RELATIONSHIP_REF_INVALID.value,
                            "message": str(exc),
                        }
                    )
                    error_row_indices.add(int(row_offset) + int(row_idx))
                    continue
                inst[target_name] = ref

    mapping_sources_set = {s for s in mapping_sources if s}
    required_union = set(required_targets) | set(pk_targets)
    for row_idx, row in enumerate(rows):
        if not mapping_sources_set:
            break
        row_has_value = False
        for source in mapping_sources_set:
            idx = col_index.get(source)
            if idx is None or idx >= len(row):
                continue
            if not worker._is_blank(row[idx]):
                row_has_value = True
                break
        if not row_has_value:
            continue
        absolute_idx = int(row_offset) + int(row_idx)
        for target in required_union:
            sources = sources_by_target.get(target) or []
            if not sources:
                continue
            present = False
            for source in sources:
                idx = col_index.get(source)
                if idx is None or idx >= len(row):
                    continue
                if not worker._is_blank(row[idx]):
                    present = True
                    break
            if not present:
                code = VC.PRIMARY_KEY_MISSING.value if target in pk_targets else "REQUIRED_FIELD_MISSING"
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "target_field": target,
                        "code": code,
                        "message": f"Missing required field '{target}'",
                    }
                )
                error_row_indices.add(absolute_idx)

    for inst, row_idx in zip(instances, instance_row_indices):
        absolute_idx = int(row_offset) + int(row_idx)
        if not isinstance(inst, dict):
            continue
        for field, value in inst.items():
            constraints = field_constraints.get(field) or {}
            if not constraints:
                continue
            raw_type = field_raw_types.get(field)
            message = worker._validate_value_constraints(value, constraints=constraints, raw_type=raw_type)
            if message:
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "target_field": field,
                        "code": VC.VALUE_CONSTRAINT_FAILED.value,
                        "message": message,
                    }
                )
                error_row_indices.add(absolute_idx)

    row_keys: List[Optional[str]] = []
    for inst, row_idx in zip(instances, instance_row_indices):
        row = rows[row_idx] if row_idx < len(rows) else None
        row_key = worker._derive_row_key(
            columns=columns,
            col_index=col_index,
            row=row,
            instance=inst,
            pk_fields=pk_fields,
            pk_targets=pk_targets,
        )
        row_keys.append(row_key)

    if seen_row_keys is not None:
        for row_key, row_idx in zip(row_keys, instance_row_indices):
            absolute_idx = int(row_offset) + int(row_idx)
            if not row_key:
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "code": VC.PRIMARY_KEY_MISSING.value,
                        "message": "Row key cannot be derived from primary key values",
                    }
                )
                error_row_indices.add(absolute_idx)
                continue
            if row_key in seen_row_keys:
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "code": VC.PRIMARY_KEY_DUPLICATE.value,
                        "row_key": row_key,
                        "message": "Duplicate primary key detected in job batch",
                    }
                )
                error_row_indices.add(absolute_idx)
                continue
            seen_row_keys.add(row_key)

    return {
        "instances": instances,
        "instance_row_indices": instance_row_indices,
        "errors": errors,
        "error_row_indices": sorted(error_row_indices),
        "row_keys": row_keys,
        "fatal": False,
    }
