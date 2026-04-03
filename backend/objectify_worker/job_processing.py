from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.models.objectify_job import ObjectifyJob
from shared.services.core.sheet_import_service import FieldMapping
from shared.services.pipeline.objectify_delta_utils import (
    DeltaResult,
    ObjectifyDeltaComputer,
    create_delta_computer_for_mapping_spec,
)
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.key_spec import normalize_key_spec

from objectify_worker import delta_processing as _delta_processing
from objectify_worker.validation_codes import ObjectifyValidationCode as VC
from objectify_worker.write_paths import WRITE_PATH_MODE_DATASET_PRIMARY_INDEX

logger = logging.getLogger(__name__)


async def process_job(
    worker: Any,
    *,
    job: ObjectifyJob,
    fail_exception_cls: type[Exception],
    compute_lakefs_delta: Any,
    extract_instance_relationships: Any,
    auto_detect_watermark_column: Any,
) -> None:
    self = worker
    ObjectifyNonRetryableError = fail_exception_cls
    _compute_lakefs_delta = compute_lakefs_delta
    _extract_instance_relationships = extract_instance_relationships
    _auto_detect_watermark_column = auto_detect_watermark_column

    if not self.objectify_registry or not self.dataset_registry:
        raise RuntimeError("ObjectifyRegistry not initialized")
    if not self.instance_write_path:
        raise RuntimeError("Objectify write path not initialized")

    async def _fail_job(error: str, *, report: Optional[Dict[str, Any]] = None) -> None:
        if report:
            errs = report.get("errors")
            if errs:
                logger.error(
                    "Objectify validation errors (job_id=%s, first 5): %s",
                    job.job_id, errs[:5],
                )
            stats = report.get("stats")
            if stats:
                logger.error("Objectify validation stats (job_id=%s): %s", job.job_id, stats)
        report_payload = self._build_error_report(error=error, report=report, job=job)
        if error.startswith("validation_failed"):
            await self._record_gate_result(
                job=job,
                status="FAIL",
                details=report_payload,
            )
        await self.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="FAILED",
            error=error[:4000],
            report=report_payload,
            completed_at=datetime.now(timezone.utc),
        )
        raise ObjectifyNonRetryableError(error)

    record = await self.objectify_registry.get_objectify_job(job_id=job.job_id)
    if record and record.status in {"SUBMITTED", "COMPLETED"}:
        logger.info("Objectify job already submitted (job_id=%s); skipping", job.job_id)
        return

    input_type, resolved_output_name, stable_seed = await self._resolve_job_input_context(
        job=job,
        fail_job=_fail_job,
    )
    mapping_spec = await self._resolve_mapping_spec_for_job(
        job=job,
        fail_job=_fail_job,
    )

    await self.objectify_registry.update_objectify_job_status(
        job_id=job.job_id,
        status="RUNNING",
    )

    options: Dict[str, Any] = dict(mapping_spec.options or {})
    if isinstance(job.options, dict):
        options.update(job.options)
    link_index_mode = str(options.get("mode") or options.get("job_type") or "").strip().lower() == "link_index"
    if link_index_mode:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_UNSUPPORTED_TYPE.value,
                        "message": (
                            "Link-index mode is no longer supported in objectify worker. "
                            "Use relationship-aware ingestion or dedicated link indexing pipeline."
                        ),
                    }
                ]
            },
        )

    max_rows = job.max_rows if job.max_rows is not None else options.get("max_rows")
    if max_rows is None:
        max_rows = self.max_rows_default
    try:
        max_rows = int(max_rows)
    except (TypeError, ValueError):
        max_rows = self.max_rows_default
    if max_rows <= 0:
        max_rows = None

    batch_size = int(job.batch_size or options.get("batch_size") or self.batch_size_default)
    batch_size = max(1, min(batch_size, 5000))
    row_batch_size = int(options.get("row_batch_size") or options.get("read_batch_size") or self.row_batch_size_default)
    row_batch_size = max(1, min(row_batch_size, 50000))
    allow_partial = bool(job.allow_partial)

    mappings = [
        FieldMapping(source_field=str(m.get("source_field") or ""), target_field=str(m.get("target_field") or ""))
        for m in mapping_spec.mappings
        if isinstance(m, dict)
    ]
    mapping_sources: List[str] = []
    mapping_targets: List[str] = []
    for m in mappings:
        if m.source_field:
            mapping_sources.append(m.source_field)
        if m.target_field:
            mapping_targets.append(m.target_field)
    mapping_sources = sorted({s for s in mapping_sources if s})
    mapping_targets = sorted({t for t in mapping_targets if t})
    sources_by_target = self._map_mappings_by_target(mappings)

    ontology_payload = await self._fetch_class_schema(job)
    prop_map, rel_map = self._extract_ontology_fields(ontology_payload)
    if not prop_map:
        await _fail_job("validation_failed", report={"errors": [{"code": VC.ONTOLOGY_SCHEMA_MISSING.value}]})

    unknown_targets = [t for t in mapping_targets if t not in prop_map and t not in rel_map]
    if unknown_targets:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_TARGET_UNKNOWN.value,
                        "targets": unknown_targets,
                        "message": "Mapping targets missing from ontology schema",
                    }
                ]
            },
        )
    relationship_targets = [t for t in mapping_targets if t in rel_map]
    relationship_meta_override = options.get("relationship_meta") if isinstance(options.get("relationship_meta"), dict) else {}
    if link_index_mode and relationship_meta_override:
        relationship_targets.extend([t for t in mapping_targets if t in relationship_meta_override])
    relationship_targets = sorted(set(relationship_targets))
    if not link_index_mode:
        unsupported_relationships: List[str] = []
        for target in relationship_targets:
            rel = rel_map.get(target) or {}
            cardinality = str(rel.get("cardinality") or "").strip().lower()
            if cardinality.endswith(":n") or cardinality.endswith(":m"):
                unsupported_relationships.append(target)
        if unsupported_relationships:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": VC.MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED.value,
                            "targets": unsupported_relationships,
                            "message": "Relationship cardinality requires join-table mapping",
                        }
                    ]
                },
            )
    elif not relationship_targets:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_RELATIONSHIP_REQUIRED.value,
                        "message": "link_index requires relationship targets",
                    }
                ]
            },
        )

    property_mappings = [m for m in mappings if m.target_field in prop_map]
    relationship_target_set = set(relationship_targets)
    relationship_mappings = [m for m in mappings if m.target_field in relationship_target_set]

    if link_index_mode:
        await self._run_link_index_job(
            job=job,
            mapping_spec=mapping_spec,
            options=options,
            mappings=mappings,
            mapping_sources=mapping_sources,
            mapping_targets=mapping_targets,
            sources_by_target=sources_by_target,
            prop_map=prop_map,
            rel_map=rel_map,
            relationship_mappings=relationship_mappings,
            stable_seed=stable_seed or job.job_id,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        )
        return

    value_type_defs = await self._load_value_type_defs_with_validation(
        job=job,
        prop_map=prop_map,
        fail_job=_fail_job,
    )
    (
        resolved_field_types,
        field_constraints,
        field_raw_types,
        required_targets,
        explicit_pk_targets,
        unsupported_targets,
    ) = self._build_property_type_context(
        prop_map=prop_map,
        value_type_defs=value_type_defs,
        target_class_id=job.target_class_id,
    )

    warnings: List[Dict[str, Any]] = []
    (
        object_type_pk_targets,
        object_type_title_targets,
        object_type_unique_keys,
        object_type_required_fields,
        object_type_nullable_fields,
    ) = await self._resolve_object_type_key_contract(
        job=job,
        ontology_payload=ontology_payload,
        prop_map=prop_map,
        fail_job=_fail_job,
        warnings=warnings,
    )
    property_targets = [t for t in mapping_targets if t in prop_map]
    unsupported_mapped = [t for t in property_targets if t in unsupported_targets or t not in resolved_field_types]
    if unsupported_mapped:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_UNSUPPORTED_TYPE.value,
                        "targets": unsupported_mapped,
                        "message": "Mapping targets include unsupported property types",
                    }
                ]
            },
        )

    pk_fields = self._normalize_pk_fields(
        options.get("primary_key_fields")
        or options.get("primary_keys")
        or options.get("row_pk_fields")
        or options.get("pk_fields")
        or options.get("primary_key_columns")
        or options.get("row_pk_columns")
    )
    requested_pk_targets = self._normalize_pk_fields(
        options.get("primary_key_targets") or options.get("target_primary_keys")
    )
    if requested_pk_targets and requested_pk_targets != object_type_pk_targets:
        if set(requested_pk_targets) == set(object_type_pk_targets):
            warnings.append(
                {
                    "code": VC.OBJECTIFY_PRIMARY_KEY_ORDER_OVERRIDE_IGNORED.value,
                    "expected": object_type_pk_targets,
                    "observed": requested_pk_targets,
                    "message": "Ignoring primary key order override; using object_type pk_spec order",
                }
            )
        else:
            warnings.append(
                {
                    "code": VC.OBJECTIFY_PRIMARY_KEY_OVERRIDE_IGNORED.value,
                    "expected": object_type_pk_targets,
                    "observed": requested_pk_targets,
                    "message": "Ignoring primary key override; using object_type pk_spec",
                }
            )
    pk_targets = object_type_pk_targets or sorted(explicit_pk_targets)

    key_spec = await self.dataset_registry.get_key_spec_for_dataset(
        dataset_id=job.dataset_id,
        dataset_version_id=job.dataset_version_id,
    )
    if key_spec:
        normalized_spec = normalize_key_spec(key_spec.spec, columns=mapping_sources)
        key_pk_sources = set(normalized_spec.get("primary_key") or [])
        mapping_source_set = {src for src in mapping_sources if src}
        if key_pk_sources:
            missing_pk_sources = sorted(key_pk_sources - mapping_source_set)
            if missing_pk_sources:
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": [
                            {
                                "code": VC.KEY_SPEC_PRIMARY_KEY_MISSING.value,
                                "sources": missing_pk_sources,
                                "message": "Key spec primary key columns are not mapped",
                            }
                        ]
                    },
                )
            invalid_pk_targets = sorted(
                {
                    m.target_field
                    for m in mappings
                    if m.source_field in key_pk_sources and m.target_field not in set(pk_targets or [])
                }
            )
            if invalid_pk_targets:
                await _fail_job(
                    "validation_failed",
                    report={
                        "errors": [
                            {
                                "code": VC.KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH.value,
                                "targets": invalid_pk_targets,
                                "message": "Key spec primary key sources must map to ontology primary keys",
                            }
                        ]
                    },
                )
        required_sources = set(normalized_spec.get("required_fields") or [])
        if required_sources:
            for mapping in mappings:
                if mapping.source_field in required_sources:
                    required_targets.add(mapping.target_field)
        options.setdefault("key_spec_id", key_spec.key_spec_id)

    title_required_targets = {t for t in object_type_title_targets if t not in object_type_nullable_fields}
    required_targets.update(object_type_required_fields)
    required_targets.update(title_required_targets)

    missing_required = sorted(required_targets - set(mapping_targets))
    if missing_required:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_REQUIRED_MISSING.value,
                        "targets": missing_required,
                        "message": "Required ontology fields are not mapped",
                    }
                ]
            },
        )
    missing_title_targets = sorted(title_required_targets - set(mapping_targets))
    if missing_title_targets:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_TITLE_KEY_MISSING.value,
                        "targets": missing_title_targets,
                        "message": "Title key targets are not mapped",
                    }
                ]
            },
        )
    unique_key_fields = sorted({field for keys in object_type_unique_keys for field in keys if field})
    missing_unique_targets = sorted(set(unique_key_fields) - set(mapping_targets))
    if missing_unique_targets:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_UNIQUE_KEY_MISSING.value,
                        "targets": missing_unique_targets,
                        "message": "Unique key targets are not mapped",
                    }
                ]
            },
        )
    if not pk_targets:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_PRIMARY_KEY_MISSING.value,
                        "message": "primary_key_targets is required when no primary key is defined on target class",
                    }
                ]
            },
        )
    missing_pk_targets = sorted(set(pk_targets) - set(mapping_targets))
    if missing_pk_targets:
        await _fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.MAPPING_SPEC_PRIMARY_KEY_MISSING.value,
                        "targets": missing_pk_targets,
                        "message": "Primary key targets are not mapped",
                    }
                ]
            },
        )

    target_field_types = {target: resolved_field_types[target] for target in property_targets}
    existing_field_types = mapping_spec.target_field_types or {}
    if existing_field_types:
        mismatches: List[Dict[str, Any]] = []
        for target in property_targets:
            expected = resolved_field_types.get(target)
            provided = existing_field_types.get(target)
            if not provided:
                mismatches.append(
                    {"target_field": target, "reason": "missing", "expected": expected}
                )
            else:
                normalized = normalize_import_target_type(provided)
                if expected and normalized != expected:
                    mismatches.append(
                        {
                            "target_field": target,
                            "reason": "mismatch",
                            "expected": expected,
                            "provided": provided,
                        }
                    )
        if mismatches:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": [
                        {
                            "code": VC.MAPPING_SPEC_TARGET_TYPE_MISMATCH.value,
                            "mismatches": mismatches,
                            "message": "Mapping spec target types do not match ontology",
                        }
                    ]
                },
            )

    ontology_version = await self._fetch_ontology_version(job)
    job_node_id = await self._record_lineage_header(
        job=job,
        mapping_spec=mapping_spec,
        ontology_version=ontology_version,
        input_type=input_type,
        artifact_output_name=resolved_output_name,
    )

    (
        key_scan_total_rows,
        key_scan_errors,
        key_scan_error_rows,
        key_scan_stats,
    ) = await self._scan_key_constraints(
        job=job,
        options=options,
        mappings=property_mappings,
        relationship_meta=rel_map,
        target_field_types=target_field_types,
        sources_by_target=sources_by_target,
        required_targets=required_targets,
        pk_targets=pk_targets,
        pk_fields=pk_fields,
        unique_keys=object_type_unique_keys,
        row_batch_size=row_batch_size,
        max_rows=max_rows,
    )
    if key_scan_total_rows == 0:
        await _fail_job("no_rows_loaded")
    if key_scan_errors:
        await _fail_job(
            "validation_failed",
            report={
                "errors": key_scan_errors[:200],
                "stats": key_scan_stats,
                "error_row_indices": key_scan_error_rows[:200],
            },
        )

    validation_errors: List[Dict[str, Any]] = []
    validation_error_rows: List[int] = []
    validation_stats: Dict[str, Any] = {}
    validated_total_rows = 0

    if not allow_partial:
        (
            validated_total_rows,
            validation_errors,
            validation_error_rows,
            validation_stats,
        ) = await self._validate_batches(
            job=job,
            options=options,
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
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        )
        if validated_total_rows == 0:
            await _fail_job("no_rows_loaded")
        if validation_errors:
            await _fail_job(
                "validation_failed",
                report={
                    "errors": validation_errors[:200],
                    "stats": validation_stats,
                    "error_row_indices": validation_error_rows[:200],
                },
            )

    command_ids: List[str] = []
    indexed_instance_ids: set[str] = set()
    prepared_instances = 0
    instance_ids_sample: List[str] = []
    error_rows: List[int] = []
    errors: List[Dict[str, Any]] = []
    total_rows_seen = 0
    lineage_remaining = self.lineage_max_links
    seen_row_keys: set[str] = set()

    execution_mode = str(options.get("execution_mode") or job.execution_mode or "full").strip().lower() or "full"
    if execution_mode not in {"full", "incremental", "delta"}:
        execution_mode = "full"
    watermark_column = job.watermark_column or options.get("watermark_column")
    previous_watermark = job.previous_watermark
    latest_watermark: Optional[str] = None
    delta_computer: Optional[ObjectifyDeltaComputer] = None
    lakefs_delta_result: Optional[DeltaResult] = None

    if execution_mode in ("incremental",) and not watermark_column:
        watermark_column = _auto_detect_watermark_column(columns=None, options=options)
        if not watermark_column:
            logger.info(
                "Incremental mode requested but no watermark column detected; falling back to full mode"
            )
            execution_mode = "full"

    if execution_mode == "delta" and job.base_commit_id:
        pk_source_columns: List[str] = []
        if pk_targets:
            mapping_by_target = {
                str(m.target_field): str(m.source_field)
                for m in (property_mappings or [])
                if getattr(m, "target_field", None) and getattr(m, "source_field", None)
            }
            for target in pk_targets:
                src = mapping_by_target.get(str(target)) or str(target)
                if src:
                    pk_source_columns.append(src)
        if not pk_source_columns:
            pk_source_columns = ["id"]
        delta_computer = ObjectifyDeltaComputer(pk_columns=pk_source_columns)
        lakefs_delta_result = await _compute_lakefs_delta(
            job=job,
            delta_computer=delta_computer,
            storage=self.storage,
        )
        if lakefs_delta_result and lakefs_delta_result.has_changes:
            logger.info(
                "LakeFS delta computed: added=%d, modified=%d, deleted=%d",
                lakefs_delta_result.stats.get("added_count", 0),
                lakefs_delta_result.stats.get("modified_count", 0),
                lakefs_delta_result.stats.get("deleted_count", 0),
            )
        elif lakefs_delta_result and not lakefs_delta_result.has_changes:
            logger.info("LakeFS delta: no changes detected between commits")
        else:
            logger.warning("LakeFS delta computation failed; falling back to full mode")
            execution_mode = "full"
            lakefs_delta_result = None
    elif execution_mode in ("incremental", "delta") and watermark_column:
        delta_computer = create_delta_computer_for_mapping_spec(
            vars(mapping_spec) if hasattr(mapping_spec, "__dict__") else dict(mapping_spec or {})
        )
        logger.info(
            "Incremental objectify mode enabled: execution_mode=%s, watermark_column=%s, previous=%s",
            execution_mode, watermark_column, previous_watermark,
        )

    if lakefs_delta_result and lakefs_delta_result.has_changes:
        delta_result = await _delta_processing.process_lakefs_delta_rows(
            self,
            job=job,
            lakefs_delta_result=lakefs_delta_result,
            delta_computer=delta_computer,
            allow_partial=allow_partial,
            errors=errors,
            batch_size=batch_size,
            rel_map=rel_map,
            ontology_version=ontology_version,
            target_field_types=target_field_types,
            property_mappings=property_mappings,
            relationship_mappings=relationship_mappings,
            mapping_sources=mapping_sources,
            sources_by_target=sources_by_target,
            required_targets=required_targets,
            pk_targets=pk_targets,
            pk_fields=pk_fields,
            field_constraints=field_constraints,
            field_raw_types=field_raw_types,
            seen_row_keys=seen_row_keys,
            stable_seed=stable_seed,
            command_ids=command_ids,
            indexed_instance_ids=indexed_instance_ids,
            instance_ids_sample=instance_ids_sample,
            fail_job=_fail_job,
            extract_instance_relationships=_extract_instance_relationships,
        )
        prepared_instances += int(delta_result.get("prepared_instances") or 0)
        total_rows_seen = int(delta_result.get("total_rows_seen") or 0)

    incremental_filtering_enabled = bool(
        execution_mode in ("incremental", "delta") and delta_computer and watermark_column
    )

    async for columns, rows, row_offset in self._iter_dataset_batches(
        job=job,
        options=options,
        row_batch_size=row_batch_size,
        max_rows=None if incremental_filtering_enabled else max_rows,
    ):
        if lakefs_delta_result and lakefs_delta_result.has_changes:
            break

        if not rows:
            continue

        if incremental_filtering_enabled:
            col_map = {col: idx for idx, col in enumerate(columns)}
            wm_col_idx = col_map.get(watermark_column)

            if wm_col_idx is not None:
                filtered_rows: List[List[Any]] = []
                for row in rows:
                    row_wm = row[wm_col_idx] if wm_col_idx < len(row) else None
                    if row_wm is None:
                        continue
                    if previous_watermark is not None:
                        cmp = delta_computer._compare_watermarks(row_wm, previous_watermark)
                        if cmp <= 0:
                            continue
                    filtered_rows.append(row)
                    if latest_watermark is None:
                        latest_watermark = str(row_wm)
                    elif delta_computer._compare_watermarks(row_wm, latest_watermark) > 0:
                        latest_watermark = str(row_wm)

                rows = filtered_rows
                if not rows:
                    continue

                if max_rows and total_rows_seen + len(rows) > max_rows:
                    rows = rows[:max(0, max_rows - total_rows_seen)]
                    if not rows:
                        break

        total_rows_seen += len(rows)

        if max_rows and total_rows_seen > max_rows:
            break

        batch = self._build_instances_with_validation(
            columns=columns,
            rows=rows,
            row_offset=row_offset,
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
            if self._has_p0_errors(batch_errors):
                await _fail_job("validation_failed", report={"errors": batch_errors[:200]})
            if allow_partial:
                remaining = max(0, 200 - len(errors))
                if remaining:
                    errors.extend(batch_errors[:remaining])
            else:
                await _fail_job("validation_failed", report={"errors": batch_errors[:200]})

        instances = batch.get("instances") or []
        instance_row_indices = batch.get("instance_row_indices") or []
        row_keys = batch.get("row_keys") or []
        error_row_indices = set(batch.get("error_row_indices") or [])

        if error_row_indices:
            for idx in sorted(error_row_indices):
                error_rows.append(int(idx))
            if allow_partial:
                filtered_instances = []
                filtered_indices = []
                filtered_row_keys = []
                for inst, row_idx, row_key in zip(instances, instance_row_indices, row_keys):
                    absolute_idx = int(row_offset) + int(row_idx)
                    if absolute_idx in error_row_indices:
                        continue
                    filtered_instances.append(inst)
                    filtered_indices.append(row_idx)
                    filtered_row_keys.append(row_key)
                instances = filtered_instances
                instance_row_indices = filtered_indices
                row_keys = filtered_row_keys
            else:
                await _fail_job("validation_failed", report={"errors": batch_errors[:200]})

        if not instances:
            continue

        if any(not key for key in row_keys):
            await _fail_job(
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

        instances, instance_ids = self._ensure_instance_ids(
            instances,
            class_id=job.target_class_id,
            stable_seed=stable_seed or job.job_id,
            mapping_spec_version=job.mapping_spec_version,
            row_keys=row_keys,
            instance_id_field=instance_id_field,
        )

        for idx in range(0, len(instances), batch_size):
            instance_batch = instances[idx : idx + batch_size]
            batch_rels: Dict[str, Dict[str, Any]] = {}
            if rel_map:
                for inst in instance_batch:
                    if not isinstance(inst, dict):
                        continue
                    iid = str(inst.get("instance_id") or "").strip()
                    if iid:
                        batch_rels[iid] = _extract_instance_relationships(
                            inst, rel_map=rel_map,
                        )
            write_result = await self.instance_write_path.write_instances(
                job=job,
                instances=instance_batch,
                ontology_version=ontology_version,
                objectify_pk_fields=pk_targets,
                objectify_instance_id_field=instance_id_field,
                instance_relationships=batch_rels if batch_rels else None,
                target_field_types=target_field_types,
            )
            if write_result.command_ids:
                command_ids.extend([str(v) for v in write_result.command_ids if str(v).strip()])
            if write_result.indexed_instance_ids:
                indexed_instance_ids.update(str(v).strip() for v in write_result.indexed_instance_ids if str(v).strip())

        prepared_instances += len(instances)
        if len(instance_ids_sample) < 10:
            instance_ids_sample.extend(instance_ids[: max(0, 10 - len(instance_ids_sample))])

        lineage_remaining = await self._record_instance_lineage(
            job=job,
            job_node_id=job_node_id,
            instance_ids=instance_ids,
            mapping_spec_id=mapping_spec.mapping_spec_id,
            mapping_spec_version=mapping_spec.version,
            column_lineage_pairs=self._build_column_lineage_pairs(getattr(mapping_spec, "mappings", None)),
            ontology_version=ontology_version,
            limit_remaining=lineage_remaining,
            input_type=input_type,
            artifact_output_name=resolved_output_name,
        )

    total_rows = validated_total_rows if not allow_partial else total_rows_seen
    if total_rows == 0:
        await _fail_job("no_rows_loaded")
    if prepared_instances == 0:
        await _fail_job(
            "no_valid_instances",
            report={
                "errors": (errors or validation_errors)[:200],
                "error_row_indices": error_rows[:200],
            },
        )

    write_path_report = await self.instance_write_path.finalize_job(
        job=job,
        execution_mode=execution_mode,
        indexed_instance_ids=indexed_instance_ids,
    )
    terminal_status = "COMPLETED"
    await self.objectify_registry.update_objectify_job_status(
        job_id=job.job_id,
        status=terminal_status,
        command_id=command_ids[0] if command_ids else None,
        report={
            "total_rows": total_rows,
            "prepared_instances": prepared_instances,
            "warnings": warnings[:200],
            "validation": {"warnings": warnings[:200]},
            "errors": (errors or validation_errors)[:200],
            "error_row_indices": (error_rows or validation_error_rows)[:200],
            "command_ids": command_ids,
            "instance_ids_sample": instance_ids_sample[:10],
            "indexed_instances": len(indexed_instance_ids),
            "write_path_mode": WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
            "write_path": write_path_report,
            "ontology_version": ontology_version or {},
        },
        completed_at=datetime.now(timezone.utc),
    )
    await self._record_gate_result(
        job=job,
        status="PASS",
        details={
            "total_rows": total_rows,
            "prepared_instances": prepared_instances,
            "command_ids": command_ids,
            "indexed_instances": len(indexed_instance_ids),
            "write_path_mode": WRITE_PATH_MODE_DATASET_PRIMARY_INDEX,
            "warning_count": len(warnings),
            "error_count": len(errors or validation_errors or []),
        },
    )
    await self._update_object_type_active_version(job=job, mapping_spec=mapping_spec)

    if execution_mode in ("incremental", "delta") and latest_watermark:
        await self._update_watermark_after_job(
            job=job,
            new_watermark=latest_watermark,
        )

    await self._emit_objectify_completed_event(
        job=job,
        total_rows=total_rows,
        prepared_instances=prepared_instances,
        indexed_instances=len(indexed_instance_ids),
        execution_mode=execution_mode,
        ontology_version=ontology_version,
    )
