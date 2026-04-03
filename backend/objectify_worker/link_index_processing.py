from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from shared.services.core.sheet_import_service import SheetImportService
from shared.utils.key_spec import normalize_object_type_key_spec

from objectify_worker.validation_codes import ObjectifyValidationCode as VC

logger = logging.getLogger(__name__)


async def run_link_index_job(
    worker: Any,
    *,
    job: Any,
    mapping_spec: Any,
    options: Dict[str, Any],
    mappings: List[Any],
    mapping_sources: List[str],
    mapping_targets: List[str],
    sources_by_target: Dict[str, List[str]],
    prop_map: Dict[str, Dict[str, Any]],
    rel_map: Dict[str, Dict[str, Any]],
    relationship_mappings: List[Any],
    stable_seed: str,
    row_batch_size: int,
    max_rows: Optional[int],
    non_retryable_error_cls: type[Exception],
) -> None:
    lineage_payload = {
        "job_id": job.job_id,
        "mapping_spec_id": mapping_spec.mapping_spec_id,
        "mapping_spec_version": mapping_spec.version,
        "dataset_id": job.dataset_id,
        "dataset_version_id": job.dataset_version_id,
    }

    async def _fail_link(errors: List[Dict[str, Any]], stats: Dict[str, Any]) -> None:
        await worker._record_gate_result(
            job=job,
            status="FAIL",
            details={"errors": errors[:200], "stats": stats},
        )
        relationship_spec_id = str(options.get("relationship_spec_id") or "").strip()
        if relationship_spec_id and worker.dataset_registry:
            try:
                await worker.dataset_registry.record_relationship_index_result(
                    relationship_spec_id=relationship_spec_id,
                    status="FAIL",
                    stats=stats,
                    errors=errors,
                    dataset_version_id=job.dataset_version_id,
                    mapping_spec_version=mapping_spec.version,
                    lineage=lineage_payload,
                )
            except Exception as exc:
                logger.warning("Failed to update relationship index status: %s", exc)
        await worker.objectify_registry.update_objectify_job_status(
            job_id=job.job_id,
            status="FAILED",
            error="validation_failed",
            report={"errors": errors[:200], "stats": stats},
            completed_at=datetime.now(timezone.utc),
        )
        raise non_retryable_error_cls("validation_failed")

    if not relationship_mappings:
        await _fail_link(
            [{"code": VC.RELATIONSHIP_MAPPING_MISSING.value, "message": "No relationship mappings provided"}],
            {"input_rows": 0},
        )

    object_type_resource = await worker._fetch_object_type_contract(job)
    object_type_data = object_type_resource.get("data") if isinstance(object_type_resource, dict) else None
    if not isinstance(object_type_data, dict):
        object_type_data = object_type_resource if isinstance(object_type_resource, dict) else {}
    object_type_spec = object_type_data.get("spec") if isinstance(object_type_data.get("spec"), dict) else {}
    if not object_type_spec:
        await _fail_link(
            [{"code": VC.OBJECT_TYPE_CONTRACT_MISSING.value, "message": "Object type contract is required"}],
            {"input_rows": 0},
        )

    status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
    if status_value != "ACTIVE":
        await _fail_link(
            [{"code": VC.OBJECT_TYPE_INACTIVE.value, "status": status_value}],
            {"input_rows": 0},
        )

    object_type_key_spec = normalize_object_type_key_spec(
        object_type_spec,
        columns=list(prop_map.keys()),
    )
    pk_targets = [str(v).strip() for v in object_type_key_spec.get("primary_key") or [] if str(v).strip()]
    if not pk_targets:
        await _fail_link(
            [{"code": VC.OBJECT_TYPE_PRIMARY_KEY_MISSING.value, "message": "pk_spec.primary_key is required"}],
            {"input_rows": 0},
        )

    pk_source_fields: List[str] = []
    pk_source_map: Dict[str, str] = {}
    for target in pk_targets:
        sources = sources_by_target.get(target) or []
        unique_sources = [s for s in sources if s]
        if len(unique_sources) != 1:
            await _fail_link(
                [
                    {
                        "code": VC.PRIMARY_KEY_MAPPING_INVALID.value,
                        "target": target,
                        "sources": unique_sources,
                    }
                ],
                {"input_rows": 0},
            )
        pk_source_map[target] = unique_sources[0]
        pk_source_fields.append(unique_sources[0])

    relationship_meta = {m.target_field: rel_map.get(m.target_field) or {} for m in relationship_mappings}
    relationship_meta_override = options.get("relationship_meta") if isinstance(options.get("relationship_meta"), dict) else {}
    if relationship_meta_override:
        for target_field, meta in relationship_meta_override.items():
            if not isinstance(meta, dict):
                continue
            existing = relationship_meta.get(target_field) or {}
            relationship_meta[target_field] = {**existing, **meta}
    rel_cardinality: Dict[str, str] = {}
    for target, meta in relationship_meta.items():
        cardinality = str(meta.get("cardinality") or "").strip().lower()
        rel_cardinality[target] = cardinality

    dangling_policy = str(options.get("dangling_policy") or "FAIL").strip().upper() or "FAIL"
    if dangling_policy not in {"FAIL", "WARN"}:
        dangling_policy = "FAIL"
    dedupe_policy = str(options.get("dedupe_policy") or options.get("dedupePolicy") or "DEDUP").strip().upper() or "DEDUP"
    if dedupe_policy not in {"DEDUP", "WARN", "FAIL"}:
        dedupe_policy = "DEDUP"
    relationship_kind = str(options.get("relationship_kind") or "").strip().lower()
    full_sync = bool(options.get("full_sync") or options.get("fullSync") or False)
    if relationship_kind in {"join_table", "object_backed"} and "full_sync" not in options and "fullSync" not in options:
        full_sync = True

    target_instances: Dict[str, set[str]] = {}
    target_fetch_errors: List[Dict[str, Any]] = []
    dangling_target_samples: List[Dict[str, Any]] = []
    missing_target_count = 0
    target_classes = sorted(
        {
            str(meta.get("target") or meta.get("linkTarget") or "").strip()
            for meta in relationship_meta.values()
            if isinstance(meta, dict)
        }
    )
    target_classes = [value for value in target_classes if value]
    if target_classes:
        branch = job.ontology_branch or job.dataset_branch or "main"
        for target_class in target_classes:
            try:
                ids: set[str] = set()
                async for instance_id in worker._iter_class_instance_ids(
                    db_name=job.db_name,
                    class_id=target_class,
                    branch=branch,
                    limit=worker.list_page_size,
                ):
                    ids.add(instance_id)
                target_instances[target_class] = ids
            except Exception as exc:
                logger.warning(
                    "Failed to prefetch target instances for class %s: %s",
                    target_class,
                    exc,
                    exc_info=True,
                )
                target_fetch_errors.append(
                    {
                        "code": VC.RELATIONSHIP_TARGET_LOOKUP_FAILED.value,
                        "target_class": target_class,
                        "error": str(exc),
                    }
                )
    if target_fetch_errors and dangling_policy == "FAIL":
        await _fail_link(
            target_fetch_errors,
            {"input_rows": 0, "target_lookup_errors": target_fetch_errors},
        )

    total_rows = 0
    error_rows: List[int] = []
    errors: List[Dict[str, Any]] = []
    dangling_count = 0
    duplicate_count = 0

    updates_by_instance: Dict[str, Dict[str, List[str]]] = {}
    row_keys_seen: set[str] = set()

    async for columns, rows, row_offset in worker._iter_dataset_batches(
        job=job,
        options=options,
        row_batch_size=row_batch_size,
        max_rows=max_rows,
    ):
        if not rows:
            continue
        col_index = SheetImportService.build_column_index(columns)
        missing_sources = sorted({s for s in mapping_sources if s and s not in col_index})
        if missing_sources:
            await _fail_link(
                [{"code": VC.SOURCE_FIELD_MISSING.value, "missing_sources": missing_sources}],
                {"input_rows": total_rows},
            )
        for idx, row in enumerate(rows):
            total_rows += 1
            absolute_idx = int(row_offset) + int(idx)
            instance_payload: Dict[str, Any] = {}
            missing_pk = False
            for target, source_field in pk_source_map.items():
                raw = row[col_index[source_field]] if source_field in col_index else None
                if worker._is_blank(raw):
                    missing_pk = True
                    break
                instance_payload[target] = str(raw)
            if missing_pk:
                error_rows.append(absolute_idx)
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "code": VC.PRIMARY_KEY_MISSING.value,
                        "message": "Primary key value is missing",
                    }
                )
                continue

            row_key = worker._derive_row_key(
                columns=columns,
                col_index=col_index,
                row=row,
                instance=instance_payload,
                pk_fields=pk_source_fields,
                pk_targets=pk_targets,
            )
            if not row_key:
                error_rows.append(absolute_idx)
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "code": VC.ROW_KEY_MISSING.value,
                        "message": "Row key could not be derived",
                    }
                )
                continue

            if row_key in row_keys_seen and dedupe_policy == "FAIL":
                error_rows.append(absolute_idx)
                errors.append(
                    {
                        "row_index": absolute_idx,
                        "code": VC.DUPLICATE_ROW_KEY.value,
                        "row_key": row_key,
                    }
                )
                continue
            row_keys_seen.add(row_key)

            _, inst_ids = worker._ensure_instance_ids(
                [instance_payload],
                class_id=job.target_class_id,
                stable_seed=stable_seed,
                mapping_spec_version=mapping_spec.version,
                row_keys=[row_key],
            )
            instance_id = inst_ids[0]

            for mapping in relationship_mappings:
                source_field = mapping.source_field
                target_field = mapping.target_field
                rel_meta = relationship_meta.get(target_field) or {}
                target_class = str(rel_meta.get("target") or rel_meta.get("linkTarget") or "").strip()
                if not target_class:
                    error_rows.append(absolute_idx)
                    errors.append(
                        {
                            "row_index": absolute_idx,
                            "code": VC.RELATIONSHIP_TARGET_MISSING.value,
                            "target_field": target_field,
                        }
                    )
                    continue
                raw = row[col_index[source_field]] if source_field in col_index else None
                if worker._is_blank(raw):
                    dangling_count += 1
                    if dangling_policy == "FAIL":
                        error_rows.append(absolute_idx)
                        errors.append(
                            {
                                "row_index": absolute_idx,
                                "code": VC.RELATIONSHIP_VALUE_MISSING.value,
                                "target_field": target_field,
                            }
                        )
                    continue
                try:
                    ref = worker._normalize_relationship_ref(raw, target_class=target_class)
                except Exception as exc:
                    logger.warning(
                        "Failed to normalize relationship ref for target_class=%s row=%s: %s",
                        target_class,
                        absolute_idx,
                        exc,
                        exc_info=True,
                    )
                    dangling_count += 1
                    if dangling_policy == "FAIL":
                        error_rows.append(absolute_idx)
                        errors.append(
                            {
                                "row_index": absolute_idx,
                                "code": VC.RELATIONSHIP_VALUE_INVALID.value,
                                "target_field": target_field,
                                "error": str(exc),
                            }
                        )
                    continue
                target_ids = target_instances.get(target_class)
                if target_ids is not None:
                    ref_id = ref.split("/", 1)[1] if "/" in ref else ref
                    if ref_id not in target_ids:
                        dangling_count += 1
                        missing_target_count += 1
                        if len(dangling_target_samples) < 20:
                            dangling_target_samples.append(
                                {
                                    "row_index": absolute_idx,
                                    "target_field": target_field,
                                    "target_class": target_class,
                                    "target_id": ref_id,
                                }
                            )
                        if dangling_policy == "FAIL":
                            error_rows.append(absolute_idx)
                            errors.append(
                                {
                                    "row_index": absolute_idx,
                                    "code": VC.RELATIONSHIP_TARGET_MISSING.value,
                                    "target_field": target_field,
                                    "target_class": target_class,
                                    "target_id": ref_id,
                                }
                            )
                        continue
                entry = updates_by_instance.setdefault(instance_id, {})
                bucket = entry.setdefault(target_field, [])
                if ref not in bucket:
                    bucket.append(ref)
                else:
                    duplicate_count += 1

    stats = {
        "input_rows": total_rows,
        "error_rows": len(error_rows),
        "dangling_count": dangling_count,
        "dangling_missing_targets": missing_target_count,
        "duplicate_count": duplicate_count,
        "dedupe_policy": dedupe_policy,
        "relationship_kind": relationship_kind,
    }
    if dangling_target_samples:
        stats["dangling_missing_target_samples"] = dangling_target_samples[:10]
    if target_fetch_errors:
        stats["dangling_target_fetch_errors"] = target_fetch_errors[:5]

    dedupe_warnings: List[Dict[str, Any]] = []
    dangling_warnings: List[Dict[str, Any]] = []
    if duplicate_count:
        if dedupe_policy == "FAIL":
            errors.append(
                {
                    "code": VC.RELATIONSHIP_DUPLICATE.value,
                    "message": "Duplicate relationship pairs detected",
                    "duplicate_count": duplicate_count,
                }
            )
            await _fail_link(errors, stats)
        elif dedupe_policy == "WARN":
            dedupe_warnings.append(
                {
                    "code": VC.RELATIONSHIP_DUPLICATE.value,
                    "message": "Duplicate relationship pairs deduped",
                    "duplicate_count": duplicate_count,
                }
            )
            stats["dedupe_warnings"] = duplicate_count
    if missing_target_count and dangling_policy != "FAIL":
        dangling_warnings.append(
            {
                "code": VC.RELATIONSHIP_TARGET_MISSING.value,
                "missing_count": missing_target_count,
                "samples": dangling_target_samples[:10],
            }
        )
    if target_fetch_errors and dangling_policy != "FAIL":
        dangling_warnings.append(
            {
                "code": VC.RELATIONSHIP_TARGET_LOOKUP_FAILED.value,
                "errors": target_fetch_errors[:5],
            }
        )

    if errors and dangling_policy == "FAIL":
        await _fail_link(errors, stats)

    link_edit_errors: List[Dict[str, Any]] = []
    link_edits_applied = 0
    if worker.dataset_registry:
        link_type_id = str(options.get("link_type_id") or "").strip()
        branch = job.ontology_branch or job.dataset_branch or "main"
        if link_type_id:
            try:
                edits = await worker.dataset_registry.list_link_edits(
                    db_name=job.db_name,
                    link_type_id=link_type_id,
                    branch=branch,
                    status="ACTIVE",
                    limit=10000,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to load link edits for class %s: %s",
                    job.target_class_id,
                    exc,
                    exc_info=True,
                )
                edits = []
                link_edit_errors.append({"code": VC.LINK_EDIT_FETCH_FAILED.value, "error": str(exc)})
            for edit in edits:
                predicate = str(edit.predicate or "").strip()
                rel_meta = relationship_meta.get(predicate) or {}
                target_class = str(rel_meta.get("target") or rel_meta.get("linkTarget") or "").strip()
                if not predicate or not target_class:
                    link_edit_errors.append(
                        {
                            "code": VC.LINK_EDIT_PREDICATE_UNKNOWN.value,
                            "predicate": predicate,
                        }
                    )
                    continue
                try:
                    ref = worker._normalize_relationship_ref(edit.target_instance_id, target_class=target_class)
                except Exception as exc:
                    logger.warning(
                        "Invalid link edit target instance ref for predicate=%s target=%s: %s",
                        predicate,
                        target_class,
                        exc,
                        exc_info=True,
                    )
                    link_edit_errors.append(
                        {
                            "code": VC.LINK_EDIT_TARGET_INVALID.value,
                            "predicate": predicate,
                            "error": str(exc),
                        }
                    )
                    continue
                target_ids = target_instances.get(target_class)
                if target_ids is not None:
                    ref_id = ref.split("/", 1)[1] if "/" in ref else ref
                    if ref_id not in target_ids:
                        link_edit_errors.append(
                            {
                                "code": VC.LINK_EDIT_TARGET_MISSING.value,
                                "predicate": predicate,
                                "target_class": target_class,
                                "target_id": ref_id,
                            }
                        )
                        continue

                entry = updates_by_instance.setdefault(edit.source_instance_id, {})
                bucket = entry.setdefault(predicate, [])
                action = str(edit.edit_type or "").strip().upper()
                if action == "ADD":
                    if ref not in bucket:
                        bucket.append(ref)
                    link_edits_applied += 1
                elif action in {"REMOVE", "DELETE"}:
                    if ref in bucket:
                        bucket.remove(ref)
                    link_edits_applied += 1
                else:
                    link_edit_errors.append(
                        {"code": VC.LINK_EDIT_TYPE_INVALID.value, "edit_type": edit.edit_type}
                    )

    if link_edits_applied or link_edit_errors:
        stats["link_edits_applied"] = link_edits_applied
        if link_edit_errors:
            stats["link_edit_errors"] = link_edit_errors[:200]

    if full_sync and relationship_kind in {"join_table", "object_backed"}:
        try:
            total_instances = 0
            cleared_instances = 0
            branch = job.ontology_branch or job.dataset_branch or "main"
            async for instance_id in worker._iter_class_instance_ids(
                db_name=job.db_name,
                class_id=job.target_class_id,
                branch=branch,
            ):
                total_instances += 1
                if instance_id in updates_by_instance:
                    continue
                entry = updates_by_instance.setdefault(instance_id, {})
                for field in relationship_meta.keys():
                    entry.setdefault(field, [])
                cleared_instances += 1
            stats["full_sync_total_instances"] = total_instances
            stats["full_sync_cleared"] = cleared_instances
        except Exception as exc:
            if isinstance(exc, (httpx.RequestError, httpx.TimeoutException)):
                raise
            await _fail_link(
                [
                    {
                        "code": VC.FULL_SYNC_FAILED.value,
                        "message": "Unable to enumerate instances for full sync",
                        "error": str(exc),
                    }
                ],
                stats,
            )

    updates: List[Dict[str, Any]] = []
    for instance_id, rel_values in updates_by_instance.items():
        payload: Dict[str, Any] = {}
        for field, refs in rel_values.items():
            cardinality = rel_cardinality.get(field) or ""
            allow_multiple = any(token in cardinality for token in (":n", ":m", "many"))
            if not allow_multiple:
                if not refs:
                    payload[field] = None
                    continue
                if len(refs) > 1:
                    errors.append(
                        {
                            "code": VC.RELATIONSHIP_CARDINALITY_VIOLATION.value,
                            "instance_id": instance_id,
                            "field": field,
                            "count": len(refs),
                        }
                    )
                    continue
                payload[field] = refs[0]
            else:
                payload[field] = refs
        if payload:
            updates.append({"instance_id": instance_id, "data": payload})

    if errors and dangling_policy == "FAIL":
        await _fail_link(errors, stats)

    if not updates:
        await _fail_link(
            [{"code": VC.NO_RELATIONSHIPS_INDEXED.value, "message": "No link updates produced"}],
            stats,
        )

    ontology_version = await worker._fetch_ontology_version(job)
    command_ids: List[str] = []
    for idx in range(0, len(updates), worker.bulk_update_batch_size):
        batch = updates[idx : idx + worker.bulk_update_batch_size]
        resp = await worker._bulk_update_instances(job, batch, ontology_version=ontology_version)
        command_id = resp.get("command_id") if isinstance(resp, dict) else None
        if command_id:
            command_ids.append(str(command_id))

    await worker.objectify_registry.update_objectify_job_status(
        job_id=job.job_id,
        status="SUBMITTED",
        command_id=command_ids[0] if command_ids else None,
        report={
            "link_index": True,
            "total_rows": total_rows,
            "update_count": len(updates),
            "command_ids": command_ids,
            "stats": stats,
        },
        completed_at=datetime.now(timezone.utc),
    )
    await worker._record_gate_result(
        job=job,
        status="PASS",
        details={"stats": stats, "command_ids": command_ids},
    )

    relationship_spec_id = str(options.get("relationship_spec_id") or "").strip()
    if relationship_spec_id and worker.dataset_registry:
        record_errors: List[Dict[str, Any]] = []
        if dangling_policy != "FAIL":
            record_errors.extend(errors)
        record_errors.extend(dedupe_warnings)
        record_errors.extend(dangling_warnings)
        record_errors.extend(link_edit_errors)
        status_value = "WARN" if record_errors else "PASS"
        try:
            await worker.dataset_registry.record_relationship_index_result(
                relationship_spec_id=relationship_spec_id,
                status=status_value,
                stats=stats,
                errors=record_errors,
                dataset_version_id=job.dataset_version_id,
                mapping_spec_version=mapping_spec.version,
                lineage=lineage_payload,
            )
        except Exception as exc:
            logger.warning("Failed to update relationship index status: %s", exc)
