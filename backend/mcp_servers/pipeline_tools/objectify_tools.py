from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Dict, List, Callable

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.observability.tracing import trace_external_call
from shared.models.objectify_job import ObjectifyJob

from mcp_servers.pipeline_mcp_errors import missing_required_params, tool_error
from mcp_servers.pipeline_mcp_http import bff_json, oms_json

logger = logging.getLogger(__name__)

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


@trace_external_call("mcp.objectify_suggest_mapping")
async def _objectify_suggest_mapping(server: Any, arguments: Dict[str, Any]) -> Any:
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    target_class_id = str(arguments.get("target_class_id") or "").strip()
    db_name = str(arguments.get("db_name") or "").strip()
    branch = str(arguments.get("branch") or "main").strip()

    if not dataset_id or not target_class_id or not db_name:
        return missing_required_params("objectify_suggest_mapping", ["dataset_id", "target_class_id", "db_name"], arguments)

    dataset_registry, _ = await server._ensure_registries()
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return tool_error(
            f"Dataset not found: {dataset_id}",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
        )

    schema_json = dataset.schema_json or {}
    columns = schema_json.get("columns", [])
    source_columns: List[str] = []
    source_types: Dict[str, str] = {}
    for col in columns:
        if isinstance(col, dict):
            col_name = str(col.get("name") or col.get("column") or "").strip()
            col_type = str(col.get("type") or col.get("data_type") or "xsd:string").strip()
        else:
            col_name = str(col).strip()
            col_type = "xsd:string"
        if col_name:
            source_columns.append(col_name)
            source_types[col_name] = col_type

    target_properties: List[Dict[str, Any]] = []
    try:
        resp = await bff_json(
            "GET",
            f"/databases/{db_name}/ontology/classes/{target_class_id}",
            db_name=db_name,
            principal_id=None,
            principal_type=None,
            params={"branch": branch},
            timeout_seconds=30.0,
        )
        if not resp.get("error"):
            data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
            target_properties = data.get("properties", []) if isinstance(data.get("properties"), list) else []
    except Exception as exc:
        logger.warning("Failed to fetch class properties: %s", exc)

    suggestions: List[Dict[str, Any]] = []
    target_prop_names = {str(p.get("name") or "").strip().lower(): p for p in target_properties if isinstance(p, dict)}

    for src_col in source_columns:
        src_lower = src_col.lower().replace("_", "").replace("-", "")
        best_match = None
        confidence = 0.0

        for tgt_name, tgt_prop in target_prop_names.items():
            tgt_lower = tgt_name.replace("_", "").replace("-", "")

            if src_lower == tgt_lower:
                best_match = tgt_prop.get("name")
                confidence = 1.0
                break
            if src_lower in tgt_lower or tgt_lower in src_lower:
                if confidence < 0.7:
                    best_match = tgt_prop.get("name")
                    confidence = 0.7
            for suffix in ["id", "name", "date", "time", "count", "amount", "price", "email", "phone"]:
                if src_lower.endswith(suffix) and tgt_lower.endswith(suffix):
                    if confidence < 0.5:
                        best_match = tgt_prop.get("name")
                        confidence = 0.5

        suggestions.append(
            {
                "source_field": src_col,
                "source_type": source_types.get(src_col, "xsd:string"),
                "target_field": best_match,
                "target_type": target_prop_names.get((best_match or "").lower(), {}).get("type"),
                "confidence": confidence,
                "auto_mapped": best_match is not None,
            }
        )

    mapped_count = sum(1 for s in suggestions if s.get("auto_mapped"))
    return {
        "status": "success",
        "dataset_id": dataset_id,
        "target_class_id": target_class_id,
        "suggestions": suggestions,
        "source_columns": source_columns,
        "target_properties": [p.get("name") for p in target_properties if isinstance(p, dict)],
        "summary": {
            "total_source_columns": len(source_columns),
            "total_target_properties": len(target_properties),
            "auto_mapped": mapped_count,
            "unmapped": len(source_columns) - mapped_count,
        },
    }


@trace_external_call("mcp.objectify_create_mapping_spec")
async def _objectify_create_mapping_spec(server: Any, arguments: Dict[str, Any]) -> Any:
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    target_class_id = str(arguments.get("target_class_id") or "").strip()
    mappings = arguments.get("mappings") or []
    db_name = str(arguments.get("db_name") or "").strip()
    dataset_branch = str(arguments.get("dataset_branch") or "").strip() or None
    auto_sync = bool(arguments.get("auto_sync", True))
    options = arguments.get("options") or {}

    if not dataset_id or not target_class_id or not mappings or not db_name:
        return missing_required_params("objectify_create_mapping_spec", ["dataset_id", "target_class_id", "mappings", "db_name"], arguments)

    normalized_mappings: List[Dict[str, str]] = []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        src = str(m.get("source_field") or "").strip()
        tgt = str(m.get("target_field") or "").strip()
        if src and tgt:
            normalized_mappings.append({"source_field": src, "target_field": tgt})

    if not normalized_mappings:
        return tool_error("No valid mappings provided")

    dataset_registry, _ = await server._ensure_registries()
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return tool_error(
            f"Dataset not found: {dataset_id}",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
        )

    objectify_registry = await server._ensure_objectify_registry()

    from shared.utils.schema_hash import compute_schema_hash

    schema_json = dataset.schema_json or {}
    schema_columns = schema_json.get("columns", [])
    schema_hash = compute_schema_hash(schema_columns) if schema_columns else None

    if not schema_hash:
        latest_version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
        if latest_version:
            schema_hash = getattr(latest_version, "schema_hash", None)
            if not schema_hash and hasattr(latest_version, "sample_json"):
                sample_json = latest_version.sample_json or {}
                sample_columns = sample_json.get("columns", [])
                schema_hash = compute_schema_hash(sample_columns) if sample_columns else None

    if not schema_hash:
        payload = tool_error(
            "Cannot determine schema_hash for dataset",
            status_code=422,
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
            context={"dataset_id": dataset_id},
        )
        payload["hint"] = "Dataset has no schema information. Try uploading a new version with schema."
        payload["dataset_id"] = dataset_id
        return payload

    # Resolve target_field_types from ontology class properties
    target_field_types: Dict[str, str] = {}
    try:
        ont_resp = await oms_json(
            "GET",
            f"/api/v1/database/{db_name}/ontology/{target_class_id}",
            params={"branch": dataset_branch or "main"},
        )
        ont_data = ont_resp.get("data") if isinstance(ont_resp, dict) else None
        if isinstance(ont_data, dict):
            ont_props = ont_data.get("properties") or []
            prop_type_map = {}
            for p in ont_props:
                if isinstance(p, dict) and p.get("name"):
                    prop_type_map[p["name"]] = p.get("type") or "xsd:string"
            for m in normalized_mappings:
                tgt = m["target_field"]
                if tgt in prop_type_map:
                    target_field_types[tgt] = prop_type_map[tgt]
    except Exception as tft_exc:
        logger.warning("Failed to resolve target_field_types for %s: %s", target_class_id, tft_exc)

    mapping_spec = await objectify_registry.create_mapping_spec(
        dataset_id=dataset_id,
        dataset_branch=dataset_branch or dataset.branch,
        artifact_output_name=dataset.name,
        schema_hash=schema_hash,
        target_class_id=target_class_id,
        mappings=normalized_mappings,
        target_field_types=target_field_types or None,
        auto_sync=auto_sync,
        status="ACTIVE",
        options=options,
    )

    # ── OMS dual-write: sync property_mappings into backing_source ──
    oms_synced = False
    try:
        branch = dataset_branch or dataset.branch or "main"
        head_resp = await oms_json(
            "GET",
            f"/api/v1/version/{db_name}/head",
            params={"branch": branch},
        )
        head_data = head_resp.get("data") if isinstance(head_resp.get("data"), dict) else {}
        head_commit = (
            head_data.get("head_commit_id")
            or head_data.get("commit")
            or head_data.get("head_commit")
            or ""
        )
        if head_commit:
            ot_resp = await oms_json(
                "GET",
                f"/api/v1/database/{db_name}/ontology/resources/object_type/{target_class_id}",
                params={"branch": branch},
            )
            ot_data = ot_resp.get("data") if isinstance(ot_resp.get("data"), dict) else ot_resp
            ot_spec = (ot_data.get("spec") if isinstance(ot_data, dict) else {}) or {}
            backing = (ot_spec.get("backing_source") if isinstance(ot_spec, dict) else {}) or {}
            if backing:
                backing["property_mappings"] = normalized_mappings
                if target_field_types:
                    backing["target_field_types"] = target_field_types
                backing["auto_sync"] = auto_sync
                backing["mapping_version"] = mapping_spec.version if hasattr(mapping_spec, "version") else 1
                ot_spec["backing_source"] = backing
                ot_data["spec"] = ot_spec
                await oms_json(
                    "PUT",
                    f"/api/v1/database/{db_name}/ontology/resources/object_type/{target_class_id}",
                    params={"branch": branch, "expected_head_commit": head_commit},
                    json_body=ot_data,
                    timeout_seconds=30.0,
                )
                oms_synced = True
    except Exception as oms_exc:
        logger.warning("OMS dual-write for mapping_spec failed (non-fatal): %s", oms_exc)

    return {
        "status": "success",
        "mapping_spec_id": mapping_spec.mapping_spec_id,
        "dataset_id": dataset_id,
        "target_class_id": target_class_id,
        "mappings_count": len(normalized_mappings),
        "auto_sync": auto_sync,
        "schema_hash": schema_hash,
        "oms_synced": oms_synced,
        "message": "Mapping spec created. Use objectify_run to execute transformation.",
    }


@trace_external_call("mcp.objectify_list_mapping_specs")
async def _objectify_list_mapping_specs(server: Any, arguments: Dict[str, Any]) -> Any:
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    limit = int(arguments.get("limit") or 50)

    if not dataset_id:
        return missing_required_params("objectify_list_mapping_specs", ["dataset_id"], arguments)

    objectify_registry = await server._ensure_objectify_registry()
    specs = await objectify_registry.list_mapping_specs(dataset_id=dataset_id, limit=limit)

    return {
        "status": "success",
        "dataset_id": dataset_id,
        "mapping_specs": [
            {
                "mapping_spec_id": s.mapping_spec_id,
                "target_class_id": s.target_class_id,
                "status": s.status,
                "auto_sync": s.auto_sync,
                "version": s.version,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in specs
        ],
        "count": len(specs),
    }


@trace_external_call("mcp.objectify_run")
async def _objectify_run(server: Any, arguments: Dict[str, Any]) -> Any:
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip() or None
    target_class_id = str(arguments.get("target_class_id") or "").strip() or None
    dataset_version_id = str(arguments.get("dataset_version_id") or "").strip() or None
    db_name = str(arguments.get("db_name") or "").strip()
    max_rows = arguments.get("max_rows")
    batch_size = arguments.get("batch_size")

    if not dataset_id or not db_name:
        return missing_required_params("objectify_run", ["dataset_id", "db_name"], arguments)

    dataset_registry, _ = await server._ensure_registries()
    objectify_registry = await server._ensure_objectify_registry()

    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
    if not dataset:
        return tool_error(
            f"Dataset not found: {dataset_id}",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
        )

    # ── Mapping resolution: OMS backing_source (by target_class_id) or PostgreSQL ──
    mapping_spec = None
    oms_mode = False

    if target_class_id and not mapping_spec_id:
        # Try OMS backing_source first
        try:
            ot_resp = await oms_json(
                "GET",
                f"/api/v1/database/{db_name}/ontology/resources/object_type/{target_class_id}",
                params={"branch": dataset.branch or "main"},
            )
            ot_data = ot_resp.get("data") if isinstance(ot_resp.get("data"), dict) else ot_resp
            ot_spec = (ot_data.get("spec") if isinstance(ot_data, dict) else {}) or {}
            backing = ot_spec.get("backing_source") or {}
            prop_mappings = backing.get("property_mappings")
            if prop_mappings and isinstance(prop_mappings, list):
                from shared.services.registries.backing_source_adapter import BackingSourceMappingSpec
                mapping_spec = BackingSourceMappingSpec(
                    mapping_spec_id=f"oms:{target_class_id}",
                    dataset_id=dataset_id,
                    dataset_branch=dataset.branch or "main",
                    artifact_output_name=dataset.name,
                    schema_hash=backing.get("schema_hash"),
                    target_class_id=target_class_id,
                    mappings=[
                        {"source_field": str(m.get("source_field", "")), "target_field": str(m.get("target_field", ""))}
                        for m in prop_mappings if isinstance(m, dict)
                    ],
                    target_field_types=backing.get("target_field_types") or {},
                    auto_sync=backing.get("auto_sync", True),
                    version=int(backing.get("mapping_version") or 1),
                )
                oms_mode = True
        except Exception as oms_exc:
            logger.warning("OMS backing_source lookup failed for %s (falling back to PG): %s", target_class_id, oms_exc)

    if not mapping_spec:
        if mapping_spec_id:
            mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
        else:
            specs = await objectify_registry.list_mapping_specs(dataset_id=dataset_id, limit=10)
            active_specs = [s for s in specs if s.status == "ACTIVE" and s.auto_sync]
            if active_specs:
                mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=active_specs[0].mapping_spec_id)

    if not mapping_spec:
        payload = tool_error(
            "No active mapping spec found for dataset",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
            external_code="MAPPING_SPEC_NOT_FOUND",
            context={"dataset_id": dataset_id, "target_class_id": target_class_id},
        )
        payload["hint"] = (
            "Use ontology_register_object_type with property_mappings to define mappings in OMS, "
            "or objectify_create_mapping_spec for PostgreSQL-based mappings."
        )
        return payload

    if dataset_version_id:
        version = await dataset_registry.get_version(version_id=dataset_version_id)
    else:
        version = await dataset_registry.get_latest_version(dataset_id=dataset_id)

    if not version:
        return tool_error(
            "No dataset version found",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
            external_code="DATASET_VERSION_MISSING",
            context={"dataset_id": dataset_id},
        )

    from uuid import uuid4
    from shared.services.registries.backing_source_adapter import is_oms_mapping_spec

    job_id = str(uuid4())
    is_oms = is_oms_mapping_spec(mapping_spec.mapping_spec_id) if mapping_spec.mapping_spec_id else oms_mode
    dedupe_spec_id = mapping_spec.target_class_id if is_oms else mapping_spec.mapping_spec_id
    dedupe_key = objectify_registry.build_dedupe_key(
        dataset_id=dataset_id,
        dataset_branch=dataset.branch,
        mapping_spec_id=dedupe_spec_id,
        mapping_spec_version=mapping_spec.version,
        dataset_version_id=version.version_id,
        artifact_id=None,
        artifact_output_name=dataset.name,
    )

    existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
    if existing:
        return {
            "status": "already_exists",
            "job_id": existing.job_id,
            "job_status": existing.status,
            "message": "An objectify job already exists for this dataset version and mapping spec",
        }

    options: Dict[str, Any] = dict(mapping_spec.options or {})
    if max_rows is not None:
        options["max_rows"] = int(max_rows)
    if batch_size is not None:
        options["batch_size"] = int(batch_size)

    # OMS-sourced: mapping_spec_id=None (can't store "oms:..." in PG uuid column)
    pg_spec_id = None if is_oms else mapping_spec.mapping_spec_id
    pg_spec_version = None if is_oms else mapping_spec.version
    job = ObjectifyJob(
        job_id=job_id,
        db_name=db_name,
        dataset_id=dataset_id,
        dataset_version_id=version.version_id,
        artifact_output_name=dataset.name,
        dedupe_key=dedupe_key,
        dataset_branch=dataset.branch,
        artifact_key=version.artifact_key or "",
        mapping_spec_id=pg_spec_id,
        mapping_spec_version=pg_spec_version,
        target_class_id=mapping_spec.target_class_id,
        ontology_branch=options.get("ontology_branch"),
        max_rows=options.get("max_rows"),
        batch_size=options.get("batch_size"),
        allow_partial=bool(options.get("allow_partial")),
        options=options,
    )

    await objectify_registry.enqueue_objectify_job(job=job)

    return {
        "status": "success",
        "job_id": job_id,
        "dataset_id": dataset_id,
        "dataset_version_id": version.version_id,
        "mapping_spec_id": mapping_spec.mapping_spec_id,
        "target_class_id": mapping_spec.target_class_id,
        "oms_mode": is_oms,
        "message": "Objectify job enqueued. Use objectify_get_status to check progress.",
    }


@trace_external_call("mcp.objectify_get_status")
async def _objectify_get_status(server: Any, arguments: Dict[str, Any]) -> Any:
    job_id = str(arguments.get("job_id") or "").strip()
    if not job_id:
        return missing_required_params("objectify_get_status", ["job_id"], arguments)

    objectify_registry = await server._ensure_objectify_registry()
    job = await objectify_registry.get_objectify_job(job_id=job_id)

    if not job:
        return tool_error(
            f"Objectify job not found: {job_id}",
            status_code=404,
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
            external_code="OBJECTIFY_JOB_NOT_FOUND",
            context={"job_id": job_id},
        )

    report = job.report or {}
    return {
        "status": "success",
        "job_id": job.job_id,
        "job_status": job.status,
        "dataset_id": job.dataset_id,
        "target_class_id": job.target_class_id,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "error": job.error,
        "rows_processed": report.get("rows_processed"),
        "rows_failed": report.get("rows_failed"),
        "instances_created": report.get("instances_created"),
    }


@trace_external_call("mcp.objectify_wait")
async def _objectify_wait(server: Any, arguments: Dict[str, Any]) -> Any:
    job_id = str(arguments.get("job_id") or "").strip()
    timeout_seconds = float(arguments.get("timeout_seconds") or 300)
    poll_interval = float(arguments.get("poll_interval_seconds") or 2)

    if not job_id:
        return missing_required_params("objectify_wait", ["job_id"], arguments)

    objectify_registry = await server._ensure_objectify_registry()

    elapsed = 0.0
    final_statuses = {"completed", "failed", "cancelled"}

    while elapsed < timeout_seconds:
        job = await objectify_registry.get_objectify_job(job_id=job_id)

        if not job:
            return tool_error(
                f"Objectify job not found: {job_id}",
                status_code=404,
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
                external_code="OBJECTIFY_JOB_NOT_FOUND",
                context={"job_id": job_id},
            )

        job_status = (job.status or "").lower()

        if job_status in final_statuses:
            report = job.report or {}
            return {
                "status": "success" if job_status == "completed" else "failed",
                "job_id": job.job_id,
                "job_status": job.status,
                "dataset_id": job.dataset_id,
                "target_class_id": job.target_class_id,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "updated_at": job.updated_at.isoformat() if job.updated_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "error": job.error,
                "rows_processed": report.get("rows_processed"),
                "rows_failed": report.get("rows_failed"),
                "instances_created": report.get("instances_created"),
                "wait_elapsed_seconds": elapsed,
            }

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    job = await objectify_registry.get_objectify_job(job_id=job_id)
    return {
        "status": "timeout",
        "error": f"Objectify job did not complete within {timeout_seconds}s",
        "job_id": job_id,
        "job_status": job.status if job else "unknown",
        "wait_elapsed_seconds": elapsed,
        "hint": "Job may still be running. Call objectify_wait again or use objectify_get_status to check.",
    }


@trace_external_call("mcp.trigger_incremental_objectify")
async def _trigger_incremental_objectify(server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip()
    execution_mode = str(arguments.get("execution_mode") or "incremental").strip()
    watermark_column = str(arguments.get("watermark_column") or "").strip()
    force_full = bool(arguments.get("force_full_refresh", False))
    branch = str(arguments.get("branch") or "main").strip()

    if not db_name or not mapping_spec_id:
        return missing_required_params("trigger_incremental_objectify", ["db_name", "mapping_spec_id"], arguments)

    try:
        objectify_registry = await server._ensure_objectify_registry()

        if force_full:
            await objectify_registry.delete_watermark(mapping_spec_id=mapping_spec_id, dataset_branch=branch)

        watermark = await objectify_registry.get_watermark(mapping_spec_id=mapping_spec_id, dataset_branch=branch)

        trigger_body = {
            "execution_mode": execution_mode,
            "watermark_column": watermark_column or (watermark.get("watermark_column") if watermark else None),
            "previous_watermark": watermark.get("watermark_value") if watermark and not force_full else None,
        }

        resp = await bff_json(
            "POST",
            f"/objectify/mapping-specs/{mapping_spec_id}/trigger",
            db_name=db_name,
            principal_id=None,
            principal_type=None,
            json_body=trigger_body,
            params={"branch": branch},
            timeout_seconds=60.0,
        )

        if resp.get("error"):
            return tool_error(
                "Objectify trigger failed",
                detail=str(resp.get("error"))[:500],
                code=ErrorCode.UPSTREAM_ERROR,
                category=ErrorCategory.UPSTREAM,
                status_code=502,
                context={"mapping_spec_id": mapping_spec_id, "branch": branch},
            )

        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        return {
            "status": "success",
            "mapping_spec_id": mapping_spec_id,
            "execution_mode": execution_mode,
            "watermark_column": trigger_body.get("watermark_column"),
            "previous_watermark": trigger_body.get("previous_watermark"),
            "job_id": resp.get("job_id") or data.get("job_id"),
        }
    except Exception as exc:
        logger.warning("trigger_incremental_objectify failed: %s", exc)
        return tool_error(
            "trigger_incremental_objectify failed",
            detail=str(exc)[:300],
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=500,
            context={"mapping_spec_id": mapping_spec_id, "branch": branch},
        )


@trace_external_call("mcp.get_objectify_watermark")
async def _get_objectify_watermark(server: Any, arguments: Dict[str, Any]) -> Any:
    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip()
    dataset_branch = str(arguments.get("dataset_branch") or "main").strip()

    if not mapping_spec_id:
        return missing_required_params("get_objectify_watermark", ["mapping_spec_id"], arguments)

    try:
        objectify_registry = await server._ensure_objectify_registry()
        watermark = await objectify_registry.get_watermark(mapping_spec_id=mapping_spec_id, dataset_branch=dataset_branch)

        if not watermark:
            return {
                "status": "not_found",
                "mapping_spec_id": mapping_spec_id,
                "message": "No watermark found - objectify has not run in incremental mode yet",
            }

        return {"status": "success", **watermark}
    except Exception as exc:
        logger.warning("get_objectify_watermark failed: %s", exc)
        return tool_error(
            "get_objectify_watermark failed",
            detail=str(exc)[:300],
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=500,
            context={"mapping_spec_id": mapping_spec_id, "dataset_branch": dataset_branch},
        )


OBJECTIFY_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "objectify_suggest_mapping": _objectify_suggest_mapping,
    "objectify_create_mapping_spec": _objectify_create_mapping_spec,
    "objectify_list_mapping_specs": _objectify_list_mapping_specs,
    "objectify_run": _objectify_run,
    "objectify_get_status": _objectify_get_status,
    "objectify_wait": _objectify_wait,
    "trigger_incremental_objectify": _trigger_incremental_objectify,
    "get_objectify_watermark": _get_objectify_watermark,
}


def build_objectify_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(OBJECTIFY_TOOL_HANDLERS)
