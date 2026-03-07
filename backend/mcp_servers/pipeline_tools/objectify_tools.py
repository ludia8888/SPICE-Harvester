from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Dict, List, Callable

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.observability.tracing import trace_external_call

from mcp_servers.feature_helpers import (
    build_objectify_job_status,
    build_objectify_run_body,
    tool_error_from_upstream_response,
    unwrap_api_payload,
)
from mcp_servers.pipeline_mcp_errors import missing_required_params, tool_error
from mcp_servers.pipeline_mcp_http import bff_json

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

    request_body: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "target_class_id": target_class_id,
        "mappings": normalized_mappings,
        "auto_sync": auto_sync,
        "options": options if isinstance(options, dict) else {},
    }
    if dataset_branch:
        request_body["dataset_branch"] = dataset_branch
    user_target_field_types = arguments.get("target_field_types")
    if isinstance(user_target_field_types, dict) and user_target_field_types:
        request_body["target_field_types"] = user_target_field_types

    response = await bff_json(
        "POST",
        "/objectify/mapping-specs",
        db_name=db_name,
        principal_id=None,
        principal_type=None,
        json_body=request_body,
        timeout_seconds=30.0,
    )
    if response.get("error"):
        return tool_error_from_upstream_response(
            response,
            default_message="Failed to create mapping spec",
            context={"dataset_id": dataset_id, "target_class_id": target_class_id},
        )

    data = unwrap_api_payload(response)
    mapping_spec = data.get("mapping_spec") if isinstance(data.get("mapping_spec"), dict) else {}
    if not mapping_spec:
        return tool_error(
            "BFF mapping-spec response missing mapping_spec payload",
            status_code=502,
            code=ErrorCode.UPSTREAM_ERROR,
            category=ErrorCategory.UPSTREAM,
            context={"dataset_id": dataset_id, "target_class_id": target_class_id},
        )
    return {
        "status": "success",
        "mapping_spec_id": mapping_spec.get("mapping_spec_id"),
        "dataset_id": dataset_id,
        "target_class_id": mapping_spec.get("target_class_id") or target_class_id,
        "mappings_count": len(normalized_mappings),
        "auto_sync": bool(mapping_spec.get("auto_sync", auto_sync)),
        "schema_hash": mapping_spec.get("schema_hash"),
        "mapping_spec_version": mapping_spec.get("version"),
        "message": str(response.get("message") or "Mapping spec created. Use objectify_run to execute transformation."),
        "data": data,
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
    db_name = str(arguments.get("db_name") or "").strip()

    if not dataset_id or not db_name:
        return missing_required_params("objectify_run", ["dataset_id", "db_name"], arguments)

    request_body = build_objectify_run_body(arguments)

    response = await bff_json(
        "POST",
        f"/objectify/datasets/{dataset_id}/run",
        db_name=db_name,
        principal_id=None,
        principal_type=None,
        json_body=request_body,
        timeout_seconds=30.0,
    )
    if response.get("error"):
        return tool_error_from_upstream_response(
            response,
            default_message="Failed to enqueue objectify job",
            context={"dataset_id": dataset_id, "target_class_id": target_class_id, "mapping_spec_id": mapping_spec_id},
        )

    data = unwrap_api_payload(response)
    message = str(response.get("message") or "Objectify job queued")
    status_value = "already_exists" if "already queued" in message.lower() else "success"
    return {
        "status": status_value,
        "job_id": data.get("job_id"),
        "dataset_id": dataset_id,
        "dataset_version_id": data.get("dataset_version_id"),
        "mapping_spec_id": data.get("mapping_spec_id") or mapping_spec_id,
        "target_class_id": target_class_id,
        "oms_mode": bool(data.get("oms_mode")),
        "job_status": data.get("status"),
        "message": message,
        "data": data,
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

    return build_objectify_job_status(job)


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
            return build_objectify_job_status(
                job,
                status="success" if job_status == "completed" else "failed",
                wait_elapsed_seconds=elapsed,
            )

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
