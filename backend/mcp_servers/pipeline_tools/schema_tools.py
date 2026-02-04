from __future__ import annotations

import logging
from typing import Any, Awaitable, Dict, List, Callable

from shared.errors.error_types import ErrorCategory, ErrorCode

from mcp_servers.pipeline_mcp_errors import missing_required_params, tool_error

logger = logging.getLogger(__name__)

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


async def _check_schema_drift(server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip()
    dataset_version_id = str(arguments.get("dataset_version_id") or "").strip() or None

    if not db_name or not mapping_spec_id:
        return missing_required_params("check_schema_drift", ["db_name", "mapping_spec_id"], arguments)

    try:
        from shared.services.core.schema_drift_detector import SchemaDriftDetector

        objectify_registry = await server._ensure_objectify_registry()
        dataset_registry, _ = await server._ensure_registries()

        spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
        if not spec:
            return tool_error(
                f"Mapping spec not found: {mapping_spec_id}",
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
                status_code=404,
                context={"mapping_spec_id": mapping_spec_id},
            )

        dataset = await dataset_registry.get_dataset(dataset_id=str(spec.dataset_id))
        if not dataset:
            return tool_error(
                f"Dataset not found: {spec.dataset_id}",
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
                status_code=404,
                context={"dataset_id": str(spec.dataset_id)},
            )

        if dataset_version_id:
            version = await dataset_registry.get_version(version_id=dataset_version_id)
        else:
            version = await dataset_registry.get_latest_version(dataset_id=str(spec.dataset_id))

        if not version:
            return tool_error(
                "No dataset version found",
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
                status_code=404,
                context={"dataset_id": str(spec.dataset_id), "version_id": dataset_version_id},
            )

        current_schema = version.schema_json or {}
        current_columns = current_schema.get("columns") or current_schema.get("fields") or []

        detector = SchemaDriftDetector()
        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id=str(spec.dataset_id),
            db_name=db_name,
            current_schema=current_columns,
            previous_hash=spec.schema_hash,
        )

        if not drift:
            return {
                "status": "compatible",
                "mapping_spec_id": mapping_spec_id,
                "message": "Schema matches expected state - no drift detected",
                "current_hash": version.schema_hash,
            }

        try:
            drift_payload = detector.to_notification_payload(drift)
            ws_service = await server._ensure_websocket_service()
            if ws_service:
                await ws_service.publish_schema_drift(db_name=db_name, drift_payload=drift_payload)
                logger.info("Schema drift broadcast sent for %s/%s", db_name, mapping_spec_id)
        except Exception as ws_exc:
            logger.warning("Failed to broadcast schema drift: %s", ws_exc)

        return {
            "status": "drift_detected",
            "mapping_spec_id": mapping_spec_id,
            "severity": drift.severity,
            "drift_type": drift.drift_type,
            "change_summary": drift.change_summary,
            "changes": [
                {"change_type": c.change_type, "column_name": c.column_name, "impact": c.impact}
                for c in drift.changes
            ],
            "is_breaking": drift.is_breaking,
            "current_hash": drift.current_hash,
            "previous_hash": drift.previous_hash,
        }
    except Exception as exc:
        logger.warning("check_schema_drift failed: %s", exc)
        return tool_error(
            "check_schema_drift failed",
            detail=str(exc)[:300],
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=500,
            context={"db_name": db_name, "mapping_spec_id": mapping_spec_id},
        )


async def _list_schema_changes(server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    subject_type = str(arguments.get("subject_type") or "").strip()
    subject_id = str(arguments.get("subject_id") or "").strip()
    severity = str(arguments.get("severity") or "").strip() or None
    limit = int(arguments.get("limit") or 20)

    if not db_name or not subject_type or not subject_id:
        return missing_required_params("list_schema_changes", ["db_name", "subject_type", "subject_id"], arguments)

    try:
        dataset_registry, _ = await server._ensure_registries()
        pool = dataset_registry._pool

        query = """
            SELECT drift_id, subject_type, subject_id, db_name,
                   previous_hash, current_hash, drift_type, severity,
                   changes, detected_at, acknowledged_at
            FROM schema_drift_history
            WHERE db_name = $1 AND subject_type = $2 AND subject_id = $3
        """
        params: List[Any] = [db_name, subject_type, subject_id]

        if severity:
            query += " AND severity = $4"
            params.append(severity)

        query += " ORDER BY detected_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        changes = [
            {
                "drift_id": str(row["drift_id"]),
                "drift_type": row["drift_type"],
                "severity": row["severity"],
                "changes": row["changes"],
                "detected_at": row["detected_at"].isoformat() if row["detected_at"] else None,
                "acknowledged": row["acknowledged_at"] is not None,
            }
            for row in rows
        ]

        return {
            "status": "success",
            "db_name": db_name,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "total_changes": len(changes),
            "changes": changes,
        }
    except Exception as exc:
        logger.warning("list_schema_changes failed: %s", exc)
        if "does not exist" in str(exc):
            return {
                "status": "success",
                "db_name": db_name,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "total_changes": 0,
                "changes": [],
                "message": "No schema change history available",
            }
        return tool_error(
            "list_schema_changes failed",
            detail=str(exc)[:300],
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=500,
            context={"db_name": db_name, "subject_type": subject_type, "subject_id": subject_id},
        )


SCHEMA_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "check_schema_drift": _check_schema_drift,
    "list_schema_changes": _list_schema_changes,
}


def build_schema_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(SCHEMA_TOOL_HANDLERS)

