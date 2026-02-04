from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List

from shared.errors.error_types import ErrorCategory, ErrorCode

from mcp_servers.pipeline_mcp_errors import missing_required_params, tool_error

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


async def _dataset_get_by_name(server: Any, arguments: Dict[str, Any]) -> Any:
    db_name = str(arguments.get("db_name") or "").strip()
    dataset_name = str(arguments.get("dataset_name") or "").strip()
    branch = str(arguments.get("branch") or "main").strip()

    if not db_name or not dataset_name:
        return missing_required_params("dataset_get_by_name", ["db_name", "dataset_name"], arguments)

    dataset_registry, _ = await server._ensure_registries()
    dataset = await dataset_registry.get_dataset_by_name(db_name=db_name, name=dataset_name, branch=branch)

    if not dataset:
        return {
            "status": "not_found",
            "error": f"Dataset not found: {dataset_name} in {db_name}/{branch}",
            "db_name": db_name,
            "dataset_name": dataset_name,
            "branch": branch,
        }

    return {
        "status": "success",
        "dataset_id": dataset.dataset_id,
        "db_name": dataset.db_name,
        "name": dataset.name,
        "branch": dataset.branch,
        "source_type": dataset.source_type,
        "schema": dataset.schema_json,
        "created_at": dataset.created_at.isoformat() if dataset.created_at else None,
    }


async def _dataset_get_latest_version(server: Any, arguments: Dict[str, Any]) -> Any:
    dataset_id = str(arguments.get("dataset_id") or "").strip()

    if not dataset_id:
        return missing_required_params("dataset_get_latest_version", ["dataset_id"], arguments)

    dataset_registry, _ = await server._ensure_registries()
    version = await dataset_registry.get_latest_version(dataset_id=dataset_id)

    if not version:
        return {"status": "not_found", "error": f"No version found for dataset: {dataset_id}", "dataset_id": dataset_id}

    return {
        "status": "success",
        "version_id": version.version_id,
        "dataset_id": version.dataset_id,
        "artifact_key": version.artifact_key,
        "lakefs_commit_id": version.lakefs_commit_id,
        "row_count": version.row_count,
        "created_at": version.created_at.isoformat() if version.created_at else None,
    }


async def _dataset_validate_columns(server: Any, arguments: Dict[str, Any]) -> Any:
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    columns = arguments.get("columns") or []

    if not dataset_id:
        return missing_required_params("dataset_validate_columns", ["dataset_id"], arguments)
    if not columns or not isinstance(columns, list):
        return missing_required_params("dataset_validate_columns", ["columns"], arguments)

    columns_to_check = [str(c).strip() for c in columns if str(c).strip()]

    dataset_registry, _ = await server._ensure_registries()
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)

    if not dataset:
        return tool_error(
            f"Dataset not found: {dataset_id}",
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
            status_code=404,
            context={"dataset_id": dataset_id},
        )

    schema_json = dataset.schema_json or {}
    schema_columns = schema_json.get("columns") or schema_json.get("fields") or []

    available_columns: List[str] = []
    for col in schema_columns:
        if isinstance(col, dict):
            col_name = col.get("name") or col.get("column_name") or ""
            if col_name:
                available_columns.append(str(col_name))
        elif isinstance(col, str):
            available_columns.append(col)

    valid_columns: List[str] = []
    invalid_columns: List[str] = []
    suggestions: Dict[str, List[str]] = {}

    available_lower = {c.lower(): c for c in available_columns}

    for col in columns_to_check:
        col_lower = col.lower()
        if col in available_columns:
            valid_columns.append(col)
        elif col_lower in available_lower:
            valid_columns.append(col)
            suggestions[col] = [available_lower[col_lower]]
        else:
            invalid_columns.append(col)
            similar = [c for c in available_columns if col_lower in c.lower() or c.lower() in col_lower][:3]
            if similar:
                suggestions[col] = similar

    return {
        "status": "valid" if not invalid_columns else "invalid",
        "dataset_id": dataset_id,
        "valid_columns": valid_columns,
        "invalid_columns": invalid_columns,
        "suggestions": suggestions if suggestions else None,
        "available_columns": available_columns[:50],
        "total_available_columns": len(available_columns),
    }


DATASET_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "dataset_get_by_name": _dataset_get_by_name,
    "dataset_get_latest_version": _dataset_get_latest_version,
    "dataset_validate_columns": _dataset_validate_columns,
}


def build_dataset_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(DATASET_TOOL_HANDLERS)

