from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
from typing import Any, Awaitable, Callable, Dict, List, Optional

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.services.pipeline.pipeline_profiler import compute_column_stats
from shared.utils.llm_safety import mask_pii
from shared.utils.s3_uri import parse_s3_uri

from mcp_servers.pipeline_mcp_errors import missing_required_params, tool_error

logger = logging.getLogger(__name__)

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]

# ---------------------------------------------------------------------------
# Shared data-loading utilities
# ---------------------------------------------------------------------------

_FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE\s+(?!TABLE\s+data\s+AS)TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)


def _parse_csv_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    """Parse CSV payload into row dicts (standalone version of PipelineExecutor helper)."""
    try:
        text = raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        return []
    if not text.strip():
        return []
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    if not first_line:
        return []
    delimiter = ","
    if "\t" in first_line and first_line.count("\t") >= first_line.count(","):
        delimiter = "\t"
    elif ";" in first_line and first_line.count(";") >= first_line.count(","):
        delimiter = ";"
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        raw_header = next(reader)
    except StopIteration:
        return []
    header = [(str(cell or "").strip() or f"column_{idx + 1}").lstrip("\ufeff") for idx, cell in enumerate(raw_header)]
    rows: List[Dict[str, Any]] = []
    resolved_limit = max(1, int(max_rows))
    for record in reader:
        if len(rows) >= resolved_limit:
            break
        if not record or not any(str(cell or "").strip() for cell in record):
            continue
        rows.append({key: str(record[idx]).strip() if idx < len(record) else "" for idx, key in enumerate(header)})
    return rows


def _parse_excel_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    try:
        import pandas as pd
        from io import BytesIO
        frame = pd.read_excel(BytesIO(raw_bytes))
        return frame.fillna("").to_dict(orient="records")[: max(1, int(max_rows))]
    except Exception:
        return []


def _parse_json_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        text = raw_bytes.decode("utf-8", errors="replace")
        rows: List[Dict[str, Any]] = []
        resolved_limit = max(1, int(max_rows))
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                rows.append(item)
            if len(rows) >= resolved_limit:
                break
        return rows
    if isinstance(payload, dict):
        rows_data = payload.get("rows") or payload.get("data")
        if isinstance(rows_data, list) and rows_data and isinstance(rows_data[0], dict):
            return rows_data[: max(1, int(max_rows))]
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[: max(1, int(max_rows))]
    return []


def _extract_sample_rows(sample: Any) -> List[Dict[str, Any]]:
    """Extract sample rows from a version's sample_json payload."""
    if not isinstance(sample, dict):
        return []
    rows = sample.get("rows")
    if isinstance(rows, list):
        if rows and isinstance(rows[0], dict):
            return rows
        columns = _extract_schema_column_names(sample)
        return [
            {columns[idx] if idx < len(columns) else f"col_{idx}": value for idx, value in enumerate(row)}
            for row in rows
            if isinstance(row, list)
        ]
    data_rows = sample.get("data")
    if isinstance(data_rows, list) and data_rows and isinstance(data_rows[0], dict):
        return data_rows
    return []


def _extract_schema_column_names(schema_json: Any) -> List[str]:
    """Extract column names from a schema JSON payload."""
    if not isinstance(schema_json, dict):
        return []
    columns_raw = schema_json.get("columns") or schema_json.get("fields") or []
    names: List[str] = []
    for col in columns_raw:
        if isinstance(col, dict):
            col_name = col.get("name") or col.get("column_name") or ""
            if col_name:
                names.append(str(col_name))
        elif isinstance(col, str):
            names.append(col)
    return names


async def _load_artifact_rows(
    storage_service: Any,
    artifact_key: str,
    *,
    max_rows: int = 200,
) -> List[Dict[str, Any]]:
    """Load rows from an S3/lakeFS artifact, parsing CSV/Excel/JSON."""
    parsed = parse_s3_uri(artifact_key)
    if not parsed or not storage_service:
        return []
    bucket, key = parsed
    resolved_limit = max(1, int(max_rows))
    try:
        extension = os.path.splitext(key)[1].lower()
        if extension == ".csv":
            estimated_bytes = max(64 * 1024, resolved_limit * 512)
            raw_bytes = await storage_service.load_bytes_lines(
                bucket, key, max_lines=resolved_limit + 1, max_bytes=estimated_bytes,
            )
        else:
            raw_bytes = await storage_service.load_bytes(bucket, key)
    except Exception:
        return []
    if extension == ".csv":
        return _parse_csv_bytes(raw_bytes, max_rows=resolved_limit)
    if extension in {".xlsx", ".xlsm"}:
        return _parse_excel_bytes(raw_bytes, max_rows=resolved_limit)
    if extension == ".json":
        return _parse_json_bytes(raw_bytes, max_rows=resolved_limit)
    # Try listing directory for nested files
    prefix = key.rstrip("/")
    if not prefix:
        return []
    prefix = f"{prefix}/"
    try:
        objects = await storage_service.list_objects(bucket, prefix=prefix)
    except Exception:
        return []
    keys = [obj.get("Key") for obj in objects or [] if obj.get("Key")]
    for candidate in keys:
        ext = os.path.splitext(candidate)[1].lower()
        if ext not in {".json", ".csv", ".xlsx", ".xlsm"}:
            continue
        try:
            candidate_bytes = await storage_service.load_bytes(bucket, candidate)
        except Exception:
            continue
        if ext == ".csv":
            return _parse_csv_bytes(candidate_bytes, max_rows=resolved_limit)
        if ext in {".xlsx", ".xlsm"}:
            return _parse_excel_bytes(candidate_bytes, max_rows=resolved_limit)
        if ext == ".json":
            return _parse_json_bytes(candidate_bytes, max_rows=resolved_limit)
    return []


async def _load_sample_rows(server: Any, dataset_id: str, *, limit: int = 200) -> List[Dict[str, Any]]:
    """Load sample rows from a dataset, trying multiple sources: profile cache → artifact → sample_json."""
    dataset_registry, profile_registry = await server._ensure_registries()

    # 1. Profile cache (fastest)
    if profile_registry:
        try:
            profile_record = await profile_registry.get_latest_profile(dataset_id=dataset_id)
            if profile_record and isinstance(profile_record.profile, dict):
                rows = (
                    profile_record.profile.get("rows")
                    or profile_record.profile.get("sample_rows")
                    or []
                )
                if isinstance(rows, list) and rows:
                    return rows[:limit]
        except Exception:
            pass

    # 2. Artifact (direct S3/lakeFS read)
    version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
    if version and version.artifact_key:
        try:
            pipeline_registry = await server._ensure_pipeline_registry()
            storage = await pipeline_registry.get_lakefs_storage()
            rows = await _load_artifact_rows(storage, version.artifact_key, max_rows=limit)
            if rows:
                return rows
        except Exception as exc:
            logger.debug("_load_sample_rows artifact read failed dataset_id=%s err=%s", dataset_id, exc)

    # 3. sample_json fallback
    if version and version.sample_json:
        return _extract_sample_rows(version.sample_json)[:limit]

    return []


def _filter_columns(rows: List[Dict[str, Any]], columns_filter: Any) -> List[Dict[str, Any]]:
    """Filter rows to only include specified columns."""
    if not isinstance(columns_filter, list) or not columns_filter:
        return rows
    col_set = set(str(c).strip() for c in columns_filter if str(c).strip())
    return [{k: v for k, v in row.items() if k in col_set} for row in rows if isinstance(row, dict)]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


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
    available_columns = _extract_schema_column_names(schema_json)

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


async def _dataset_list(server: Any, arguments: Dict[str, Any]) -> Any:
    """List all datasets in a given database/project, with schema summaries."""
    db_name = str(arguments.get("db_name") or "").strip()
    branch = str(arguments.get("branch") or "main").strip()

    if not db_name:
        return missing_required_params("dataset_list", ["db_name"], arguments)

    dataset_registry, _ = await server._ensure_registries()
    datasets = await dataset_registry.list_datasets(db_name=db_name, branch=branch)

    if not datasets:
        return {"status": "success", "datasets": [], "total": 0, "db_name": db_name, "branch": branch}

    result: List[Dict[str, Any]] = []
    for ds in datasets[:50]:
        column_names = _extract_schema_column_names(ds.schema_json)
        result.append({
            "dataset_id": ds.dataset_id,
            "name": ds.name,
            "source_type": ds.source_type,
            "columns": column_names[:60],
            "column_count": len(column_names),
            "created_at": ds.created_at.isoformat() if ds.created_at else None,
        })

    return {"status": "success", "datasets": result, "total": len(result), "db_name": db_name, "branch": branch}


async def _dataset_sample(server: Any, arguments: Dict[str, Any]) -> Any:
    """Return a small sample of actual data rows from a dataset (PII-masked).

    Tries multiple sources: profile cache → S3/lakeFS artifact → version sample_json.
    """
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    limit = min(max(int(arguments.get("limit") or 20), 1), 50)
    columns_filter = arguments.get("columns")

    if not dataset_id:
        return missing_required_params("dataset_sample", ["dataset_id"], arguments)

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

    rows = await _load_sample_rows(server, dataset_id, limit=limit)

    if rows:
        filtered = _filter_columns(rows[:limit], columns_filter)
        masked_rows = [mask_pii(row) if isinstance(row, dict) else row for row in filtered]
        return {
            "status": "success",
            "dataset_id": dataset_id,
            "name": dataset.name,
            "rows": masked_rows,
            "row_count": len(masked_rows),
        }

    # Fallback: return schema info only
    column_names = _extract_schema_column_names(dataset.schema_json)
    return {
        "status": "partial",
        "dataset_id": dataset_id,
        "name": dataset.name,
        "rows": [],
        "row_count": 0,
        "columns": column_names[:60],
        "column_count": len(column_names),
        "note": "No data available. Upload data or check artifact storage.",
    }


async def _dataset_profile(server: Any, arguments: Dict[str, Any]) -> Any:
    """Get column-level statistics for a dataset: null ratio, distinct count, top values, histogram."""
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    columns_filter = arguments.get("columns")

    if not dataset_id:
        return missing_required_params("dataset_profile", ["dataset_id"], arguments)

    dataset_registry, profile_registry = await server._ensure_registries()
    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)

    if not dataset:
        return tool_error(
            f"Dataset not found: {dataset_id}",
            code=ErrorCode.RESOURCE_NOT_FOUND,
            category=ErrorCategory.RESOURCE,
            status_code=404,
            context={"dataset_id": dataset_id},
        )

    # 1. Profile cache
    if profile_registry:
        try:
            profile_record = await profile_registry.get_latest_profile(dataset_id=dataset_id)
            if profile_record and isinstance(profile_record.profile, dict):
                cached_stats = profile_record.profile.get("column_stats")
                if isinstance(cached_stats, dict):
                    result_stats = dict(cached_stats)
                    if isinstance(columns_filter, list) and columns_filter:
                        col_set = set(str(c).strip() for c in columns_filter if str(c).strip())
                        cols_data = result_stats.get("columns", {})
                        result_stats["columns"] = {k: v for k, v in cols_data.items() if k in col_set}
                    return {
                        "status": "success",
                        "dataset_id": dataset_id,
                        "name": dataset.name,
                        "column_stats": result_stats,
                        "source": "profile_cache",
                    }
        except Exception as exc:
            logger.debug("dataset_profile cache lookup failed dataset_id=%s err=%s", dataset_id, exc)

    # 2. Compute from sample rows
    rows = await _load_sample_rows(server, dataset_id, limit=200)
    if rows:
        all_keys: List[str] = []
        seen: set = set()
        for row in rows:
            if isinstance(row, dict):
                for k in row:
                    if k not in seen:
                        all_keys.append(k)
                        seen.add(k)
        columns_for_stats = [{"name": k} for k in all_keys]
        if isinstance(columns_filter, list) and columns_filter:
            col_set = set(str(c).strip() for c in columns_filter if str(c).strip())
            columns_for_stats = [c for c in columns_for_stats if c["name"] in col_set]
        stats = compute_column_stats(rows=rows, columns=columns_for_stats)
        return {
            "status": "success",
            "dataset_id": dataset_id,
            "name": dataset.name,
            "column_stats": stats,
            "source": "computed",
        }

    return {
        "status": "partial",
        "dataset_id": dataset_id,
        "name": dataset.name,
        "note": "No data available for profiling.",
    }


async def _data_query(server: Any, arguments: Dict[str, Any]) -> Any:
    """Execute ad-hoc SQL query on dataset sample rows using DuckDB."""
    dataset_id = str(arguments.get("dataset_id") or "").strip()
    sql = str(arguments.get("sql") or "").strip()
    limit = min(max(int(arguments.get("limit") or 100), 1), 500)

    if not dataset_id:
        return missing_required_params("data_query", ["dataset_id"], arguments)
    if not sql:
        return missing_required_params("data_query", ["sql"], arguments)

    # Safety: block DDL/DML statements
    if _FORBIDDEN_SQL_PATTERN.search(sql):
        return tool_error(
            "Only SELECT queries are allowed. DDL/DML statements are blocked.",
            code=ErrorCode.VALIDATION_ERROR,
            category=ErrorCategory.VALIDATION,
            status_code=400,
            context={"sql": sql[:200]},
        )

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

    rows = await _load_sample_rows(server, dataset_id, limit=5000)
    if not rows:
        return {
            "status": "error",
            "error": "No data available for query. Upload data first.",
            "dataset_id": dataset_id,
        }

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "error": "DuckDB is not installed. Install with: pip install duckdb",
        }

    conn: Optional[Any] = None
    try:
        import pandas as pd
        conn = duckdb.connect(":memory:")
        # Register sample rows as a queryable table named 'data'
        df = pd.DataFrame(rows)
        conn.register("data", df)
        result = conn.execute(sql).fetchdf()
        result_rows = result.head(limit).to_dict(orient="records")
        masked = [mask_pii(row) if isinstance(row, dict) else row for row in result_rows]
        columns = list(result.columns)
        return {
            "status": "success",
            "dataset_id": dataset_id,
            "name": dataset.name,
            "rows": masked,
            "row_count": len(masked),
            "columns": columns,
            "total_source_rows": len(rows),
            "note": f"Query executed on {len(rows)} sample rows (not full dataset)",
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": f"SQL query failed: {exc}",
            "sql": sql[:500],
            "dataset_id": dataset_id,
        }
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

DATASET_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "dataset_get_by_name": _dataset_get_by_name,
    "dataset_get_latest_version": _dataset_get_latest_version,
    "dataset_validate_columns": _dataset_validate_columns,
    "dataset_list": _dataset_list,
    "dataset_sample": _dataset_sample,
    "dataset_profile": _dataset_profile,
    "data_query": _data_query,
}


def build_dataset_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(DATASET_TOOL_HANDLERS)
