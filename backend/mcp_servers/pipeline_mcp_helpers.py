from __future__ import annotations

from typing import Any, Dict, List, Tuple


def coerce_to_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def normalize_aggregates(value: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Normalize aggregates list and return (aggregates, warnings)."""
    raw = value if isinstance(value, list) else []
    out: List[Dict[str, Any]] = []
    warnings: List[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            warnings.append(f"aggregates[{idx}]: skipped non-dict item")
            continue
        column = str(item.get("column") or "").strip()
        op = str(item.get("op") or item.get("function") or item.get("agg") or "").strip().lower()
        if not column:
            warnings.append(f"aggregates[{idx}]: skipped item with missing 'column'")
            continue
        if not op:
            warnings.append(f"aggregates[{idx}]: skipped item with missing 'op' (column={column})")
            continue
        alias = str(item.get("alias") or "").strip() or None
        payload: Dict[str, Any] = {"column": column, "op": op}
        if alias:
            payload["alias"] = alias
        out.append(payload)
    return out, warnings


def extract_spark_error_details(run: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract error details from a pipeline run's output_json.
    Returns a dict with error_summary, errors list, and optional stack trace.
    """
    output_json = run.get("output_json") if isinstance(run.get("output_json"), dict) else {}

    errors: List[str] = []
    raw_errors = output_json.get("errors")
    if isinstance(raw_errors, list):
        errors = [str(item).strip() for item in raw_errors if str(item).strip()]

    error_summary = output_json.get("error") or output_json.get("error_message") or ""
    if isinstance(error_summary, dict):
        error_summary = error_summary.get("message") or str(error_summary)

    # Extract stack trace if available
    stack_trace = output_json.get("stack_trace") or output_json.get("traceback") or ""

    # Extract exception type if available
    exception_type = output_json.get("exception_type") or output_json.get("error_type") or ""

    result: Dict[str, Any] = {}
    if errors:
        result["errors"] = errors
    if error_summary:
        result["error_summary"] = str(error_summary)[:500]
    if exception_type:
        result["exception_type"] = str(exception_type)
    if stack_trace:
        result["stack_trace"] = str(stack_trace)[:2000]

    # If no errors extracted but status is FAILED, add a generic message
    if not result:
        result["error_summary"] = "Job failed (no detailed error message available)"

    return result


# ==================== Trimming Constants ====================
# These control how much data is preserved in tool responses.
# Consistent limits help LLMs understand data without context overflow.
TRIM_PREVIEW_ROWS = 10  # Rows in preview results
TRIM_BUILD_OUTPUT_ROWS = 8  # Rows per output in build results
TRIM_BUILD_MAX_OUTPUTS = 10  # Max number of outputs in build results
TRIM_MAX_WARNINGS = 50  # Max warnings to return
TRIM_MAX_ERRORS = 50  # Max errors to return


def trim_preview_payload(preview: Dict[str, Any], *, max_rows: int = TRIM_PREVIEW_ROWS) -> Dict[str, Any]:
    if not isinstance(preview, dict):
        return {}
    output = dict(preview)
    rows = output.get("rows")
    if isinstance(rows, list):
        output["rows"] = rows[: max(0, int(max_rows))]
    return output


def trim_build_output(output_json: Dict[str, Any], *, max_rows: int = TRIM_BUILD_OUTPUT_ROWS) -> Dict[str, Any]:
    if not isinstance(output_json, dict):
        return {}
    out: Dict[str, Any] = {k: v for k, v in output_json.items() if k not in {"outputs"}}
    outputs = output_json.get("outputs")
    if not isinstance(outputs, list):
        return out
    trimmed_outputs: List[Dict[str, Any]] = []
    for item in outputs[:TRIM_BUILD_MAX_OUTPUTS]:
        if not isinstance(item, dict):
            continue
        rows = item.get("rows")
        trimmed = {
            "node_id": item.get("node_id"),
            "dataset_name": item.get("dataset_name") or item.get("datasetName"),
            "row_count": item.get("row_count"),
            "delta_row_count": item.get("delta_row_count") or item.get("deltaRowCount"),
            "columns": item.get("columns"),
            "sample_row_count": item.get("sample_row_count") or item.get("sampleRowCount"),
            "artifact_key": item.get("artifact_key") or item.get("artifactKey"),
            "artifact_prefix": item.get("artifact_prefix") or item.get("artifactPrefix"),
        }
        if isinstance(rows, list):
            trimmed["rows"] = rows[: max(0, int(max_rows))]
        trimmed_outputs.append(trimmed)
    out["outputs"] = trimmed_outputs
    return out
