from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from shared.utils.schema_columns import extract_schema_column_names

logger = logging.getLogger(__name__)


def _normalize_sample_key(key: Any, *, strip_bom: bool) -> str:
    name = str(key or "")
    if strip_bom:
        name = name.lstrip("\ufeff") or name
    return name


def _normalize_sample_row(
    row: Dict[str, Any],
    *,
    strip_bom: bool,
    dedupe_keys: bool,
) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in row.items():
        name = _normalize_sample_key(key, strip_bom=strip_bom)
        if not dedupe_keys or name not in normalized:
            normalized[name] = value
            continue
        base = name
        idx = 1
        while f"{base}__{idx}" in normalized:
            idx += 1
        normalized[f"{base}__{idx}"] = value
    return normalized


def extract_sample_rows(
    sample: Any,
    *,
    strip_bom: bool = False,
    dedupe_keys: bool = False,
    columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(sample, dict):
        return []
    rows = sample.get("rows")
    if isinstance(rows, list):
        if rows and isinstance(rows[0], dict):
            return [
                _normalize_sample_row(
                    row,
                    strip_bom=strip_bom,
                    dedupe_keys=dedupe_keys,
                )
                for row in rows
                if isinstance(row, dict)
            ]
        resolved_columns = columns or extract_schema_column_names(
            sample,
            strip_bom=strip_bom,
            dedupe=True,
        )
        output: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, list):
                continue
            output.append(
                {
                    (
                        resolved_columns[idx]
                        if idx < len(resolved_columns)
                        else f"col_{idx}"
                    ): value
                    for idx, value in enumerate(row)
                }
            )
        return output
    data_rows = sample.get("data")
    if isinstance(data_rows, list) and data_rows and isinstance(data_rows[0], dict):
        return [
            _normalize_sample_row(
                row,
                strip_bom=strip_bom,
                dedupe_keys=dedupe_keys,
            )
            for row in data_rows
            if isinstance(row, dict)
        ]
    return []


def extract_sample_matrix(
    sample: Any,
    *,
    strip_bom: bool = False,
) -> Tuple[List[str], List[List[Any]]]:
    columns = extract_schema_column_names(sample, strip_bom=strip_bom, dedupe=True)
    row_dicts = extract_sample_rows(
        sample,
        strip_bom=strip_bom,
        dedupe_keys=True,
        columns=columns,
    )
    if not columns and row_dicts:
        seen: set[str] = set()
        ordered: List[str] = []
        for row in row_dicts:
            for key in row.keys():
                if key in seen:
                    continue
                seen.add(key)
                ordered.append(key)
        columns = ordered
    if not columns:
        return [], []
    return columns, [[row.get(column) for column in columns] for row in row_dicts]


def parse_csv_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    """Parse CSV payload into normalized row dicts."""
    try:
        text = raw_bytes.decode("utf-8", errors="replace")
    except (UnicodeDecodeError, AttributeError) as exc:
        logger.warning("Failed to decode CSV bytes for sample parse: %s", exc, exc_info=True)
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

    header: List[str] = []
    for idx, cell in enumerate(raw_header):
        name = (str(cell or "").strip() or f"column_{idx + 1}").lstrip("\ufeff")
        header.append(name)

    rows: List[Dict[str, Any]] = []
    resolved_limit = max(1, int(max_rows))
    for record in reader:
        if len(rows) >= resolved_limit:
            break
        if not record or not any(str(cell or "").strip() for cell in record):
            continue
        rows.append(
            {
                key: str(record[idx]).strip() if idx < len(record) else ""
                for idx, key in enumerate(header)
            }
        )
    return rows


def parse_excel_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    try:
        import pandas as pd
        from io import BytesIO

        frame = pd.read_excel(BytesIO(raw_bytes))
        resolved_limit = max(1, int(max_rows))
        return frame.fillna("").to_dict(orient="records")[:resolved_limit]
    except (ImportError, ValueError, TypeError, OSError) as exc:
        logger.warning("Failed to parse Excel bytes for sample parse: %s", exc, exc_info=True)
        return []


def parse_json_bytes(raw_bytes: bytes, *, max_rows: int = 200) -> List[Dict[str, Any]]:
    resolved_limit = max(1, int(max_rows))
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Failed to parse JSON bytes, falling back to JSONL scan: %s", exc, exc_info=True)
        text = raw_bytes.decode("utf-8", errors="replace")
        rows: List[Dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
            if len(rows) >= resolved_limit:
                break
        return rows
    if isinstance(payload, dict):
        rows = extract_sample_rows(payload, strip_bom=True, dedupe_keys=True)
        return rows[:resolved_limit]
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return [
            _normalize_sample_row(
                row,
                strip_bom=True,
                dedupe_keys=True,
            )
            for row in payload[:resolved_limit]
            if isinstance(row, dict)
        ]
    return []
