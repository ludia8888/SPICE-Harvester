from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from shared.services.pipeline_schema_utils import normalize_schema_type


def infer_xsd_type_from_values(values: Iterable[Any]) -> str:
    sample = list(values)
    if not sample:
        return "xsd:string"

    def is_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return True
        if isinstance(value, str):
            return value.strip().lower() in {"true", "false"}
        return False

    def is_int(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        if isinstance(value, str):
            try:
                int(value.strip())
                return True
            except Exception:
                return False
        return False

    def is_decimal(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, float):
            return True
        if isinstance(value, str):
            try:
                float(value.strip())
                return True
            except Exception:
                return False
        return False

    def is_datetime(value: Any) -> bool:
        if isinstance(value, datetime):
            return True
        if isinstance(value, str):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return True
            except Exception:
                return False
        return False

    if all(is_bool(value) for value in sample):
        return "xsd:boolean"
    if all(is_int(value) for value in sample):
        return "xsd:integer"
    if all(is_decimal(value) for value in sample):
        return "xsd:decimal"
    if all(is_datetime(value) for value in sample):
        return "xsd:dateTime"
    return "xsd:string"


def normalize_cast_target(target: Any) -> str:
    return normalize_schema_type(target)


def spark_type_to_xsd(data_type: Any) -> str:
    if data_type is None:
        return "xsd:string"
    type_name = data_type.__class__.__name__
    if type_name == "BooleanType":
        return "xsd:boolean"
    if type_name in {"IntegerType", "LongType", "ShortType", "ByteType"}:
        return "xsd:integer"
    if type_name in {"FloatType", "DoubleType", "DecimalType"}:
        return "xsd:decimal"
    if type_name in {"DateType", "TimestampType"}:
        return "xsd:dateTime"
    return "xsd:string"
