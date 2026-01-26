from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Iterable, Optional

from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type

_XSD_TO_SPARK_TYPE: dict[str, str] = {
    "xsd:string": "string",
    "xsd:boolean": "boolean",
    "xsd:integer": "bigint",
    "xsd:decimal": "double",
    "xsd:dateTime": "timestamp",
    "xsd:date": "date",
}

_CURRENCY_CODE_RE = re.compile(r"^(?:[A-Za-z]{1,5})\s+|\s+(?:[A-Za-z]{1,5})$")
_TIMEZONE_SUFFIX_RE = re.compile(r"\s*(UTC|GMT)$", re.IGNORECASE)
_TIMEZONE_COLON_RE = re.compile(r"([+-]\d{2}):(\d{2})$")


def _normalize_numeric_text(text: str) -> Optional[tuple[str, bool]]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1].strip()
    cleaned = cleaned.replace("$", "")
    cleaned = _CURRENCY_CODE_RE.sub("", cleaned)
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1].strip()
    if cleaned.startswith(("+", "-")):
        if cleaned[0] == "-":
            negative = True
        cleaned = cleaned[1:].strip()
    cleaned = re.sub(r"[\s_]", "", cleaned)
    if not cleaned:
        return None

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "")
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        if cleaned.count(",") == 1:
            head, tail = cleaned.split(",")
            if 1 <= len(tail) <= 2 and head.isdigit() and tail.isdigit():
                cleaned = f"{head}.{tail}"
            else:
                cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(",", "")
    elif cleaned.count(".") > 1:
        last = cleaned.split(".")[-1]
        if last.isdigit() and len(last) == 3:
            cleaned = cleaned.replace(".", "")
    return cleaned, negative


def parse_int_text(text: str) -> Optional[int]:
    normalized = _normalize_numeric_text(text)
    if not normalized:
        return None
    cleaned, negative = normalized
    if not re.match(r"^\d+$", cleaned):
        return None
    value = int(cleaned)
    return -value if negative else value


def parse_decimal_text(text: str) -> Optional[float]:
    normalized = _normalize_numeric_text(text)
    if not normalized:
        return None
    cleaned, negative = normalized
    if not re.match(r"^\d+(?:\.\d+)?$", cleaned):
        return None
    value = float(cleaned)
    return -value if negative else value


def parse_datetime_text(text: str, *, allow_ambiguous: bool = True) -> Optional[datetime]:
    raw = str(text or "").strip()
    if not raw:
        return None
    normalized = _TIMEZONE_SUFFIX_RE.sub("+00:00", raw)
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    match = _TIMEZONE_COLON_RE.search(normalized)
    if match:
        normalized = normalized[: match.start()] + f"{match.group(1)}{match.group(2)}"

    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        pass

    base = normalized.replace("T", " ")
    formats = [
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M%z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y.%m.%d %H:%M:%S",
        "%Y.%m.%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(base, fmt)
        except Exception:
            continue

    if "/" in base or "-" in base:
        parts = re.split(r"[/-]", base.split(" ")[0])
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            first, second, year = (int(parts[0]), int(parts[1]), int(parts[2]))
            if year >= 1000:
                month_first = "%m/%d/%Y" if "/" in base else "%m-%d-%Y"
                day_first = "%d/%m/%Y" if "/" in base else "%d-%m-%Y"
                if first > 12 and second <= 12:
                    candidates = [day_first]
                elif second > 12 and first <= 12:
                    candidates = [month_first]
                elif allow_ambiguous:
                    candidates = [month_first, day_first]
                else:
                    candidates = []
                for fmt in candidates:
                    try:
                        return datetime.strptime(base, fmt)
                    except Exception:
                        continue
    return None


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
            return parse_int_text(value) is not None
        return False

    def is_decimal(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, float):
            return True
        if isinstance(value, str):
            return parse_decimal_text(value) is not None
        return False

    def is_datetime(value: Any) -> bool:
        if isinstance(value, datetime):
            return True
        if isinstance(value, str):
            return parse_datetime_text(value, allow_ambiguous=False) is not None
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


def normalize_cast_mode(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"SAFE_NULL", "STRICT"}:
        return normalized
    return "SAFE_NULL"


def xsd_to_spark_type(value: Any) -> str:
    normalized = normalize_schema_type(value)
    return _XSD_TO_SPARK_TYPE.get(normalized, "string")


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
