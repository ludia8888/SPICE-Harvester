from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Iterable, List, Optional, Tuple

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


@dataclass(frozen=True)
class NumericParseResult:
    """
    Enterprise Enhancement (2026-01):
    Result of parsing a numeric string with locale ambiguity detection.

    Attributes:
        value: The parsed numeric value (primary interpretation)
        is_negative: Whether the value is negative
        is_ambiguous: Whether the value could be interpreted differently
        alternative_value: Alternative interpretation if ambiguous
        ambiguity_warning: Human-readable warning about the ambiguity
    """
    value: float
    is_negative: bool
    is_ambiguous: bool
    alternative_value: Optional[float] = None
    ambiguity_warning: Optional[str] = None


@dataclass(frozen=True)
class DateParseResult:
    """
    Enterprise Enhancement (2026-01):
    Result of parsing a date string with format ambiguity detection.

    Attributes:
        value: The parsed datetime (primary interpretation)
        is_ambiguous: Whether the date format is ambiguous
        alternative_value: Alternative interpretation if ambiguous
        ambiguity_warning: Human-readable warning about the ambiguity
        format_used: The format string that was used
    """
    value: datetime
    is_ambiguous: bool
    alternative_value: Optional[datetime] = None
    ambiguity_warning: Optional[str] = None
    format_used: Optional[str] = None


def _normalize_numeric_text(text: str) -> Optional[tuple[str, bool]]:
    """
    Normalize numeric text for parsing. Returns (cleaned_string, is_negative).

    Note: This function uses heuristics for locale detection which may be
    incorrect. Use parse_decimal_text_with_ambiguity for cases where
    locale ambiguity matters.
    """
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


def parse_decimal_text_with_ambiguity(text: str) -> Optional[NumericParseResult]:
    """
    Enterprise Enhancement (2026-01):
    Parse numeric text with locale ambiguity detection.

    Detects cases where the number could be interpreted differently:
    - "1.234" could be 1.234 (US) or 1234 (German thousands separator)
    - "1,234" could be 1234 (US thousands) or 1.234 (European decimal)

    Returns NumericParseResult with ambiguity information, or None if unparseable.
    """
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

    # Check for locale ambiguity
    is_ambiguous = False
    alternative_value: Optional[float] = None
    ambiguity_warning: Optional[str] = None

    if "," in cleaned and "." in cleaned:
        # Both separators present - less ambiguous
        if cleaned.rfind(",") > cleaned.rfind("."):
            # European: 1.234,56
            primary = cleaned.replace(".", "").replace(",", ".")
        else:
            # US: 1,234.56
            primary = cleaned.replace(",", "")
    elif "," in cleaned:
        if cleaned.count(",") == 1:
            head, tail = cleaned.split(",")
            if 1 <= len(tail) <= 2 and head.isdigit() and tail.isdigit():
                # Could be European decimal: 1,23 = 1.23
                primary = f"{head}.{tail}"
                # Also could be US thousands: 1,234 = 1234
                if len(tail) == 3 and len(head) <= 3:
                    is_ambiguous = True
                    alternative_value = float(head + tail)
                    if negative:
                        alternative_value = -alternative_value
                    ambiguity_warning = (
                        f"LOCALE AMBIGUITY: '{text}' could be {primary} (European decimal) "
                        f"or {head + tail} (US thousands). Assumed European decimal. "
                        "Verify locale or use explicit decimal format."
                    )
            else:
                primary = cleaned.replace(",", "")
        else:
            primary = cleaned.replace(",", "")
    elif "." in cleaned:
        if cleaned.count(".") == 1:
            head, tail = cleaned.split(".")
            # Single period with 3 digits after could be ambiguous
            if len(tail) == 3 and head.isdigit() and tail.isdigit():
                # Could be US decimal: 1.234 = 1.234
                # Or German thousands: 1.234 = 1234
                primary = cleaned  # Assume US decimal
                is_ambiguous = True
                alternative_value = float(head + tail)
                if negative:
                    alternative_value = -alternative_value
                ambiguity_warning = (
                    f"LOCALE AMBIGUITY: '{text}' could be {cleaned} (decimal) "
                    f"or {head + tail} (thousands separator). Assumed decimal. "
                    "This could cause 1000x calculation errors. Verify locale."
                )
            else:
                primary = cleaned
        else:
            # Multiple periods - likely thousands separators
            last = cleaned.split(".")[-1]
            if last.isdigit() and len(last) == 3:
                primary = cleaned.replace(".", "")
            else:
                primary = cleaned
    else:
        primary = cleaned

    if not re.match(r"^\d+(?:\.\d+)?$", primary):
        return None

    value = float(primary)
    if negative:
        value = -value

    return NumericParseResult(
        value=value,
        is_negative=negative,
        is_ambiguous=is_ambiguous,
        alternative_value=alternative_value,
        ambiguity_warning=ambiguity_warning,
    )


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
    """Parse datetime text, returning the primary interpretation."""
    result = parse_datetime_text_with_ambiguity(text, allow_ambiguous=allow_ambiguous)
    return result.value if result else None


def parse_datetime_text_with_ambiguity(
    text: str,
    *,
    allow_ambiguous: bool = True,
) -> Optional[DateParseResult]:
    """
    Enterprise Enhancement (2026-01):
    Parse datetime text with format ambiguity detection.

    Detects cases where the date format is ambiguous:
    - "01/02/2024" could be Jan 2 (US) or Feb 1 (European)
    - "03/04/2024" is ambiguous between March 4 and April 3

    Returns DateParseResult with ambiguity information, or None if unparseable.
    """
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
        result = datetime.fromisoformat(normalized)
        return DateParseResult(
            value=result,
            is_ambiguous=False,
            format_used="ISO-8601",
        )
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
            result = datetime.strptime(base, fmt)
            return DateParseResult(
                value=result,
                is_ambiguous=False,
                format_used=fmt,
            )
        except Exception:
            continue

    if "/" in base or "-" in base:
        parts = re.split(r"[/-]", base.split(" ")[0])
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            first, second, year = (int(parts[0]), int(parts[1]), int(parts[2]))
            if year >= 1000:
                separator = "/" if "/" in base else "-"
                month_first = f"%m{separator}%d{separator}%Y"
                day_first = f"%d{separator}%m{separator}%Y"

                is_ambiguous = False
                alternative_value: Optional[datetime] = None
                ambiguity_warning: Optional[str] = None

                if first > 12 and second <= 12:
                    # Unambiguous: first must be day (European format)
                    candidates = [(day_first, False)]
                elif second > 12 and first <= 12:
                    # Unambiguous: second must be day (US format)
                    candidates = [(month_first, False)]
                elif first <= 12 and second <= 12:
                    # AMBIGUOUS: both could be month or day
                    if allow_ambiguous:
                        candidates = [(month_first, True)]
                        is_ambiguous = True
                        # Try to parse alternative
                        try:
                            alternative_value = datetime.strptime(base, day_first)
                            ambiguity_warning = (
                                f"DATE FORMAT AMBIGUITY: '{text}' could be "
                                f"{first}/{second}/{year} (US: month/day) = {first:02d}/{second:02d}/{year} "
                                f"or {second}/{first}/{year} (European: day/month) = {second:02d}/{first:02d}/{year}. "
                                "Assumed US format (month/day). Verify date format for your data source."
                            )
                        except Exception:
                            pass
                    else:
                        candidates = []
                else:
                    candidates = []

                for fmt, amb in candidates:
                    try:
                        result = datetime.strptime(base, fmt)
                        return DateParseResult(
                            value=result,
                            is_ambiguous=is_ambiguous,
                            alternative_value=alternative_value,
                            ambiguity_warning=ambiguity_warning,
                            format_used=fmt,
                        )
                    except Exception:
                        continue
    return None


def infer_xsd_type_from_values(values: Iterable[Any]) -> str:
    sample = list(values)
    if not sample:
        return "xsd:string"

    from shared.services.pipeline.pipeline_value_predicates import (
        is_bool_like,
        is_datetime_like,
        is_decimal_like,
        is_int_like,
    )

    if all(is_bool_like(value) for value in sample):
        return "xsd:boolean"
    if all(is_int_like(value) for value in sample):
        return "xsd:integer"
    if all(is_decimal_like(value) for value in sample):
        return "xsd:decimal"
    if all(is_datetime_like(value, allow_ambiguous=False) for value in sample):
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
