"""
Sheet import service (shared).

This is a domain-neutral transform layer:
- applies field mappings (source_field → target_field)
- coerces values to target xsd types (best-effort)
- reports row/field-level validation errors

Used by:
- BFF import endpoints (Google Sheets / Excel)
- connector sync workers (auto-import after mapping is confirmed)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.validators.money_validator import MoneyValidator


@dataclass(frozen=True)
class FieldMapping:
    source_field: str
    target_field: str


class SheetImportService:
    """Pure helpers; no network/IO."""

    @staticmethod
    def build_column_index(columns: List[Any]) -> Dict[str, int]:
        index: Dict[str, int] = {}
        for i, c in enumerate(columns):
            if c is None:
                continue
            key = str(c).strip()
            if not key:
                continue
            # keep first occurrence for stability
            index.setdefault(key, i)
        return index

    @staticmethod
    def _is_blank(value: Any) -> bool:
        if value is None:
            return True
        return str(value).strip() == ""

    @staticmethod
    def _strip_numeric_affixes(raw: str) -> str:
        """
        Remove common affixes around numeric strings (currency symbols/units/codes, percent).

        This is intentionally conservative and only touches leading/trailing tokens.
        """
        s = raw.strip()
        if not s:
            return s

        # Parentheses for negatives: (123) -> -123
        negative = False
        if s.startswith("(") and s.endswith(")"):
            negative = True
            s = s[1:-1].strip()

        # Leading sign
        sign = ""
        if s.startswith(("+", "-")):
            sign = s[0]
            s = s[1:].strip()
        if negative and sign != "-":
            sign = "-"

        # Trailing percent
        s = s.rstrip()
        if s.endswith("%"):
            s = s[:-1].strip()

        # Strip known currency symbols (prefix/suffix)
        currency_syms = set(MoneyValidator.CURRENCY_SYMBOLS.keys()) | set(MoneyValidator.AMBIGUOUS_SYMBOLS)
        if s and s[0] in currency_syms:
            s = s[1:].strip()
        if s and s[-1] in currency_syms:
            s = s[:-1].strip()

        # Strip common unit words (prefix/suffix)
        unit_words = set(MoneyValidator.CURRENCY_WORD_ALIASES.keys())
        for w in sorted(unit_words, key=len, reverse=True):
            if s.endswith(w):
                s = s[: -len(w)].strip()
                break
        for w in sorted(unit_words, key=len, reverse=True):
            if s.startswith(w):
                s = s[len(w) :].strip()
                break

        # Strip currency codes (3 letters) as prefix/suffix: USD 123 / 123 USD
        m = re.match(r"^([A-Za-z]{3})\s+(.+)$", s)
        if m:
            s = m.group(2).strip()
        m = re.match(r"^(.+?)\s+([A-Za-z]{3})$", s)
        if m:
            s = m.group(1).strip()

        return f"{sign}{s}".strip()

    @classmethod
    def coerce_value(cls, value: Any, *, target_type: str) -> Tuple[Any, Optional[str]]:
        """
        Coerce a cell value into a JSON-serializable value compatible with target_type.

        Returns:
            (coerced_value, error_message)
        """
        if cls._is_blank(value):
            return None, None

        t = (target_type or "xsd:string").strip()

        # Basic string-like types
        if t in {"xsd:string", "xsd:anyURI"}:
            return str(value), None

        # Boolean
        if t == "xsd:boolean":
            if isinstance(value, bool):
                return value, None
            s = str(value).strip().lower()
            if s in {"true", "t", "yes", "y", "1"}:
                return True, None
            if s in {"false", "f", "no", "n", "0"}:
                return False, None
            return None, f"Cannot parse boolean from '{value}'"

        # Date / DateTime
        if t in {"xsd:date", "xsd:dateTime"}:
            if isinstance(value, datetime):
                if t == "xsd:date":
                    return value.date().isoformat(), None
                return value.isoformat(sep=" "), None
            if isinstance(value, date):
                if t == "xsd:dateTime":
                    return datetime.combine(value, datetime.min.time()).isoformat(sep=" "), None
                return value.isoformat(), None

            s = str(value).strip()
            try:
                if t == "xsd:date":
                    # Accept datetime strings by taking the date part, but validate.
                    d = s.split("T", 1)[0].split(" ", 1)[0].strip()
                    # Common separators
                    if "/" in d:
                        d = d.replace("/", "-")
                    if "." in d:
                        d = d.replace(".", "-")
                    # Compact YYYYMMDD
                    if re.fullmatch(r"\d{8}", d):
                        d = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

                    date.fromisoformat(d)  # validate
                    return d, None
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return dt.isoformat(sep=" "), None
            except Exception:
                return None, f"Cannot parse {t} from '{value}'"

        # Numeric types
        if t in {"xsd:integer", "xsd:decimal", "xsd:float", "xsd:double", "xsd:long"}:
            if isinstance(value, bool):
                return None, f"Cannot parse number from boolean '{value}'"

            if isinstance(value, int) and t != "xsd:decimal":
                return int(value), None

            if isinstance(value, (int, float, Decimal)):
                try:
                    dec = Decimal(str(value))
                except Exception:
                    return None, f"Cannot parse number from '{value}'"
            else:
                raw = cls._strip_numeric_affixes(str(value))
                normalized = MoneyValidator._normalize_number_string(raw)
                if normalized is None:
                    return None, f"Cannot parse number from '{value}'"
                try:
                    dec = Decimal(normalized)
                except (ValueError, InvalidOperation):
                    return None, f"Cannot parse number from '{value}'"

            if t in {"xsd:integer", "xsd:long"}:
                if dec != dec.to_integral_value():
                    return None, f"Expected integer but got '{value}'"
                try:
                    return int(dec), None
                except Exception:
                    return None, f"Integer out of range: '{value}'"

            # decimal/float/double -> JSON number (float) or int if integral
            if dec == dec.to_integral_value():
                try:
                    return int(dec), None
                except Exception:
                    pass
            try:
                return float(dec), None
            except Exception:
                return None, f"Decimal out of range: '{value}'"

        # Relationship / unsupported types
        if t == "link" or not t.startswith("xsd:"):
            return None, f"Unsupported target type for import: '{t}'"

        # Default to string for unknown xsd types
        return str(value), None

    @classmethod
    def build_instances(
        cls,
        *,
        columns: List[Any],
        rows: List[List[Any]],
        mappings: List[FieldMapping],
        target_field_types: Dict[str, str],
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Apply mappings and type coercion to build target instances.

        Args:
            columns: Source headers
            rows: Source rows (list-of-lists)
            mappings: Field mappings
            target_field_types: target_field -> xsd type (or 'link')
            max_rows: Optional cap
        """
        col_index = cls.build_column_index(columns)
        warnings: List[str] = []
        errors: List[Dict[str, Any]] = []
        instances: List[Dict[str, Any]] = []
        instance_row_indices: List[int] = []

        if not mappings:
            return {
                "instances": [],
                "errors": [{"message": "No mappings provided"}],
                "warnings": [],
                "stats": {"input_rows": len(rows), "output_instances": 0, "error_rows": 0},
            }

        seen_targets = set()
        for m in mappings:
            if m.target_field in seen_targets:
                warnings.append(f"Duplicate target_field mapping: '{m.target_field}' (last wins)")
            seen_targets.add(m.target_field)

        rows_iter: Iterable[Tuple[int, List[Any]]]
        if max_rows is not None:
            rows_iter = enumerate(rows[: max(0, int(max_rows))])
        else:
            rows_iter = enumerate(rows)

        error_rows = set()
        for row_idx, row in rows_iter:
            out: Dict[str, Any] = {}
            row_has_value = False

            for mapping in mappings:
                source_name = mapping.source_field
                target_name = mapping.target_field

                if target_name not in target_field_types:
                    errors.append(
                        {
                            "row_index": row_idx,
                            "source_field": source_name,
                            "target_field": target_name,
                            "code": "TARGET_FIELD_UNKNOWN",
                            "message": f"Unknown target_field '{target_name}'",
                        }
                    )
                    error_rows.add(row_idx)
                    continue

                if source_name not in col_index:
                    errors.append(
                        {
                            "row_index": row_idx,
                            "source_field": source_name,
                            "target_field": target_name,
                            "code": "SOURCE_FIELD_UNKNOWN",
                            "message": f"Unknown source_field '{source_name}'",
                        }
                    )
                    error_rows.add(row_idx)
                    continue

                idx = col_index[source_name]
                raw = row[idx] if idx < len(row) else None
                target_type = target_field_types[target_name]
                coerced, err = cls.coerce_value(raw, target_type=target_type)
                if err:
                    code = (
                        "UNSUPPORTED_TARGET_TYPE"
                        if target_type == "link" or not str(target_type).startswith("xsd:")
                        else "TYPE_COERCION_FAILED"
                    )
                    errors.append(
                        {
                            "row_index": row_idx,
                            "source_field": source_name,
                            "target_field": target_name,
                            "raw_value": raw,
                            "code": code,
                            "message": err,
                        }
                    )
                    error_rows.add(row_idx)
                    continue

                if coerced is None:
                    continue

                out[target_name] = coerced
                row_has_value = True

            if row_has_value:
                instances.append(out)
                instance_row_indices.append(row_idx)

        stats = {
            "input_rows": len(rows[:max_rows]) if max_rows is not None else len(rows),
            "output_instances": len(instances),
            "error_rows": len(error_rows),
            "error_count": len(errors),
        }
        return {
            "instances": instances,
            "instance_row_indices": instance_row_indices,
            "error_row_indices": sorted(error_rows),
            "errors": errors,
            "warnings": warnings,
            "stats": stats,
        }
