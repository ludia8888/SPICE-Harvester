from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Optional


@dataclass(frozen=True)
class SchemaCheckSpec:
    rule: str
    column: str
    value: Any


@dataclass(frozen=True)
class ExpectationSpec:
    rule: str
    column: str
    value: Any


@dataclass(frozen=True)
class SchemaContractSpec:
    column: str
    expected_type: str
    required: bool


def normalize_schema_type(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip().lower()
    if not raw:
        return ""
    if raw.startswith("xsd:"):
        return raw
    mapping = {
        "string": "xsd:string",
        "text": "xsd:string",
        "str": "xsd:string",
        "integer": "xsd:integer",
        "int": "xsd:integer",
        "long": "xsd:integer",
        "float": "xsd:decimal",
        "double": "xsd:decimal",
        "decimal": "xsd:decimal",
        "number": "xsd:decimal",
        "bool": "xsd:boolean",
        "boolean": "xsd:boolean",
        "date": "xsd:dateTime",
        "datetime": "xsd:dateTime",
        "timestamp": "xsd:dateTime",
    }
    return mapping.get(raw, raw)


def normalize_schema_checks(checks: Any) -> List[SchemaCheckSpec]:
    if not isinstance(checks, list):
        return []
    output: List[SchemaCheckSpec] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        rule = str(check.get("rule") or "").strip().lower()
        column = str(check.get("column") or "").strip()
        value = check.get("value")
        if not rule or not column:
            continue
        output.append(SchemaCheckSpec(rule=rule, column=column, value=value))
    return output


def normalize_expectations(expectations: Any) -> List[ExpectationSpec]:
    if not isinstance(expectations, list):
        return []
    output: List[ExpectationSpec] = []
    for exp in expectations:
        if not isinstance(exp, dict):
            continue
        rule = str(exp.get("rule") or "").strip().lower()
        column = str(exp.get("column") or "").strip()
        value = exp.get("value")
        if not rule:
            continue
        output.append(ExpectationSpec(rule=rule, column=column, value=value))
    return output


def normalize_schema_contract(contract: Any) -> List[SchemaContractSpec]:
    if not isinstance(contract, list):
        return []
    output: List[SchemaContractSpec] = []
    for item in contract:
        if not isinstance(item, dict):
            continue
        column = str(item.get("column") or "").strip()
        if not column:
            continue
        required = item.get("required")
        required = True if required is None else bool(required)
        expected_type = normalize_schema_type(item.get("type"))
        output.append(SchemaContractSpec(column=column, expected_type=expected_type, required=required))
    return output


def normalize_value_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [value]


def normalize_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
