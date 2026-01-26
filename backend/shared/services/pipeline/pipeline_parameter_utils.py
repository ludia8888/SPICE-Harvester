from __future__ import annotations

from typing import Any, Dict


def normalize_parameters(parameters_raw: Any) -> Dict[str, Any]:
    if not isinstance(parameters_raw, list):
        return {}
    output: Dict[str, Any] = {}
    for param in parameters_raw:
        if not isinstance(param, dict):
            continue
        name = str(param.get("name") or "").strip()
        if not name:
            continue
        value = param.get("value")
        output[name] = value
        output[f"${name}"] = value
    return output


def apply_parameters(expression: str, parameters: Dict[str, Any]) -> str:
    if not parameters:
        return expression
    output = expression
    for name, value in parameters.items():
        token = f"${name}"
        if token in output:
            replacement = value
            if isinstance(value, str) and not value.isnumeric() and not value.startswith("'"):
                replacement = f"'{value}'"
            output = output.replace(token, str(replacement))
        brace_token = f"{{{{{name}}}}}"
        if brace_token in output:
            output = output.replace(brace_token, str(value))
    return output
