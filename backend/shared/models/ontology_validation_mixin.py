from __future__ import annotations

import re
from typing import Any, Dict, List


_VALID_CARDINALITIES = {"1:1", "1:n", "n:1", "n:m"}


class CardinalityValidationMixin:
    cardinality: str

    def is_valid_cardinality(self) -> bool:
        return str(getattr(self, "cardinality", "")) in _VALID_CARDINALITIES


class PropertyValueValidationMixin:
    name: str
    type: str
    required: bool
    constraints: Dict[str, Any]

    def validate_value(self, value: Any) -> List[str]:
        errors: List[str] = []

        if bool(getattr(self, "required", False)) and value is None:
            errors.append(f"Property '{self.name}' is required")

        data_type = str(getattr(self, "type", ""))
        if value is not None:
            if data_type == "xsd:string" and not isinstance(value, str):
                errors.append(f"Property '{self.name}' must be a string")
            elif data_type == "xsd:integer" and not isinstance(value, int):
                errors.append(f"Property '{self.name}' must be an integer")
            elif data_type == "xsd:boolean" and not isinstance(value, bool):
                errors.append(f"Property '{self.name}' must be a boolean")

        constraints = getattr(self, "constraints", None)
        if isinstance(constraints, dict) and value is not None:
            if "min" in constraints and value < constraints["min"]:
                errors.append(f"Property '{self.name}' must be >= {constraints['min']}")
            if "max" in constraints and value > constraints["max"]:
                errors.append(f"Property '{self.name}' must be <= {constraints['max']}")
            if "pattern" in constraints and not re.match(str(constraints["pattern"]), str(value)):
                errors.append(f"Property '{self.name}' does not match pattern")

        return errors
