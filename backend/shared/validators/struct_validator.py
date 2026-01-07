"""
Struct validator for SPICE HARVESTER.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base_validator import BaseValidator, ValidationResult
from .object_validator import ObjectValidator


class StructValidator(BaseValidator):
    """Validator for struct values (flat object without nested arrays/objects)."""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        if constraints is None:
            constraints = {}

        object_validator = ObjectValidator()
        object_result = object_validator.validate(value, constraints)
        if not object_result.is_valid:
            return object_result

        normalized = object_result.normalized_value
        if not isinstance(normalized, dict):
            return ValidationResult(is_valid=False, message="Struct must be an object")

        disallow_nested = bool(constraints.get("noNestedStructs", True))
        disallow_arrays = bool(constraints.get("noArrayFields", True))

        if disallow_nested or disallow_arrays:
            for field_value in normalized.values():
                if disallow_arrays and isinstance(field_value, (list, tuple)):
                    return ValidationResult(is_valid=False, message="Struct fields cannot be arrays")
                if disallow_nested and isinstance(field_value, dict):
                    return ValidationResult(is_valid=False, message="Nested structs are not allowed")

        return ValidationResult(
            is_valid=True,
            message="Struct validation passed",
            normalized_value=normalized,
        )

    def normalize(self, value: Any) -> Any:
        return ObjectValidator().normalize(value)

    def get_supported_types(self) -> List[str]:
        return ["struct"]
