"""
Marking validator for SPICE HARVESTER.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base_validator import BaseValidator, ValidationResult
from .constraint_validator import ConstraintValidator


class MarkingValidator(BaseValidator):
    """Validator for marking values."""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        if constraints is None:
            constraints = {}

        if not isinstance(value, str):
            return ValidationResult(is_valid=False, message="Marking value must be a string")

        constraint_result = ConstraintValidator.validate_constraints(value, "string", constraints)
        if not constraint_result.is_valid:
            return ValidationResult(is_valid=False, message=constraint_result.message)

        return ValidationResult(is_valid=True, message="Marking validation passed", normalized_value=value)

    def normalize(self, value: Any) -> Any:
        return value

    def get_supported_types(self) -> List[str]:
        return ["marking"]
