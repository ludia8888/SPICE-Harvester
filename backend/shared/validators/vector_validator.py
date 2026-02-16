"""
Vector validator for SPICE HARVESTER.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .base_validator import BaseValidator, ValidationResult
from .constraint_validator import ConstraintValidator
from shared.utils.json_utils import maybe_decode_json
import logging


class VectorValidator(BaseValidator):
    """Validator for numeric vector payloads."""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        if constraints is None:
            constraints = {}

        normalized = value
        if isinstance(value, str):
            try:
                normalized = json.loads(value)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at shared/validators/vector_validator.py:28", exc_info=True)
                return ValidationResult(is_valid=False, message="Invalid JSON vector format")

        if not isinstance(normalized, list):
            return ValidationResult(is_valid=False, message="Vector must be a list of numbers")

        for item in normalized:
            if not isinstance(item, (int, float)) or isinstance(item, bool):
                return ValidationResult(is_valid=False, message="Vector elements must be numeric")

        dimensions = constraints.get("dimensions") or constraints.get("dimension")
        if dimensions is not None:
            try:
                expected = int(dimensions)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at shared/validators/vector_validator.py:42", exc_info=True)
                return ValidationResult(is_valid=False, message="Invalid vector dimensions constraint")
            if len(normalized) != expected:
                return ValidationResult(
                    is_valid=False,
                    message=f"Vector length {len(normalized)} != {expected}",
                    normalized_value=normalized,
                )

        constraint_result = ConstraintValidator.validate_constraints(
            normalized, "array", constraints
        )
        if not constraint_result.is_valid:
            return ValidationResult(
                is_valid=False,
                message=constraint_result.message,
                normalized_value=normalized,
            )

        return ValidationResult(
            is_valid=True,
            message="Vector validation passed",
            normalized_value=normalized,
        )

    def normalize(self, value: Any) -> Any:
        return maybe_decode_json(value)

    def get_supported_types(self) -> List[str]:
        return ["vector"]
