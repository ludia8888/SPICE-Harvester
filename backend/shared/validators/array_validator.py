"""
Array type validator for SPICE HARVESTER
"""

import json
from typing import Any, Dict, List, Optional
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult
from .constraint_validator import ConstraintValidator

logger = logging.getLogger(__name__)


class ArrayValidator(BaseValidator):
    """Validator for array/list data types"""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate array with constraints"""
        if constraints is None:
            constraints = {}

        # Handle string JSON arrays
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return ValidationResult(is_valid=False, message="Invalid JSON array format")

        # Check type
        if not isinstance(value, (list, tuple)):
            return ValidationResult(
                is_valid=False, message=f"Expected array/list, got {type(value).__name__}"
            )

        # Convert to list for normalization
        normalized = list(value)

        if constraints.get("noNullItems"):
            if any(item is None for item in normalized):
                return ValidationResult(is_valid=False, message="Array items cannot be null")

        if constraints.get("noNestedArrays"):
            if any(isinstance(item, (list, tuple)) for item in normalized):
                return ValidationResult(is_valid=False, message="Nested arrays are not allowed")

        # Validate constraints using ConstraintValidator
        constraint_result = ConstraintValidator.validate_constraints(
            normalized, "array", constraints
        )
        if not constraint_result.is_valid:
            return ValidationResult(
                is_valid=False, message=constraint_result.message, normalized_value=normalized
            )

        # Validate item types if specified
        if "itemType" in constraints:
            result = self._validate_item_types(normalized, constraints["itemType"])
            if not result.is_valid:
                return result
            normalized = result.normalized_value

        return ValidationResult(
            is_valid=True,
            message="Array validation passed",
            normalized_value=normalized,
            metadata={"length": len(normalized), "type": "array"},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize array value"""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                logger.debug(f"Failed to parse JSON array from string '{value}': {e}")
                return value

        if isinstance(value, (list, tuple)):
            return list(value)

        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.ARRAY.value, "array", "list"]

    def _validate_item_types(self, array: List[Any], item_type: str) -> ValidationResult:
        """Validate types of array items"""
        from . import get_validator  # Import here to avoid circular dependency

        normalized = []
        validator = get_validator(item_type)

        if not validator:
            # Basic type validation fallback
            for i, item in enumerate(array):
                if not self._validate_basic_type(item, item_type):
                    return ValidationResult(
                        is_valid=False,
                        message=f"Item {i} validation failed: Expected {item_type}, got {type(item).__name__}",
                        normalized_value=array,
                    )
                normalized.append(item)
        else:
            # Use specialized validator
            for i, item in enumerate(array):
                result = validator.validate(item)
                if not result.is_valid:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Item {i} validation failed: {result.message}",
                        normalized_value=array,
                    )
                normalized.append(result.normalized_value)

        return ValidationResult(
            is_valid=True, message="Item type validation passed", normalized_value=normalized
        )

    def _validate_basic_type(self, value: Any, type_name: str) -> bool:
        """Validate basic types"""
        if type_name in [DataType.STRING.value, "string"]:
            return isinstance(value, str)
        elif type_name in [DataType.INTEGER.value, "integer", "int"]:
            return isinstance(value, int) and not isinstance(value, bool)
        elif type_name in [DataType.FLOAT.value, DataType.DOUBLE.value, "float", "double"]:
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif type_name in [DataType.BOOLEAN.value, "boolean", "bool"]:
            return isinstance(value, bool)
        else:
            # For unknown types, accept any value
            return True
