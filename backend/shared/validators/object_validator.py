"""
Object/JSON validator for SPICE HARVESTER
"""

import json
from typing import Any, Dict, List, Optional
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

logger = logging.getLogger(__name__)


class ObjectValidator(BaseValidator):
    """Validator for object/JSON data types"""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate object/JSON with constraints"""
        if constraints is None:
            constraints = {}

        # Handle string JSON objects
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return ValidationResult(is_valid=False, message="Invalid JSON object format")

        # Check type
        if not isinstance(value, dict):
            return ValidationResult(
                is_valid=False, message=f"Expected object/dict, got {type(value).__name__}"
            )

        # Check required fields
        if "required" in constraints:
            for field in constraints["required"]:
                if field not in value:
                    return ValidationResult(
                        is_valid=False, message=f"Required field '{field}' is missing"
                    )

        # Check additional properties
        if not constraints.get("additionalProperties", True):
            if "schema" in constraints:
                allowed_fields = set(constraints["schema"].keys())
                actual_fields = set(value.keys())
                extra_fields = actual_fields - allowed_fields
                if extra_fields:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Additional properties not allowed: {list(extra_fields)}",
                    )

        # Validate schema
        normalized = dict(value)
        if "schema" in constraints:
            schema = constraints["schema"]
            for field, field_schema in schema.items():
                if field in value:
                    field_type = field_schema.get("type")
                    if field_type:
                        # Validate field type
                        field_result = self._validate_field_type(
                            value[field], field_type, field_schema.get("constraints")
                        )
                        if not field_result.is_valid:
                            return ValidationResult(
                                is_valid=False,
                                message=f"Field '{field}' validation failed: {field_result.message}",
                                normalized_value=value,
                            )
                        if field_result.normalized_value is not None:
                            normalized[field] = field_result.normalized_value

        return ValidationResult(
            is_valid=True,
            message="Object validation passed",
            normalized_value=normalized,
            metadata={"type": "object", "field_count": len(normalized)},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize object value"""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logger.debug(f"Failed to parse JSON object from string '{value}': {e}")
                return value

        if isinstance(value, dict):
            return dict(value)

        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.OBJECT.value, "object", "json", "dict"]

    def _validate_field_type(
        self, value: Any, field_type: str, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate individual field type"""
        from . import get_validator  # Import here to avoid circular dependency

        # Try to get specialized validator
        validator = get_validator(field_type)
        if validator:
            return validator.validate(value, constraints)

        # Fallback to basic type validation
        if field_type in [DataType.STRING.value, "string"]:
            if not isinstance(value, str):
                return ValidationResult(
                    is_valid=False, message=f"Expected string, got {type(value).__name__}"
                )
        elif field_type in [DataType.INTEGER.value, "integer", "int"]:
            if not isinstance(value, int) or isinstance(value, bool):
                return ValidationResult(
                    is_valid=False, message=f"Expected integer, got {type(value).__name__}"
                )
        elif field_type in [
            DataType.FLOAT.value,
            DataType.DOUBLE.value,
            "float",
            "double",
            "number",
        ]:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return ValidationResult(
                    is_valid=False, message=f"Expected number, got {type(value).__name__}"
                )
        elif field_type in [DataType.BOOLEAN.value, "boolean", "bool"]:
            if not isinstance(value, bool):
                return ValidationResult(
                    is_valid=False, message=f"Expected boolean, got {type(value).__name__}"
                )

        return ValidationResult(
            is_valid=True, message="Field validation passed", normalized_value=value
        )
