"""
Enum validator for SPICE HARVESTER
"""

from typing import Any, Dict, List, Optional

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class EnumValidator(BaseValidator):
    """Validator for enum data types"""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate enum value"""
        if constraints is None:
            constraints = {}

        # Get allowed values
        allowed_values = constraints.get("enum", [])
        if not allowed_values:
            return ValidationResult(is_valid=False, message="No enum values defined in constraints")

        # Check if value is in allowed list
        if value not in allowed_values:
            return ValidationResult(
                is_valid=False, message=f"Value must be one of: {allowed_values}"
            )

        return ValidationResult(
            is_valid=True,
            message="Valid enum value",
            normalized_value=value,
            metadata={
                "type": "enum",
                "allowed_values": allowed_values,
                "value_index": allowed_values.index(value),
            },
        )

    def normalize(self, value: Any) -> Any:
        """Normalize enum value"""
        # For enums, we typically don't normalize
        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.ENUM.value, "enum"]

    @classmethod
    def create_constraints(cls, allowed_values: List[Any]) -> Dict[str, Any]:
        """
        Create enum constraints

        Args:
            allowed_values: List of allowed values

        Returns:
            Constraints dictionary
        """
        return {"enum": allowed_values}
