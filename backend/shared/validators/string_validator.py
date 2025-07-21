"""
String validator for SPICE HARVESTER
"""

import re
from typing import Any, Dict, List, Optional

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class StringValidator(BaseValidator):
    """Validator for strings with constraints"""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate string with constraints"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        # Handle string-specific formats first
        if "format" in constraints:
            format_type = constraints["format"]

            if format_type == "alphanumeric":
                if not value.replace(" ", "").isalnum():
                    return ValidationResult(
                        is_valid=False, message="String must contain only alphanumeric characters"
                    )
            elif format_type == "alpha":
                if not value.replace(" ", "").isalpha():
                    return ValidationResult(
                        is_valid=False, message="String must contain only alphabetic characters"
                    )
            elif format_type == "numeric":
                if not value.isdigit():
                    return ValidationResult(
                        is_valid=False, message="String must contain only numeric characters"
                    )
            elif format_type == "lowercase":
                if value != value.lower():
                    return ValidationResult(is_valid=False, message="String must be lowercase")
            elif format_type == "uppercase":
                if value != value.upper():
                    return ValidationResult(is_valid=False, message="String must be uppercase")

        # Use ConstraintValidator for remaining string constraints (excluding format)
        from .constraint_validator import ConstraintValidator
        
        # Create constraints copy without string-specific formats to avoid duplicate validation
        constraint_copy = dict(constraints)
        if "format" in constraint_copy:
            format_type = constraint_copy["format"]
            if format_type in ["alphanumeric", "alpha", "numeric", "lowercase", "uppercase"]:
                # Remove these formats as they're handled above
                constraint_copy = {k: v for k, v in constraint_copy.items() if k != "format"}

        result = ConstraintValidator.validate_constraints(value, "string", constraint_copy)

        if not result.is_valid:
            return result

        # Check if string should be trimmed (only once)
        if constraints.get("trim", False):
            value = value.strip()

        # Check blacklist
        if "blacklist" in constraints:
            if value in constraints["blacklist"]:
                return ValidationResult(is_valid=False, message=f"String is blacklisted: {value}")

        # Check whitelist
        if "whitelist" in constraints:
            if value not in constraints["whitelist"]:
                return ValidationResult(
                    is_valid=False, message=f"String must be one of: {constraints['whitelist']}"
                )

        normalized_value = value

        # Apply transformations (trim already applied above if needed)
        if constraints.get("toLowerCase", False):
            normalized_value = normalized_value.lower()
        elif constraints.get("toUpperCase", False):
            normalized_value = normalized_value.upper()

        return ValidationResult(
            is_valid=True,
            message="String validation passed",
            normalized_value=normalized_value,
            metadata={"type": "string", "length": len(normalized_value)},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize string"""
        if not isinstance(value, str):
            return value

        # Basic normalization - strip leading/trailing whitespace
        return value.strip()

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return ["string", "text", "str"]
