"""
Constraint validator for SPICE HARVESTER
Provides enhanced constraint validation for complex types
"""

import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from .base_validator import ValidationResult


class ConstraintValidator:
    """Enhanced constraint validation for complex types"""

    @classmethod
    def validate_constraints(
        cls, value: Any, data_type: str, constraints: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate a value against a set of constraints

        Args:
            value: The value to validate
            data_type: The data type of the value
            constraints: Dictionary of constraints to apply

        Returns:
            ValidationResult
        """
        # String constraints
        if isinstance(value, str):
            result = cls._validate_string_constraints(value, constraints)
            if not result.is_valid:
                return result

        # Numeric constraints
        if isinstance(value, (int, float, Decimal)):
            result = cls._validate_numeric_constraints(value, constraints)
            if not result.is_valid:
                return result

        # Collection constraints
        if isinstance(value, (list, tuple, set)):
            result = cls._validate_collection_constraints(value, constraints)
            if not result.is_valid:
                return result

        # Format constraints
        if "format" in constraints:
            result = cls._validate_format(value, constraints["format"])
            if not result.is_valid:
                return result

        # Pattern constraints
        if "pattern" in constraints and isinstance(value, str):
            result = cls._validate_pattern(value, constraints["pattern"])
            if not result.is_valid:
                return result

        # Custom validation function
        if "validator" in constraints and callable(constraints["validator"]):
            result = cls._validate_custom(value, constraints["validator"])
            if not result.is_valid:
                return result

        return ValidationResult(
            is_valid=True, message="All constraints validated", normalized_value=value
        )

    @classmethod
    def _validate_string_constraints(
        cls, value: str, constraints: Dict[str, Any]
    ) -> ValidationResult:
        """Validate string-specific constraints"""
        # Length constraints
        if "minLength" in constraints and len(value) < constraints["minLength"]:
            return ValidationResult(
                is_valid=False,
                message=f"String too short: {len(value)} < {constraints['minLength']}",
            )

        if "maxLength" in constraints and len(value) > constraints["maxLength"]:
            return ValidationResult(
                is_valid=False,
                message=f"String too long: {len(value)} > {constraints['maxLength']}",
            )

        # Character constraints
        if "allowedChars" in constraints:
            allowed = set(constraints["allowedChars"])
            invalid_chars = [c for c in value if c not in allowed]
            if invalid_chars:
                return ValidationResult(
                    is_valid=False, message=f"Invalid characters found: {invalid_chars}"
                )

        # Case constraints
        if constraints.get("uppercase") and value != value.upper():
            return ValidationResult(is_valid=False, message="String must be uppercase")

        if constraints.get("lowercase") and value != value.lower():
            return ValidationResult(is_valid=False, message="String must be lowercase")

        return ValidationResult(is_valid=True)

    @classmethod
    def _validate_numeric_constraints(
        cls, value: Any, constraints: Dict[str, Any]
    ) -> ValidationResult:
        """Validate numeric constraints"""
        num_value = float(value)

        # Range constraints
        if "minimum" in constraints and num_value < constraints["minimum"]:
            return ValidationResult(
                is_valid=False, message=f"Value too small: {num_value} < {constraints['minimum']}"
            )

        if "maximum" in constraints and num_value > constraints["maximum"]:
            return ValidationResult(
                is_valid=False, message=f"Value too large: {num_value} > {constraints['maximum']}"
            )

        # Exclusive range constraints
        if "exclusiveMinimum" in constraints and num_value <= constraints["exclusiveMinimum"]:
            return ValidationResult(
                is_valid=False,
                message=f"Value must be greater than {constraints['exclusiveMinimum']}",
            )

        if "exclusiveMaximum" in constraints and num_value >= constraints["exclusiveMaximum"]:
            return ValidationResult(
                is_valid=False, message=f"Value must be less than {constraints['exclusiveMaximum']}"
            )

        # Multiple constraints
        if "multipleOf" in constraints:
            multiple = constraints["multipleOf"]
            if multiple != 0 and num_value % multiple != 0:
                return ValidationResult(
                    is_valid=False, message=f"Value must be multiple of {multiple}"
                )

        # Precision constraints
        if "precision" in constraints and isinstance(value, (float, Decimal)):
            str_value = str(value)
            if "." in str_value:
                decimal_places = len(str_value.split(".")[1])
                if decimal_places > constraints["precision"]:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Too many decimal places: {decimal_places} > {constraints['precision']}",
                    )

        return ValidationResult(is_valid=True)

    @classmethod
    def _validate_collection_constraints(
        cls, value: Any, constraints: Dict[str, Any]
    ) -> ValidationResult:
        """Validate collection constraints"""
        collection = list(value)

        # Size constraints (support both array and collection terminology)
        min_items = constraints.get("minItems") or constraints.get("minLength")
        max_items = constraints.get("maxItems") or constraints.get("maxLength")
        
        if min_items is not None and len(collection) < min_items:
            return ValidationResult(
                is_valid=False,
                message=f"Too few items: {len(collection)} < {min_items}",
            )

        if max_items is not None and len(collection) > max_items:
            return ValidationResult(
                is_valid=False,
                message=f"Too many items: {len(collection)} > {max_items}",
            )

        # Uniqueness constraint
        if constraints.get("uniqueItems"):
            # Convert to strings for comparison to handle complex types
            str_items = [str(item) for item in collection]
            if len(str_items) != len(set(str_items)):
                return ValidationResult(
                    is_valid=False, message="Collection contains duplicate items"
                )

        # Contains constraint
        if "contains" in constraints:
            required_item = constraints["contains"]
            if required_item not in collection:
                return ValidationResult(
                    is_valid=False, message=f"Collection must contain: {required_item}"
                )

        # Not contains constraint
        if "notContains" in constraints:
            forbidden_item = constraints["notContains"]
            if forbidden_item in collection:
                return ValidationResult(
                    is_valid=False, message=f"Collection must not contain: {forbidden_item}"
                )

        return ValidationResult(is_valid=True)

    @classmethod
    def _validate_format(cls, value: Any, format_name: str) -> ValidationResult:
        """Validate common format constraints"""
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False,
                message=f"Format validation requires string, got {type(value).__name__}",
            )

        # Common formats
        formats = {
            "date": r"^\d{4}-\d{2}-\d{2}$",
            "time": r"^\d{2}:\d{2}:\d{2}(?:\.\d+)?$",
            "date-time": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$",
            "email": r"^[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}$",
            "hostname": r"^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$",
            "ipv4": r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
            "ipv6": r"^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$",
            "uri": r"^[a-zA-Z][a-zA-Z0-9+.-]*:",
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "credit-card": r"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})$",
            "isbn": r"^(?:ISBN(?:-1[03])?:? )?(?=[0-9X]{10}$|(?=(?:[0-9]+[- ]){3})[- 0-9X]{13}$|97[89][0-9]{10}$|(?=(?:[0-9]+[- ]){4})[- 0-9]{17}$)(?:97[89][- ]?)?[0-9]{1,5}[- ]?[0-9]+[- ]?[0-9]+[- ]?[0-9X]$",
            "alpha": r"^[a-zA-Z]+$",
            "alphanumeric": r"^[a-zA-Z0-9]+$",
            "numeric": r"^[0-9]+$",
            "hexadecimal": r"^[0-9a-fA-F]+$",
            "base64": r"^[A-Za-z0-9+/]*={0,2}$",
        }

        if format_name not in formats:
            return ValidationResult(is_valid=False, message=f"Unknown format: {format_name}")

        pattern = formats[format_name]
        if not re.match(pattern, value):
            return ValidationResult(is_valid=False, message=f"Invalid {format_name} format")

        return ValidationResult(is_valid=True)

    @classmethod
    def _validate_pattern(cls, value: str, pattern: str) -> ValidationResult:
        """Validate against regex pattern"""
        try:
            if not re.match(pattern, value):
                return ValidationResult(
                    is_valid=False, message=f"Value does not match pattern: {pattern}"
                )
        except re.error as e:
            return ValidationResult(is_valid=False, message=f"Invalid regex pattern: {e}")

        return ValidationResult(is_valid=True)

    @classmethod
    def _validate_custom(cls, value: Any, validator_func: Callable[[Any], Any]) -> ValidationResult:
        """Validate using custom validation function"""
        try:
            result = validator_func(value)
            if isinstance(result, bool):
                return ValidationResult(
                    is_valid=result, message="" if result else "Custom validation failed"
                )
            elif isinstance(result, tuple) and len(result) >= 2:
                return ValidationResult(
                    is_valid=result[0], message=result[1] if len(result) > 1 else ""
                )
            elif isinstance(result, ValidationResult):
                return result
            else:
                return ValidationResult(
                    is_valid=False, message="Invalid return from custom validator"
                )
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Custom validation error: {str(e)}")

    @classmethod
    def merge_constraints(cls, *constraint_sets: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple constraint sets with proper precedence

        Later constraint sets override earlier ones
        """
        merged = {}
        for constraints in constraint_sets:
            if constraints:
                merged.update(constraints)
        return merged

    @classmethod
    def validate_constraint_compatibility(cls, constraints: Dict[str, Any]) -> ValidationResult:
        """
        Check if constraints are compatible with each other

        For example, minLength > maxLength would be invalid
        """
        # Check string length constraints
        if "minLength" in constraints and "maxLength" in constraints:
            if constraints["minLength"] > constraints["maxLength"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"Incompatible constraints: minLength ({constraints['minLength']}) > maxLength ({constraints['maxLength']})",
                )

        # Check numeric range constraints
        if "minimum" in constraints and "maximum" in constraints:
            if constraints["minimum"] > constraints["maximum"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"Incompatible constraints: minimum ({constraints['minimum']}) > maximum ({constraints['maximum']})",
                )

        # Check collection size constraints (support both array and collection terminology)
        min_items = constraints.get("minItems") or constraints.get("minLength")
        max_items = constraints.get("maxItems") or constraints.get("maxLength")
        
        if min_items is not None and max_items is not None:
            if min_items > max_items:
                constraint_names = "minItems/minLength" if "minItems" in constraints or "minLength" in constraints else "minItems"
                constraint_names += f" ({min_items}) > "
                constraint_names += "maxItems/maxLength" if "maxItems" in constraints or "maxLength" in constraints else "maxItems"
                constraint_names += f" ({max_items})"
                return ValidationResult(
                    is_valid=False,
                    message=f"Incompatible constraints: {constraint_names}",
                )

        return ValidationResult(is_valid=True, message="Constraints are compatible")
