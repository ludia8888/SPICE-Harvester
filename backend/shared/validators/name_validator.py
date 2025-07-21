"""
Name validator for SPICE HARVESTER
Supports various naming conventions and specific name patterns
"""

import re
from enum import Enum
from typing import Any, Dict, List, Optional

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class NamingConvention(Enum):
    """Supported naming conventions"""

    CAMEL_CASE = "camelCase"
    PASCAL_CASE = "PascalCase"
    SNAKE_CASE = "snake_case"
    KEBAB_CASE = "kebab-case"
    UPPER_SNAKE_CASE = "UPPER_SNAKE_CASE"
    LOWER_CASE = "lowercase"
    UPPER_CASE = "UPPERCASE"
    DOT_NOTATION = "dot.notation"
    PATH_NOTATION = "path/notation"
    CUSTOM = "custom"


class NameValidator(BaseValidator):
    """Validator for various naming conventions and patterns"""

    # Predefined patterns for common naming conventions
    PATTERNS = {
        NamingConvention.CAMEL_CASE: r"^[a-z][a-zA-Z0-9]*$",
        NamingConvention.PASCAL_CASE: r"^[A-Z][a-zA-Z0-9]*$",
        NamingConvention.SNAKE_CASE: r"^[a-z][a-z0-9_]*$",
        NamingConvention.KEBAB_CASE: r"^[a-z][a-z0-9-]*$",
        NamingConvention.UPPER_SNAKE_CASE: r"^[A-Z][A-Z0-9_]*$",
        NamingConvention.LOWER_CASE: r"^[a-z]+$",
        NamingConvention.UPPER_CASE: r"^[A-Z]+$",
        NamingConvention.DOT_NOTATION: r"^[a-zA-Z][a-zA-Z0-9]*(\.[a-zA-Z][a-zA-Z0-9]*)*$",
        NamingConvention.PATH_NOTATION: r"^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$",
    }

    # Specific patterns for database/system names
    SPECIFIC_PATTERNS = {
        "database_name": r"^[a-zA-Z0-9_-]{1,63}$",
        "class_id": r"^[a-zA-Z0-9_-]{1,100}$",
        "branch_name": r"^[a-zA-Z0-9_/-]{1,100}$",
        "predicate": r"^[a-zA-Z][a-zA-Z0-9_]*$",
        "identifier": r"^[a-zA-Z_][a-zA-Z0-9_]*$",
        "slug": r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "filename": r"^[a-zA-Z0-9_.-]+$",
        "variable": r"^[a-zA-Z_$][a-zA-Z0-9_$]*$",
    }

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate name according to constraints"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        # Empty check
        if not value:
            return ValidationResult(is_valid=False, message="Name cannot be empty")

        # Get validation parameters
        convention = constraints.get("convention")
        pattern = constraints.get("pattern")
        specific_type = constraints.get("type")
        min_length = constraints.get("minLength", 1)
        max_length = constraints.get("maxLength", None)

        # Length validation
        if len(value) < min_length:
            return ValidationResult(
                is_valid=False, message=f"Name must be at least {min_length} characters long"
            )

        if max_length and len(value) > max_length:
            return ValidationResult(
                is_valid=False, message=f"Name must be at most {max_length} characters long"
            )

        # Specific type validation (highest priority)
        if specific_type and specific_type in self.SPECIFIC_PATTERNS:
            pattern_to_use = self.SPECIFIC_PATTERNS[specific_type]
            if not re.match(pattern_to_use, value):
                return ValidationResult(is_valid=False, message=f"Invalid {specific_type} format")
        # Custom pattern validation
        elif pattern:
            try:
                if not re.match(pattern, value):
                    return ValidationResult(
                        is_valid=False, message=f"Name does not match required pattern: {pattern}"
                    )
            except re.error:
                return ValidationResult(is_valid=False, message=f"Invalid regex pattern: {pattern}")
        # Convention validation
        elif convention:
            try:
                conv_enum = (
                    NamingConvention(convention) if isinstance(convention, str) else convention
                )
                if conv_enum in self.PATTERNS:
                    pattern_to_use = self.PATTERNS[conv_enum]
                    if not re.match(pattern_to_use, value):
                        return ValidationResult(
                            is_valid=False,
                            message=f"Name does not follow {conv_enum.value} convention",
                        )
            except ValueError:
                return ValidationResult(
                    is_valid=False, message=f"Unknown naming convention: {convention}"
                )

        # Check for forbidden patterns
        if "forbiddenPatterns" in constraints:
            for forbidden in constraints["forbiddenPatterns"]:
                if re.search(forbidden, value):
                    return ValidationResult(
                        is_valid=False, message=f"Name contains forbidden pattern: {forbidden}"
                    )

        # Check for reserved words
        if "reservedWords" in constraints:
            reserved = constraints["reservedWords"]
            if value.lower() in [w.lower() for w in reserved]:
                return ValidationResult(is_valid=False, message=f"Name is a reserved word: {value}")

        # Detect the actual convention used
        detected_convention = self._detect_convention(value)

        result = {"name": value, "length": len(value), "detectedConvention": detected_convention}

        if specific_type:
            result["type"] = specific_type

        return ValidationResult(
            is_valid=True,
            message="Name validation passed",
            normalized_value=result,
            metadata={"type": "name", "convention": detected_convention},
        )

    def _detect_convention(self, value: str) -> str:
        """Detect the naming convention used"""
        for convention, pattern in self.PATTERNS.items():
            if re.match(pattern, value):
                return convention.value
        return "unknown"

    def normalize(self, value: Any) -> Any:
        """Normalize name"""
        if not isinstance(value, str):
            return value

        # Basic normalization - strip whitespace
        return value.strip()

    def convert_convention(self, value: str, from_convention: str, to_convention: str) -> str:
        """Convert between naming conventions"""
        # Split the name into words based on source convention
        words = []

        if from_convention == NamingConvention.CAMEL_CASE.value:
            # Split on uppercase letters
            words = re.findall(r"[a-z]+|[A-Z][a-z]*", value)
        elif from_convention == NamingConvention.PASCAL_CASE.value:
            # Split on uppercase letters
            words = re.findall(r"[A-Z][a-z]*", value)
        elif from_convention == NamingConvention.SNAKE_CASE.value:
            words = value.lower().split("_")
        elif from_convention == NamingConvention.KEBAB_CASE.value:
            words = value.lower().split("-")
        elif from_convention == NamingConvention.UPPER_SNAKE_CASE.value:
            words = value.split("_")
        else:
            words = [value]

        # Convert to lowercase for processing
        words = [w.lower() for w in words if w]

        # Convert to target convention
        if to_convention == NamingConvention.CAMEL_CASE.value:
            return words[0] + "".join(w.capitalize() for w in words[1:])
        elif to_convention == NamingConvention.PASCAL_CASE.value:
            return "".join(w.capitalize() for w in words)
        elif to_convention == NamingConvention.SNAKE_CASE.value:
            return "_".join(words)
        elif to_convention == NamingConvention.KEBAB_CASE.value:
            return "-".join(words)
        elif to_convention == NamingConvention.UPPER_SNAKE_CASE.value:
            return "_".join(w.upper() for w in words)
        elif to_convention == NamingConvention.LOWER_CASE.value:
            return "".join(words)
        elif to_convention == NamingConvention.UPPER_CASE.value:
            return "".join(words).upper()
        else:
            return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return ["name", "identifier", "variable"] + list(self.SPECIFIC_PATTERNS.keys())
