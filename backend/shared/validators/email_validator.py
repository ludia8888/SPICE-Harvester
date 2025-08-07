"""
Email validator for SPICE HARVESTER
"""

import re
from typing import Any, Dict, List, Optional

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

# email-validator is now a required dependency in shared/pyproject.toml
from email_validator import EmailNotValidError
from email_validator import validate_email as validate_email_lib


class EmailValidator(BaseValidator):
    """Validator for email addresses"""

    # Updated pattern to support unicode characters
    EMAIL_PATTERN = r"^[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}$"

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate email address"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        # Basic regex validation with unicode support
        if not re.match(self.EMAIL_PATTERN, value, re.UNICODE):
            return ValidationResult(is_valid=False, message="Invalid email format")

        # Domain validation
        if "allowedDomains" in constraints:
            domain = value.split("@")[1]
            if domain not in constraints["allowedDomains"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"Email domain must be one of: {constraints['allowedDomains']}",
                )

        # Use email-validator library for thorough validation
        try:
            # Disable DNS check for validation
            validated = validate_email_lib(value, check_deliverability=False)
            result_data = {
                "email": validated.email,
                "local": validated.local_part,
                "domain": validated.domain,
            }
        except EmailNotValidError as e:
            return ValidationResult(is_valid=False, message=f"Invalid email address: {str(e)}")

        return ValidationResult(
            is_valid=True,
            message="Valid email",
            normalized_value=result_data,
            metadata={"type": "email", "has_unicode": any(ord(c) > 127 for c in value)},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize email address"""
        if not isinstance(value, str):
            return value

        # Basic normalization - lowercase domain
        if "@" in value:
            local, domain = value.rsplit("@", 1)
            return f"{local}@{domain.lower()}"

        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.EMAIL.value, "email"]

    @classmethod
    def is_valid_email(cls, email: str) -> bool:
        """Quick check if email is valid"""
        return bool(re.match(cls.EMAIL_PATTERN, email, re.UNICODE))
