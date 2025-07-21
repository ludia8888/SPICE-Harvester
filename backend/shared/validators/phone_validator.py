"""
Phone number validator for SPICE HARVESTER
"""

import re
from typing import Any, Dict, List, Optional
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

logger = logging.getLogger(__name__)

try:
    import phonenumbers
    from phonenumbers import NumberParseException

    PHONENUMBERS_AVAILABLE = True
except ImportError:
    PHONENUMBERS_AVAILABLE = False


class PhoneValidator(BaseValidator):
    """Validator for phone numbers"""

    # Basic phone pattern for fallback validation
    PHONE_PATTERN = r"^(\+\d{1,3}[- ]?)?\(?\d{3}\)?[- ]?\d{3}[- ]?\d{4}$"

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate phone number"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        if not PHONENUMBERS_AVAILABLE:
            # Basic validation without phonenumbers library
            if not re.match(self.PHONE_PATTERN, value):
                return ValidationResult(is_valid=False, message="Invalid phone format")
            normalized = re.sub(r"[- ()]", "", value)
            return ValidationResult(
                is_valid=True,
                message="Phone validation passed",
                normalized_value={"original": value, "normalized": normalized},
                metadata={"type": "phone", "validation_method": "regex"},
            )

        # Use phonenumbers library for comprehensive validation
        try:
            default_region = constraints.get("defaultRegion", "US")
            parsed = phonenumbers.parse(value, default_region)

            if not phonenumbers.is_valid_number(parsed):
                return ValidationResult(is_valid=False, message="Invalid phone number")

            # Check allowed regions
            if "allowedRegions" in constraints:
                region = phonenumbers.region_code_for_number(parsed)
                if region not in constraints["allowedRegions"]:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Phone region must be one of: {constraints['allowedRegions']}",
                    )

            result = {
                "original": value,
                "e164": phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164),
                "international": phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL
                ),
                "national": phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.NATIONAL
                ),
                "country_code": parsed.country_code,
                "national_number": parsed.national_number,
            }

            return ValidationResult(
                is_valid=True,
                message="Valid phone",
                normalized_value=result,
                metadata={
                    "type": "phone",
                    "validation_method": "phonenumbers",
                    "region": phonenumbers.region_code_for_number(parsed),
                },
            )
        except NumberParseException as e:
            return ValidationResult(is_valid=False, message=f"Invalid phone number: {str(e)}")

    def normalize(self, value: Any) -> Any:
        """Normalize phone number"""
        if not isinstance(value, str):
            return value

        # Basic normalization - remove common separators
        return re.sub(r"[- ()]", "", value)

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.PHONE.value, "phone", "telephone", "tel"]

    @classmethod
    def format_phone(cls, phone: str, format_type: str = "international") -> str:
        """
        Format phone number

        Args:
            phone: Phone number to format
            format_type: Format type (international, national, e164)

        Returns:
            Formatted phone number
        """
        if not PHONENUMBERS_AVAILABLE:
            return phone

        try:
            parsed = phonenumbers.parse(phone, "US")
            if format_type == "international":
                return phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL
                )
            elif format_type == "national":
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)
            elif format_type == "e164":
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception as e:
            # Return original phone if formatting fails
            logger.debug(f"Failed to format phone number '{phone}' as {format_type}: {e}")

        return phone
