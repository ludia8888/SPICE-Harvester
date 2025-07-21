"""
Address validator for SPICE HARVESTER
"""

import re
from typing import Any, Dict, List, Optional

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class AddressValidator(BaseValidator):
    """Validator for addresses"""

    # Common address fields
    ADDRESS_FIELDS = [
        "street",
        "street2",
        "city",
        "state",
        "province",
        "postalCode",
        "zipCode",
        "country",
        "region",
    ]

    # ISO 3166-1 alpha-2 country codes (subset)
    COUNTRY_CODES = {
        "US",
        "CA",
        "GB",
        "FR",
        "DE",
        "JP",
        "KR",
        "CN",
        "AU",
        "NZ",
        "MX",
        "BR",
        "IN",
        "IT",
        "ES",
        "NL",
        "BE",
        "CH",
        "SE",
        "NO",
    }

    def _validate_string_address(self, value: str) -> ValidationResult:
        """Validate simple string address."""
        if not value.strip():
            return ValidationResult(is_valid=False, message="Address cannot be empty")
        
        result = {"formatted": value, "type": "unstructured"}
        return ValidationResult(
            is_valid=True,
            message="Valid address",
            normalized_value=result,
            metadata={"type": "address", "structured": False},
        )
    
    def _validate_required_fields(self, value: dict, required_fields: list) -> Optional[ValidationResult]:
        """Validate required fields in structured address."""
        for field in required_fields:
            if field not in value or not str(value.get(field, "")).strip():
                return ValidationResult(
                    is_valid=False,
                    message=f"Required address field '{field}' is missing or empty",
                )
        return None
    
    def _validate_us_address(self, value: dict) -> Optional[ValidationResult]:
        """Validate US address components."""
        # Validate US postal code
        postal_code = value.get("postalCode") or value.get("zipCode")
        if postal_code:
            if not re.match(r"^\d{5}(-\d{4})?$", str(postal_code)):
                return ValidationResult(
                    is_valid=False,
                    message="Invalid US postal code format (12345 or 12345-6789)",
                )
        
        # Validate state
        state = value.get("state")
        if state and len(state) == 2:
            US_STATES = {
                "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            }
            if state.upper() not in US_STATES:
                return ValidationResult(
                    is_valid=False, message=f"Invalid US state code: {state}"
                )
        return None
    
    def _validate_canadian_address(self, value: dict) -> Optional[ValidationResult]:
        """Validate Canadian address components."""
        postal_code = value.get("postalCode")
        if postal_code:
            if not re.match(r"^[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d$", str(postal_code)):
                return ValidationResult(
                    is_valid=False, message="Invalid Canadian postal code format (A1A 1A1)"
                )
        return None
    
    def _validate_uk_address(self, value: dict) -> Optional[ValidationResult]:
        """Validate UK address components."""
        postal_code = value.get("postalCode") or value.get("postcode")
        if postal_code:
            if not re.match(
                r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$", str(postal_code).upper()
            ):
                return ValidationResult(
                    is_valid=False, message="Invalid UK postcode format"
                )
        return None
    
    def _validate_country_specific(self, value: dict, country: str) -> Optional[ValidationResult]:
        """Validate country-specific address components."""
        country_validators = {
            "US": self._validate_us_address,
            "CA": self._validate_canadian_address,
            "GB": self._validate_uk_address,
        }
        
        validator = country_validators.get(country)
        if validator:
            return validator(value)
        return None
    
    def _format_address(self, value: dict) -> dict:
        """Format structured address for display."""
        parts = []
        for field in ["street", "street2", "city", "state", "postalCode", "country"]:
            if field in value and value[field]:
                parts.append(str(value[field]))
        
        result = dict(value)
        result["formatted"] = ", ".join(parts)
        result["type"] = "structured"
        return result
    
    def _validate_structured_address(self, value: dict, constraints: dict) -> ValidationResult:
        """Validate structured address."""
        # Check required fields
        required_fields = constraints.get("requiredFields", [])
        field_error = self._validate_required_fields(value, required_fields)
        if field_error:
            return field_error
        
        # Validate country code
        country = value.get("country")
        if country:
            if len(country) == 2:
                # Validate ISO 3166-1 alpha-2 code
                if country.upper() not in self.COUNTRY_CODES:
                    # Still allow it but add warning
                    pass
            elif len(country) != 3:
                # Not alpha-2 or alpha-3, treat as full country name
                pass
        
        # Country-specific validation
        country_error = self._validate_country_specific(value, country)
        if country_error:
            return country_error
        
        # Format address
        result = self._format_address(value)
        
        return ValidationResult(
            is_valid=True,
            message="Valid address",
            normalized_value=result,
            metadata={"type": "address", "structured": True, "country": country},
        )
    
    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate address using type-specific validators."""
        if constraints is None:
            constraints = {}
        
        if isinstance(value, str):
            return self._validate_string_address(value)
        elif isinstance(value, dict):
            return self._validate_structured_address(value, constraints)
        else:
            return ValidationResult(is_valid=False, message="Address must be string or object")

    def normalize(self, value: Any) -> Any:
        """Normalize address value"""
        if isinstance(value, dict):
            # Normalize country code to uppercase
            normalized = dict(value)
            if "country" in normalized and isinstance(normalized["country"], str):
                if len(normalized["country"]) == 2:
                    normalized["country"] = normalized["country"].upper()

            # Normalize state code to uppercase for US
            if normalized.get("country") == "US" and "state" in normalized:
                if isinstance(normalized["state"], str) and len(normalized["state"]) == 2:
                    normalized["state"] = normalized["state"].upper()

            return normalized

        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.ADDRESS.value, "address", "location_address"]
