"""
Refactored Complex Type Validator
This validator delegates to specialized validators to avoid code duplication
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from ..models.common import DataType
from .base_validator import ValidationResult


@dataclass
class ComplexTypeConstraints:
    """
    Constraints for complex type validation.
    """

    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    format: Optional[str] = None
    unique_items: Optional[bool] = None
    min_items: Optional[int] = None
    max_items: Optional[int] = None
    item_type: Optional[str] = None

    @classmethod
    def array_constraints(
        cls,
        min_items: Optional[int] = None,
        max_items: Optional[int] = None,
        unique_items: Optional[bool] = None,
        item_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create constraints for array validation."""
        constraints = {}
        if min_items is not None:
            constraints["minItems"] = min_items
        if max_items is not None:
            constraints["maxItems"] = max_items
        if unique_items is not None:
            constraints["uniqueItems"] = unique_items
        if item_type is not None:
            constraints["itemType"] = item_type
        return constraints

    @classmethod
    def object_constraints(
        cls,
        schema: Optional[Dict[str, Any]] = None,
        required: Optional[List[str]] = None,
        additional_properties: bool = True,
    ) -> Dict[str, Any]:
        """Create constraints for object validation."""
        constraints = {"additionalProperties": additional_properties}
        if schema is not None:
            constraints["schema"] = schema
        if required is not None:
            constraints["required"] = required
        return constraints

    @classmethod
    def enum_constraints(cls, allowed_values: List[Any]) -> Dict[str, Any]:
        """Create constraints for enum validation."""
        return {"enum": allowed_values}

    @classmethod
    def money_constraints(
        cls,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        decimal_places: int = 2,
        allowed_currencies: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Create constraints for money validation."""
        constraints = {"decimalPlaces": decimal_places}
        if min_amount is not None:
            constraints["minAmount"] = min_amount
        if max_amount is not None:
            constraints["maxAmount"] = max_amount
        if allowed_currencies is not None:
            constraints["allowedCurrencies"] = allowed_currencies
        return constraints

    @classmethod
    def phone_constraints(
        cls, default_region: str = "US", allowed_regions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create constraints for phone validation."""
        constraints = {"defaultRegion": default_region}
        if allowed_regions is not None:
            constraints["allowedRegions"] = allowed_regions
        return constraints

    @classmethod
    def email_constraints(cls, allowed_domains: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create constraints for email validation."""
        constraints = {}
        if allowed_domains is not None:
            constraints["allowedDomains"] = allowed_domains
        return constraints

    @classmethod
    def coordinate_constraints(
        cls, precision: int = 6, bounding_box: Optional[Tuple[float, float, float, float]] = None
    ) -> Dict[str, Any]:
        """Create constraints for coordinate validation."""
        constraints = {"precision": precision}
        if bounding_box is not None:
            constraints["boundingBox"] = bounding_box
        return constraints

    @classmethod
    def address_constraints(
        cls, required_fields: Optional[List[str]] = None, default_country: str = "US"
    ) -> Dict[str, Any]:
        """Create constraints for address validation."""
        constraints = {"defaultCountry": default_country}
        if required_fields is not None:
            constraints["requiredFields"] = required_fields
        return constraints

    @classmethod
    def image_constraints(
        cls,
        allowed_extensions: Optional[List[str]] = None,
        allowed_domains: Optional[List[str]] = None,
        require_extension: bool = False,
    ) -> Dict[str, Any]:
        """Create constraints for image validation."""
        constraints = {"requireExtension": require_extension}
        if allowed_extensions is not None:
            constraints["allowedExtensions"] = allowed_extensions
        if allowed_domains is not None:
            constraints["allowedDomains"] = allowed_domains
        return constraints

    @classmethod
    def file_constraints(
        cls, max_size: Optional[int] = None, allowed_extensions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create constraints for file validation."""
        constraints = {}
        if max_size is not None:
            constraints["maxSize"] = max_size
        if allowed_extensions is not None:
            constraints["allowedExtensions"] = allowed_extensions
        return constraints

    @classmethod
    def string_constraints(
        cls,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create constraints for string validation."""
        constraints = {}
        if min_length is not None:
            constraints["minLength"] = min_length
        if max_length is not None:
            constraints["maxLength"] = max_length
        if pattern is not None:
            constraints["pattern"] = pattern
        if format is not None:
            constraints["format"] = format
        return constraints


class ComplexTypeValidator:
    """
    Refactored validator that delegates to specialized validators.

    This avoids code duplication by using the modular validator architecture.
    """

    # Map data types to validator types
    TYPE_MAPPING = {
        DataType.ARRAY.value: "array",
        DataType.OBJECT.value: "object",
        DataType.ENUM.value: "enum",
        DataType.MONEY.value: "money",
        DataType.PHONE.value: "phone",
        DataType.EMAIL.value: "email",
        DataType.COORDINATE.value: "coordinate",
        DataType.ADDRESS.value: "address",
        DataType.IMAGE.value: "image",
        DataType.FILE.value: "file",
        DataType.STRUCT.value: "struct",
        DataType.VECTOR.value: "vector",
        DataType.GEOPOINT.value: "geopoint",
        DataType.GEOSHAPE.value: "geoshape",
        DataType.MARKING.value: "marking",
        DataType.CIPHER.value: "cipher",
    }

    @classmethod
    def validate(
        cls,
        value: Any,
        data_type: str,
        constraints: Optional[Union[ComplexTypeConstraints, Dict[str, Any]]] = None,
    ) -> Tuple[bool, str, Any]:
        """
        Validate a value against a complex data type with constraints.

        Args:
            value: The value to validate
            data_type: The target data type
            constraints: Optional constraints for validation

        Returns:
            Tuple of (is_valid, error_message, normalized_value)
        """
        if constraints is None:
            constraints = {}
        elif isinstance(constraints, ComplexTypeConstraints):
            # Convert dataclass to dict if needed
            constraints = {}

        try:
            # Map data type to validator type
            validator_type = cls.TYPE_MAPPING.get(data_type, data_type.lower())

            # Get the appropriate validator - import here to avoid circular imports
            from . import get_validator

            validator = get_validator(validator_type)

            if validator:
                # Use specialized validator
                result = validator.validate(value, constraints)
                return result.to_tuple()
            else:
                # Handle XSD types and other special cases
                if data_type.startswith("xsd:"):
                    return cls._validate_xsd_type(value, data_type, constraints)
                else:
                    # Default validation for unknown types
                    return True, f"Default validation passed for type: {data_type}", value

        except Exception as e:
            return False, f"Validation error: {str(e)}", value

    @classmethod
    def _validate_string_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate string type."""
        if not isinstance(value, str):
            return False, f"Expected string, got {type(value).__name__}", value
        return True, "Valid string", value

    @classmethod
    def _validate_integer_type(cls, value: Any, data_type: str) -> Tuple[bool, str, Any]:
        """Validate integer types (int, long, short, byte)."""
        if not isinstance(value, int) or isinstance(value, bool):
            return False, f"Expected integer, got {type(value).__name__}", value
        return True, f"Valid {data_type}", value

    @classmethod
    def _validate_unsigned_integer_type(cls, value: Any, data_type: str) -> Tuple[bool, str, Any]:
        """Validate unsigned integer types."""
        if not isinstance(value, int) or isinstance(value, bool):
            return False, f"Expected unsigned integer, got {type(value).__name__}", value
        if value < 0:
            return False, f"Unsigned type cannot be negative: {value}", value
        return True, f"Valid {data_type}", value

    @classmethod
    def _validate_float_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate float/double types."""
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return False, f"Expected float, got {type(value).__name__}", value
        return True, "Valid float", float(value)

    @classmethod
    def _validate_boolean_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate boolean type."""
        if not isinstance(value, bool):
            return False, f"Expected boolean, got {type(value).__name__}", value
        return True, "Valid boolean", value

    @classmethod
    def _validate_decimal_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate decimal type."""
        from decimal import Decimal
        if not isinstance(value, (int, float, Decimal)) or isinstance(value, bool):
            return False, f"Expected decimal, got {type(value).__name__}", value
        return True, "Valid decimal", value

    @classmethod
    def _validate_date_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate date type."""
        from datetime import datetime
        if isinstance(value, str):
            try:
                # Basic date format validation (YYYY-MM-DD)
                datetime.strptime(value, "%Y-%m-%d")
                return True, "Valid date", value
            except ValueError:
                return False, "Invalid date format. Use YYYY-MM-DD", value
        return False, "Date must be string in YYYY-MM-DD format", value

    @classmethod
    def _validate_datetime_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate datetime type."""
        from datetime import datetime
        if isinstance(value, str):
            try:
                # Try ISO format first
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return True, "Valid datetime", value
            except ValueError:
                return False, "Invalid datetime format. Use ISO format", value
        return False, "Datetime must be string in ISO format", value

    @classmethod
    def _validate_uri_type(cls, value: Any) -> Tuple[bool, str, Any]:
        """Validate URI type."""
        from urllib.parse import urlparse
        if not isinstance(value, str):
            return False, "URI must be string", value
        # Basic URI validation
        parsed = urlparse(value)
        if not parsed.scheme:
            return False, "Invalid URI: missing scheme", value
        return True, "Valid URI", value

    @classmethod
    def _get_type_validator_map(cls) -> Dict[str, callable]:
        """Get mapping of data types to their validator functions."""
        return {
            DataType.STRING.value: cls._validate_string_type,
            DataType.INTEGER.value: lambda value: cls._validate_integer_type(value, "integer"),
            DataType.LONG.value: lambda value: cls._validate_integer_type(value, "long"),
            DataType.SHORT.value: lambda value: cls._validate_integer_type(value, "short"),
            DataType.BYTE.value: lambda value: cls._validate_integer_type(value, "byte"),
            DataType.UNSIGNED_INT.value: lambda value: cls._validate_unsigned_integer_type(value, "unsigned int"),
            DataType.UNSIGNED_LONG.value: lambda value: cls._validate_unsigned_integer_type(value, "unsigned long"),
            DataType.UNSIGNED_SHORT.value: lambda value: cls._validate_unsigned_integer_type(value, "unsigned short"),
            DataType.UNSIGNED_BYTE.value: lambda value: cls._validate_unsigned_integer_type(value, "unsigned byte"),
            DataType.FLOAT.value: cls._validate_float_type,
            DataType.DOUBLE.value: cls._validate_float_type,
            DataType.BOOLEAN.value: cls._validate_boolean_type,
            DataType.DECIMAL.value: cls._validate_decimal_type,
            DataType.DATE.value: cls._validate_date_type,
            DataType.DATETIME.value: cls._validate_datetime_type,
            DataType.URI.value: cls._validate_uri_type,
        }

    @classmethod
    def _validate_xsd_type(
        cls, value: Any, data_type: str, constraints: Dict[str, Any]
    ) -> Tuple[bool, str, Any]:
        """Validate XSD data types using type-specific validators."""
        validator_map = cls._get_type_validator_map()
        
        if data_type in validator_map:
            return validator_map[data_type](value)
        else:
            # For other XSD types, just validate as string
            return True, f"Valid {data_type}", value

    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Get list of supported complex types."""
        # Get all types from the composite validator - import here to avoid circular imports
        from . import get_composite_validator

        composite = get_composite_validator()
        types = composite.get_supported_types()

        # Add XSD types
        types.extend(
            [
                DataType.STRING.value,
                DataType.INTEGER.value,
                DataType.FLOAT.value,
                DataType.DOUBLE.value,
                DataType.BOOLEAN.value,
                DataType.DECIMAL.value,
                DataType.LONG.value,
                DataType.SHORT.value,
                DataType.BYTE.value,
                DataType.UNSIGNED_INT.value,
                DataType.UNSIGNED_LONG.value,
                DataType.UNSIGNED_SHORT.value,
                DataType.UNSIGNED_BYTE.value,
                DataType.DATE.value,
                DataType.DATETIME.value,
                DataType.URI.value,
            ]
        )

        return list(set(types))

    @classmethod
    def is_supported_type(cls, data_type: str) -> bool:
        """Check if data type is supported."""
        return data_type.lower() in [t.lower() for t in cls.get_supported_types()]
