"""
Validators for SPICE HARVESTER services
"""

from typing import Dict, Optional, Type

from .address_validator import AddressValidator
from .array_validator import ArrayValidator
from .base_validator import BaseValidator, CompositeValidator, ValidationResult
from .complex_type_validator import ComplexTypeConstraints, ComplexTypeValidator
from .coordinate_validator import CoordinateValidator
from .cipher_validator import CipherValidator
from .email_validator import EmailValidator
from .enum_validator import EnumValidator
from .file_validator import FileValidator
from .geopoint_validator import GeoPointValidator
from .geoshape_validator import GeoShapeValidator
from .google_sheets_validator import GoogleSheetsValidator
from .image_validator import ImageValidator
from .ip_validator import IpValidator
from .marking_validator import MarkingValidator
from .money_validator import MoneyValidator
from .name_validator import NameValidator

# Import all validators
from .object_validator import ObjectValidator
from .phone_validator import PhoneValidator
from .struct_validator import StructValidator
from .string_validator import StringValidator
from .url_validator import UrlValidator
from .uuid_validator import UuidValidator
from .vector_validator import VectorValidator

# Registry of validators by type
_VALIDATOR_REGISTRY: Dict[str, Type[BaseValidator]] = {
    "array": ArrayValidator,
    "cipher": CipherValidator,
    "email": EmailValidator,
    "object": ObjectValidator,
    "enum": EnumValidator,
    "geopoint": GeoPointValidator,
    "geoshape": GeoShapeValidator,
    "marking": MarkingValidator,
    "money": MoneyValidator,
    "phone": PhoneValidator,
    "coordinate": CoordinateValidator,
    "address": AddressValidator,
    "image": ImageValidator,
    "file": FileValidator,
    "struct": StructValidator,
    "url": UrlValidator,
    "ip": IpValidator,
    "uuid": UuidValidator,
    "vector": VectorValidator,
    "media": StringValidator,
    "attachment": StringValidator,
    "time_series": StringValidator,
    "timeseries": StringValidator,
    "string": StringValidator,
    "google_sheets_url": GoogleSheetsValidator,
    "name": NameValidator,
    "database_name": NameValidator,
    "class_id": NameValidator,
    "branch_name": NameValidator,
    "predicate": NameValidator,
    "identifier": NameValidator,
}


def get_validator(data_type: str) -> Optional[BaseValidator]:
    """
    Get validator instance for a specific data type

    Args:
        data_type: The data type to get validator for

    Returns:
        Validator instance or None if not found
    """
    validator_class = _VALIDATOR_REGISTRY.get(data_type.lower())
    if validator_class:
        return validator_class()
    return None


def register_validator(data_type: str, validator_class: Type[BaseValidator]):
    """
    Register a new validator

    Args:
        data_type: The data type name
        validator_class: The validator class
    """
    _VALIDATOR_REGISTRY[data_type.lower()] = validator_class


def get_composite_validator() -> CompositeValidator:
    """
    Get a composite validator with all registered validators

    Returns:
        CompositeValidator instance
    """
    validators = []
    for validator_class in _VALIDATOR_REGISTRY.values():
        validators.append(validator_class())
    return CompositeValidator(validators)


__all__ = [
    "ComplexTypeValidator",
    "ComplexTypeConstraints",
    "BaseValidator",
    "ValidationResult",
    "CompositeValidator",
    "ArrayValidator",
    "CipherValidator",
    "EmailValidator",
    "ObjectValidator",
    "EnumValidator",
    "GeoPointValidator",
    "GeoShapeValidator",
    "MarkingValidator",
    "MoneyValidator",
    "PhoneValidator",
    "CoordinateValidator",
    "AddressValidator",
    "ImageValidator",
    "FileValidator",
    "StructValidator",
    "UrlValidator",
    "IpValidator",
    "UuidValidator",
    "VectorValidator",
    "StringValidator",
    "GoogleSheetsValidator",
    "NameValidator",
    "get_validator",
    "register_validator",
    "get_composite_validator",
]
