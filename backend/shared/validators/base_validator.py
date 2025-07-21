"""
Base validator interface for SPICE HARVESTER
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ValidationResult:
    """Result of validation operation"""

    is_valid: bool
    message: str = ""
    normalized_value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def error(self) -> Optional[str]:
        """Get error message if validation failed"""
        return self.message if not self.is_valid else None

    def to_tuple(self) -> Tuple[bool, str, Any]:
        """Convert to legacy tuple format for backward compatibility"""
        return (self.is_valid, self.message, self.normalized_value)


class BaseValidator(ABC):
    """Abstract base class for validators"""

    @abstractmethod
    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """
        Validate a value against constraints

        Args:
            value: The value to validate
            constraints: Optional constraints to apply

        Returns:
            ValidationResult object
        """
        pass

    @abstractmethod
    def normalize(self, value: Any) -> Any:
        """
        Normalize a value to standard format

        Args:
            value: The value to normalize

        Returns:
            Normalized value
        """
        pass

    def is_supported_type(self, data_type: str) -> bool:
        """
        Check if this validator supports the given data type

        Args:
            data_type: The data type to check

        Returns:
            True if supported, False otherwise
        """
        return data_type in self.get_supported_types()

    @abstractmethod
    def get_supported_types(self) -> List[str]:
        """
        Get list of supported data types

        Returns:
            List of supported type names
        """
        pass

    def get_type_info(self) -> Dict[str, Any]:
        """
        Get information about this validator

        Returns:
            Dictionary with validator metadata
        """
        return {
            "name": self.__class__.__name__,
            "supported_types": self.get_supported_types(),
            "description": self.__class__.__doc__ or "No description available",
        }


class CompositeValidator(BaseValidator):
    """Validator that combines multiple validators"""

    def __init__(self, validators: List[BaseValidator]):
        self.validators = validators
        self._type_map = {}
        for validator in validators:
            for data_type in validator.get_supported_types():
                self._type_map[data_type] = validator

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate using appropriate sub-validator"""
        # For CompositeValidator, data_type should be provided in constraints
        if not constraints or 'data_type' not in constraints:
            return ValidationResult(is_valid=False, message="data_type must be provided in constraints")
        
        data_type = constraints['data_type']
        validator = self._type_map.get(data_type)
        if not validator:
            return ValidationResult(is_valid=False, message=f"Unsupported data type: {data_type}")

        return validator.validate(value, constraints)

    def normalize(self, value: Any) -> Any:
        """Normalize is not applicable for composite validator"""
        return value

    def get_supported_types(self) -> List[str]:
        """Get all supported types from sub-validators"""
        return list(self._type_map.keys())

    def add_validator(self, validator: BaseValidator) -> None:
        """Add a new validator"""
        self.validators.append(validator)
        for data_type in validator.get_supported_types():
            self._type_map[data_type] = validator
