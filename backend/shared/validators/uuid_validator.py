"""
UUID validator for SPICE HARVESTER
"""

import re
import uuid
from typing import Any, Dict, List, Optional
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

logger = logging.getLogger(__name__)


class UuidValidator(BaseValidator):
    """Validator for UUIDs"""

    # UUID patterns for different versions
    UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate UUID"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        # Basic pattern validation (case-insensitive)
        if not re.match(self.UUID_PATTERN, value.lower()):
            return ValidationResult(is_valid=False, message="Invalid UUID format")

        # Try to parse as UUID
        try:
            uuid_obj = uuid.UUID(value)

            # Check version constraint
            if "version" in constraints:
                required_version = constraints["version"]
                if isinstance(required_version, list):
                    if uuid_obj.version not in required_version:
                        return ValidationResult(
                            is_valid=False,
                            message=f"UUID version must be one of: {required_version}",
                        )
                else:
                    if uuid_obj.version != required_version:
                        return ValidationResult(
                            is_valid=False, message=f"UUID must be version {required_version}"
                        )

            # Check variant constraint
            if "variant" in constraints:
                required_variant = constraints["variant"]
                if uuid_obj.variant != required_variant:
                    return ValidationResult(
                        is_valid=False, message=f"UUID must have variant: {required_variant}"
                    )

            # Check if nil UUID is allowed
            if constraints.get("allowNil", True) is False and uuid_obj == uuid.UUID(
                "00000000-0000-0000-0000-000000000000"
            ):
                return ValidationResult(is_valid=False, message="Nil UUID is not allowed")

            # Format constraint (uppercase, lowercase, no dashes)
            format_type = constraints.get("format", "lowercase")
            formatted_value = str(uuid_obj)

            if format_type == "uppercase":
                formatted_value = formatted_value.upper()
            elif format_type == "lowercase":
                formatted_value = formatted_value.lower()
            elif format_type == "nodashes":
                formatted_value = formatted_value.replace("-", "")
            elif format_type == "urn":
                formatted_value = f"urn:uuid:{formatted_value}"

            result = {
                "uuid": formatted_value,
                "version": uuid_obj.version,
                "variant": str(uuid_obj.variant),
                "isNil": uuid_obj == uuid.UUID("00000000-0000-0000-0000-000000000000"),
            }

            # Add timestamp for version 1 UUIDs
            if uuid_obj.version == 1:
                try:
                    result["timestamp"] = uuid_obj.time
                except (ValueError, AttributeError):
                    # Some UUID v1 objects may not have timestamp info
                    pass

            return ValidationResult(
                is_valid=True,
                message="UUID validation passed",
                normalized_value=result,
                metadata={"type": "uuid", "version": uuid_obj.version},
            )

        except ValueError as e:
            return ValidationResult(is_valid=False, message=f"Invalid UUID: {str(e)}")

    def normalize(self, value: Any) -> Any:
        """Normalize UUID"""
        if not isinstance(value, str):
            return value

        # Strip whitespace
        value = value.strip()

        # Try to parse and return lowercase canonical form
        try:
            uuid_obj = uuid.UUID(value)
            return str(uuid_obj).lower()
        except (ValueError, TypeError, AttributeError) as e:
            logger.debug(f"Failed to normalize UUID '{value}': {e}")
            return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return ["uuid", "guid", "identifier"]
