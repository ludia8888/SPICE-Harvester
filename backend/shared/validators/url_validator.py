"""
URL validator for SPICE HARVESTER
"""

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

logger = logging.getLogger(__name__)


class UrlValidator(BaseValidator):
    """Validator for URL strings"""

    # URL validation pattern
    URL_PATTERN = r"^https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?$"

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate URL"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        # Basic URL validation
        if not re.match(self.URL_PATTERN, value):
            return ValidationResult(is_valid=False, message="Invalid URL format")

        # Parse URL for additional validation
        try:
            parsed = urlparse(value)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse URL '{value}': {e}")
            return ValidationResult(is_valid=False, message="Failed to parse URL")

        # Check HTTPS requirement
        if constraints.get("requireHttps", False) and parsed.scheme != "https":
            return ValidationResult(is_valid=False, message="URL must use HTTPS")

        # Check allowed schemes
        if "allowedSchemes" in constraints:
            if parsed.scheme not in constraints["allowedSchemes"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"URL scheme must be one of: {constraints['allowedSchemes']}",
                )

        # Check allowed domains
        if "allowedDomains" in constraints:
            domain = parsed.netloc.lower()
            # Remove port if present
            if ":" in domain:
                domain = domain.split(":")[0]

            allowed = False
            for allowed_domain in constraints["allowedDomains"]:
                if domain == allowed_domain.lower() or domain.endswith(
                    "." + allowed_domain.lower()
                ):
                    allowed = True
                    break

            if not allowed:
                return ValidationResult(
                    is_valid=False,
                    message=f"Domain must be one of: {constraints['allowedDomains']}",
                )

        # Check path requirements
        if "requirePath" in constraints and constraints["requirePath"]:
            if not parsed.path or parsed.path == "/":
                return ValidationResult(is_valid=False, message="URL must include a path")

        # Check query requirements
        if "requireQuery" in constraints and constraints["requireQuery"]:
            if not parsed.query:
                return ValidationResult(is_valid=False, message="URL must include query parameters")

        result = {
            "url": value,
            "scheme": parsed.scheme,
            "domain": parsed.netloc,
            "path": parsed.path,
            "query": parsed.query,
            "fragment": parsed.fragment,
            "isSecure": parsed.scheme == "https",
        }

        return ValidationResult(
            is_valid=True,
            message="URL validation passed",
            normalized_value=result,
            metadata={"type": "url"},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize URL"""
        if not isinstance(value, str):
            return value

        # Basic normalization - strip whitespace
        return value.strip()

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return ["url", "uri", "link", "href"]
