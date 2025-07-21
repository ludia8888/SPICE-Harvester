"""
Image URL validator for SPICE HARVESTER
"""

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class ImageValidator(BaseValidator):
    """Validator for image URLs"""

    # Common image file extensions
    IMAGE_EXTENSIONS = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".bmp",
        ".tiff",
        ".tif",
        ".svg",
        ".ico",
    }

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate image URL"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(is_valid=False, message="Image must be a URL string")

        # Validate URL format
        try:
            parsed = urlparse(value)
        except (ValueError, TypeError, UnicodeError):
            return ValidationResult(is_valid=False, message="Invalid URL format")

        # Allow file:// URLs which don't have netloc
        if not parsed.scheme:
            return ValidationResult(
                is_valid=False, message="Invalid URL format - missing scheme"
            )
        
        # For non-file schemes, require netloc (domain)
        if parsed.scheme != "file" and not parsed.netloc:
            return ValidationResult(
                is_valid=False, message="Invalid URL format - missing domain"
            )

        # Check HTTPS requirement
        if constraints.get("requireHttps", False) and parsed.scheme != "https":
            return ValidationResult(is_valid=False, message="Image URL must use HTTPS")

        # Check allowed domains
        if "allowedDomains" in constraints and parsed.netloc:
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

        # Check extension
        path = parsed.path.lower()
        extension = None

        for ext in self.IMAGE_EXTENSIONS:
            if path.endswith(ext):
                extension = ext
                break

        # Check if extension is required
        if constraints.get("requireExtension", False) and not extension:
            return ValidationResult(
                is_valid=False,
                message=f"URL must have a valid image extension: {sorted(self.IMAGE_EXTENSIONS)}",
            )

        # Check allowed extensions
        if "allowedExtensions" in constraints and extension:
            if extension not in constraints["allowedExtensions"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"Image extension must be one of: {constraints['allowedExtensions']}",
                )

        # Extract filename
        filename = None
        if parsed.path:
            path_parts = [part for part in parsed.path.split("/") if part]  # Filter out empty parts
            if path_parts:
                filename = path_parts[-1]

        result = {
            "url": value,
            "extension": extension,
            "filename": filename,
            "domain": parsed.netloc.split(":")[0] if parsed.netloc and ":" in parsed.netloc else parsed.netloc,
            "isSecure": parsed.scheme == "https",
            "path": parsed.path,
        }

        # Add query parameters if present
        if parsed.query:
            result["hasQuery"] = True
            result["query"] = parsed.query

        return ValidationResult(
            is_valid=True,
            message="Valid image",
            normalized_value=result,
            metadata={"type": "image", "hasExtension": extension is not None},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize image URL"""
        if not isinstance(value, str):
            return value

        # Basic normalization - ensure proper URL encoding
        return value.strip()

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.IMAGE.value, "image", "image_url", "photo"]

    @classmethod
    def is_image_extension(cls, filename: str) -> bool:
        """Check if filename has image extension"""
        if not filename:
            return False

        filename_lower = filename.lower()
        return any(filename_lower.endswith(ext) for ext in cls.IMAGE_EXTENSIONS)
