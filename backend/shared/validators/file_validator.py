"""
File validator for SPICE HARVESTER
"""

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

logger = logging.getLogger(__name__)


class FileValidator(BaseValidator):
    """Validator for file URLs and metadata"""

    # Common document extensions
    DOCUMENT_EXTENSIONS = {
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".txt",
        ".csv",
        ".json",
        ".xml",
        ".yaml",
        ".yml",
    }

    # Common code extensions
    CODE_EXTENSIONS = {
        ".py",
        ".js",
        ".ts",
        ".java",
        ".cpp",
        ".c",
        ".h",
        ".go",
        ".rs",
        ".php",
        ".rb",
        ".swift",
        ".kt",
        ".scala",
        ".r",
    }

    # Common archive extensions
    ARCHIVE_EXTENSIONS = {".zip", ".tar", ".gz", ".bz2", ".7z", ".rar", ".tar.gz"}

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate file"""
        if constraints is None:
            constraints = {}

        if isinstance(value, str):
            # Simple URL string
            try:
                parsed = urlparse(value)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse file URL '{value}': {e}")
                return ValidationResult(is_valid=False, message="Invalid file URL format")

            if not parsed.scheme or not parsed.netloc:
                return ValidationResult(
                    is_valid=False, message="Invalid URL format - missing scheme or domain"
                )

            # Extract filename and extension
            filename = None
            extension = None
            if parsed.path:
                path_parts = parsed.path.split("/")
                if path_parts:
                    filename = path_parts[-1]
                    if "." in filename:
                        extension = "." + filename.split(".")[-1].lower()

            result = {
                "url": value,
                "name": filename,
                "extension": extension,
                "type": self._get_file_type(extension),
            }

        elif isinstance(value, dict):
            # File with metadata
            if "url" not in value:
                return ValidationResult(is_valid=False, message="File object must have 'url' field")

            result = dict(value)

            # Get extension if not provided
            if "extension" not in result and "name" in result:
                name = result["name"]
                if "." in name:
                    result["extension"] = "." + name.split(".")[-1].lower()

            # Get file type if not provided
            if "type" not in result and "extension" in result:
                result["type"] = self._get_file_type(result["extension"])

        else:
            return ValidationResult(is_valid=False, message="File must be URL string or object")

        # Validate size constraint
        if "maxSize" in constraints and "size" in result:
            if result["size"] > constraints["maxSize"]:
                max_size_mb = constraints["maxSize"] / (1024 * 1024)
                return ValidationResult(
                    is_valid=False, message=f"File size exceeds maximum of {max_size_mb:.2f} MB"
                )

        # Validate extension constraint
        if "allowedExtensions" in constraints:
            extension = result.get("extension")
            if not extension:
                return ValidationResult(
                    is_valid=False, message="File extension could not be determined"
                )

            if extension not in constraints["allowedExtensions"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"File extension must be one of: {constraints['allowedExtensions']}",
                )

        # Validate MIME type constraint
        if "allowedMimeTypes" in constraints and "mimeType" in result:
            if result["mimeType"] not in constraints["allowedMimeTypes"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"File MIME type must be one of: {constraints['allowedMimeTypes']}",
                )

        # Add metadata
        if "size" in result:
            result["sizeFormatted"] = self._format_file_size(result["size"])

        return ValidationResult(
            is_valid=True,
            message="Valid file",
            normalized_value=result,
            metadata={
                "type": "file",
                "hasExtension": result.get("extension") is not None,
                "fileType": result.get("type", "unknown"),
            },
        )

    def normalize(self, value: Any) -> Any:
        """Normalize file value"""
        if isinstance(value, str):
            return value.strip()
        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.FILE.value, "file", "attachment", "document"]

    def _get_file_type(self, extension: Optional[str]) -> str:
        """Determine file type from extension"""
        if not extension:
            return "unknown"

        ext_lower = extension.lower()

        # Check against known categories
        if ext_lower in self.DOCUMENT_EXTENSIONS:
            return "document"
        elif ext_lower in self.CODE_EXTENSIONS:
            return "code"
        elif ext_lower in self.ARCHIVE_EXTENSIONS:
            return "archive"
        elif ext_lower in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}:
            return "image"
        elif ext_lower in {".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm"}:
            return "video"
        elif ext_lower in {".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma"}:
            return "audio"
        else:
            return "other"

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
