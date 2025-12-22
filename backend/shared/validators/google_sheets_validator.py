"""
Google Sheets URL validator for SPICE HARVESTER
"""

import re
from typing import Any, Dict, List, Optional

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class GoogleSheetsValidator(BaseValidator):
    """Validator for Google Sheets URLs"""

    # Google Sheets URL pattern
    GOOGLE_SHEETS_PATTERN = r"^https://docs\.google\.com/spreadsheets/(?:u/\d+/)?d/([a-zA-Z0-9-_]+)"

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate Google Sheets URL"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        value = value.strip()
        value = re.sub(
            r"(https://docs\.google\.com/spreadsheets)/u/\d+/",
            r"\1/",
            value,
        )

        # Basic pattern matching
        match = re.match(self.GOOGLE_SHEETS_PATTERN, value)
        if not match:
            return ValidationResult(
                is_valid=False,
                message="Invalid Google Sheets URL format. Expected: https://docs.google.com/spreadsheets/d/[spreadsheet-id]",
            )

        # Extract spreadsheet ID
        spreadsheet_id = match.group(1)

        # Validate spreadsheet ID length (typically 44 characters but can vary)
        if len(spreadsheet_id) < 10:
            return ValidationResult(is_valid=False, message="Invalid spreadsheet ID: too short")

        # Check if edit/view mode is required
        if constraints.get("requireEditMode", False):
            if not value.endswith("/edit"):
                return ValidationResult(
                    is_valid=False, message="URL must include /edit suffix for edit mode"
                )

        # Check if specific worksheet is required
        if constraints.get("requireWorksheet", False):
            if "#gid=" not in value:
                return ValidationResult(
                    is_valid=False, message="URL must include worksheet ID (#gid=...)"
                )

        # Extract full URL components
        result = {
            "url": value,
            "spreadsheetId": spreadsheet_id,
            "isEditMode": value.endswith("/edit"),
            "hasWorksheet": "#gid=" in value,
        }

        # Extract worksheet ID if present
        worksheet_match = re.search(r"#gid=(\d+)", value)
        if worksheet_match:
            result["worksheetId"] = worksheet_match.group(1)

        return ValidationResult(
            is_valid=True,
            message="Google Sheets URL validation passed",
            normalized_value=result,
            metadata={"type": "google_sheets_url", "spreadsheetId": spreadsheet_id},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize Google Sheets URL"""
        if not isinstance(value, str):
            return value

        # Strip whitespace
        value = value.strip()

        value = re.sub(
            r"(https://docs\.google\.com/spreadsheets)/u/\d+/",
            r"\1/",
            value,
        )

        # Remove trailing slashes
        if value.endswith("/"):
            value = value[:-1]

        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return ["google_sheets_url", "sheets_url", "spreadsheet_url"]
