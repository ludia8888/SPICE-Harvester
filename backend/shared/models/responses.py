"""
Response models for SPICE HARVESTER services

This module contains standardized response models used across all API endpoints.
All services should use these models for consistent API responses.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ApiResponse:
    """
    Standardized API response model for all SPICE HARVESTER services

    This model provides consistent response structure across all API endpoints.
    All services should use this model for API responses.
    """

    status: str
    message: str
    data: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response"""
        result = {"status": self.status, "message": self.message}
        if self.data is not None:
            result["data"] = self.data
        if self.errors is not None:
            result["errors"] = self.errors
        return result

    @classmethod
    def success(cls, message: str, data: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        """Create success response (200 OK)"""
        return cls(status="success", message=message, data=data)

    @classmethod
    def created(cls, message: str, data: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        """Create resource created response (201 Created)"""
        return cls(status="created", message=message, data=data)

    @classmethod
    def accepted(cls, message: str, data: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        """Create accepted response (202 Accepted)"""
        return cls(status="accepted", message=message, data=data)

    @classmethod
    def no_content(cls, message: str) -> "ApiResponse":
        """Create no content response (204 No Content)"""
        return cls(status="success", message=message)

    @classmethod
    def error(cls, message: str, errors: Optional[List[str]] = None) -> "ApiResponse":
        """Create error response (4xx/5xx status codes)"""
        return cls(status="error", message=message, errors=errors)

    @classmethod
    def warning(cls, message: str, data: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        """Create warning response (successful but with warnings)"""
        return cls(status="warning", message=message, data=data)

    @classmethod
    def partial(
        cls, message: str, data: Optional[Dict[str, Any]] = None, errors: Optional[List[str]] = None
    ) -> "ApiResponse":
        """Create partial success response (some operations succeeded, some failed)"""
        return cls(status="partial", message=message, data=data, errors=errors)

    @classmethod
    def health_check(
        cls, service_name: str, version: str, description: Optional[str] = None
    ) -> "ApiResponse":
        """Create standardized health check response"""
        health_data = {"service": service_name, "version": version, "status": "healthy"}
        if description:
            health_data["description"] = description

        return cls(status="success", message="Service is healthy", data=health_data)

    def is_success(self) -> bool:
        """Check if response indicates success"""
        return self.status in ["success", "created", "accepted"]

    def is_error(self) -> bool:
        """Check if response indicates error"""
        return self.status == "error"

    def is_warning(self) -> bool:
        """Check if response has warnings"""
        return self.status in ["warning", "partial"]
