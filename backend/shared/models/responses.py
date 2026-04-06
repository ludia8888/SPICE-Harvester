"""
Response models for SPICE HARVESTER services

This module contains standardized response models used across all API endpoints.
All services should use these models for consistent API responses.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

RUNTIME_STATE_READY = "ready"
RUNTIME_STATE_DEGRADED = "degraded"
RUNTIME_STATE_HARD_DOWN = "hard_down"


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
        health_data = {"service": service_name, "version": version, "status": RUNTIME_STATE_READY}
        if description:
            health_data["description"] = description

        return cls(status="success", message="Service is ready", data=health_data)

    def is_success(self) -> bool:
        """Check if response indicates success"""
        return self.status in ["success", "created", "accepted"]

    def is_error(self) -> bool:
        """Check if response indicates error"""
        return self.status == "error"

    def is_warning(self) -> bool:
        """Check if response has warnings"""
        return self.status in ["warning", "partial"]


def normalize_health_state(value: Any, *, default: str = RUNTIME_STATE_READY) -> str:
    token = str(value or "").strip().lower()
    if token in {RUNTIME_STATE_READY, RUNTIME_STATE_DEGRADED, RUNTIME_STATE_HARD_DOWN}:
        return token
    if token in {"healthy", "ok"}:
        return RUNTIME_STATE_READY
    if token in {"unhealthy", "down", "unready", "unavailable"}:
        return RUNTIME_STATE_HARD_DOWN
    return default


def health_http_status(value: Any) -> int:
    state = normalize_health_state(value)
    return 200 if state in {RUNTIME_STATE_READY, RUNTIME_STATE_DEGRADED} else 503


def health_message_for_state(value: Any) -> str:
    state = normalize_health_state(value)
    if state == RUNTIME_STATE_DEGRADED:
        return "Service is degraded"
    if state == RUNTIME_STATE_HARD_DOWN:
        return "Service is not ready"
    return "Service is ready"


def build_health_data(
    *,
    service_name: str,
    version: str,
    description: Optional[str] = None,
    availability: Optional[Mapping[str, Any]] = None,
    extra_data: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    data = {
        "service": service_name,
        "version": version,
        "status": RUNTIME_STATE_READY,
    }
    if description:
        data["description"] = description
    if isinstance(availability, Mapping):
        data.update(dict(availability))
    if isinstance(extra_data, Mapping):
        data.update(dict(extra_data))
    return data


def build_wrapped_health_response(
    *,
    service_name: str,
    version: str,
    description: Optional[str] = None,
    availability: Optional[Mapping[str, Any]] = None,
    extra_data: Optional[Mapping[str, Any]] = None,
    message: Optional[str] = None,
    response_status: Optional[str] = None,
) -> Dict[str, Any]:
    data = build_health_data(
        service_name=service_name,
        version=version,
        description=description,
        availability=availability,
        extra_data=extra_data,
    )
    state = normalize_health_state(data.get("status"))
    return ApiResponse(
        status=response_status or ("success" if state == RUNTIME_STATE_READY else "warning" if state == RUNTIME_STATE_DEGRADED else "error"),
        message=message or health_message_for_state(state),
        data=data,
    ).to_dict()
