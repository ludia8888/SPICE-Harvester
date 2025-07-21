"""
Dependency injection for SPICE HARVESTER services
"""

from .type_inference import (
    configure_type_inference_service,
    get_type_inference_service,
    reset_type_inference_service,
    type_inference_dependency,
)

__all__ = [
    "configure_type_inference_service",
    "get_type_inference_service",
    "type_inference_dependency",
    "reset_type_inference_service",
]
