"""
GeoShape (GeoJSON) validator for SPICE HARVESTER.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .base_validator import BaseValidator, ValidationResult


_ALLOWED_GEOMETRIES = {
    "Point",
    "MultiPoint",
    "LineString",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
    "GeometryCollection",
}


class GeoShapeValidator(BaseValidator):
    """Validator for GeoJSON geometry payloads."""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        if constraints is None:
            constraints = {}

        geometry = None
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return ValidationResult(is_valid=False, message="Geoshape must be non-empty")
            try:
                geometry = json.loads(raw)
            except Exception:
                return ValidationResult(is_valid=False, message="Invalid GeoJSON string")
        elif isinstance(value, dict):
            geometry = value
        else:
            return ValidationResult(is_valid=False, message="Geoshape must be a GeoJSON object")

        if not isinstance(geometry, dict):
            return ValidationResult(is_valid=False, message="Geoshape must be an object")

        geo_type = geometry.get("type")
        if geo_type not in _ALLOWED_GEOMETRIES:
            return ValidationResult(is_valid=False, message=f"Unsupported GeoJSON type: {geo_type}")

        if geo_type == "GeometryCollection":
            geometries = geometry.get("geometries")
            if not isinstance(geometries, list) or not geometries:
                return ValidationResult(is_valid=False, message="GeometryCollection requires geometries list")
            for item in geometries:
                if not isinstance(item, dict) or "type" not in item:
                    return ValidationResult(is_valid=False, message="Invalid geometry in collection")
            return ValidationResult(is_valid=True, message="Geoshape validation passed", normalized_value=geometry)

        coordinates = geometry.get("coordinates")
        if not _coordinates_valid(coordinates):
            return ValidationResult(is_valid=False, message="Invalid GeoJSON coordinates")

        return ValidationResult(is_valid=True, message="Geoshape validation passed", normalized_value=geometry)

    def normalize(self, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
        return value

    def get_supported_types(self) -> List[str]:
        return ["geoshape"]


def _coordinates_valid(value: Any) -> bool:
    if isinstance(value, (list, tuple)):
        if not value:
            return False
        return all(_coordinates_valid(item) for item in value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    return False
