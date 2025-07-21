"""
Coordinate validator for SPICE HARVESTER
"""

from typing import Any, Dict, List, Optional, Tuple

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult


class CoordinateValidator(BaseValidator):
    """Validator for geographic coordinates"""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate coordinate"""
        if constraints is None:
            constraints = {}

        lat, lon = None, None

        # Parse coordinate value
        if isinstance(value, str):
            # Format: "lat,lon"
            parts = value.split(",")
            if len(parts) != 2:
                return ValidationResult(
                    is_valid=False, message="Invalid coordinate format. Use 'lat,lon'"
                )
            try:
                lat, lon = float(parts[0].strip()), float(parts[1].strip())
            except ValueError:
                return ValidationResult(is_valid=False, message="Invalid coordinate numbers")
        elif isinstance(value, dict):
            lat = value.get("latitude") or value.get("lat")
            lon = value.get("longitude") or value.get("lon") or value.get("lng")
            if lat is None or lon is None:
                return ValidationResult(
                    is_valid=False, message="Coordinate object must have latitude and longitude"
                )
            try:
                lat, lon = float(lat), float(lon)
            except (ValueError, TypeError):
                return ValidationResult(is_valid=False, message="Invalid coordinate numbers")
        elif isinstance(value, (list, tuple)) and len(value) == 2:
            try:
                lat, lon = float(value[0]), float(value[1])
            except (ValueError, TypeError):
                return ValidationResult(is_valid=False, message="Invalid coordinate numbers")
        else:
            return ValidationResult(
                is_valid=False, message="Coordinate must be string, object, or array"
            )

        # Validate ranges
        if not (-90 <= lat <= 90):
            return ValidationResult(is_valid=False, message="Latitude must be between -90 and 90")

        if not (-180 <= lon <= 180):
            return ValidationResult(
                is_valid=False, message="Longitude must be between -180 and 180"
            )

        # Check bounding box constraint
        if "boundingBox" in constraints:
            bbox = constraints["boundingBox"]
            if len(bbox) == 4:
                min_lat, min_lon, max_lat, max_lon = bbox
                if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                    return ValidationResult(
                        is_valid=False, message="Coordinate is outside allowed bounding box"
                    )

        # Apply precision
        precision = constraints.get("precision", 6)
        lat = round(lat, precision)
        lon = round(lon, precision)

        result = {
            "latitude": lat,
            "longitude": lon,
            "lat": lat,  # Short form
            "lon": lon,  # Short form
            "formatted": f"{lat},{lon}",
            "geojson": {
                "type": "Point",
                "coordinates": [lon, lat],  # GeoJSON uses [lon, lat] order
            },
        }

        return ValidationResult(
            is_valid=True,
            message="Valid coordinate",
            normalized_value=result,
            metadata={"type": "coordinate", "precision": precision},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize coordinate value"""
        result = self.validate(value)
        if result.is_valid:
            return result.normalized_value
        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.COORDINATE.value, "coordinate", "coordinates", "geo", "location", "latlng"]

    @classmethod
    def calculate_distance(cls, coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """
        Calculate distance between two coordinates using Haversine formula

        Args:
            coord1: (latitude, longitude) tuple
            coord2: (latitude, longitude) tuple

        Returns:
            Distance in kilometers
        """
        import math

        lat1, lon1 = coord1
        lat2, lon2 = coord2

        # Convert to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))

        # Earth's radius in kilometers
        r = 6371

        return c * r
